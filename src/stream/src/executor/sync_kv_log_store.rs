// Copyright 2025 RisingWave Labs
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

//! This contains the synced kv log store implementation.
//! It's meant to buffer a large number of records emitted from upstream,
//! to avoid overwhelming the downstream executor.
//!
//! The synced kv log store polls two futures:
//!
//! 1. Upstream: upstream message source
//!
//!   It will write stream messages to the log store buffer. e.g. `Message::Barrier`, `Message::Chunk`, ...
//!   When writing a stream chunk, if the log store buffer is full, it will:
//!     a. Flush the buffer to the log store.
//!     b. Convert the stream chunk into a reference (`LogStoreBufferItem::Flushed`)
//!       which can read the corresponding chunks in the log store.
//!       We will compact adjacent references,
//!       so it can read multiple chunks if there's a build up.
//!
//!   On receiving barriers, it will:
//!     a. Apply truncation to historical data in the logstore.
//!     b. Flush and checkpoint the logstore data.
//!
//! 2. State store + buffer + recently flushed chunks: the storage components of the logstore.
//!
//!   It will read all historical data from the logstore first. This can be done just by
//!   constructing a state store stream, which will read all data until the latest epoch.
//!   This is a static snapshot of data.
//!   For any subsequently flushed chunks, we will read them via
//!   `flushed_chunk_future`. See the next paragraph below.
//!
//!   We will next read `flushed_chunk_future` (if there's one pre-existing one), see below for how
//!   it's constructed, what it is.
//!
//!   Finally we will pop the earliest item in the buffer.
//!   - If it's a chunk yield it.
//!   - If it's a watermark yield it.
//!   - If it's a flushed chunk reference (`LogStoreBufferItem::Flushed`),
//!     we will read the corresponding chunks in the log store.
//!     This is done by constructing a `flushed_chunk_future` which will read the log store
//!     using the `seq_id`.
//!   - Barrier,
//!     because they are directly propagated from the upstream when polling it.
//!
//! TODO(kwannoel):
//! - [] Add dedicated metrics for sync log store, namespace according to the upstream.
//! - [] Add tests
//! - [] Handle paused stream

use std::collections::VecDeque;
use std::future::pending;
use std::mem::replace;
use std::pin::Pin;

use anyhow::anyhow;
use futures::future::{BoxFuture, Either, select};
use futures::stream::StreamFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::must_match;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult};
use risingwave_pb::id::FragmentId;
use risingwave_storage::StateStore;
use risingwave_storage::store::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, OpConsistencyLevel, StateStoreRead,
};
use rw_futures_util::drop_either_future;
use tokio::time::{Duration, Instant, Sleep, sleep_until};
use tokio_stream::adapters::Peekable;

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::state::{
    LogStorePostSealCurrentEpoch, LogStoreReadState, LogStoreStateWriteChunkFuture,
    LogStoreWriteState, new_log_store_state,
};
use crate::common::log_store_impl::kv_log_store::{
    Epoch, FIRST_SEQ_ID, FlushInfo, LogStoreVnodeProgress, SeqId,
};
use crate::executor::prelude::*;
use crate::executor::sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
use crate::executor::{
    Barrier, BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult, Watermark,
};

pub mod metrics {
    use risingwave_common::id::FragmentId;
    use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedIntGauge};

    use crate::common::log_store_impl::kv_log_store::KvLogStoreReadMetrics;
    use crate::executor::monitor::StreamingMetrics;
    use crate::task::ActorId;

    #[derive(Clone)]
    pub struct SyncedKvLogStoreMetrics {
        // state of the log store
        pub unclean_state: LabelGuardedIntCounter,
        pub clean_state: LabelGuardedIntCounter,
        pub wait_next_poll_ns: LabelGuardedIntCounter,

        // Write metrics
        pub storage_write_count: LabelGuardedIntCounter,
        pub storage_write_size: LabelGuardedIntCounter,
        pub pause_duration_ns: LabelGuardedIntCounter,

        // Buffer metrics
        pub buffer_unconsumed_item_count: LabelGuardedIntGauge,
        pub buffer_unconsumed_row_count: LabelGuardedIntGauge,
        pub buffer_unconsumed_epoch_count: LabelGuardedIntGauge,
        pub buffer_unconsumed_min_epoch: LabelGuardedIntGauge,
        pub buffer_memory_bytes: LabelGuardedIntGauge,
        pub buffer_read_count: LabelGuardedIntCounter,
        pub buffer_read_size: LabelGuardedIntCounter,

        // Read metrics
        pub total_read_count: LabelGuardedIntCounter,
        pub total_read_size: LabelGuardedIntCounter,
        pub persistent_log_read_metrics: KvLogStoreReadMetrics,
        pub flushed_buffer_read_metrics: KvLogStoreReadMetrics,
    }

    impl SyncedKvLogStoreMetrics {
        /// `id`: refers to a unique way to identify the logstore. This can be the sink id,
        ///       or for joins, it can be the `fragment_id`.
        /// `name`: refers to the MV / Sink that the log store is associated with.
        /// `target`: refers to the target of the log store,
        ///           for instance `MySql` Sink, PG sink, etc...
        ///           or unaligned join.
        pub(crate) fn new(
            metrics: &StreamingMetrics,
            actor_id: ActorId,
            fragment_id: FragmentId,
            name: &str,
            target: &'static str,
        ) -> Self {
            let actor_id_str = actor_id.to_string();
            let fragment_id_str = fragment_id.to_string();
            let labels = &[&actor_id_str, target, &fragment_id_str, name];

            let unclean_state = metrics.sync_kv_log_store_state.with_guarded_label_values(&[
                "dirty",
                &actor_id_str,
                target,
                &fragment_id_str,
                name,
            ]);
            let clean_state = metrics.sync_kv_log_store_state.with_guarded_label_values(&[
                "clean",
                &actor_id_str,
                target,
                &fragment_id_str,
                name,
            ]);
            let wait_next_poll_ns = metrics
                .sync_kv_log_store_wait_next_poll_ns
                .with_guarded_label_values(labels);

            let storage_write_size = metrics
                .sync_kv_log_store_storage_write_size
                .with_guarded_label_values(labels);
            let storage_write_count = metrics
                .sync_kv_log_store_storage_write_count
                .with_guarded_label_values(labels);
            let storage_pause_duration_ns = metrics
                .sync_kv_log_store_write_pause_duration_ns
                .with_guarded_label_values(labels);

            let buffer_unconsumed_item_count = metrics
                .sync_kv_log_store_buffer_unconsumed_item_count
                .with_guarded_label_values(labels);
            let buffer_unconsumed_row_count = metrics
                .sync_kv_log_store_buffer_unconsumed_row_count
                .with_guarded_label_values(labels);
            let buffer_unconsumed_epoch_count = metrics
                .sync_kv_log_store_buffer_unconsumed_epoch_count
                .with_guarded_label_values(labels);
            let buffer_unconsumed_min_epoch = metrics
                .sync_kv_log_store_buffer_unconsumed_min_epoch
                .with_guarded_label_values(labels);
            let buffer_memory_bytes = metrics
                .sync_kv_log_store_buffer_memory_bytes
                .with_guarded_label_values(labels);
            let buffer_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    "buffer",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let buffer_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    "buffer",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let total_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    "total",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let total_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    "total",
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            const READ_PERSISTENT_LOG: &str = "persistent_log";
            const READ_FLUSHED_BUFFER: &str = "flushed_buffer";

            let persistent_log_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    READ_PERSISTENT_LOG,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let persistent_log_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    READ_PERSISTENT_LOG,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let flushed_buffer_read_size = metrics
                .sync_kv_log_store_read_size
                .with_guarded_label_values(&[
                    READ_FLUSHED_BUFFER,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            let flushed_buffer_read_count = metrics
                .sync_kv_log_store_read_count
                .with_guarded_label_values(&[
                    READ_FLUSHED_BUFFER,
                    &actor_id_str,
                    target,
                    &fragment_id_str,
                    name,
                ]);

            Self {
                unclean_state,
                clean_state,
                wait_next_poll_ns,
                storage_write_size,
                storage_write_count,
                pause_duration_ns: storage_pause_duration_ns,
                buffer_unconsumed_item_count,
                buffer_unconsumed_row_count,
                buffer_unconsumed_epoch_count,
                buffer_unconsumed_min_epoch,
                buffer_memory_bytes,
                buffer_read_count,
                buffer_read_size,
                total_read_count,
                total_read_size,
                persistent_log_read_metrics: KvLogStoreReadMetrics {
                    storage_read_size: persistent_log_read_size,
                    storage_read_count: persistent_log_read_count,
                },
                flushed_buffer_read_metrics: KvLogStoreReadMetrics {
                    storage_read_count: flushed_buffer_read_count,
                    storage_read_size: flushed_buffer_read_size,
                },
            }
        }

        #[cfg(test)]
        pub(crate) fn for_test() -> Self {
            SyncedKvLogStoreMetrics {
                unclean_state: LabelGuardedIntCounter::test_int_counter::<5>(),
                clean_state: LabelGuardedIntCounter::test_int_counter::<5>(),
                wait_next_poll_ns: LabelGuardedIntCounter::test_int_counter::<4>(),
                storage_write_count: LabelGuardedIntCounter::test_int_counter::<4>(),
                storage_write_size: LabelGuardedIntCounter::test_int_counter::<4>(),
                pause_duration_ns: LabelGuardedIntCounter::test_int_counter::<4>(),
                buffer_unconsumed_item_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_unconsumed_row_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_unconsumed_epoch_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_unconsumed_min_epoch: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_memory_bytes: LabelGuardedIntGauge::test_int_gauge::<4>(),
                buffer_read_count: LabelGuardedIntCounter::test_int_counter::<5>(),
                buffer_read_size: LabelGuardedIntCounter::test_int_counter::<5>(),
                total_read_count: LabelGuardedIntCounter::test_int_counter::<5>(),
                total_read_size: LabelGuardedIntCounter::test_int_counter::<5>(),
                persistent_log_read_metrics: KvLogStoreReadMetrics::for_test(),
                flushed_buffer_read_metrics: KvLogStoreReadMetrics::for_test(),
            }
        }
    }
}

pub(crate) type ReadFlushedChunkFuture =
    BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, Epoch)>>;

pub struct SyncKvLogStoreContext<S: StateStore> {
    pub table_id: TableId,
    pub fragment_id: FragmentId,
    pub metrics: SyncedKvLogStoreMetrics,
    pub serde: LogStoreRowSerde,
    pub state_store: S,
    pub max_buffer_size: usize,
    pub chunk_size: usize,
    pub pause_duration_ms: Duration,
    pub aligned: bool,
}

pub struct SyncedKvLogStoreExecutor<S: StateStore> {
    actor_context: ActorContextRef,
    upstream: Executor,
    logstore_context: SyncKvLogStoreContext<S>,
}

// Stream interface
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        actor_context: ActorContextRef,
        table_id: TableId,
        metrics: SyncedKvLogStoreMetrics,
        serde: LogStoreRowSerde,
        state_store: S,
        buffer_size: usize,
        chunk_size: usize,
        upstream: Executor,
        pause_duration_ms: Duration,
        aligned: bool,
    ) -> Self {
        let logstore_context = SyncKvLogStoreContext {
            table_id,
            fragment_id: actor_context.fragment_id,
            metrics,
            serde,
            state_store,
            max_buffer_size: buffer_size,
            chunk_size,
            pause_duration_ms,
            aligned,
        };
        Self {
            actor_context,
            upstream,
            logstore_context,
        }
    }
}

pub(crate) struct FlushedChunkInfo {
    epoch: u64,
    start_seq_id: SeqId,
    end_seq_id: SeqId,
    flush_info: FlushInfo,
    vnode_bitmap: Bitmap,
}

pub(crate) enum WriteFuture<S: LocalStateStore> {
    /// We trigger a brief pause to let the `ReadFuture` be polled in the following scenarios:
    /// - When seeing an upstream data chunk, when the buffer becomes full, and the state is clean.
    /// - When seeing a checkpoint barrier, when the buffer is not empty, and the state is clean.
    ///
    /// On pausing, we will transition to a dirty state.
    ///
    /// We trigger resume to let the `ReadFuture` to be polled in the following scenarios:
    /// - After the pause duration.
    /// - After the read future consumes a chunk.
    Paused {
        start_instant: Instant,
        sleep_future: Option<Pin<Box<Sleep>>>,
        barrier: Barrier,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>, // Just used to hold the state
    },
    ReceiveFromUpstream {
        future: StreamFuture<BoxedMessageStream>,
        write_state: LogStoreWriteState<S>,
    },
    FlushingChunk {
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        future: Pin<Box<LogStoreStateWriteChunkFuture<S>>>,
        stream: BoxedMessageStream,
    },
    Empty,
}

pub(crate) enum WriteFutureEvent {
    UpstreamMessageReceived(Message),
    ChunkFlushed(FlushedChunkInfo),
}

impl<S: LocalStateStore> WriteFuture<S> {
    fn flush_chunk(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
        chunk: StreamChunk,
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
    ) -> Self {
        tracing::trace!(
            start_seq_id,
            end_seq_id,
            epoch,
            cardinality = chunk.cardinality(),
            "write_future: flushing chunk"
        );
        Self::FlushingChunk {
            epoch,
            start_seq_id,
            end_seq_id,
            future: Box::pin(write_state.into_write_chunk_future(
                chunk,
                epoch,
                start_seq_id,
                end_seq_id,
            )),
            stream,
        }
    }

    pub(crate) fn receive_from_upstream(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        Self::ReceiveFromUpstream {
            future: stream.into_future(),
            write_state,
        }
    }

    pub(crate) fn paused(
        duration: Duration,
        barrier: Barrier,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        let now = Instant::now();
        tracing::trace!(?now, ?duration, "write_future_pause");
        Self::Paused {
            start_instant: now,
            sleep_future: Some(Box::pin(sleep_until(now + duration))),
            barrier,
            stream,
            write_state,
        }
    }

    pub(crate) async fn next_event(
        &mut self,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamExecutorResult<(BoxedMessageStream, LogStoreWriteState<S>, WriteFutureEvent)> {
        match self {
            WriteFuture::Paused {
                start_instant,
                sleep_future,
                ..
            } => {
                if let Some(sleep_future) = sleep_future {
                    sleep_future.await;
                    metrics
                        .pause_duration_ns
                        .inc_by(start_instant.elapsed().as_nanos() as _);
                    tracing::trace!("resuming write future");
                }
                must_match!(replace(self, WriteFuture::Empty), WriteFuture::Paused { stream, write_state, barrier, .. } => {
                    Ok((stream, write_state, WriteFutureEvent::UpstreamMessageReceived(Message::Barrier(barrier))))
                })
            }
            WriteFuture::ReceiveFromUpstream { future, .. } => {
                let (opt, stream) = future.await;
                must_match!(replace(self, WriteFuture::Empty), WriteFuture::ReceiveFromUpstream { write_state, .. } => {
                    opt
                    .ok_or_else(|| anyhow!("end of upstream input").into())
                    .and_then(|result| result.map(|item| {
                        (stream, write_state, WriteFutureEvent::UpstreamMessageReceived(item))
                    }))
                })
            }
            WriteFuture::FlushingChunk { future, .. } => {
                let (write_state, result) = future.await;
                let result = must_match!(replace(self, WriteFuture::Empty), WriteFuture::FlushingChunk { epoch, start_seq_id, end_seq_id, stream, ..  } => {
                    result.map(|(flush_info, vnode_bitmap)| {
                        (stream, write_state, WriteFutureEvent::ChunkFlushed(FlushedChunkInfo {
                            epoch,
                            start_seq_id,
                            end_seq_id,
                            flush_info,
                            vnode_bitmap,
                        }))
                    })
                });
                result.map_err(Into::into)
            }
            WriteFuture::Empty => {
                unreachable!("should not be polled after ready")
            }
        }
    }
}

pub(crate) type LocalLogStoreReadState<S> =
    LogStoreReadState<<<S as StateStore>::Local as LocalStateStore>::FlushedSnapshotReader>;
pub(crate) type LocalLogStoreWriteState<S> = LogStoreWriteState<<S as StateStore>::Local>;

// Stream interface
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    pub(crate) async fn init_local_log_store_state(
        context: &SyncKvLogStoreContext<S>,
        first_write_epoch: EpochPair,
    ) -> StreamExecutorResult<(LocalLogStoreReadState<S>, LocalLogStoreWriteState<S>)> {
        let local_state_store = context
            .state_store
            .new_local(NewLocalOptions {
                table_id: context.table_id,
                fragment_id: context.fragment_id,
                op_consistency_level: OpConsistencyLevel::Inconsistent,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
                vnodes: context.serde.vnodes().clone(),
                upload_on_flush: false,
            })
            .await;

        let (read_state, mut initial_write_state) = new_log_store_state(
            context.table_id,
            local_state_store,
            context.serde.clone(),
            context.chunk_size,
        );
        initial_write_state.init(first_write_epoch).await?;
        Ok((read_state, initial_write_state))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn aligned_message_stream(
        actor_id: ActorId,
        input: BoxedMessageStream,
        read_state: LocalLogStoreReadState<S>,
        mut initial_write_state: LocalLogStoreWriteState<S>,
        metrics: SyncedKvLogStoreMetrics,
        initial_write_epoch: EpochPair,
    ) {
        tracing::info!("aligned mode");
        // We want to realign the buffer and the stream.
        // We just block the upstream input stream,
        // and wait until the persisted logstore is empty.
        // Then after that we can consume the input stream.
        let log_store_stream = read_state
            .read_persisted_log_store(
                metrics.persistent_log_read_metrics.clone(),
                initial_write_epoch.curr,
                LogStoreReadStateStreamRangeStart::Unbounded,
            )
            .await?;

        #[for_await]
        for message in log_store_stream {
            let (_epoch, message) = message?;
            match message {
                KvLogStoreItem::Barrier { .. } => {
                    continue;
                }
                KvLogStoreItem::StreamChunk { chunk, .. } => {
                    yield Message::Chunk(chunk);
                }
            }
        }

        let mut realigned_logstore = false;

        #[for_await]
        for message in input {
            match message? {
                Message::Barrier(barrier) => {
                    let is_checkpoint = barrier.is_checkpoint();
                    let mut progress = LogStoreVnodeProgress::None;
                    progress.apply_aligned(read_state.vnodes().clone(), barrier.epoch.prev, None);
                    // Truncate the logstore.
                    let post_seal =
                        initial_write_state.seal_current_epoch(barrier.epoch.curr, progress.take());
                    barrier.assume_no_update_vnode_bitmap(actor_id)?;
                    yield Message::Barrier(barrier);
                    post_seal.post_yield_barrier(None).await?;
                    if !realigned_logstore && is_checkpoint {
                        realigned_logstore = true;
                        tracing::info!("realigned logstore");
                    }
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }

    pub(crate) fn apply_pause_resume_mutation(barrier: &Barrier, pause_stream: &mut bool) {
        if let Some(mutation) = barrier.mutation.as_deref() {
            match mutation {
                Mutation::Pause => {
                    *pause_stream = true;
                }
                Mutation::Resume => {
                    *pause_stream = false;
                }
                _ => {}
            }
        }
    }

    pub(crate) fn process_upstream_chunk(
        seq_id: SeqId,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S::Local>,
        chunk: StreamChunk,
        buffer: &mut SyncedLogStoreBuffer,
    ) -> (SeqId, WriteFuture<S::Local>) {
        let cardinality = chunk.cardinality();
        if cardinality == 0 {
            tracing::warn!(
                epoch = write_state.epoch().curr,
                "received empty chunk (cardinality=0), skipping"
            );
            return (
                seq_id,
                WriteFuture::receive_from_upstream(stream, write_state),
            );
        }

        let start_seq_id = seq_id;
        let new_seq_id = seq_id + cardinality as SeqId;
        let end_seq_id = new_seq_id - 1;
        let epoch = write_state.epoch().curr;
        tracing::trace!(
            start_seq_id,
            end_seq_id,
            new_seq_id,
            epoch,
            cardinality,
            "received chunk"
        );
        let next_write_future = if let Some(chunk_to_flush) =
            buffer.add_or_flush_chunk(start_seq_id, end_seq_id, chunk, epoch)
        {
            WriteFuture::flush_chunk(
                stream,
                write_state,
                chunk_to_flush,
                epoch,
                start_seq_id,
                end_seq_id,
            )
        } else {
            WriteFuture::receive_from_upstream(stream, write_state)
        };
        (new_seq_id, next_write_future)
    }

    pub(crate) fn process_flushed_chunk(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S::Local>,
        info: FlushedChunkInfo,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> WriteFuture<S::Local> {
        buffer.add_flushed_item_to_buffer(
            info.start_seq_id,
            info.end_seq_id,
            info.vnode_bitmap,
            info.epoch,
        );
        metrics
            .storage_write_count
            .inc_by(info.flush_info.flush_count as _);
        metrics
            .storage_write_size
            .inc_by(info.flush_info.flush_size as _);
        WriteFuture::receive_from_upstream(stream, write_state)
    }

    #[try_stream(ok= Message, error = StreamExecutorError)]
    pub async fn execute_monitored(self) {
        let wait_next_poll_ns = self.logstore_context.metrics.wait_next_poll_ns.clone();
        #[for_await]
        for message in self.execute_inner() {
            let current_time = Instant::now();
            yield message?;
            wait_next_poll_ns.inc_by(current_time.elapsed().as_nanos() as _);
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let mut input = self.upstream.execute();

        // init first epoch + local state store
        let first_barrier = expect_first_barrier(&mut input).await?;
        let first_write_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier.clone());

        let (read_state, initial_write_state) =
            Self::init_local_log_store_state(&self.logstore_context, first_write_epoch).await?;

        let mut pause_stream = first_barrier.is_pause_on_startup();
        let initial_write_epoch = first_write_epoch;

        if self.logstore_context.aligned {
            let aligned_stream = Self::aligned_message_stream(
                self.actor_context.id,
                input,
                read_state,
                initial_write_state,
                self.logstore_context.metrics.clone(),
                initial_write_epoch,
            );
            #[for_await]
            for message in aligned_stream {
                yield message?;
            }
            return Ok(());
        }

        let mut seq_id = FIRST_SEQ_ID;
        let mut buffer = SyncedLogStoreBuffer::new(
            self.logstore_context.max_buffer_size,
            self.logstore_context.chunk_size,
            &self.logstore_context.metrics,
        );

        let log_store_stream = read_state
            .read_persisted_log_store(
                self.logstore_context
                    .metrics
                    .persistent_log_read_metrics
                    .clone(),
                initial_write_epoch.curr,
                LogStoreReadStateStreamRangeStart::Unbounded,
            )
            .await?;

        let mut log_store_stream = tokio_stream::StreamExt::peekable(log_store_stream);
        let mut clean_state = log_store_stream.peek().await.is_none();
        tracing::trace!(?clean_state);

        let mut read_future_state = ReadFuture::ReadingPersistedStream(log_store_stream);

        let mut write_future_state = WriteFuture::receive_from_upstream(input, initial_write_state);

        let mut progress = LogStoreVnodeProgress::None;

        loop {
            let select_result = {
                let read_future = async {
                    if pause_stream {
                        pending().await
                    } else {
                        read_future_state
                            .next_message(
                                &mut progress,
                                &read_state,
                                &mut buffer,
                                &self.logstore_context.metrics,
                            )
                            .await
                    }
                };
                pin_mut!(read_future);
                let write_future = write_future_state.next_event(&self.logstore_context.metrics);
                pin_mut!(write_future);
                let output = select(write_future, read_future).await;
                drop_either_future(output)
            };
            match select_result {
                Either::Left(result) => {
                    // drop the future to ensure that the future must be reset later
                    drop(write_future_state);
                    let (stream, mut write_state, either) = result?;
                    match either {
                        WriteFutureEvent::UpstreamMessageReceived(msg) => match msg {
                            Message::Barrier(barrier) => {
                                if clean_state && barrier.kind.is_checkpoint() && !buffer.is_empty()
                                {
                                    write_future_state = WriteFuture::paused(
                                        self.logstore_context.pause_duration_ms,
                                        barrier,
                                        stream,
                                        write_state,
                                    );
                                    clean_state = false;
                                    self.logstore_context.metrics.unclean_state.inc();
                                } else {
                                    Self::apply_pause_resume_mutation(&barrier, &mut pause_stream);
                                    let write_state_post_write_barrier = Self::write_barrier(
                                        self.actor_context.id,
                                        &mut write_state,
                                        barrier.clone(),
                                        &self.logstore_context.metrics,
                                        progress.take(),
                                        &mut buffer,
                                    )
                                    .await?;
                                    seq_id = FIRST_SEQ_ID;
                                    barrier.assume_no_update_vnode_bitmap(self.actor_context.id)?;

                                    yield Message::Barrier(barrier);

                                    write_state_post_write_barrier
                                        .post_yield_barrier(None)
                                        .await?;

                                    write_future_state =
                                        WriteFuture::receive_from_upstream(stream, write_state);
                                }
                            }
                            Message::Chunk(chunk) => {
                                let (new_seq_id, next_write_future) = Self::process_upstream_chunk(
                                    seq_id,
                                    stream,
                                    write_state,
                                    chunk,
                                    &mut buffer,
                                );
                                seq_id = new_seq_id;
                                write_future_state = next_write_future;
                            }
                            Message::Watermark(watermark) => {
                                buffer.add_watermark(write_state.epoch().curr, watermark);
                                write_future_state =
                                    WriteFuture::receive_from_upstream(stream, write_state);
                            }
                        },
                        WriteFutureEvent::ChunkFlushed(info) => {
                            write_future_state = Self::process_flushed_chunk(
                                stream,
                                write_state,
                                info,
                                &mut buffer,
                                &self.logstore_context.metrics,
                            );
                        }
                    }
                }
                Either::Right(result) => {
                    let clean_state_reached = read_future_state.mark_clean_state(
                        &mut clean_state,
                        &buffer,
                        &self.logstore_context.metrics,
                    );

                    if clean_state_reached {
                        // Let write future resume immediately
                        if let WriteFuture::Paused { sleep_future, .. } = &mut write_future_state {
                            tracing::trace!("resuming paused future");
                            assert!(buffer.has_available_capacity());
                            *sleep_future = None;
                        }
                    }
                    let message = result?;
                    if let Message::Chunk(chunk) = &message {
                        self.logstore_context
                            .metrics
                            .total_read_count
                            .inc_by(chunk.cardinality() as _);
                    }

                    yield message;
                }
            }
        }
    }
}

pub(crate) type PersistedStream<S> =
    Peekable<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>;

pub(crate) enum ReadFuture<S: StateStoreRead> {
    ReadingPersistedStream(PersistedStream<S>),
    ReadingFlushedChunk {
        future: ReadFlushedChunkFuture,
        end_seq_id: SeqId,
    },
    Idle,
}

// Read methods
impl<S: StateStoreRead> ReadFuture<S> {
    pub(crate) fn mark_clean_state(
        &self,
        clean_state: &mut bool,
        buffer: &SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> bool {
        if !*clean_state && matches!(self, ReadFuture::Idle) && buffer.is_empty() {
            *clean_state = true;
            metrics.clean_state.inc();
            true
        } else {
            false
        }
    }

    pub(crate) async fn next_message(
        &mut self,
        progress: &mut LogStoreVnodeProgress,
        read_state: &LogStoreReadState<S>,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamExecutorResult<Message> {
        match self {
            ReadFuture::ReadingPersistedStream(stream) => {
                while let Some((epoch, item)) = stream.try_next().await? {
                    match item {
                        KvLogStoreItem::Barrier { vnodes, .. } => {
                            tracing::trace!(epoch, "read logstore barrier");
                            // update the progress
                            progress.apply_aligned(vnodes, epoch, None);
                            continue;
                        }
                        KvLogStoreItem::StreamChunk {
                            chunk,
                            progress: chunk_progress,
                        } => {
                            tracing::trace!("read logstore chunk of size: {}", chunk.cardinality());
                            progress.apply_per_vnode(epoch, chunk_progress);
                            return Ok(Message::Chunk(chunk));
                        }
                    }
                }
                *self = ReadFuture::Idle;
            }
            ReadFuture::ReadingFlushedChunk { .. } | ReadFuture::Idle => {}
        }
        match self {
            ReadFuture::ReadingPersistedStream(_) => {
                unreachable!("must have finished read persisted stream when reaching here")
            }
            ReadFuture::ReadingFlushedChunk { .. } => {}
            ReadFuture::Idle => loop {
                let Some((item_epoch, item)) = buffer.pop_front() else {
                    return pending().await;
                };
                match item {
                    SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::StreamChunk {
                        chunk,
                        start_seq_id,
                        end_seq_id,
                        flushed,
                        ..
                    }) => {
                        metrics.buffer_read_count.inc_by(chunk.cardinality() as _);
                        tracing::trace!(
                            start_seq_id,
                            end_seq_id,
                            flushed,
                            cardinality = chunk.cardinality(),
                            "read buffered chunk of size"
                        );
                        progress.apply_aligned(
                            read_state.vnodes().clone(),
                            item_epoch,
                            Some(end_seq_id),
                        );
                        return Ok(Message::Chunk(chunk));
                    }
                    SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Flushed {
                        vnode_bitmap,
                        start_seq_id,
                        end_seq_id,
                        chunk_id,
                    }) => {
                        tracing::trace!(start_seq_id, end_seq_id, chunk_id, "read flushed chunk");
                        let read_metrics = metrics.flushed_buffer_read_metrics.clone();
                        let future = read_state
                            .read_flushed_chunk(
                                vnode_bitmap,
                                chunk_id,
                                start_seq_id,
                                end_seq_id,
                                item_epoch,
                                read_metrics,
                            )
                            .boxed();
                        *self = ReadFuture::ReadingFlushedChunk { future, end_seq_id };
                        break;
                    }
                    SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Barrier { .. }) => {
                        tracing::trace!(item_epoch, "read buffer barrier");
                        progress.apply_aligned(read_state.vnodes().clone(), item_epoch, None);
                        continue;
                    }
                    SyncedLogStoreBufferItem::Watermark(watermark) => {
                        return Ok(Message::Watermark(watermark));
                    }
                }
            },
        }

        let (future, end_seq_id) = match self {
            ReadFuture::ReadingPersistedStream(_) | ReadFuture::Idle => {
                unreachable!("should be at ReadingFlushedChunk")
            }
            ReadFuture::ReadingFlushedChunk { future, end_seq_id } => (future, *end_seq_id),
        };

        let (_, chunk, epoch) = future.await?;
        progress.apply_aligned(read_state.vnodes().clone(), epoch, Some(end_seq_id));
        tracing::trace!(
            end_seq_id,
            "read flushed chunk of size: {}",
            chunk.cardinality()
        );
        *self = ReadFuture::Idle;
        Ok(Message::Chunk(chunk))
    }
}

// Write methods
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    pub(crate) async fn write_barrier<'a>(
        actor_id: ActorId,
        write_state: &'a mut LogStoreWriteState<S::Local>,
        barrier: Barrier,
        metrics: &SyncedKvLogStoreMetrics,
        progress: LogStoreVnodeProgress,
        buffer: &mut SyncedLogStoreBuffer,
    ) -> StreamExecutorResult<LogStorePostSealCurrentEpoch<'a, S::Local>> {
        tracing::trace!(%actor_id, ?progress, "applying truncation");
        // TODO(kwannoel): As an optimization we can also change flushed chunks to be flushed items
        // to reduce memory consumption of logstore.

        let epoch = barrier.epoch.prev;
        let mut writer = write_state.start_writer(false);
        writer.write_barrier(epoch, barrier.is_checkpoint())?;

        if barrier.is_checkpoint() {
            for (epoch, item) in buffer.buffer.iter_mut().rev() {
                match item {
                    SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::StreamChunk {
                        chunk,
                        start_seq_id,
                        end_seq_id,
                        flushed,
                        ..
                    }) => {
                        if !*flushed {
                            writer.write_chunk(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                            *flushed = true;
                        } else {
                            break;
                        }
                    }
                    SyncedLogStoreBufferItem::LogStore(
                        LogStoreBufferItem::Flushed { .. } | LogStoreBufferItem::Barrier { .. },
                    )
                    | SyncedLogStoreBufferItem::Watermark(_) => {}
                }
            }
        }

        // Apply truncation
        let (flush_info, _) = writer.finish().await?;
        metrics
            .storage_write_count
            .inc_by(flush_info.flush_count as _);
        metrics
            .storage_write_size
            .inc_by(flush_info.flush_size as _);
        let post_seal = write_state.seal_current_epoch(barrier.epoch.curr, progress);

        // Add to buffer
        buffer.buffer.push_back((
            epoch,
            SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Barrier {
                is_checkpoint: barrier.is_checkpoint(),
                next_epoch: barrier.epoch.curr,
                schema_change: None,
                is_stop: false,
            }),
        ));
        buffer.next_chunk_id = 0;
        buffer.update_buffer_metrics();

        Ok(post_seal)
    }
}

#[derive(EstimateSize)]
enum SyncedLogStoreBufferItem {
    LogStore(LogStoreBufferItem),
    Watermark(Watermark),
}

pub(crate) struct SyncedLogStoreBuffer {
    buffer: VecDeque<(u64, SyncedLogStoreBufferItem)>,
    current_size: usize,
    max_size: usize,
    max_chunk_size: usize,
    next_chunk_id: ChunkId,
    metrics: SyncedKvLogStoreMetrics,
    flushed_count: usize,
}

impl SyncedLogStoreBuffer {
    pub fn new(
        max_buffer_size: usize,
        chunk_size: usize,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> Self {
        SyncedLogStoreBuffer {
            buffer: VecDeque::new(),
            current_size: 0,
            max_size: max_buffer_size,
            max_chunk_size: chunk_size,
            next_chunk_id: 0,
            metrics: metrics.clone(),
            flushed_count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    pub fn has_available_capacity(&self) -> bool {
        self.current_size < self.max_size
    }

    fn add_or_flush_chunk(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        chunk: StreamChunk,
        epoch: u64,
    ) -> Option<StreamChunk> {
        let current_size = self.current_size;
        let chunk_size = chunk.cardinality();

        tracing::trace!(
            current_size,
            chunk_size,
            max_size = self.max_size,
            "checking chunk size"
        );
        let should_flush_chunk = current_size + chunk_size > self.max_size;
        if should_flush_chunk {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "flushing chunk",);
            Some(chunk)
        } else {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "buffering chunk",);
            self.add_chunk_to_buffer(chunk, start_seq_id, end_seq_id, epoch);
            None
        }
    }

    /// After flushing a chunk, we will preserve a `FlushedItem` inside the buffer.
    /// This doesn't contain any data, but it contains the metadata to read the flushed chunk.
    fn add_flushed_item_to_buffer(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        new_vnode_bitmap: Bitmap,
        epoch: u64,
    ) {
        let new_chunk_size = (end_seq_id - start_seq_id + 1) as usize;

        if let Some((
            item_epoch,
            SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Flushed {
                start_seq_id: prev_start_seq_id,
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            }),
        )) = self.buffer.back_mut()
            && let flushed_chunk_size = (*prev_end_seq_id - *prev_start_seq_id + 1) as usize
            && let projected_flushed_chunk_size = flushed_chunk_size + new_chunk_size
            && projected_flushed_chunk_size <= self.max_chunk_size
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            assert_eq!(
                epoch, *item_epoch,
                "epoch of newly added flushed item must be the same as the last flushed item"
            );
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.buffer.push_back((
                epoch,
                SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                }),
            ));
            self.flushed_count += 1;
            tracing::trace!(
                "adding flushed item to buffer: start_seq_id: {start_seq_id}, end_seq_id: {end_seq_id}, chunk_id: {chunk_id}"
            );
        }
        // FIXME(kwannoel): Seems these metrics are updated _after_ the flush info is reported.
        self.update_buffer_metrics();
    }

    fn add_chunk_to_buffer(
        &mut self,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        epoch: u64,
    ) {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
        self.current_size += chunk.cardinality();
        self.buffer.push_back((
            epoch,
            SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed: false,
                chunk_id,
            }),
        ));
        self.update_buffer_metrics();
    }

    pub(crate) fn add_watermark(&mut self, epoch: u64, watermark: Watermark) {
        self.buffer
            .push_back((epoch, SyncedLogStoreBufferItem::Watermark(watermark)));
        self.update_buffer_metrics();
    }

    fn pop_front(&mut self) -> Option<(u64, SyncedLogStoreBufferItem)> {
        let item = self.buffer.pop_front();
        match &item {
            Some((_, SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Flushed { .. }))) => {
                self.flushed_count -= 1;
            }
            Some((
                _,
                SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::StreamChunk {
                    chunk, ..
                }),
            )) => {
                self.current_size -= chunk.cardinality();
            }
            _ => {}
        }
        self.update_buffer_metrics();
        item
    }

    fn update_buffer_metrics(&self) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        let mut memory_bytes = 0;
        for (_, item) in &self.buffer {
            match item {
                SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::StreamChunk {
                    chunk,
                    ..
                }) => {
                    row_count += chunk.cardinality();
                }
                SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    ..
                }) => {
                    row_count += (end_seq_id - start_seq_id) as usize;
                }
                SyncedLogStoreBufferItem::LogStore(LogStoreBufferItem::Barrier { .. }) => {
                    epoch_count += 1;
                }
                SyncedLogStoreBufferItem::Watermark(_) => {}
            }
            memory_bytes += item.estimated_size();
        }
        self.metrics.buffer_unconsumed_epoch_count.set(epoch_count);
        self.metrics.buffer_unconsumed_row_count.set(row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.buffer.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.buffer
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
        self.metrics.buffer_memory_bytes.set(memory_bytes as _);
    }
}

impl<S> Execute for SyncedKvLogStoreExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_monitored().boxed()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::test_prelude::*;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::assert_stream_chunk_eq;
    use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V2_INFO;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_stream_chunk_eq, gen_test_log_store_table, test_payload_schema,
    };
    use crate::executor::sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
    use crate::executor::test_utils::MockSource;

    fn init_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .try_init();
    }

    // test read/write buffer
    #[tokio::test]
    async fn test_read_write_buffer() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type))
            .collect_vec();
        let schema = Schema { fields };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), stream_key.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            SyncedKvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            10,
            256,
            source,
            Duration::from_millis(256),
            false,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U-  9  10
            U+ 10  11",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_int64_watermark(0, 7);
        tx.push_chunk(chunk_2.clone());

        let mut stream = log_store_executor.execute();

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(1));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_1);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Watermark(watermark))) => {
                assert_eq!(watermark, Watermark::new(0, DataType::Int64, 7_i64.into()));
            }
            other => panic!("Expected a watermark message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_2);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        tx.push_barrier(test_epoch(2), false);

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(2));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }
    }

    // test barrier persisted read
    //
    // sequence of events (earliest -> latest):
    // barrier(1) -> chunk(1) -> watermark -> chunk(2) -> poll(4) items -> barrier(2)
    // * poll just means we read from the executor stream.
    #[tokio::test]
    async fn test_barrier_persisted_read() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type))
            .collect_vec();
        let schema = Schema { fields };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), stream_key.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            SyncedKvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            10,
            256,
            source,
            Duration::from_millis(256),
            false,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U- 10  11
            U+ 10  10",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_int64_watermark(0, 7);
        tx.push_chunk(chunk_2.clone());

        tx.push_barrier(test_epoch(2), false);

        let mut stream = log_store_executor.execute();

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(1));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_1);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Watermark(watermark))) => {
                assert_eq!(watermark, Watermark::new(0, DataType::Int64, 7_i64.into()));
            }
            other => panic!("Expected a watermark message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_stream_chunk_eq!(chunk, chunk_2);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(2));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }
    }

    // When we hit buffer max_chunk, we only store placeholder `FlushedItem`.
    // So we just let capacity = 0, and we will always flush incoming chunks to state store.
    #[tokio::test]
    async fn test_max_chunk_persisted_read() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type))
            .collect_vec();
        let schema = Schema { fields };
        let stream_key = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), stream_key.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            SyncedKvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            0,
            256,
            source,
            Duration::from_millis(256),
            false,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U- 10  11
            U+ 10  10",
        );

        let chunk_3 = StreamChunk::from_pretty(
            "   I   T
            +  11  12",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());
        tx.push_int64_watermark(0, 7);
        tx.push_chunk(chunk_3.clone());

        tx.push_barrier(test_epoch(2), false);

        let mut stream = log_store_executor.execute();

        for i in 1..=2 {
            match stream.next().await {
                Some(Ok(Message::Barrier(barrier))) => {
                    assert_eq!(barrier.epoch.curr, test_epoch(i));
                }
                other => panic!("Expected a barrier message, got {:?}", other),
            }
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(actual))) => {
                let expected = StreamChunk::from_pretty(
                    "   I   T
                    +   5  10
                    +   6  10
                    +   8  10
                    +   9  10
                    +  10  11
                    -   5  10
                    -   6  10
                    -   8  10
                    U- 10  11
                    U+ 10  10",
                );
                assert_stream_chunk_eq!(actual, expected);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Watermark(watermark))) => {
                assert_eq!(watermark, Watermark::new(0, DataType::Int64, 7_i64.into()));
            }
            other => panic!("Expected a watermark message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(actual))) => {
                assert_stream_chunk_eq!(actual, chunk_3);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }
    }
}
