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

use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::{Either, select};
use pin_project::pin_project;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::must_match;
use risingwave_pb::stream_plan;
use risingwave_storage::StateStore;
use risingwave_storage::store::StateStoreRead;
use rw_futures_util::drop_either_future;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{DispatchExecutor, DispatchExecutorInner, dispatch_message_batch};
use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::state::LogStoreReadState;
use crate::common::log_store_impl::kv_log_store::{
    FIRST_SEQ_ID, KV_LOG_STORE_V2_INFO, LogStoreVnodeProgress,
};
use crate::executor::prelude::*;
use crate::executor::sync_kv_log_store::{
    ReadFuture, SyncKvLogStoreContext, SyncedKvLogStoreExecutor, SyncedLogStoreBuffer, WriteFuture,
    WriteFutureEvent,
};
use crate::executor::{MessageBatch, StreamConsumer, SyncedKvLogStoreMetrics};
use crate::task::NewOutputRequest;

/// Executor that pairs a synced KV log store with a dispatcher so the log store can
/// advance and write independently of downstream backpressure.
///
/// This keeps upstream executors being polled even when downstream is slow or blocked, decoupling
/// log store progress from downstream polling.
pub struct SyncLogStoreDispatchExecutor<S: StateStore> {
    pub(super) input: Executor,
    pub(super) inner: DispatchExecutorInner,
    pub(super) log_store_context: SyncKvLogStoreContext<S>,
}

impl<S: StateStore> SyncLogStoreDispatchExecutor<S> {
    pub(crate) async fn new(
        input: Executor,
        new_output_request_rx: UnboundedReceiver<(ActorId, NewOutputRequest)>,
        dispatchers: Vec<stream_plan::Dispatcher>,
        actor_context: &ActorContextRef,
        sync: &stream_plan::SyncLogStoreNode,
        vnode_bitmap: Option<Bitmap>,
        state_store: S,
    ) -> StreamResult<Self> {
        let chunk_size = actor_context.config.developer.chunk_size;
        let fragment_id = actor_context.fragment_id;
        let log_store_metrics = SyncedKvLogStoreMetrics::new(
            &actor_context.streaming_metrics,
            actor_context.id,
            fragment_id,
            "sync_log_store_dispatch",
            "sync_log_store_dispatch",
        );

        let table = sync
            .log_store_table
            .as_ref()
            .ok_or_else(|| anyhow!("missing log_store_table in SyncLogStoreNode"))?;

        let pause_duration_ms = actor_context
            .config
            .developer
            .sync_log_store_pause_duration_ms;
        let max_buffer_size = actor_context.config.developer.sync_log_store_buffer_size;

        let serde =
            LogStoreRowSerde::new(table, vnode_bitmap.map(Into::into), &KV_LOG_STORE_V2_INFO);
        let log_store_context = SyncKvLogStoreContext {
            table_id: table.id,
            fragment_id,
            serde,
            state_store,
            max_buffer_size,
            pause_duration_ms: Duration::from_millis(pause_duration_ms as _),
            aligned: sync.aligned,
            chunk_size,
            metrics: log_store_metrics,
        };

        Self::new_with_log_store_context(
            input,
            new_output_request_rx,
            dispatchers,
            actor_context,
            log_store_context,
        )
        .await
    }

    async fn new_with_log_store_context(
        input: Executor,
        new_output_request_rx: UnboundedReceiver<(ActorId, NewOutputRequest)>,
        dispatchers: Vec<stream_plan::Dispatcher>,
        actor_context: &ActorContextRef,
        log_store_context: SyncKvLogStoreContext<S>,
    ) -> StreamResult<Self> {
        let DispatchExecutor { input, inner } =
            DispatchExecutor::new(input, new_output_request_rx, dispatchers, actor_context).await?;

        tracing::info!(
            actor_id = %actor_context.id,
            "synclogstore dispatch executor info"
        );

        Ok(Self {
            input,
            inner,
            log_store_context,
        })
    }
}

type DispatchingFuture =
    impl Future<Output = (DispatchExecutorInner, StreamResult<Option<Barrier>>)> + 'static;

#[define_opaque(DispatchingFuture)]
fn dispatching_future(mut inner: DispatchExecutorInner, message: Message) -> DispatchingFuture {
    async move {
        let batch: MessageBatch = message.into();
        let r = dispatch_message_batch(&mut inner, batch)
            .await
            .map(|barrier_batch| {
                barrier_batch.map(|mut barrier_batch| {
                    debug_assert_eq!(barrier_batch.len(), 1);
                    barrier_batch
                        .pop()
                        .expect("barrier batch should contain one barrier")
                })
            });
        (inner, r)
    }
}

/// State machine for the consumer side, which reads chunks from the log store and dispatches
/// chunks or barriers downstream.
#[pin_project(project = ConsumerFutureProj, project_replace = ConsumerFutureProjReplace)]
enum ConsumerFuture {
    /// Polls the log store for the next chunk. The read future is kept outside this state machine
    /// so both meaningful states can share it.
    ReadingChunk { inner: DispatchExecutorInner },
    /// Dispatches the current message downstream. Any barrier received while dispatching is queued
    /// here and dispatched before reading another chunk.
    Dispatching {
        #[pin]
        future: DispatchingFuture,
        barrier_queue: VecDeque<Message>,
    },
    /// Temporary placeholder used while moving fields out during state transitions.
    PlaceHolder,
}

enum ConsumerFutureEvent {
    BarrierDispatched(Barrier),
    CleanStateReached,
}

impl ConsumerFuture {
    fn dispatch(inner: DispatchExecutorInner, message: Message) -> Self {
        tracing::trace!("consumer_future: dispatching future created");
        Self::Dispatching {
            future: dispatching_future(inner, message),
            barrier_queue: VecDeque::new(),
        }
    }

    fn read_chunk(inner: DispatchExecutorInner) -> Self {
        tracing::trace!("consumer_future: reading chunk future created");
        Self::ReadingChunk { inner }
    }

    fn push_barrier(mut self: Pin<&mut Self>, barrier: Barrier) {
        let message = Message::Barrier(barrier);
        match self.as_mut().project() {
            ConsumerFutureProj::ReadingChunk { .. } => {
                let inner = must_match!(
                    self.as_mut().project_replace(ConsumerFuture::PlaceHolder),
                    ConsumerFutureProjReplace::ReadingChunk { inner } => inner
                );
                self.set(Self::dispatch(inner, message));
            }
            ConsumerFutureProj::Dispatching { barrier_queue, .. } => {
                barrier_queue.push_front(message);
            }
            ConsumerFutureProj::PlaceHolder => {
                unreachable!("ConsumerFuture::PlaceHolder should be handled!")
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn next_event<S: StateStoreRead>(
        mut self: Pin<&mut Self>,
        read_future: &mut ReadFuture<S>,
        read_paused: bool,
        clean_state: &mut bool,
        progress: &mut LogStoreVnodeProgress,
        read_state: &LogStoreReadState<S>,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamResult<ConsumerFutureEvent> {
        loop {
            match self.as_mut().project() {
                ConsumerFutureProj::ReadingChunk { .. } => {
                    if read_paused {
                        pending().await
                    }

                    let message = read_future
                        .next_message(progress, read_state, buffer, metrics)
                        .await?;
                    if let Message::Chunk(chunk) = &message {
                        metrics.total_read_count.inc_by(chunk.cardinality() as _);
                    }

                    let clean_state_reached =
                        read_future.mark_clean_state(clean_state, buffer, metrics);
                    let inner = must_match!(
                        self.as_mut().project_replace(ConsumerFuture::PlaceHolder),
                        ConsumerFutureProjReplace::ReadingChunk { inner } => inner
                    );
                    self.set(Self::dispatch(inner, message));

                    if clean_state_reached {
                        return Ok(ConsumerFutureEvent::CleanStateReached);
                    }
                    continue;
                }
                ConsumerFutureProj::Dispatching {
                    future,
                    barrier_queue,
                } => {
                    let (inner, result) = future.await;
                    let barrier = result?;

                    if let Some(next_barrier) = barrier_queue.pop_back() {
                        tracing::trace!("consumer_future: dispatching future created");
                        let ConsumerFutureProj::Dispatching { mut future, .. } =
                            self.as_mut().project()
                        else {
                            unreachable!("ConsumerFuture::ReadingChunk should be handled!")
                        };
                        future.set(dispatching_future(inner, next_barrier));
                    } else {
                        self.set(Self::read_chunk(inner));
                    }

                    if let Some(barrier) = barrier {
                        return Ok(ConsumerFutureEvent::BarrierDispatched(barrier));
                    }
                }
                ConsumerFutureProj::PlaceHolder => {
                    unreachable!("ConsumerFuture::PlaceHolder should be handled!")
                }
            }
        }
    }
}

impl<S: StateStore> StreamConsumer for SyncLogStoreDispatchExecutor<S> {
    type BarrierStream = impl Stream<Item = StreamResult<Barrier>> + Send;

    fn execute(mut self: Box<Self>) -> Self::BarrierStream {
        #[try_stream]
        async move {
            let actor_id = self.inner.actor_id;
            let log_store_config = self.log_store_context;

            let mut input = self.input.execute();

            let first_barrier = expect_first_barrier(&mut input).await?;
            let first_write_epoch = first_barrier.epoch;

            // Dispatch the first barrier before initializing the log store states
            let first_barrier_batch = dispatch_message_batch(
                &mut self.inner,
                Message::Barrier(first_barrier.clone()).into(),
            )
            .await?;
            debug_assert_eq!(
                first_barrier_batch
                    .as_ref()
                    .map(|barrier_batch| barrier_batch.len()),
                Some(1)
            );
            yield first_barrier.clone();

            let (read_state, initial_write_state) =
                SyncedKvLogStoreExecutor::<S>::init_local_log_store_state(
                    &log_store_config,
                    first_write_epoch,
                )
                .await?;

            let initial_write_epoch = first_write_epoch;
            let mut pause_stream = first_barrier.is_pause_on_startup();

            if log_store_config.aligned {
                let aligned_stream = SyncedKvLogStoreExecutor::<S>::aligned_message_stream(
                    actor_id,
                    input,
                    read_state,
                    initial_write_state,
                    log_store_config.metrics.clone(),
                    initial_write_epoch,
                );

                #[for_await]
                for message in aligned_stream {
                    if let Some(barrier_batch) =
                        dispatch_message_batch(&mut self.inner, message?.into()).await?
                    {
                        // Now Synclogstoredispatchexecutor only support sending out single barrier
                        debug_assert_eq!(barrier_batch.len(), 1);
                        for barrier in barrier_batch {
                            yield barrier;
                        }
                    }
                }
                return Ok(());
            }

            let mut seq_id = FIRST_SEQ_ID;
            let mut buffer = SyncedLogStoreBuffer::new(
                log_store_config.max_buffer_size,
                log_store_config.chunk_size,
                &log_store_config.metrics,
            );

            let log_store_stream = read_state
                .read_persisted_log_store(
                    log_store_config.metrics.persistent_log_read_metrics.clone(),
                    initial_write_epoch.curr,
                    LogStoreReadStateStreamRangeStart::Unbounded,
                )
                .await?;

            let mut log_store_stream = tokio_stream::StreamExt::peekable(log_store_stream);
            let mut clean_state = log_store_stream.peek().await.is_none();
            tracing::trace!(?clean_state);

            let mut progress = LogStoreVnodeProgress::None;
            let mut read_future_state = ReadFuture::ReadingPersistedStream(log_store_stream);
            let consumer_future_state = ConsumerFuture::ReadingChunk { inner: self.inner };
            pin_mut!(consumer_future_state);

            let mut write_future_state =
                WriteFuture::receive_from_upstream(input, initial_write_state);
            let mut end_of_stream = false;

            loop {
                let select_result = {
                    let consumer_future = async {
                        consumer_future_state
                            .as_mut()
                            .next_event(
                                &mut read_future_state,
                                pause_stream,
                                &mut clean_state,
                                &mut progress,
                                &read_state,
                                &mut buffer,
                                &log_store_config.metrics,
                            )
                            .await
                    };
                    pin_mut!(consumer_future);
                    let write_future = async {
                        if end_of_stream {
                            pending().await
                        } else {
                            write_future_state
                                .next_event(&log_store_config.metrics)
                                .await
                        }
                    };
                    pin_mut!(write_future);
                    let output = select(write_future, consumer_future).await;
                    drop_either_future(output)
                };

                match select_result {
                    Either::Left(_write_result) => {
                        drop(write_future_state);
                        let (stream, mut write_state, either) = _write_result?;
                        match either {
                            WriteFutureEvent::UpstreamMessageReceived(msg) => match msg {
                                Message::Chunk(chunk) => {
                                    let (new_seq_id, next_write_future) =
                                        SyncedKvLogStoreExecutor::<S>::process_upstream_chunk(
                                            seq_id,
                                            stream,
                                            write_state,
                                            chunk,
                                            &mut buffer,
                                        );
                                    seq_id = new_seq_id;
                                    write_future_state = next_write_future;
                                }
                                Message::Barrier(barrier) => {
                                    if clean_state
                                        && barrier.kind.is_checkpoint()
                                        && !buffer.is_empty()
                                    {
                                        write_future_state = WriteFuture::paused(
                                            log_store_config.pause_duration_ms,
                                            barrier,
                                            stream,
                                            write_state,
                                        );
                                        clean_state = false;
                                        log_store_config.metrics.unclean_state.inc();
                                    } else {
                                        SyncedKvLogStoreExecutor::<S>::apply_pause_resume_mutation(
                                            &barrier,
                                            &mut pause_stream,
                                        );
                                        let write_state_post_write_barrier =
                                            SyncedKvLogStoreExecutor::<S>::write_barrier(
                                                actor_id,
                                                &mut write_state,
                                                barrier.clone(),
                                                &log_store_config.metrics,
                                                progress.take(),
                                                &mut buffer,
                                            )
                                            .await?;
                                        seq_id = FIRST_SEQ_ID;
                                        barrier.assume_no_update_vnode_bitmap(actor_id)?;

                                        write_state_post_write_barrier
                                            .post_yield_barrier(None)
                                            .await?;

                                        let is_stop_barrier = barrier.is_stop(actor_id);
                                        if is_stop_barrier {
                                            // Stop polling upstream after the stop barrier is
                                            // written into the log store.
                                            end_of_stream = true;
                                            write_future_state = WriteFuture::Empty;
                                        } else {
                                            write_future_state = WriteFuture::receive_from_upstream(
                                                stream,
                                                write_state,
                                            );
                                        }
                                        consumer_future_state.as_mut().push_barrier(barrier);
                                    }
                                }
                                Message::Watermark(watermark) => {
                                    buffer.add_watermark(write_state.epoch().curr, watermark);
                                    write_future_state =
                                        WriteFuture::receive_from_upstream(stream, write_state);
                                }
                            },
                            WriteFutureEvent::ChunkFlushed(info) => {
                                write_future_state =
                                    SyncedKvLogStoreExecutor::<S>::process_flushed_chunk(
                                        stream,
                                        write_state,
                                        info,
                                        &mut buffer,
                                        &log_store_config.metrics,
                                    );
                            }
                        }
                    }
                    Either::Right(consumer_result) => {
                        let event = consumer_result?;
                        match event {
                            ConsumerFutureEvent::CleanStateReached => {
                                if let WriteFuture::Paused { sleep_future, .. } =
                                    &mut write_future_state
                                {
                                    assert!(buffer.has_available_capacity());
                                    *sleep_future = None;
                                }
                            }
                            ConsumerFutureEvent::BarrierDispatched(barrier) => {
                                yield barrier;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::array::{StreamChunk, StreamChunkTestExt};
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::{DispatcherType, PbDispatchOutputMapping};
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::assert_stream_chunk_eq;
    use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V2_INFO;
    use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_stream_chunk_eq, gen_test_log_store_table,
    };
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, BarrierInner as Barrier};
    use crate::task::ActorId;

    const ACTOR_ID: u32 = 4242;
    const DOWNSTREAM_ACTOR_ID: u32 = 5252;

    fn init_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .try_init();
    }

    async fn run_barrier_chunk_ordering_test(aligned: bool) {
        init_logger();

        let actor_id = ActorId::new(ACTOR_ID);
        let downstream_actor = ActorId::new(DOWNSTREAM_ACTOR_ID);
        let (new_output_request_tx, new_output_request_rx) = unbounded_channel();
        let (down_tx, mut down_rx) = channel_for_test();
        new_output_request_tx
            .send((downstream_actor, NewOutputRequest::Local(down_tx)))
            .unwrap();

        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            + 10  11",
        );
        let chunk_2 = StreamChunk::from_pretty(
            "  I   T
            -  5  10
            -  6  10
            -  8  10
            U- 10  11
            U+ 10  10",
        );
        let dispatcher = stream_plan::Dispatcher {
            r#type: DispatcherType::Simple as _,
            dispatcher_id: 7.into(),
            downstream_actor_id: vec![DOWNSTREAM_ACTOR_ID.into()],
            output_mapping: PbDispatchOutputMapping::identical(2).into(),
            ..Default::default()
        };
        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);
        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let serde = LogStoreRowSerde::new(&table, vnodes, pk_info);
        let log_store_config = SyncKvLogStoreContext {
            table_id: table.id,
            fragment_id: 0.into(),
            serde,
            state_store: MemoryStateStore::new(),
            max_buffer_size: 1024,
            pause_duration_ms: Duration::from_millis(10),
            aligned,
            chunk_size: 1024,
            metrics: SyncedKvLogStoreMetrics::for_test(),
        };
        let (mut input_tx, source) = MockSource::channel();
        let input = source.into_executor(
            Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Varchar),
                ],
            },
            vec![0],
        );

        let executor = SyncLogStoreDispatchExecutor::new_with_log_store_context(
            input,
            new_output_request_rx,
            vec![dispatcher],
            &ActorContext::for_test(actor_id),
            log_store_config,
        )
        .await
        .unwrap();

        let (barrier_out_tx, mut barrier_out_rx) = unbounded_channel();
        let barrier_driver = tokio::spawn(async move {
            let barrier_stream = Box::new(executor).execute();
            futures::pin_mut!(barrier_stream);
            while let Some(item) = barrier_stream.next().await {
                barrier_out_tx.send(item).ok();
            }
        });

        input_tx.send_barrier(barrier1.clone());
        let observed1 = timeout(Duration::from_secs(1), barrier_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observed1.epoch.curr, test_epoch(1));

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive barrier(1)");
        let barriers = msg.as_barrier_batch().unwrap();
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].epoch.curr, test_epoch(1));

        input_tx.push_chunk(chunk_1.clone());
        input_tx.push_int64_watermark(0, 7);
        input_tx.push_chunk(chunk_2.clone());
        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive chunk(1)");
        assert_stream_chunk_eq!(msg.as_chunk().unwrap(), chunk_1);

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive watermark");
        assert_eq!(
            msg.as_watermark().unwrap(),
            &Watermark::new(0, DataType::Int64, 7_i64.into())
        );

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive chunk(2)");
        assert_stream_chunk_eq!(msg.as_chunk().unwrap(), chunk_2);

        input_tx.send_barrier(barrier2.clone());
        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive barrier(2)");
        let barriers = msg.as_barrier_batch().unwrap();
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].epoch.curr, test_epoch(2));

        let observed2 = timeout(Duration::from_secs(1), barrier_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observed2.epoch.curr, test_epoch(2));

        barrier_driver.abort();
    }

    /// Mirror `sync_kv_log_store::test_barrier_persisted_read`, but assert the dispatched output
    /// order: chunk(1) -> chunk(2) -> barrier(2), while barrier(1) is surfaced via barrier stream.
    #[tokio::test]
    async fn test_barrier_chunk_ordering_in_dispatch() {
        run_barrier_chunk_ordering_test(false).await;
    }

    #[tokio::test]
    async fn test_aligned_barrier_chunk_ordering_in_dispatch() {
        run_barrier_chunk_ordering_test(true).await;
    }
}
