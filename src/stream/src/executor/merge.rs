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

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use risingwave_common::array::StreamChunkBuilder;
use tokio::sync::mpsc;

use super::exchange::input::BoxedActorInput;
use super::*;
use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

pub type SelectReceivers = DynamicReceivers<ActorId, ()>;

pub type MergeUpstream = BufferChunks<SelectReceivers>;
pub type SingletonUpstream = BoxedActorInput;

pub(crate) enum MergeExecutorUpstream {
    Singleton(SingletonUpstream),
    Merge(MergeUpstream),
}

pub(crate) struct MergeExecutorInput {
    upstream: MergeExecutorUpstream,
    actor_context: ActorContextRef,
    upstream_fragment_id: UpstreamFragmentId,
    local_barrier_manager: LocalBarrierManager,
    executor_stats: Arc<StreamingMetrics>,
    pub(crate) info: ExecutorInfo,
}

impl MergeExecutorInput {
    pub(crate) fn new(
        upstream: MergeExecutorUpstream,
        actor_context: ActorContextRef,
        upstream_fragment_id: UpstreamFragmentId,
        local_barrier_manager: LocalBarrierManager,
        executor_stats: Arc<StreamingMetrics>,
        info: ExecutorInfo,
    ) -> Self {
        Self {
            upstream,
            actor_context,
            upstream_fragment_id,
            local_barrier_manager,
            executor_stats,
            info,
        }
    }

    pub(crate) fn into_executor(self, barrier_rx: mpsc::UnboundedReceiver<Barrier>) -> Executor {
        let fragment_id = self.actor_context.fragment_id;
        let executor = match self.upstream {
            MergeExecutorUpstream::Singleton(input) => ReceiverExecutor::new(
                self.actor_context,
                fragment_id,
                self.upstream_fragment_id,
                input,
                self.local_barrier_manager,
                self.executor_stats,
                barrier_rx,
            )
            .boxed(),
            MergeExecutorUpstream::Merge(inputs) => MergeExecutor::new(
                self.actor_context,
                fragment_id,
                self.upstream_fragment_id,
                inputs,
                self.local_barrier_manager,
                self.executor_stats,
                barrier_rx,
            )
            .boxed(),
        };
        (self.info, executor).into()
    }
}

impl Stream for MergeExecutorInput {
    type Item = DispatcherMessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().upstream {
            MergeExecutorUpstream::Singleton(input) => input.poll_next_unpin(cx),
            MergeExecutorUpstream::Merge(inputs) => inputs.poll_next_unpin(cx),
        }
    }
}

mod upstream {
    use super::*;

    /// Trait unifying operations on [`MergeUpstream`] and [`SingletonUpstream`], so that we can
    /// reuse code between [`MergeExecutor`] and [`ReceiverExecutor`].
    pub trait Upstream:
        Stream<Item = StreamExecutorResult<DispatcherMessage>> + Unpin + Send + 'static
    {
        fn upstream_input_ids(&self) -> impl Iterator<Item = ActorId> + '_;
        fn is_empty(&self) -> bool;
        fn flush_buffered_watermarks(&mut self);
        fn update(&mut self, to_add: Vec<BoxedActorInput>, to_remove: &HashSet<ActorId>);
    }

    impl Upstream for MergeUpstream {
        fn upstream_input_ids(&self) -> impl Iterator<Item = ActorId> + '_ {
            self.inner.upstream_input_ids()
        }

        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        fn flush_buffered_watermarks(&mut self) {
            self.inner.flush_buffered_watermarks();
        }

        fn update(&mut self, to_add: Vec<BoxedActorInput>, to_remove: &HashSet<ActorId>) {
            if !to_add.is_empty() {
                self.inner.add_upstreams_from(to_add);
            }
            if !to_remove.is_empty() {
                self.inner.remove_upstreams(to_remove);
            }
        }
    }

    impl Upstream for SingletonUpstream {
        fn upstream_input_ids(&self) -> impl Iterator<Item = ActorId> + '_ {
            std::iter::once(self.id())
        }

        fn is_empty(&self) -> bool {
            false
        }

        fn flush_buffered_watermarks(&mut self) {
            // For single input, we won't buffer watermarks so there's nothing to do.
        }

        fn update(&mut self, to_add: Vec<BoxedActorInput>, to_remove: &HashSet<ActorId>) {
            assert_eq!(
                to_remove,
                &HashSet::from_iter([self.id()]),
                "the removed upstream actor should be the same as the current input"
            );

            // Replace the single input.
            *self = Itertools::exactly_one(to_add.into_iter())
                .expect("receiver should have exactly one new upstream");
        }
    }
}
use upstream::Upstream;

/// The core of `MergeExecutor` and `ReceiverExecutor`.
pub struct MergeExecutorInner<U> {
    /// The context of the actor.
    actor_context: ActorContextRef,

    /// Upstream channels.
    upstream: U,

    /// Belonged fragment id.
    fragment_id: FragmentId,

    /// Upstream fragment id.
    upstream_fragment_id: FragmentId,

    local_barrier_manager: LocalBarrierManager,

    /// Streaming metrics.
    metrics: Arc<StreamingMetrics>,

    barrier_rx: mpsc::UnboundedReceiver<Barrier>,
}

impl<U> MergeExecutorInner<U> {
    pub fn new(
        ctx: ActorContextRef,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
        upstream: U,
        local_barrier_manager: LocalBarrierManager,
        metrics: Arc<StreamingMetrics>,
        barrier_rx: mpsc::UnboundedReceiver<Barrier>,
    ) -> Self {
        Self {
            actor_context: ctx,
            upstream,
            fragment_id,
            upstream_fragment_id,
            local_barrier_manager,
            metrics,
            barrier_rx,
        }
    }
}

/// `MergeExecutor` merges data from multiple upstream actors and aligns them with barriers.
pub type MergeExecutor = MergeExecutorInner<MergeUpstream>;

impl MergeExecutor {
    #[cfg(test)]
    pub fn for_test(
        actor_id: impl Into<ActorId>,
        inputs: Vec<super::exchange::permit::Receiver>,
        local_barrier_manager: crate::task::LocalBarrierManager,
        schema: Schema,
        chunk_size: usize,
        barrier_rx: Option<mpsc::UnboundedReceiver<Barrier>>,
    ) -> Self {
        let actor_id = actor_id.into();
        use super::exchange::input::LocalInput;
        use crate::executor::exchange::input::ActorInput;

        let barrier_rx =
            barrier_rx.unwrap_or_else(|| local_barrier_manager.subscribe_barrier(actor_id));

        let metrics = StreamingMetrics::unused();
        let actor_ctx = ActorContext::for_test(actor_id);
        let upstream = Self::new_merge_upstream(
            inputs
                .into_iter()
                .enumerate()
                .map(|(idx, input)| LocalInput::new(input, ActorId::new(idx as u32)).boxed_input())
                .collect(),
            &metrics,
            &actor_ctx,
            chunk_size,
            schema,
        );

        Self::new(
            actor_ctx,
            514.into(),
            1919.into(),
            upstream,
            local_barrier_manager,
            metrics.into(),
            barrier_rx,
        )
    }

    pub(crate) fn new_merge_upstream(
        upstreams: Vec<BoxedActorInput>,
        metrics: &StreamingMetrics,
        actor_context: &ActorContext,
        chunk_size: usize,
        schema: Schema,
    ) -> MergeUpstream {
        let merge_barrier_align_duration = Some(
            metrics
                .merge_barrier_align_duration
                .with_guarded_label_values(&[
                    &actor_context.id.to_string(),
                    &actor_context.fragment_id.to_string(),
                ]),
        );

        BufferChunks::new(
            // Futures of all active upstreams.
            SelectReceivers::new(upstreams, None, merge_barrier_align_duration),
            chunk_size,
            schema,
        )
    }
}

impl<U> MergeExecutorInner<U>
where
    U: Upstream,
{
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn execute_inner(mut self: Box<Self>) {
        let mut upstream = self.upstream;
        let actor_id = self.actor_context.id;

        let mut metrics = self.metrics.new_actor_input_metrics(
            actor_id,
            self.fragment_id,
            self.upstream_fragment_id,
        );

        let mut barrier_buffer = DispatchBarrierBuffer::new(
            self.barrier_rx,
            actor_id,
            self.upstream_fragment_id,
            self.local_barrier_manager,
            self.metrics.clone(),
            self.fragment_id,
            self.actor_context.config.clone(),
        );

        loop {
            let upstream_is_empty = upstream.is_empty();
            let msg = barrier_buffer
                .await_next_message(&mut upstream, &metrics, upstream_is_empty)
                .await?;
            let msg = match msg {
                DispatcherMessage::Watermark(watermark) => Message::Watermark(watermark),
                DispatcherMessage::Chunk(chunk) => {
                    metrics.actor_in_record_cnt.inc_by(chunk.cardinality() as _);
                    Message::Chunk(chunk)
                }
                DispatcherMessage::Barrier(barrier) => {
                    let (barrier, new_inputs) =
                        barrier_buffer.pop_barrier_with_inputs(barrier).await?;

                    if let Some(Mutation::Update(UpdateMutation { dispatchers, .. })) =
                        barrier.mutation.as_deref()
                        && upstream
                            .upstream_input_ids()
                            .any(|actor_id| dispatchers.contains_key(&actor_id))
                    {
                        // `Watermark` of upstream may become stale after downstream scaling.
                        upstream.flush_buffered_watermarks();
                    }

                    if let Some(update) =
                        barrier.as_update_merge(self.actor_context.id, self.upstream_fragment_id)
                    {
                        let new_upstream_fragment_id = update
                            .new_upstream_fragment_id
                            .unwrap_or(self.upstream_fragment_id);
                        let removed_upstream_actor_id: HashSet<_> =
                            if update.new_upstream_fragment_id.is_some() {
                                upstream.upstream_input_ids().collect()
                            } else {
                                update.removed_upstream_actor_id.iter().copied().collect()
                            };

                        // `Watermark` of upstream may become stale after upstream scaling.
                        upstream.flush_buffered_watermarks();

                        // Add and remove upstreams.
                        upstream.update(new_inputs.unwrap_or_default(), &removed_upstream_actor_id);

                        self.upstream_fragment_id = new_upstream_fragment_id;
                        metrics = self.metrics.new_actor_input_metrics(
                            actor_id,
                            self.fragment_id,
                            self.upstream_fragment_id,
                        );
                    }

                    let is_stop = barrier.is_stop(actor_id);
                    let msg = Message::Barrier(barrier);
                    if is_stop {
                        yield msg;
                        break;
                    }

                    msg
                }
            };

            yield msg;
        }
    }
}

impl<U> Execute for MergeExecutorInner<U>
where
    U: Upstream,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

/// A wrapper that buffers the `StreamChunk`s from upstream until no more ready items are available.
/// Besides, any message other than `StreamChunk` will trigger the buffered `StreamChunk`s
/// to be emitted immediately along with the message itself.
pub struct BufferChunks<S: Stream> {
    inner: S,
    chunk_builder: StreamChunkBuilder,

    /// The items to be emitted. Whenever there's something here, we should return a `Poll::Ready` immediately.
    pending_items: VecDeque<S::Item>,
}

impl<S: Stream> BufferChunks<S> {
    pub(super) fn new(inner: S, chunk_size: usize, schema: Schema) -> Self {
        assert!(chunk_size > 0);
        let chunk_builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
        Self {
            inner,
            chunk_builder,
            pending_items: VecDeque::new(),
        }
    }
}

impl<S: Stream> std::ops::Deref for BufferChunks<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Stream> std::ops::DerefMut for BufferChunks<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S: Stream> Stream for BufferChunks<S>
where
    S: Stream<Item = DispatcherMessageStreamItem> + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(item) = self.pending_items.pop_front() {
                return Poll::Ready(Some(item));
            }

            match self.inner.poll_next_unpin(cx) {
                Poll::Pending => {
                    return if let Some(chunk_out) = self.chunk_builder.take() {
                        Poll::Ready(Some(Ok(MessageInner::Chunk(chunk_out))))
                    } else {
                        Poll::Pending
                    };
                }

                Poll::Ready(Some(result)) => {
                    if let Ok(MessageInner::Chunk(chunk)) = result {
                        for row in chunk.records() {
                            if let Some(chunk_out) = self.chunk_builder.append_record(row) {
                                self.pending_items
                                    .push_back(Ok(MessageInner::Chunk(chunk_out)));
                            }
                        }
                    } else {
                        return if let Some(chunk_out) = self.chunk_builder.take() {
                            self.pending_items.push_back(result);
                            Poll::Ready(Some(Ok(MessageInner::Chunk(chunk_out))))
                        } else {
                            Poll::Ready(Some(result))
                        };
                    }
                }

                Poll::Ready(None) => {
                    return if let Some(chunk_out) = self.chunk_builder.take() {
                        Poll::Ready(Some(Ok(MessageInner::Chunk(chunk_out))))
                    } else {
                        Poll::Pending
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use assert_matches::assert_matches;
    use futures::future::try_join_all;
    use futures::{FutureExt, poll};
    use risingwave_common::array::Op;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::task_service::stream_exchange_service_server::{
        StreamExchangeService, StreamExchangeServiceServer,
    };
    use risingwave_pb::task_service::{GetStreamRequest, GetStreamResponse, PbPermits};
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status, Streaming};

    use super::*;
    use crate::executor::exchange::input::{ActorInput, LocalInput, RemoteInput};
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::{BarrierInner as Barrier, MessageInner as Message};
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;
    use crate::task::test_utils::helper_make_local_actor;
    use crate::task::{NewOutputRequest, TEST_PARTIAL_GRAPH_ID};

    fn build_test_chunk(size: u64) -> StreamChunk {
        let ops = vec![Op::Insert; size as usize];
        StreamChunk::new(ops, vec![])
    }

    #[tokio::test]
    async fn test_buffer_chunks() {
        let test_env = LocalBarrierTestEnv::for_test().await;

        let (tx, rx) = channel_for_test();
        let input = LocalInput::new(rx, 1.into()).boxed_input();
        let mut buffer = BufferChunks::new(input, 100, Schema::new(vec![]));

        // Send a chunk
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            assert_eq!(chunk.ops().len() as u64, 10);
        });

        // Send 2 chunks and expect them to be merged.
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            assert_eq!(chunk.ops().len() as u64, 20);
        });

        // Send a watermark.
        tx.send(
            Message::Watermark(Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(233),
            })
            .into(),
        )
        .await
        .unwrap();
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Watermark(watermark) => {
            assert_eq!(watermark.val, ScalarImpl::Int64(233));
        });

        // Send 2 chunks before a watermark. Expect the 2 chunks to be merged and the watermark to be emitted.
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        tx.send(
            Message::Watermark(Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(233),
            })
            .into(),
        )
        .await
        .unwrap();
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            assert_eq!(chunk.ops().len() as u64, 20);
        });
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Watermark(watermark) => {
            assert_eq!(watermark.val, ScalarImpl::Int64(233));
        });

        // Send a barrier.
        let barrier = Barrier::new_test_barrier(test_epoch(1));
        test_env.inject_barrier(&barrier, [2.into()]);
        tx.send(Message::Barrier(barrier.clone().into_dispatcher()).into())
            .await
            .unwrap();
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, .. }) => {
            assert_eq!(barrier_epoch.curr, test_epoch(1));
        });

        // Send 2 chunks before a barrier. Expect the 2 chunks to be merged and the barrier to be emitted.
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        tx.send(Message::Chunk(build_test_chunk(10)).into())
            .await
            .unwrap();
        let barrier = Barrier::new_test_barrier(test_epoch(2));
        test_env.inject_barrier(&barrier, [2.into()]);
        tx.send(Message::Barrier(barrier.clone().into_dispatcher()).into())
            .await
            .unwrap();
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            assert_eq!(chunk.ops().len() as u64, 20);
        });
        assert_matches!(buffer.next().await.unwrap().unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, .. }) => {
            assert_eq!(barrier_epoch.curr, test_epoch(2));
        });
    }

    #[tokio::test]
    async fn test_merger() {
        const CHANNEL_NUMBER: usize = 10;
        let mut txs = Vec::with_capacity(CHANNEL_NUMBER);
        let mut rxs = Vec::with_capacity(CHANNEL_NUMBER);
        for _i in 0..CHANNEL_NUMBER {
            let (tx, rx) = channel_for_test();
            txs.push(tx);
            rxs.push(rx);
        }
        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let actor_id = 233.into();
        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        let epochs = (10..1000u64)
            .step_by(10)
            .map(|idx| (idx, test_epoch(idx)))
            .collect_vec();
        let mut prev_epoch = 0;
        let prev_epoch = &mut prev_epoch;
        let barriers: HashMap<_, _> = epochs
            .iter()
            .map(|(_, epoch)| {
                let barrier = Barrier::with_prev_epoch_for_test(*epoch, *prev_epoch);
                *prev_epoch = *epoch;
                barrier_test_env.inject_barrier(&barrier, [actor_id]);
                (*epoch, barrier)
            })
            .collect();
        let b2 = Barrier::with_prev_epoch_for_test(test_epoch(1000), *prev_epoch)
            .with_mutation(Mutation::Stop(StopMutation::default()));
        barrier_test_env.inject_barrier(&b2, [actor_id]);
        barrier_test_env.flush_all_events().await;

        for (tx_id, tx) in txs.into_iter().enumerate() {
            let epochs = epochs.clone();
            let barriers = barriers.clone();
            let b2 = b2.clone();
            let handle = tokio::spawn(async move {
                for (idx, epoch) in epochs {
                    if idx % 20 == 0 {
                        tx.send(Message::Chunk(build_test_chunk(10)).into())
                            .await
                            .unwrap();
                    } else {
                        tx.send(
                            Message::Watermark(Watermark {
                                col_idx: (idx as usize / 20 + tx_id) % CHANNEL_NUMBER,
                                data_type: DataType::Int64,
                                val: ScalarImpl::Int64(idx as i64),
                            })
                            .into(),
                        )
                        .await
                        .unwrap();
                    }
                    tx.send(Message::Barrier(barriers[&epoch].clone().into_dispatcher()).into())
                        .await
                        .unwrap();
                    sleep(Duration::from_millis(1)).await;
                }
                tx.send(Message::Barrier(b2.clone().into_dispatcher()).into())
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        let merger = MergeExecutor::for_test(
            actor_id,
            rxs,
            barrier_test_env.local_barrier_manager.clone(),
            Schema::new(vec![]),
            100,
            None,
        );
        let mut merger = merger.boxed().execute();
        for (idx, epoch) in epochs {
            if idx % 20 == 0 {
                // expect 1 or more chunks with 100 rows in total
                let mut count = 0usize;
                while count < 100 {
                    assert_matches!(merger.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
                        count += chunk.ops().len();
                    });
                }
                assert_eq!(count, 100);
            } else if idx as usize / 20 >= CHANNEL_NUMBER - 1 {
                // expect n watermarks
                for _ in 0..CHANNEL_NUMBER {
                    assert_matches!(merger.next().await.unwrap().unwrap(), Message::Watermark(watermark) => {
                        assert_eq!(watermark.val, ScalarImpl::Int64((idx - 20 * (CHANNEL_NUMBER as u64 - 1)) as i64));
                    });
                }
            }
            // expect a barrier
            assert_matches!(merger.next().await.unwrap().unwrap(), Message::Barrier(Barrier{epoch:barrier_epoch,..}) => {
                assert_eq!(barrier_epoch.curr, epoch);
            });
        }
        assert_matches!(
            merger.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier {
                mutation,
                ..
            }) if mutation.as_deref().unwrap().is_stop_mutation()
        );

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn empty_dynamic_merge_progresses_across_attach_detach_and_reattach() {
        let actor_id = 999.into();
        let resolver_1 = 1000.into();
        let resolver_2 = 1001.into();
        let empty_fragment = 0.into();
        let resolver_fragment_1 = 500.into();
        let resolver_fragment_2 = 501.into();
        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let e1 = Barrier::new_test_barrier(test_epoch(1));
        barrier_test_env.inject_barrier(&e1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let metrics = Arc::new(StreamingMetrics::unused());
        let actor_ctx = ActorContext::for_test(actor_id);
        let barrier_rx = barrier_test_env
            .local_barrier_manager
            .subscribe_barrier(actor_id);
        let upstream = MergeExecutor::new_merge_upstream(
            vec![],
            &metrics,
            &actor_ctx,
            100,
            Schema::empty().clone(),
        );
        let merge = MergeExecutor::new(
            actor_ctx,
            514.into(),
            empty_fragment,
            upstream,
            barrier_test_env.local_barrier_manager.clone(),
            metrics,
            barrier_rx,
        );
        let mut merge = merge.boxed().execute();

        macro_rules! assert_recv_pending {
            () => {
                assert_matches!(
                    poll!(merge.as_mut().next()),
                    Poll::Pending,
                    "an empty dynamic merge must wait for a barrier rather than terminate or busy-loop"
                );
            };
        }

        macro_rules! recv_barrier {
            ($epoch:expr) => {
                assert_matches!(
                    merge.next().await.unwrap().unwrap(),
                    Message::Barrier(Barrier { epoch, .. }) if epoch.curr == test_epoch($epoch)
                );
            };
        }

        async fn take_upstream_tx(
            barrier_test_env: &LocalBarrierTestEnv,
            upstream_actor_id: ActorId,
            downstream_actor_id: ActorId,
        ) -> crate::executor::exchange::permit::Sender {
            let mut requests = barrier_test_env
                .take_pending_new_output_requests(upstream_actor_id)
                .await;
            assert_eq!(requests.len(), 1);
            let (actor_id, request) = requests.pop().unwrap();
            assert_eq!(actor_id, downstream_actor_id);
            let NewOutputRequest::Local(tx) = request else {
                unreachable!()
            };
            tx
        }

        assert_recv_pending!();

        recv_barrier!(1);
        assert_recv_pending!();

        let b1 = Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Update(
            UpdateMutation {
                merges: maplit::hashmap! {
                    (actor_id, empty_fragment) => MergeUpdate {
                        actor_id,
                        upstream_fragment_id: empty_fragment,
                        new_upstream_fragment_id: Some(resolver_fragment_1),
                        added_upstream_actors: vec![helper_make_local_actor(resolver_1)],
                        removed_upstream_actor_id: vec![],
                    },
                },
                ..Default::default()
            },
        ));
        barrier_test_env.inject_barrier(&b1, [actor_id]);
        barrier_test_env.flush_all_events().await;
        assert_recv_pending!();
        barrier_test_env.flush_all_events().await;
        let resolver_1_tx = take_upstream_tx(&barrier_test_env, resolver_1, actor_id).await;
        resolver_1_tx
            .send(Message::Barrier(b1.clone().into_dispatcher()).into())
            .await
            .unwrap();
        recv_barrier!(2);

        resolver_1_tx
            .send(Message::Chunk(build_test_chunk(1)).into())
            .await
            .unwrap();
        assert_matches!(merge.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            assert_eq!(chunk.cardinality(), 1);
        });

        let b2 = Barrier::new_test_barrier(test_epoch(3)).with_mutation(Mutation::Update(
            UpdateMutation {
                merges: maplit::hashmap! {
                    (actor_id, resolver_fragment_1) => MergeUpdate {
                        actor_id,
                        upstream_fragment_id: resolver_fragment_1,
                        new_upstream_fragment_id: Some(empty_fragment),
                        added_upstream_actors: vec![],
                        removed_upstream_actor_id: vec![resolver_1],
                    },
                },
                ..Default::default()
            },
        ));
        barrier_test_env.inject_barrier(&b2, [actor_id]);
        resolver_1_tx
            .send(Message::Barrier(b2.clone().into_dispatcher()).into())
            .await
            .unwrap();
        recv_barrier!(3);
        assert_recv_pending!();

        let e3 = Barrier::new_test_barrier(test_epoch(4));
        barrier_test_env.inject_barrier(&e3, [actor_id]);
        recv_barrier!(4);
        assert_recv_pending!();

        let b4 = Barrier::new_test_barrier(test_epoch(5)).with_mutation(Mutation::Update(
            UpdateMutation {
                merges: maplit::hashmap! {
                    (actor_id, empty_fragment) => MergeUpdate {
                        actor_id,
                        upstream_fragment_id: empty_fragment,
                        new_upstream_fragment_id: Some(resolver_fragment_2),
                        added_upstream_actors: vec![helper_make_local_actor(resolver_2)],
                        removed_upstream_actor_id: vec![],
                    },
                },
                ..Default::default()
            },
        ));
        barrier_test_env.inject_barrier(&b4, [actor_id]);
        barrier_test_env.flush_all_events().await;
        assert_recv_pending!();
        barrier_test_env.flush_all_events().await;
        let resolver_2_tx = take_upstream_tx(&barrier_test_env, resolver_2, actor_id).await;
        resolver_2_tx
            .send(Message::Barrier(b4.into_dispatcher()).into())
            .await
            .unwrap();
        recv_barrier!(5);

        resolver_2_tx
            .send(Message::Chunk(build_test_chunk(1)).into())
            .await
            .unwrap();
        assert_matches!(merge.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            assert_eq!(chunk.cardinality(), 1);
        });
    }

    #[tokio::test]
    async fn test_configuration_change() {
        let actor_id = 233.into();
        let (untouched, old, new) = (234.into(), 235.into(), 238.into()); // upstream actors
        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let metrics = Arc::new(StreamingMetrics::unused());

        // untouched -> actor_id
        // old -> actor_id
        // new -> actor_id

        let (upstream_fragment_id, fragment_id) = (10.into(), 18.into());

        let actor_ctx = ActorContext::for_test(actor_id);

        let inputs: Vec<_> =
            try_join_all([untouched, old].into_iter().map(async |upstream_actor_id| {
                new_input(
                    &barrier_test_env.local_barrier_manager,
                    metrics.clone(),
                    actor_id,
                    fragment_id,
                    &helper_make_local_actor(upstream_actor_id),
                    upstream_fragment_id,
                    actor_ctx.config.clone(),
                )
                .await
            }))
            .await
            .unwrap();

        let merge_updates = maplit::hashmap! {
            (actor_id, upstream_fragment_id) => MergeUpdate {
                actor_id,
                upstream_fragment_id,
                new_upstream_fragment_id: None,
                added_upstream_actors: vec![helper_make_local_actor(new)],
                removed_upstream_actor_id: vec![old],
            }
        };

        let b1 = Barrier::new_test_barrier(test_epoch(1)).with_mutation(Mutation::Update(
            UpdateMutation {
                merges: merge_updates,
                ..Default::default()
            },
        ));
        barrier_test_env.inject_barrier(&b1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let barrier_rx = barrier_test_env
            .local_barrier_manager
            .subscribe_barrier(actor_id);
        let upstream = MergeExecutor::new_merge_upstream(
            inputs,
            &metrics,
            &actor_ctx,
            100,
            Schema::empty().clone(),
        );

        let mut merge = MergeExecutor::new(
            actor_ctx,
            fragment_id,
            upstream_fragment_id,
            upstream,
            barrier_test_env.local_barrier_manager.clone(),
            metrics.clone(),
            barrier_rx,
        )
        .boxed()
        .execute();

        let mut txs = HashMap::new();
        macro_rules! send {
            ($actors:expr, $msg:expr) => {
                for actor in $actors {
                    txs.get(&actor).unwrap().send($msg).await.unwrap();
                }
            };
        }

        macro_rules! assert_recv_pending {
            () => {
                assert!(
                    merge
                        .next()
                        .now_or_never()
                        .flatten()
                        .transpose()
                        .unwrap()
                        .is_none()
                );
            };
        }
        macro_rules! recv {
            () => {
                merge.next().await.transpose().unwrap()
            };
        }

        macro_rules! collect_upstream_tx {
            ($actors:expr) => {
                for upstream_id in $actors {
                    let mut output_requests = barrier_test_env
                        .take_pending_new_output_requests(upstream_id)
                        .await;
                    assert_eq!(output_requests.len(), 1);
                    let (downstream_actor_id, request) = output_requests.pop().unwrap();
                    assert_eq!(downstream_actor_id, actor_id);
                    let NewOutputRequest::Local(tx) = request else {
                        unreachable!()
                    };
                    txs.insert(upstream_id, tx);
                }
            };
        }

        assert_recv_pending!();
        barrier_test_env.flush_all_events().await;

        // 2. Take downstream receivers.
        collect_upstream_tx!([untouched, old]);

        // 3. Send a chunk.
        send!([untouched, old], Message::Chunk(build_test_chunk(1)).into());
        assert_eq!(2, recv!().unwrap().as_chunk().unwrap().cardinality()); // We should be able to receive the chunk twice.
        assert_recv_pending!();

        send!(
            [untouched, old],
            Message::Barrier(b1.clone().into_dispatcher()).into()
        );
        assert_recv_pending!(); // We should not receive the barrier, since merger is waiting for the new upstream new.

        collect_upstream_tx!([new]);

        send!([new], Message::Barrier(b1.clone().into_dispatcher()).into());
        recv!().unwrap().as_barrier().unwrap(); // We should now receive the barrier.

        // 5. Send a chunk.
        send!([untouched, new], Message::Chunk(build_test_chunk(1)).into());
        assert_eq!(2, recv!().unwrap().as_chunk().unwrap().cardinality()); // We should be able to receive the chunk twice.
        assert_recv_pending!();
    }

    struct FakeExchangeService {
        rpc_called: Arc<AtomicBool>,
    }

    fn exchange_client_test_barrier() -> crate::executor::Barrier {
        Barrier::new_test_barrier(test_epoch(1))
    }

    #[async_trait::async_trait]
    impl StreamExchangeService for FakeExchangeService {
        type GetStreamStream = ReceiverStream<std::result::Result<GetStreamResponse, Status>>;

        async fn get_stream(
            &self,
            _request: Request<Streaming<GetStreamRequest>>,
        ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
            let (tx, rx) = tokio::sync::mpsc::channel(10);
            self.rpc_called.store(true, Ordering::SeqCst);
            // send stream_chunk
            let stream_chunk = StreamChunk::default().to_protobuf();
            tx.send(Ok(GetStreamResponse {
                message: Some(PbStreamMessageBatch {
                    stream_message_batch: Some(
                        risingwave_pb::stream_plan::stream_message_batch::StreamMessageBatch::StreamChunk(
                            stream_chunk,
                        ),
                    ),
                }),
                permits: Some(PbPermits::default()),
            }))
            .await
            .unwrap();
            // send barrier
            let barrier = exchange_client_test_barrier();
            tx.send(Ok(GetStreamResponse {
                message: Some(PbStreamMessageBatch {
                    stream_message_batch: Some(
                        risingwave_pb::stream_plan::stream_message_batch::StreamMessageBatch::BarrierBatch(
                            BarrierBatch {
                                barriers: vec![barrier.to_protobuf()],
                            },
                        ),
                    ),
                }),
                permits: Some(PbPermits::default()),
            }))
            .await
            .unwrap();
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test]
    async fn test_stream_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr = "127.0.0.1:12348".parse().unwrap();

        // Start a server.
        let (shutdown_send, shutdown_recv) = tokio::sync::oneshot::channel();
        let exchange_svc = StreamExchangeServiceServer::new(FakeExchangeService {
            rpc_called: rpc_called.clone(),
        });
        let cp_server_run = server_run.clone();
        let join_handle = tokio::spawn(async move {
            cp_server_run.store(true, Ordering::SeqCst);
            tonic::transport::Server::builder()
                .add_service(exchange_svc)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.await.unwrap();
                })
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(1)).await;
        assert!(server_run.load(Ordering::SeqCst));

        let test_env = LocalBarrierTestEnv::for_test().await;

        let remote_input = {
            RemoteInput::new(
                &test_env.local_barrier_manager,
                addr.into(),
                TEST_PARTIAL_GRAPH_ID,
                (0.into(), 0.into()),
                (0.into(), 0.into()),
                Arc::new(StreamingMetrics::unused()),
                Arc::new(StreamingConfig::default()),
            )
            .await
            .unwrap()
        };

        test_env.inject_barrier(&exchange_client_test_barrier(), [remote_input.id()]);

        pin_mut!(remote_input);

        assert_matches!(remote_input.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            assert!(ops.is_empty());
            assert!(columns.is_empty());
            assert!(visibility.is_empty());
        });
        assert_matches!(remote_input.next().await.unwrap().unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, .. }) => {
            assert_eq!(barrier_epoch.curr, test_epoch(1));
        });
        assert!(rpc_called.load(Ordering::SeqCst));

        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
