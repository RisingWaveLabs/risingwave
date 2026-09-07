// Copyright 2024 RisingWave Labs
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

use std::cmp::min;
use std::collections::VecDeque;
use std::future::{Future, pending, ready};
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::{Either, try_join_all};
use futures::{FutureExt, Stream, TryFutureExt, TryStreamExt, pin_mut};
use risingwave_common::array::StreamChunk;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_common_rate_limit::{MonitoredRateLimiter, RateLimit, RateLimiter};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::batch_plan::ScanRange;
use risingwave_pb::common::PbThrottleType;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::ChangeLogRow;
use risingwave_storage::table::batch_table::{BatchTable, PkScanRange};
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::sleep;

use crate::executor::backfill::snapshot_backfill::receive_next_barrier;
use crate::executor::backfill::snapshot_backfill::state::{
    BackfillState, EpochBackfillProgress, VnodeBackfillProgress,
};
use crate::executor::backfill::snapshot_backfill::vnode_stream::VnodeStream;
use crate::executor::backfill::utils::{
    UpstreamStreamKeyUpdateNormalizer, create_builder, mapping_message,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::prelude::{StateTable, StreamExt, try_stream};
use crate::executor::{
    ActorContextRef, Barrier, BoxedMessageStream, DispatcherBarrier, DispatcherMessage, Execute,
    MergeExecutorInput, Message, Mutation, StreamExecutorError, StreamExecutorResult,
    expect_first_barrier,
};
use crate::task::CreateMviewProgressReporter;

pub struct SnapshotBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: BatchTable<S>,

    /// Backfill progress table
    progress_state_table: StateTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: Option<MergeExecutorInput>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// Current executor stream-key indices in the output schema.
    stream_key: Vec<usize>,

    progress: CreateMviewProgressReporter,

    chunk_size: usize,
    rate_limiter: MonitoredRateLimiter,

    barrier_rx: UnboundedReceiver<Barrier>,

    actor_ctx: ActorContextRef,
    metrics: Arc<StreamingMetrics>,

    snapshot_epoch: Option<u64>,
    /// (`eq_prefix`, `range_bounds`) for pk scan range pushdown.
    pk_scan_range: PkScanRange,
}

impl<S: StateStore> SnapshotBackfillExecutor<S> {
    fn build_pk_scan_range(
        pb_scan_range: Option<&ScanRange>,
        upstream_table: &BatchTable<S>,
    ) -> StreamExecutorResult<PkScanRange> {
        match pb_scan_range {
            Some(scan_range) => Ok(PkScanRange::new(
                scan_range.clone(),
                upstream_table.pk_serializer().get_data_types().to_vec(),
            )?),
            None => Ok(PkScanRange::full()),
        }
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        upstream_table: BatchTable<S>,
        progress_state_table: StateTable<S>,
        upstream: Option<MergeExecutorInput>,
        pb_pk_scan_range: Option<&ScanRange>,
        output_indices: Vec<usize>,
        stream_key: Vec<usize>,
        actor_ctx: ActorContextRef,
        progress: CreateMviewProgressReporter,
        chunk_size: usize,
        rate_limit: RateLimit,
        barrier_rx: UnboundedReceiver<Barrier>,
        metrics: Arc<StreamingMetrics>,
        snapshot_epoch: Option<u64>,
    ) -> StreamExecutorResult<Self> {
        if let Some(upstream) = &upstream {
            assert_eq!(&upstream.info.schema, upstream_table.schema());
        }
        if upstream_table.pk_in_output_indices().is_none() {
            panic!(
                "storage table should include all pk columns in output: pk_indices: {:?}, output_indices: {:?}, schema: {:?}",
                upstream_table.pk_indices(),
                upstream_table.output_indices(),
                upstream_table.schema()
            )
        };
        assert!(
            stream_key.iter().all(|idx| *idx < output_indices.len()),
            "stream key indices should refer to output schema: stream_key: {:?}, output_indices: {:?}",
            stream_key,
            output_indices
        );
        let pk_scan_range = Self::build_pk_scan_range(pb_pk_scan_range, &upstream_table)?;
        if !matches!(rate_limit, RateLimit::Disabled) {
            trace!(
                ?rate_limit,
                "create snapshot backfill executor with rate limit"
            );
        }
        let rate_limiter = RateLimiter::new(rate_limit).monitored(upstream_table.table_id());
        Ok(Self {
            upstream_table,
            progress_state_table,
            upstream,
            output_indices,
            stream_key,
            progress,
            chunk_size,
            rate_limiter,
            barrier_rx,
            actor_ctx,
            metrics,
            snapshot_epoch,
            pk_scan_range,
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        trace!("snapshot backfill executor start");
        let upstream = if let Some(mut upstream) = self.upstream {
            let first_upstream_barrier = expect_first_barrier(&mut upstream).await?;
            trace!(epoch = ?first_upstream_barrier.epoch, "get first upstream barrier");
            Some((first_upstream_barrier, upstream))
        } else {
            None
        };
        let first_recv_barrier = receive_next_barrier(&mut self.barrier_rx).await?;
        trace!(epoch = ?first_recv_barrier.epoch, "get first inject barrier");
        let should_snapshot_backfill: Option<u64> = if let Some(snapshot_epoch) =
            self.snapshot_epoch
        {
            if let Some((first_upstream_barrier, _)) = &upstream {
                if first_upstream_barrier.epoch != first_recv_barrier.epoch {
                    assert!(snapshot_epoch <= first_upstream_barrier.epoch.prev);
                    Some(snapshot_epoch)
                } else {
                    None
                }
            } else {
                // must go through snapshot backfill when having no upstream
                Some(snapshot_epoch)
            }
        } else {
            // when snapshot epoch is not set, the StreamNode must be created previously and has finished the backfill
            if cfg!(debug_assertions) {
                panic!(
                    "snapshot epoch not set. first_upstream_epoch: {:?}, first_recv_epoch: {:?}",
                    upstream.map(|(first_upstream_barrier, _)| first_upstream_barrier.epoch),
                    first_recv_barrier.epoch
                );
            } else {
                let (first_upstream_barrier, _) = upstream
                    .as_ref()
                    .ok_or_else(|| anyhow!("no upstream while snapshot epoch not set"))?;
                warn!(first_upstream_epoch = ?first_upstream_barrier.epoch, first_recv_epoch=?first_recv_barrier.epoch, "snapshot epoch not set");
                assert_eq!(first_upstream_barrier.epoch, first_recv_barrier.epoch);
                None
            }
        };
        let first_recv_barrier_epoch = first_recv_barrier.epoch;
        let initial_backfill_paused =
            first_recv_barrier.is_backfill_pause_on_startup(self.actor_ctx.fragment_id);
        yield Message::Barrier(first_recv_barrier);
        let mut backfill_state = BackfillState::new(
            self.progress_state_table,
            first_recv_barrier_epoch,
            self.upstream_table.pk_serializer().clone(),
        )
        .await?;

        let (mut barrier_epoch, mut need_report_finish, upstream) = {
            if let Some(snapshot_epoch) = should_snapshot_backfill {
                let table_id_str = format!("{}", self.upstream_table.table_id());
                let actor_id_str = format!("{}", self.actor_ctx.id);

                let consume_upstream_row_count = self
                    .metrics
                    .snapshot_backfill_consume_row_count
                    .with_guarded_label_values(&[
                        table_id_str.as_str(),
                        actor_id_str.as_str(),
                        "consume_upstream",
                    ]);

                let mut upstream_buffer = if let Some((first_upstream_barrier, upstream)) = upstream
                {
                    SnapshotBackfillUpstream::Buffer(UpstreamBuffer::new(
                        upstream,
                        first_upstream_barrier,
                        consume_upstream_row_count,
                    ))
                } else {
                    SnapshotBackfillUpstream::Empty
                };

                // Phase 1: consume upstream snapshot
                let (mut barrier_epoch, upstream_buffer) = if first_recv_barrier_epoch.prev
                    < snapshot_epoch
                {
                    trace!(
                        table_id = %self.upstream_table.table_id(),
                        snapshot_epoch,
                        barrier_epoch = ?first_recv_barrier_epoch,
                        "start consuming snapshot"
                    );
                    {
                        let consuming_snapshot_row_count = self
                            .metrics
                            .snapshot_backfill_consume_row_count
                            .with_guarded_label_values(&[
                                table_id_str.as_str(),
                                actor_id_str.as_str(),
                                "consuming_snapshot",
                            ]);
                        let snapshot_stream = make_consume_snapshot_stream(
                            &self.upstream_table,
                            snapshot_epoch,
                            self.chunk_size,
                            &self.rate_limiter,
                            &mut self.barrier_rx,
                            &mut self.progress,
                            &mut backfill_state,
                            first_recv_barrier_epoch,
                            initial_backfill_paused,
                            &self.actor_ctx,
                            &self.pk_scan_range,
                        );

                        pin_mut!(snapshot_stream);

                        while let Some(message) = upstream_buffer
                            .run_future(snapshot_stream.try_next())
                            .await?
                        {
                            if let Message::Chunk(chunk) = &message {
                                consuming_snapshot_row_count.inc_by(chunk.cardinality() as _);
                            }
                            yield message;
                        }
                    }

                    let recv_barrier = self.barrier_rx.recv().await.expect("should exist");
                    let recv_barrier_epoch = recv_barrier.epoch;
                    assert_eq!(snapshot_epoch, recv_barrier_epoch.prev);
                    let post_commit = backfill_state.commit(recv_barrier.epoch).await?;
                    yield Message::Barrier(recv_barrier);
                    post_commit.post_yield_barrier(None).await?;
                    (
                        recv_barrier_epoch,
                        upstream_buffer.start_consuming_log_store(snapshot_epoch),
                    )
                } else {
                    trace!(
                        table_id = %self.upstream_table.table_id(),
                        snapshot_epoch,
                        barrier_epoch = ?first_recv_barrier_epoch,
                        "skip consuming snapshot"
                    );
                    (
                        first_recv_barrier_epoch,
                        upstream_buffer.start_consuming_log_store(first_recv_barrier_epoch.prev),
                    )
                };

                // Phase 2: consume upstream log store
                match upstream_buffer {
                    Either::Left(mut upstream_buffer) => {
                        let initial_pending_lag =
                            if let SnapshotBackfillUpstream::Buffer(upstream_buffer) =
                                &upstream_buffer
                            {
                                Some(Duration::from_millis(
                                    Epoch(upstream_buffer.pending_epoch_lag()).physical_time(),
                                ))
                            } else {
                                None
                            };
                        trace!(
                            ?barrier_epoch,
                            table_id = %self.upstream_table.table_id(),
                            ?initial_pending_lag,
                            "start consuming log store"
                        );

                        let consuming_log_store_row_count = self
                            .metrics
                            .snapshot_backfill_consume_row_count
                            .with_guarded_label_values(&[
                                table_id_str.as_str(),
                                actor_id_str.as_str(),
                                "consuming_log_store",
                            ]);
                        let mut pending_non_checkpoint_barrier: Vec<EpochPair> = vec![];
                        loop {
                            let barrier = receive_next_barrier(&mut self.barrier_rx).await?;
                            assert_eq!(barrier_epoch.curr, barrier.epoch.prev);
                            let is_finished = upstream_buffer.consumed_epoch(barrier.epoch).await?;
                            // Disable calling next_epoch, because, if barrier_epoch.prev is a checkpoint epoch,
                            // next_epoch(barrier_epoch.prev) is actually waiting for the committed epoch.
                            // However, upstream_buffer's is_polling_epoch_data can be false, since just received
                            // the checkpoint barrier_epoch.prev. And then the upstream_buffer may stop polling upstream
                            // when the max_pending_epoch_lag is small. When upstream is not polled, the barrier of the next
                            // committed epoch cannot be collected.
                            // {
                            //     // we must call `next_epoch` after `consumed_epoch`, and otherwise in `next_epoch`
                            //     // we may block the upstream, and the upstream never get a chance to finish the `next_epoch`
                            //     let next_prev_epoch = upstream_buffer
                            //         .run_future(self.upstream_table.next_epoch(barrier_epoch.prev))
                            //         .await?;
                            //     assert_eq!(next_prev_epoch, barrier.epoch.prev);
                            // }
                            barrier_epoch = barrier.epoch;
                            if barrier.kind.is_checkpoint() {
                                let pending_non_checkpoint_barrier =
                                    take(&mut pending_non_checkpoint_barrier);
                                let end_epoch = barrier_epoch.prev;
                                let start_epoch = pending_non_checkpoint_barrier
                                    .first()
                                    .map(|epoch| epoch.prev)
                                    .unwrap_or(end_epoch);
                                trace!(?barrier_epoch, kind = ?barrier.kind, ?pending_non_checkpoint_barrier, "start consume epoch change log");
                                // use `upstream_buffer.run_future` to poll upstream concurrently so that we won't have back-pressure
                                // on the upstream. Otherwise, in `batch_iter_log_with_pk_bounds`, we may wait upstream epoch to be committed,
                                // and the back-pressure may cause the upstream unable to consume the barrier and then cause deadlock.
                                let mut stream = upstream_buffer
                                    .run_future(make_log_stream(
                                        &self.upstream_table,
                                        start_epoch,
                                        end_epoch,
                                        None,
                                        self.chunk_size,
                                    ))
                                    .await?;
                                while let Some(chunk) =
                                    upstream_buffer.run_future(stream.try_next()).await?
                                {
                                    trace!(
                                        ?barrier_epoch,
                                        size = chunk.cardinality(),
                                        "consume change log yield chunk",
                                    );
                                    consuming_log_store_row_count.inc_by(chunk.cardinality() as _);
                                    yield Message::Chunk(chunk);
                                }

                                trace!(?barrier_epoch, "after consume change log");

                                stream
                                    .for_vnode_pk_progress(|vnode, row_count, progress| {
                                        assert_eq!(progress, None);
                                        backfill_state.finish_epoch(
                                            vnode,
                                            barrier.epoch.prev,
                                            row_count,
                                        );
                                    })
                                    .await?;
                            } else {
                                pending_non_checkpoint_barrier.push(barrier.epoch);
                            }

                            if let SnapshotBackfillUpstream::Buffer(upstream_buffer) =
                                &upstream_buffer
                            {
                                if is_finished {
                                    assert_eq!(upstream_buffer.pending_epoch_lag(), 0);
                                    assert!(pending_non_checkpoint_barrier.is_empty());
                                    self.progress.finish_consuming_log_store(barrier.epoch);
                                } else {
                                    self.progress.update_create_mview_log_store_progress(
                                        barrier.epoch,
                                        upstream_buffer.pending_epoch_lag(),
                                    );
                                }
                            }

                            let post_commit = backfill_state.commit(barrier.epoch).await?;
                            let update_vnode_bitmap =
                                barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                            yield Message::Barrier(barrier);
                            post_commit.post_yield_barrier(None).await?;
                            if update_vnode_bitmap.is_some() {
                                return Err(anyhow!(
                                    "should not update vnode bitmap during consuming log store"
                                )
                                .into());
                            }

                            if is_finished {
                                assert!(
                                    pending_non_checkpoint_barrier.is_empty(),
                                    "{pending_non_checkpoint_barrier:?}"
                                );
                                break;
                            }
                        }
                        trace!(
                            ?barrier_epoch,
                            table_id = %self.upstream_table.table_id(),
                            "finish consuming log store"
                        );

                        (
                            barrier_epoch,
                            false,
                            upstream_buffer.start_consuming_upstream(),
                        )
                    }
                    Either::Right(upstream) => {
                        trace!(
                            ?barrier_epoch,
                            table_id = %self.upstream_table.table_id(),
                            "skip consuming log store and start consuming upstream directly"
                        );

                        (barrier_epoch, true, upstream)
                    }
                }
            } else {
                let (first_upstream_barrier, _) = upstream
                    .as_ref()
                    .expect("should have upstream when skipping snapshot backfill");
                backfill_state
                    .latest_progress()
                    .for_each(|(vnode, progress)| {
                        let progress = progress.expect("should not be empty");
                        assert_eq!(
                            progress.epoch, first_upstream_barrier.epoch.prev,
                            "vnode: {:?}",
                            vnode
                        );
                        assert_eq!(
                            progress.progress,
                            EpochBackfillProgress::Consumed,
                            "vnode: {:?}",
                            vnode
                        );
                    });
                trace!(
                    table_id = %self.upstream_table.table_id(),
                    "skip backfill"
                );
                let (first_upstream_barrier, upstream) =
                    upstream.expect("should have upstream when skipping snapshot backfill");
                assert_eq!(first_upstream_barrier.epoch, first_recv_barrier_epoch);
                (first_upstream_barrier.epoch, true, upstream)
            }
        };
        let current_stream_key_indices = self
            .stream_key
            .iter()
            .map(|idx| self.output_indices[*idx])
            .collect();
        let update_normalizer = UpstreamStreamKeyUpdateNormalizer::new(
            &upstream.info.stream_key,
            current_stream_key_indices,
        );
        let mut upstream = upstream.into_executor(self.barrier_rx).execute();
        let mut epoch_row_count = 0;
        // Phase 3: consume upstream
        while let Some(msg) = upstream.try_next().await? {
            let Some(msg) = update_normalizer.normalize_message(msg) else {
                continue;
            };
            match msg {
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
                    self.upstream_table
                        .vnodes()
                        .iter_vnodes()
                        .for_each(|vnode| {
                            // Note: the `epoch_row_count` is the accumulated row count of all vnodes of the current
                            // executor.
                            backfill_state.finish_epoch(vnode, barrier.epoch.prev, epoch_row_count);
                        });
                    epoch_row_count = 0;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                    barrier_epoch = barrier.epoch;
                    if need_report_finish {
                        need_report_finish = false;
                        self.progress.finish_consuming_log_store(barrier_epoch);
                    }
                    let post_commit = backfill_state.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                    if let Some(new_vnode_bitmap) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                    {
                        let _prev_vnode_bitmap =
                            self.upstream_table.update_vnode_bitmap(new_vnode_bitmap);
                        backfill_state
                            .latest_progress()
                            .for_each(|(vnode, progress)| {
                                let progress = progress.expect("should not be empty");
                                assert_eq!(
                                    progress.epoch, barrier_epoch.prev,
                                    "vnode {:?} has unexpected progress epoch",
                                    vnode
                                );
                                assert_eq!(
                                    progress.progress,
                                    EpochBackfillProgress::Consumed,
                                    "vnode {:?} has unexpected progress",
                                    vnode
                                );
                            });
                    }
                }
                msg => {
                    if let Message::Chunk(chunk) = &msg {
                        epoch_row_count += chunk.cardinality();
                    }
                    yield msg;
                }
            }
        }
    }
}

impl<S: StateStore> Execute for SnapshotBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let output_indices = self.output_indices.clone();
        self.execute_inner()
            .filter_map(move |result| {
                ready({
                    match result {
                        Ok(message) => mapping_message(message, &output_indices).map(Ok),
                        Err(e) => Some(Err(e)),
                    }
                })
            })
            .boxed()
    }
}

struct ConsumingSnapshot;
struct ConsumingLogStore;

#[derive(Debug)]
struct PendingBarriers {
    first_upstream_barrier_epoch: EpochPair,

    /// Pending non-checkpoint barriers before receiving the next checkpoint barrier
    /// Newer barrier at the front
    pending_non_checkpoint_barriers: VecDeque<DispatcherBarrier>,

    /// In the outer `VecDeque`, newer barriers at the front.
    /// In the inner `VecDeque`, newer barrier at the front, with the first barrier as checkpoint barrier,
    /// and others as non-checkpoint barrier
    checkpoint_barrier_groups: VecDeque<VecDeque<DispatcherBarrier>>,
}

impl PendingBarriers {
    fn new(first_upstream_barrier: DispatcherBarrier) -> Self {
        Self {
            first_upstream_barrier_epoch: first_upstream_barrier.epoch,
            pending_non_checkpoint_barriers: Default::default(),
            checkpoint_barrier_groups: VecDeque::from_iter([VecDeque::from_iter([
                first_upstream_barrier,
            ])]),
        }
    }

    fn add(&mut self, barrier: DispatcherBarrier) {
        let is_checkpoint = barrier.kind.is_checkpoint();
        self.pending_non_checkpoint_barriers.push_front(barrier);
        if is_checkpoint {
            self.checkpoint_barrier_groups
                .push_front(take(&mut self.pending_non_checkpoint_barriers));
        }
    }

    fn pop(&mut self) -> Option<VecDeque<DispatcherBarrier>> {
        self.checkpoint_barrier_groups.pop_back()
    }

    fn consume_epoch(&mut self, epoch: EpochPair) {
        let barriers = self
            .checkpoint_barrier_groups
            .back_mut()
            .expect("non-empty");
        let oldest_upstream_barrier = barriers.back().expect("non-empty");
        assert!(
            oldest_upstream_barrier.epoch.prev >= epoch.prev,
            "oldest upstream barrier has epoch {:?} earlier than epoch to consume {:?}",
            oldest_upstream_barrier.epoch,
            epoch
        );
        if oldest_upstream_barrier.epoch.prev == epoch.prev {
            assert_eq!(oldest_upstream_barrier.epoch, epoch);
            barriers.pop_back();
            if barriers.is_empty() {
                self.checkpoint_barrier_groups.pop_back();
            }
        }
    }

    fn latest_epoch(&self) -> Option<EpochPair> {
        self.pending_non_checkpoint_barriers
            .front()
            .or_else(|| {
                self.checkpoint_barrier_groups
                    .front()
                    .and_then(|barriers| barriers.front())
            })
            .map(|barrier| barrier.epoch)
    }

    fn checkpoint_epoch_count(&self) -> usize {
        self.checkpoint_barrier_groups.len()
    }

    fn has_checkpoint_epoch(&self) -> bool {
        !self.checkpoint_barrier_groups.is_empty()
    }
}

enum SnapshotBackfillUpstream<S> {
    Empty,
    Buffer(UpstreamBuffer<S>),
}

impl<S> SnapshotBackfillUpstream<S> {
    async fn run_future<T, E: Into<StreamExecutorError>>(
        &mut self,
        future: impl Future<Output = Result<T, E>>,
    ) -> StreamExecutorResult<T> {
        match self {
            SnapshotBackfillUpstream::Empty => future.await.map_err(Into::into),
            SnapshotBackfillUpstream::Buffer(buffer) => buffer.run_future(future).await,
        }
    }
}

impl SnapshotBackfillUpstream<ConsumingSnapshot> {
    fn start_consuming_log_store(
        self,
        consumed_epoch: u64,
    ) -> Either<SnapshotBackfillUpstream<ConsumingLogStore>, MergeExecutorInput> {
        match self {
            SnapshotBackfillUpstream::Empty => Either::Left(SnapshotBackfillUpstream::Empty),
            SnapshotBackfillUpstream::Buffer(buffer) => {
                match buffer.start_consuming_log_store(consumed_epoch) {
                    Either::Left(buffer) => Either::Left(SnapshotBackfillUpstream::Buffer(buffer)),
                    Either::Right(input) => Either::Right(input),
                }
            }
        }
    }
}

impl SnapshotBackfillUpstream<ConsumingLogStore> {
    async fn consumed_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<bool> {
        match self {
            SnapshotBackfillUpstream::Empty => Ok(false),
            SnapshotBackfillUpstream::Buffer(buffer) => buffer.consumed_epoch(epoch).await,
        }
    }

    fn start_consuming_upstream(self) -> MergeExecutorInput {
        match self {
            SnapshotBackfillUpstream::Empty => {
                unreachable!("unlike to start consuming upstream when having no upstream")
            }
            SnapshotBackfillUpstream::Buffer(buffer) => buffer.start_consuming_upstream(),
        }
    }
}

struct UpstreamBuffer<S> {
    upstream: MergeExecutorInput,
    max_pending_epoch_lag: u64,
    consumed_epoch: u64,
    /// Barriers received from upstream but not yet received the barrier from local barrier worker.
    upstream_pending_barriers: PendingBarriers,
    /// Whether we have started polling any upstream data before the next checkpoint barrier.
    /// When `true`, we should continue polling until the next checkpoint barrier, because
    /// some data in this epoch have been discarded and data in this epoch
    /// must be read from log store
    is_polling_epoch_data: bool,
    consume_upstream_row_count: LabelGuardedIntCounter,
    _phase: S,
}

impl UpstreamBuffer<ConsumingSnapshot> {
    fn new(
        upstream: MergeExecutorInput,
        first_upstream_barrier: DispatcherBarrier,
        consume_upstream_row_count: LabelGuardedIntCounter,
    ) -> Self {
        Self {
            upstream,
            is_polling_epoch_data: false,
            consume_upstream_row_count,
            upstream_pending_barriers: PendingBarriers::new(first_upstream_barrier),
            // no limit on the number of pending barrier in the beginning
            max_pending_epoch_lag: u64::MAX,
            consumed_epoch: 0,
            _phase: ConsumingSnapshot {},
        }
    }

    fn start_consuming_log_store(
        mut self,
        consumed_epoch: u64,
    ) -> Either<UpstreamBuffer<ConsumingLogStore>, MergeExecutorInput> {
        if self
            .upstream_pending_barriers
            .first_upstream_barrier_epoch
            .prev
            == consumed_epoch
        {
            assert_eq!(
                1,
                self.upstream_pending_barriers
                    .pop()
                    .expect("non-empty")
                    .len()
            );
        }
        let max_pending_epoch_lag = self.pending_epoch_lag();
        let buffer = UpstreamBuffer {
            upstream: self.upstream,
            upstream_pending_barriers: self.upstream_pending_barriers,
            max_pending_epoch_lag,
            is_polling_epoch_data: self.is_polling_epoch_data,
            consume_upstream_row_count: self.consume_upstream_row_count,
            consumed_epoch,
            _phase: ConsumingLogStore {},
        };
        if buffer.is_finished() {
            Either::Right(buffer.upstream)
        } else {
            Either::Left(buffer)
        }
    }
}

impl<S> UpstreamBuffer<S> {
    fn can_consume_upstream(&self) -> bool {
        self.is_polling_epoch_data || self.pending_epoch_lag() < self.max_pending_epoch_lag
    }

    async fn concurrently_consume_upstream(&mut self) -> StreamExecutorError {
        {
            loop {
                if let Err(e) = try {
                    if !self.can_consume_upstream() {
                        // pause the future to block consuming upstream
                        sleep(Duration::from_secs(30)).await;
                        warn!(pending_barrier = ?self.upstream_pending_barriers, "not polling upstream but timeout");
                        return pending().await;
                    }
                    self.consume_until_next_checkpoint_barrier().await?;
                } {
                    break e;
                }
            }
        }
    }

    /// Consume the upstream until seeing the next barrier.
    async fn consume_until_next_checkpoint_barrier(&mut self) -> StreamExecutorResult<()> {
        loop {
            let msg: DispatcherMessage = self
                .upstream
                .try_next()
                .await?
                .ok_or_else(|| anyhow!("end of upstream"))?;
            match msg {
                DispatcherMessage::Chunk(chunk) => {
                    self.is_polling_epoch_data = true;
                    self.consume_upstream_row_count
                        .inc_by(chunk.cardinality() as _);
                }
                DispatcherMessage::Barrier(barrier) => {
                    let is_checkpoint = barrier.kind.is_checkpoint();
                    self.upstream_pending_barriers.add(barrier);
                    if is_checkpoint {
                        self.is_polling_epoch_data = false;
                        break;
                    } else {
                        self.is_polling_epoch_data = true;
                    }
                }
                DispatcherMessage::Watermark(_) => {
                    self.is_polling_epoch_data = true;
                }
            }
        }
        Ok(())
    }
}

impl UpstreamBuffer<ConsumingLogStore> {
    #[await_tree::instrument("consumed_epoch: {:?}", epoch)]
    async fn consumed_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<bool> {
        assert!(!self.is_finished());
        if !self.upstream_pending_barriers.has_checkpoint_epoch() {
            // when upstream_pending_barriers is empty and not polling any intermediate epoch data,
            // we must have returned true to indicate finish, and should not be called again.
            assert!(self.is_polling_epoch_data);
            self.consume_until_next_checkpoint_barrier().await?;
            assert_eq!(self.upstream_pending_barriers.checkpoint_epoch_count(), 1);
        }
        self.upstream_pending_barriers.consume_epoch(epoch);

        {
            {
                let prev_epoch = epoch.prev;
                assert!(self.consumed_epoch < prev_epoch);
                let elapsed_epoch = prev_epoch - self.consumed_epoch;
                self.consumed_epoch = prev_epoch;
                if self.upstream_pending_barriers.has_checkpoint_epoch() {
                    // try consuming ready upstreams when we haven't yielded all pending barriers yet.
                    while self.can_consume_upstream()
                        && let Some(result) =
                            self.consume_until_next_checkpoint_barrier().now_or_never()
                    {
                        result?;
                    }
                }
                // sub to ensure that the lag is monotonically decreasing.
                // here we subtract half the elapsed epoch, so that approximately when downstream progresses two epochs,
                // the upstream can at least progress for one epoch.
                self.max_pending_epoch_lag = min(
                    self.pending_epoch_lag(),
                    self.max_pending_epoch_lag.saturating_sub(elapsed_epoch / 2),
                );
            }
        }
        Ok(self.is_finished())
    }

    fn is_finished(&self) -> bool {
        if cfg!(debug_assertions) && !self.is_polling_epoch_data {
            assert!(
                self.upstream_pending_barriers
                    .pending_non_checkpoint_barriers
                    .is_empty()
            )
        }
        !self.upstream_pending_barriers.has_checkpoint_epoch() && !self.is_polling_epoch_data
    }

    fn start_consuming_upstream(self) -> MergeExecutorInput {
        assert!(self.is_finished());
        assert_eq!(self.pending_epoch_lag(), 0);
        self.upstream
    }
}

impl<S> UpstreamBuffer<S> {
    /// Run a future while concurrently polling the upstream so that the upstream
    /// won't be back-pressured.
    async fn run_future<T, E: Into<StreamExecutorError>>(
        &mut self,
        future: impl Future<Output = Result<T, E>>,
    ) -> StreamExecutorResult<T> {
        select! {
            biased;
            e = self.concurrently_consume_upstream() => {
                Err(e)
            }
            // this arm won't be starved, because the first arm is always pending unless returning with error
            result = future => {
                result.map_err(Into::into)
            }
        }
    }

    fn pending_epoch_lag(&self) -> u64 {
        self.upstream_pending_barriers
            .latest_epoch()
            .map(|epoch| {
                epoch
                    .prev
                    .checked_sub(self.consumed_epoch)
                    .expect("pending epoch must be later than consumed_epoch")
            })
            .unwrap_or(0)
    }
}

#[await_tree::instrument("make_log_stream: {start_epoch}-{end_epoch} table {}", upstream_table.table_id())]
async fn make_log_stream(
    upstream_table: &BatchTable<impl StateStore>,
    start_epoch: u64,
    end_epoch: u64,
    start_pk: Option<OwnedRow>,
    chunk_size: usize,
) -> StreamExecutorResult<VnodeStream<impl super::vnode_stream::ChangeLogRowStream>> {
    let data_types = upstream_table.schema().data_types();
    let start_pk = start_pk.as_ref();
    // TODO: may avoid polling all vnodes concurrently at the same time but instead with a limit on concurrency.
    let vnode_streams = try_join_all(upstream_table.vnodes().iter_vnodes().map(move |vnode| {
        upstream_table
            .batch_iter_vnode_log(
                start_epoch,
                HummockReadEpoch::Committed(end_epoch),
                start_pk,
                vnode,
            )
            .map_ok(move |stream| {
                let stream = stream.map_err(Into::into);
                (vnode, stream, 0)
            })
    }))
    .await?;
    let builder = create_builder(RateLimit::Disabled, chunk_size, data_types.clone());
    Ok(VnodeStream::new(
        vnode_streams,
        upstream_table.pk_in_output_indices().expect("should exist"),
        builder,
    ))
}

async fn make_snapshot_stream(
    upstream_table: &BatchTable<impl StateStore>,
    snapshot_epoch: u64,
    backfill_state: &BackfillState<impl StateStore>,
    rate_limit: RateLimit,
    chunk_size: usize,
    snapshot_rebuild_interval: Duration,
    pk_scan_range: &PkScanRange,
) -> StreamExecutorResult<VnodeStream<impl super::vnode_stream::ChangeLogRowStream>> {
    let data_types = upstream_table.schema().data_types();
    let vnode_streams = try_join_all(backfill_state.latest_progress().filter_map(
        move |(vnode, progress)| {
            let start_pk = match progress {
                None => Some((None, 0)),
                Some(VnodeBackfillProgress {
                    row_count,
                    progress: EpochBackfillProgress::Consuming { latest_pk },
                    ..
                }) => Some((Some(latest_pk), *row_count)),
                Some(VnodeBackfillProgress {
                    progress: EpochBackfillProgress::Consumed,
                    ..
                }) => None,
            };
            start_pk.map(|(start_pk, row_count)| {
                upstream_table
                    .batch_iter_vnode_with_pk_range(
                        HummockReadEpoch::Committed(snapshot_epoch),
                        start_pk,
                        &pk_scan_range.pk_prefix,
                        &pk_scan_range.range_bounds,
                        vnode,
                        PrefetchOptions::prefetch_for_large_range_scan(),
                        snapshot_rebuild_interval,
                    )
                    .map_ok(move |stream| {
                        let stream = stream.map_ok(ChangeLogRow::Insert).map_err(Into::into);
                        (vnode, stream, row_count)
                    })
            })
        },
    ))
    .await?;
    let builder = create_builder(rate_limit, chunk_size, data_types.clone());
    Ok(VnodeStream::new(
        vnode_streams,
        upstream_table.pk_in_output_indices().expect("should exist"),
        builder,
    ))
}

#[expect(clippy::too_many_arguments)]
#[try_stream(ok = Message, error = StreamExecutorError)]
async fn make_consume_snapshot_stream<'a, S: StateStore>(
    upstream_table: &'a BatchTable<S>,
    snapshot_epoch: u64,
    chunk_size: usize,
    rate_limiter: &'a MonitoredRateLimiter,
    barrier_rx: &'a mut UnboundedReceiver<Barrier>,
    progress: &'a mut CreateMviewProgressReporter,
    backfill_state: &'a mut BackfillState<S>,
    first_recv_barrier_epoch: EpochPair,
    initial_backfill_paused: bool,
    actor_ctx: &'a ActorContextRef,
    pk_scan_range: &'a PkScanRange,
) {
    let mut barrier_epoch = first_recv_barrier_epoch;

    // start consume upstream snapshot
    let mut snapshot_stream = make_snapshot_stream(
        upstream_table,
        snapshot_epoch,
        &*backfill_state,
        rate_limiter.rate_limit(),
        chunk_size,
        actor_ctx.config.developer.snapshot_iter_rebuild_interval(),
        pk_scan_range,
    )
    .await?;

    async fn select_barrier_and_snapshot_stream(
        barrier_rx: &mut UnboundedReceiver<Barrier>,
        snapshot_stream: &mut (impl Stream<Item = StreamExecutorResult<StreamChunk>> + Unpin),
        throttle_snapshot_stream: bool,
        backfill_paused: bool,
    ) -> StreamExecutorResult<Either<Barrier, Option<StreamChunk>>> {
        select! {
            biased;

            result = receive_next_barrier(barrier_rx) => {
                Ok(Either::Left(result?))
            },
            result = snapshot_stream.try_next(), if !throttle_snapshot_stream && !backfill_paused => {
                Ok(Either::Right(result?))
            }
        }
    }

    let mut backfill_paused = initial_backfill_paused;
    loop {
        let throttle_snapshot_stream = matches!(rate_limiter.rate_limit(), RateLimit::Pause);
        match select_barrier_and_snapshot_stream(
            barrier_rx,
            &mut snapshot_stream,
            throttle_snapshot_stream,
            backfill_paused,
        )
        .await?
        {
            Either::Left(barrier) => {
                assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
                barrier_epoch = barrier.epoch;

                if barrier_epoch.curr >= snapshot_epoch {
                    return Err(anyhow!("should not receive barrier with epoch {barrier_epoch:?} later than snapshot epoch {snapshot_epoch}").into());
                }
                if barrier.should_start_fragment_backfill(actor_ctx.fragment_id) {
                    backfill_paused = false;
                }
                if let Some(chunk) = snapshot_stream.consume_builder() {
                    rate_limiter.wait(chunk.cardinality() as _).await;
                    yield Message::Chunk(chunk);
                }
                snapshot_stream
                    .for_vnode_pk_progress(|vnode, row_count, pk_progress| {
                        if let Some(pk) = pk_progress {
                            backfill_state.update_epoch_progress(
                                vnode,
                                snapshot_epoch,
                                row_count,
                                pk,
                            );
                        } else {
                            backfill_state.finish_epoch(vnode, snapshot_epoch, row_count);
                        }
                    })
                    .await?;
                let count = backfill_state.total_row_count();
                let post_commit = backfill_state.commit(barrier.epoch).await?;
                trace!(?barrier_epoch, count, "update progress");
                progress.update(barrier_epoch, barrier_epoch.prev, count as _);

                let new_rate_limit = barrier.mutation.as_ref().and_then(|m| {
                    if let Mutation::Throttle(config) = &**m
                        && let Some(config) = config.get(&actor_ctx.fragment_id)
                        && config.throttle_type() == PbThrottleType::Backfill
                    {
                        Some(config.rate_limit)
                    } else {
                        None
                    }
                });
                yield Message::Barrier(barrier);
                post_commit.post_yield_barrier(None).await?;

                if let Some(new_rate_limit) = new_rate_limit {
                    let new_rate_limit = new_rate_limit.into();
                    rate_limiter.update(new_rate_limit);
                    snapshot_stream.update_rate_limiter(new_rate_limit, chunk_size);
                }
            }
            Either::Right(Some(chunk)) => {
                if backfill_paused {
                    return Err(
                        anyhow!("snapshot backfill paused, but received snapshot chunk").into(),
                    );
                }
                rate_limiter.wait(chunk.cardinality() as _).await;
                yield Message::Chunk(chunk);
            }
            Either::Right(None) => {
                break;
            }
        }
    }

    // finish consuming upstream snapshot, report finish
    let barrier_to_report_finish = receive_next_barrier(barrier_rx).await?;
    assert_eq!(barrier_to_report_finish.epoch.prev, barrier_epoch.curr);
    barrier_epoch = barrier_to_report_finish.epoch;
    snapshot_stream
        .for_vnode_pk_progress(|vnode, row_count, pk_progress| {
            assert_eq!(pk_progress, None);
            backfill_state.finish_epoch(vnode, snapshot_epoch, row_count);
        })
        .await?;
    let count = backfill_state.total_row_count();
    trace!(?barrier_epoch, count, "report finish");
    let post_commit = backfill_state.commit(barrier_epoch).await?;
    progress.finish(barrier_epoch, count as _);
    yield Message::Barrier(barrier_to_report_finish);
    post_commit.post_yield_barrier(None).await?;

    // keep receiving remaining barriers until receiving a barrier with epoch as snapshot_epoch
    loop {
        let barrier = receive_next_barrier(barrier_rx).await?;
        assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
        barrier_epoch = barrier.epoch;
        let post_commit = backfill_state.commit(barrier.epoch).await?;
        yield Message::Barrier(barrier);
        post_commit.post_yield_barrier(None).await?;
        if barrier_epoch.curr == snapshot_epoch {
            break;
        }
    }
    trace!(?barrier_epoch, "finish consuming snapshot");
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_test::test_utils::{HummockTestEnv, prepare_hummock_test_env};
    use risingwave_rpc_client::HummockMetaClient;
    use risingwave_storage::hummock::HummockStorage;
    use risingwave_storage::table::batch_table::BatchTable;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::common::table::state_table::{
        StateTable, StateTableBuilder, StateTableOpConsistencyLevel,
    };
    use crate::common::table::test_utils::gen_pbtable_with_value_indices;
    use crate::executor::exchange::input::{Input, LocalInput};
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::{ActorContext, DispatcherMessage, ExecutorInfo, MergeExecutorUpstream};
    use crate::task::LocalBarrierManager;

    const SOURCE_TABLE_ID: TableId = TableId::new(0x233);
    const PROGRESS_TABLE_ID: TableId = TableId::new(0x234);

    fn source_table_pb() -> risingwave_pb::catalog::PbTable {
        gen_pbtable_with_value_indices(
            SOURCE_TABLE_ID,
            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
            vec![OrderType::ascending()],
            vec![0],
            0,
            vec![0],
        )
    }

    fn progress_table_pb() -> risingwave_pb::catalog::PbTable {
        gen_pbtable_with_value_indices(
            PROGRESS_TABLE_ID,
            vec![
                ColumnDesc::unnamed(ColumnId::new(0), DataType::Int16),
                ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
                ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
                ColumnDesc::unnamed(ColumnId::new(3), DataType::Boolean),
                ColumnDesc::unnamed(ColumnId::new(4), DataType::Int64),
            ],
            vec![OrderType::ascending()],
            vec![0],
            1,
            vec![1, 2, 3, 4],
        )
    }

    fn source_batch_table(store: HummockStorage) -> BatchTable<HummockStorage> {
        BatchTable::for_test(
            store,
            SOURCE_TABLE_ID,
            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
            vec![OrderType::ascending()],
            vec![0],
            vec![0],
        )
    }

    async fn source_state_table(store: HummockStorage) -> StateTable<HummockStorage> {
        StateTableBuilder::new(&source_table_pb(), store, None)
            .with_op_consistency_level(StateTableOpConsistencyLevel::LogStoreEnabled)
            .forbid_preload_all_rows()
            .build()
            .await
    }

    async fn progress_state_table(store: HummockStorage) -> StateTable<HummockStorage> {
        StateTable::from_table_catalog(&progress_table_pb(), store, None).await
    }

    async fn commit_insert_epoch(
        test_env: &HummockTestEnv,
        source_state_table: &mut StateTable<HummockStorage>,
        epoch: &mut EpochPair,
        table_ids: HashSet<TableId>,
        values: &[i64],
    ) {
        for value in values {
            source_state_table.insert(OwnedRow::new(vec![Some((*value).into())]));
        }
        epoch.inc_for_test();
        test_env.storage.start_epoch(epoch.curr, table_ids);
        source_state_table.commit_for_test(*epoch).await.unwrap();
        let res = test_env
            .storage
            .seal_and_sync_epoch(epoch.prev, HashSet::from_iter([SOURCE_TABLE_ID]))
            .await
            .unwrap();
        test_env
            .meta_client
            .commit_epoch_with_change_log(epoch.prev, res, Some(vec![epoch.prev]))
            .await
            .unwrap();
        test_env
            .storage
            .wait_version(test_env.manager.get_current_version().await)
            .await;
    }

    fn start_progress_epochs(test_env: &HummockTestEnv, max_epoch: u64) {
        for epoch in 1..=max_epoch {
            test_env
                .storage
                .start_epoch(test_epoch(epoch), HashSet::from_iter([PROGRESS_TABLE_ID]));
        }
    }

    fn make_upstream_input(
        barrier_manager: LocalBarrierManager,
        actor_ctx: ActorContextRef,
        rx: crate::executor::exchange::permit::Receiver,
    ) -> MergeExecutorInput {
        MergeExecutorInput::new(
            MergeExecutorUpstream::Singleton(LocalInput::new(rx, 1001.into()).boxed_input()),
            actor_ctx,
            1919.into(),
            barrier_manager,
            Arc::new(StreamingMetrics::unused()),
            ExecutorInfo::for_test(
                Schema::new(vec![Field::unnamed(DataType::Int64)]),
                vec![0],
                "SnapshotBackfillUpstream".to_owned(),
                0,
            ),
        )
    }

    async fn expect_barrier_with_timeout(
        executor: &mut BoxedMessageStream,
        reason: &str,
    ) -> Barrier {
        let message = timeout(Duration::from_secs(10), executor.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for barrier: {reason}"))
            .unwrap()
            .unwrap();
        match message {
            Message::Barrier(barrier) => barrier,
            other => panic!("expected barrier for {reason}, got {other:?}"),
        }
    }

    async fn expect_chunk_with_timeout(
        executor: &mut BoxedMessageStream,
        reason: &str,
    ) -> StreamChunk {
        let message = timeout(Duration::from_secs(10), executor.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for chunk: {reason}"))
            .unwrap()
            .unwrap();
        match message {
            Message::Chunk(chunk) => chunk,
            other => panic!("expected chunk for {reason}, got {other:?}"),
        }
    }

    async fn expect_pending_with_timeout(executor: &mut BoxedMessageStream, reason: &str) {
        assert!(
            timeout(Duration::from_millis(200), executor.next())
                .await
                .is_err(),
            "executor unexpectedly produced a message while waiting for {reason}"
        );
    }

    #[tokio::test]
    async fn test_snapshot_backfill_without_upstream_on_hummock() {
        let source_env = prepare_hummock_test_env().await;
        source_env.register_table(source_table_pb()).await;
        let progress_env = prepare_hummock_test_env().await;
        progress_env.register_table(progress_table_pb()).await;

        let mut source_state_table = source_state_table(source_env.storage.clone()).await;
        let source_table = source_batch_table(source_env.storage.clone());
        let progress_state_table = progress_state_table(progress_env.storage.clone()).await;

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        source_env
            .storage
            .start_epoch(epoch.curr, HashSet::from_iter([SOURCE_TABLE_ID]));
        source_state_table.init_epoch(epoch).await.unwrap();

        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[1],
        )
        .await;
        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[2],
        )
        .await;
        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[3],
        )
        .await;
        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[],
        )
        .await;
        start_progress_epochs(&progress_env, 5);

        let barrier_manager = LocalBarrierManager::for_test();
        let progress = CreateMviewProgressReporter::for_test(barrier_manager);
        let actor_ctx = ActorContext::for_test(1234);
        let (barrier_tx, barrier_rx) = unbounded_channel();
        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(1)))
            .unwrap();

        let mut executor = SnapshotBackfillExecutor::new(
            source_table,
            progress_state_table,
            None,
            None,
            vec![0],
            vec![0],
            actor_ctx,
            progress,
            1024,
            RateLimit::Disabled,
            barrier_rx,
            Arc::new(StreamingMetrics::unused()),
            Some(test_epoch(3)),
        )
        .expect("snapshot backfill executor should be created")
        .boxed()
        .execute();

        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "initial injected barrier")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(1)).epoch
        );
        assert_eq!(
            expect_chunk_with_timeout(&mut executor, "snapshot chunk without upstream").await,
            StreamChunk::from_pretty(
                " I
                + 1
                + 2
                + 3"
            )
        );
        expect_pending_with_timeout(&mut executor, "snapshot finish barrier 2").await;

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(2)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "snapshot progress barrier 2")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(2)).epoch
        );

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(3)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "snapshot progress barrier 3")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(3)).epoch
        );

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(4)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "post-snapshot barrier 4")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(4)).epoch
        );

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(5)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "steady-state barrier 5")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(5)).epoch
        );

        expect_pending_with_timeout(&mut executor, "next local barrier").await;
    }

    #[tokio::test]
    async fn test_snapshot_backfill_with_upstream_on_hummock() {
        let source_env = prepare_hummock_test_env().await;
        source_env.register_table(source_table_pb()).await;
        let progress_env = prepare_hummock_test_env().await;
        progress_env.register_table(progress_table_pb()).await;

        let mut source_state_table = source_state_table(source_env.storage.clone()).await;
        let source_table = source_batch_table(source_env.storage.clone());
        let progress_state_table = progress_state_table(progress_env.storage.clone()).await;

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        source_env
            .storage
            .start_epoch(epoch.curr, HashSet::from_iter([SOURCE_TABLE_ID]));
        source_state_table.init_epoch(epoch).await.unwrap();

        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[],
        )
        .await;
        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[],
        )
        .await;
        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[],
        )
        .await;
        commit_insert_epoch(
            &source_env,
            &mut source_state_table,
            &mut epoch,
            HashSet::from_iter([SOURCE_TABLE_ID]),
            &[4],
        )
        .await;
        start_progress_epochs(&progress_env, 6);

        let barrier_manager = LocalBarrierManager::for_test();
        let progress = CreateMviewProgressReporter::for_test(barrier_manager.clone());
        let actor_ctx = ActorContext::for_test(1235);
        let (barrier_tx, barrier_rx) = unbounded_channel();
        let (upstream_tx, upstream_rx) = channel_for_test();

        upstream_tx
            .send(
                DispatcherMessage::Barrier(
                    Barrier::new_test_barrier(test_epoch(5)).into_dispatcher(),
                )
                .into(),
            )
            .await
            .unwrap();
        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(1)))
            .unwrap();

        let mut executor = SnapshotBackfillExecutor::new(
            source_table,
            progress_state_table,
            Some(make_upstream_input(
                barrier_manager,
                actor_ctx.clone(),
                upstream_rx,
            )),
            None,
            vec![0],
            vec![0],
            actor_ctx,
            progress,
            1024,
            RateLimit::Disabled,
            barrier_rx,
            Arc::new(StreamingMetrics::unused()),
            Some(test_epoch(3)),
        )
        .expect("snapshot backfill executor should be created")
        .boxed()
        .execute();

        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "initial injected barrier")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(1)).epoch
        );
        expect_pending_with_timeout(&mut executor, "snapshot finish barrier 2").await;
        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(2)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "snapshot progress barrier 2")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(2)).epoch
        );

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(3)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "snapshot progress barrier 3")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(3)).epoch
        );

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(4)))
            .unwrap();
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "snapshot completion barrier 4")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(4)).epoch
        );

        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(5)))
            .unwrap();
        assert_eq!(
            expect_chunk_with_timeout(&mut executor, "log-store replay chunk").await,
            StreamChunk::from_pretty(
                " I
                + 4"
            )
        );
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "log-store completion barrier")
                .await
                .epoch,
            Barrier::new_test_barrier(test_epoch(5)).epoch
        );

        upstream_tx
            .send(DispatcherMessage::Chunk(StreamChunk::from_pretty(" I\n + 5")).into())
            .await
            .unwrap();
        let stop_barrier = Barrier::new_test_barrier(test_epoch(6)).with_stop();
        upstream_tx
            .send(DispatcherMessage::Barrier(stop_barrier.clone().into_dispatcher()).into())
            .await
            .unwrap();
        barrier_tx.send(stop_barrier.clone()).unwrap();

        assert_eq!(
            expect_chunk_with_timeout(&mut executor, "live upstream chunk after handoff").await,
            StreamChunk::from_pretty(" I\n + 5")
        );
        assert_eq!(
            expect_barrier_with_timeout(&mut executor, "final stop barrier")
                .await
                .epoch,
            stop_barrier.epoch
        );
    }
}
