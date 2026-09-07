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

use std::future::ready;

use anyhow::anyhow;
use futures::future::{Either, select};
use futures::{FutureExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common_rate_limit::{MonitoredRateLimiter, RateLimit, RateLimiter};
use risingwave_pb::common::ThrottleType;
use risingwave_storage::StateStore;
use rw_futures_util::drop_either_future;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::backfill::snapshot_backfill::consume_upstream::stream::ConsumeUpstreamStream;
use crate::executor::backfill::snapshot_backfill::consume_upstream::upstream_table_trait::UpstreamTable;
use crate::executor::backfill::snapshot_backfill::receive_next_barrier;
use crate::executor::backfill::snapshot_backfill::state::{BackfillState, EpochBackfillProgress};
use crate::executor::backfill::utils::mapping_message;
use crate::executor::monitor::BackfillMetrics;
use crate::executor::prelude::{StateTable, *};
use crate::executor::{Barrier, Message, Mutation, StreamExecutorError};
use crate::task::CreateMviewProgressReporter;

pub struct UpstreamTableExecutor<T: UpstreamTable, S: StateStore> {
    upstream_table_id: TableId,
    upstream_table: T,
    progress_state_table: StateTable<S>,
    snapshot_epoch: u64,
    output_indices: Vec<usize>,

    chunk_size: usize,
    rate_limiter: MonitoredRateLimiter,
    actor_ctx: ActorContextRef,
    barrier_rx: UnboundedReceiver<Barrier>,
    progress: CreateMviewProgressReporter,
    crossdb_last_consumed_min_epoch: LabelGuardedIntGauge,
    metrics: BackfillMetrics,
}

impl<T: UpstreamTable, S: StateStore> UpstreamTableExecutor<T, S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        upstream_table_id: TableId,
        upstream_table: T,
        progress_state_table: StateTable<S>,
        snapshot_epoch: u64,
        output_indices: Vec<usize>,

        chunk_size: usize,
        rate_limit: RateLimit,
        actor_ctx: ActorContextRef,
        barrier_rx: UnboundedReceiver<Barrier>,
        progress: CreateMviewProgressReporter,
    ) -> Self {
        let rate_limiter = RateLimiter::new(rate_limit).monitored(upstream_table_id);
        let table_id_label = upstream_table_id.to_string();
        let actor_id_label = actor_ctx.id.to_string();
        let fragment_id_label = actor_ctx.fragment_id.to_string();
        let crossdb_last_consumed_min_epoch = actor_ctx
            .streaming_metrics
            .crossdb_last_consumed_min_epoch
            .with_guarded_label_values(&[
                table_id_label.as_str(),
                actor_id_label.as_str(),
                fragment_id_label.as_str(),
            ]);
        let metrics = actor_ctx
            .streaming_metrics
            .new_backfill_metrics(upstream_table_id, actor_ctx.id);
        Self {
            upstream_table_id,
            upstream_table,
            progress_state_table,
            snapshot_epoch,
            output_indices,
            chunk_size,
            rate_limiter,
            actor_ctx,
            barrier_rx,
            progress,
            crossdb_last_consumed_min_epoch,
            metrics,
        }
    }

    fn extract_last_consumed_min_epoch(progress_state: &BackfillState<S>) -> u64 {
        let mut min_epoch = u64::MAX;
        for (_, progress) in progress_state.latest_progress() {
            let Some(progress) = progress else {
                // If any vnode has no progress yet, report `0` explicitly to indicate the
                // progress is not fully ready, instead of hiding this state.
                return 0;
            };
            min_epoch = min_epoch.min(progress.epoch);
        }
        if min_epoch == u64::MAX { 0 } else { min_epoch }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_stream(mut self) {
        self.upstream_table
            .check_initial_vnode_bitmap(self.progress_state_table.vnodes())?;
        let first_barrier = receive_next_barrier(&mut self.barrier_rx).await?;
        let first_barrier_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier);
        let mut progress_state = BackfillState::new(
            self.progress_state_table,
            first_barrier_epoch,
            self.upstream_table.pk_serde(),
        )
        .await?;
        let mut finish_reported = false;
        let mut prev_reported_row_count = 0;
        let mut upstream_table = self.upstream_table;
        let snapshot_rebuild_interval = self
            .actor_ctx
            .config
            .developer
            .snapshot_iter_rebuild_interval();
        let mut stream = ConsumeUpstreamStream::new(
            progress_state.latest_progress(),
            &upstream_table,
            self.snapshot_epoch,
            self.chunk_size,
            self.rate_limiter.rate_limit(),
            snapshot_rebuild_interval,
        );

        'on_new_stream: loop {
            loop {
                let barrier = {
                    loop {
                        if self.rate_limiter.rate_limit().is_paused() {
                            break receive_next_barrier(&mut self.barrier_rx).await?;
                        }
                        let future1 = receive_next_barrier(&mut self.barrier_rx);
                        let future2 = stream.try_next().map(|result| {
                            result
                                .and_then(|opt| opt.ok_or_else(|| anyhow!("end of stream").into()))
                        });
                        pin_mut!(future1);
                        pin_mut!(future2);
                        match drop_either_future(select(future1, future2).await) {
                            Either::Left(Ok(barrier)) => {
                                break barrier;
                            }
                            Either::Right(Ok(chunk)) => {
                                assert!(!self.rate_limiter.rate_limit().is_paused());
                                self.rate_limiter.wait(chunk.cardinality() as _).await;
                                yield Message::Chunk(chunk);
                            }
                            Either::Left(Err(e)) | Either::Right(Err(e)) => {
                                return Err(e);
                            }
                        }
                    }
                };

                if let Some(chunk) = stream.consume_builder() {
                    self.rate_limiter.wait(chunk.cardinality() as _).await;
                    yield Message::Chunk(chunk);
                }
                stream
                    .for_vnode_pk_progress(|vnode, epoch, row_count, progress| {
                        if let Some(progress) = progress {
                            progress_state.update_epoch_progress(vnode, epoch, row_count, progress);
                        } else {
                            progress_state.finish_epoch(vnode, epoch, row_count);
                        }
                    })
                    .await?;

                let last_consumed_min_epoch =
                    Self::extract_last_consumed_min_epoch(&progress_state);
                self.crossdb_last_consumed_min_epoch
                    .set(last_consumed_min_epoch as i64);

                if !finish_reported {
                    let mut row_count = 0;
                    let mut is_finished = true;
                    for (_, progress) in progress_state.latest_progress() {
                        if let Some(progress) = progress {
                            if progress.epoch == self.snapshot_epoch {
                                if let EpochBackfillProgress::Consuming { .. } = &progress.progress
                                {
                                    is_finished = false;
                                }
                                row_count += progress.row_count;
                            }
                        } else {
                            is_finished = false;
                        }
                    }
                    // ensure that the reported row count is non-decreasing.
                    let row_count_to_report = std::cmp::max(prev_reported_row_count, row_count);
                    self.metrics
                        .backfill_snapshot_read_row_count
                        .inc_by(row_count_to_report.saturating_sub(prev_reported_row_count) as _);
                    prev_reported_row_count = row_count_to_report;

                    if is_finished {
                        self.progress
                            .finish(barrier.epoch, row_count_to_report as _);
                        finish_reported = true;
                    } else {
                        self.progress.update(
                            barrier.epoch,
                            self.snapshot_epoch,
                            row_count_to_report as _,
                        );
                    }
                }

                let post_commit = progress_state.commit(barrier.epoch).await?;
                let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                if let Some(new_rate_limit) = barrier.mutation.as_ref().and_then(|mutation| {
                    if let Mutation::Throttle(config) = &**mutation
                        && let Some(config) = config.get(&self.actor_ctx.fragment_id)
                        && config.throttle_type() == ThrottleType::Backfill
                    {
                        Some(config.rate_limit)
                    } else {
                        None
                    }
                }) {
                    let new_rate_limit = new_rate_limit.into();
                    let old_rate_limit = self.rate_limiter.update(new_rate_limit);
                    if old_rate_limit != new_rate_limit {
                        stream.update_rate_limiter(new_rate_limit);
                        tracing::info!(
                            old_rate_limit = ?old_rate_limit,
                            new_rate_limit = ?new_rate_limit,
                            upstream_table_id = %self.upstream_table_id,
                            actor_id = %self.actor_ctx.id,
                            "cross-db backfill rate limit changed",
                        );
                    }
                }
                yield Message::Barrier(barrier);
                if let Some(new_vnode_bitmap) =
                    post_commit.post_yield_barrier(update_vnode_bitmap).await?
                {
                    drop(stream);
                    upstream_table.update_vnode_bitmap(new_vnode_bitmap);
                    // recreate the stream on update vnode bitmap
                    stream = ConsumeUpstreamStream::new(
                        progress_state.latest_progress(),
                        &upstream_table,
                        self.snapshot_epoch,
                        self.chunk_size,
                        self.rate_limiter.rate_limit(),
                        snapshot_rebuild_interval,
                    );
                    continue 'on_new_stream;
                }
            }
        }
    }
}

impl<T: UpstreamTable, S: StateStore> Execute for UpstreamTableExecutor<T, S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let output_indices = self.output_indices.clone();
        self.into_stream()
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
