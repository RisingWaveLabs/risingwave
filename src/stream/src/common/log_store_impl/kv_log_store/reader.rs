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

use std::future::Future;
use std::mem::replace;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use foyer::Hint;
use futures::future::{BoxFuture, try_join_all};
use futures::{FutureExt, TryFutureExt};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common::util::epoch::{EpochExt, EpochPair};
use risingwave_connector::sink::log_store::{
    ChunkId, LogReader, LogStoreReadItem, LogStoreResult, TruncateOffset,
};
use risingwave_hummock_sdk::key::prefixed_range_with_vnode;
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::store::timeout_auto_rebuild::{
    TimeoutAutoRebuildIter, iter_with_timeout_rebuild,
};
use risingwave_storage::store::{PrefetchOptions, ReadOptions, StateStoreRead};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tokio::time::sleep;
use tokio_stream::StreamExt;

use crate::common::log_store_impl::kv_log_store::buffer::{
    LogStoreBufferItem, LogStoreBufferReceiver,
};
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, merge_log_store_item_stream,
};
use crate::common::log_store_impl::kv_log_store::state::LogStoreReadState;
use crate::common::log_store_impl::kv_log_store::{
    Epoch, KvLogStoreMetrics, KvLogStoreReadMetrics, SeqId,
};

pub(crate) const REWIND_INITIAL_DELAY: Duration = Duration::from_secs(1);
pub(crate) const REWIND_BACKOFF_MULTIPLIER: u64 = 2;
pub(crate) const REWIND_MAX_DELAY: Duration = Duration::from_secs(180);

mod rewind_backoff_policy {
    use std::time::Duration;

    use risingwave_common::util::retry::exponential_backoff;

    use crate::common::log_store_impl::kv_log_store::{
        REWIND_BACKOFF_MULTIPLIER, REWIND_INITIAL_DELAY, REWIND_MAX_DELAY,
    };

    pub(super) type RewindBackoffPolicy = impl Iterator<Item = Duration>;

    #[define_opaque(RewindBackoffPolicy)]
    pub(super) fn initial_rewind_backoff_policy() -> RewindBackoffPolicy {
        exponential_backoff(
            REWIND_INITIAL_DELAY,
            REWIND_BACKOFF_MULTIPLIER,
            REWIND_MAX_DELAY,
        )
        .map(tokio_retry::strategy::jitter)
    }
}

use rewind_backoff_policy::*;
use risingwave_common::must_match;

struct RewindDelay {
    last_rewind_truncate_offset: Option<TruncateOffset>,
    backoff_policy: RewindBackoffPolicy,
    rewind_count: LabelGuardedIntCounter,
    rewind_delay: LabelGuardedHistogram,
}

impl RewindDelay {
    fn new(metrics: &KvLogStoreMetrics) -> Self {
        Self {
            last_rewind_truncate_offset: None,
            backoff_policy: initial_rewind_backoff_policy(),
            rewind_count: metrics.rewind_count.clone(),
            rewind_delay: metrics.rewind_delay.clone(),
        }
    }

    async fn rewind_delay(&mut self, truncate_offset: Option<TruncateOffset>) {
        match (&self.last_rewind_truncate_offset, &truncate_offset) {
            (Some(prev_rewind_truncate_offset), Some(truncate_offset)) => {
                if truncate_offset > prev_rewind_truncate_offset {
                    self.last_rewind_truncate_offset = Some(*truncate_offset);
                    // Have new truncate progress before this round of rewind.
                    // Reset rewind backoff
                    self.backoff_policy = initial_rewind_backoff_policy();
                }
            }
            (None, _) => {
                self.last_rewind_truncate_offset = truncate_offset;
            }
            _ => {}
        };
        self.rewind_count.inc();
        if let Some(delay) = self.backoff_policy.next() {
            self.rewind_delay.observe(delay.as_secs_f64());
            if !cfg!(test) {
                sleep(delay).await;
            }
        }
    }
}

enum KvLogStoreReaderFutureState<S: StateStoreRead> {
    /// consuming historical log data
    ReadStateStoreStream(
        Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>,
        /// Held permit limiting concurrent historical reads. Automatically released
        /// when this variant is replaced (i.e. stream fully consumed or reset).
        Option<OwnedSemaphorePermit>,
    ),
    ReadFlushedChunk(BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, u64)>>),
    Reset,
    Empty,
}

impl<S: StateStoreRead> KvLogStoreReaderFutureState<S> {
    async fn set_and_drive_future<F: Future + Unpin>(
        &mut self,
        future_state: Self,
        get_future: impl FnOnce(&mut Self) -> &mut F,
    ) -> F::Output {
        // Store the future in case that in the subsequent pending await point,
        // the future is cancelled, and we lose an item.
        must_match!(replace(self, future_state),
                    KvLogStoreReaderFutureState::Empty => {});

        // for cancellation test
        #[cfg(test)]
        {
            sleep(Duration::from_secs(1)).await;
        }

        let output = get_future(self).await;
        *self = KvLogStoreReaderFutureState::Empty;
        output
    }
}

macro_rules! set_and_drive_future {
    ($future_state:expr, $item_name:ident, $future:expr) => {
        $future_state.set_and_drive_future(
            KvLogStoreReaderFutureState::$item_name($future),
            |future_state| match future_state {
                KvLogStoreReaderFutureState::$item_name(future) => future,
                _ => {
                    unreachable!()
                }
            },
        )
    };
}

pub struct KvLogStoreReader<S: StateStoreRead> {
    state: LogStoreReadState<S>,

    rx: LogStoreBufferReceiver,
    init_epoch_rx: Option<oneshot::Receiver<EpochPair>>,
    update_vnode_bitmap_rx: UnboundedReceiver<(Arc<Bitmap>, u64)>,

    /// The first epoch that newly written by the log writer
    first_write_epoch: Option<u64>,

    future_state: KvLogStoreReaderFutureState<S>,

    latest_offset: Option<TruncateOffset>,

    truncate_offset: Option<TruncateOffset>,

    metrics: KvLogStoreMetrics,

    is_paused: watch::Receiver<bool>,

    identity: String,

    rewind_delay: RewindDelay,

    /// Shared semaphore to limit concurrent historical reads across the compute node.
    /// `None` means unlimited.
    historical_read_semaphore: Option<Arc<Semaphore>>,
}

impl<S: StateStoreRead> KvLogStoreReader<S> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: LogStoreReadState<S>,
        rx: LogStoreBufferReceiver,
        init_epoch_rx: oneshot::Receiver<EpochPair>,
        update_vnode_bitmap_rx: UnboundedReceiver<(Arc<Bitmap>, u64)>,
        metrics: KvLogStoreMetrics,
        is_paused: watch::Receiver<bool>,
        identity: String,
        historical_read_semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        let rewind_delay = RewindDelay::new(&metrics);
        Self {
            state,
            rx,
            init_epoch_rx: Some(init_epoch_rx),
            update_vnode_bitmap_rx,
            first_write_epoch: None,
            future_state: KvLogStoreReaderFutureState::Empty,
            latest_offset: None,
            truncate_offset: None,
            metrics,
            is_paused,
            identity,
            rewind_delay,
            historical_read_semaphore,
        }
    }
}
impl<S: StateStoreRead> KvLogStoreReader<S> {
    fn read_persisted_log_store(
        &self,
        range_start: LogStoreReadStateStreamRangeStart,
    ) -> impl Future<
        Output = LogStoreResult<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>,
    > + Send {
        self.state.read_persisted_log_store(
            self.metrics.persistent_log_read_metrics.clone(),
            self.first_write_epoch.expect("should have init"),
            range_start,
        )
    }
}

impl<S: StateStoreRead> LogReader for KvLogStoreReader<S> {
    async fn start_from(&mut self, start_offset: Option<u64>) -> LogStoreResult<()> {
        // init or rewind must be executed before start_from.
        let KvLogStoreReaderFutureState::Reset = &self.future_state else {
            panic!("future state is not Reset");
        };

        // Construct the log reader's read stream based on start_offset, aligned_range_start or persisted_epoch.
        let range_start = match start_offset {
            Some(rewind_start_offset) => {
                tracing::info!(
                    "Sink error occurred. Rebuild the log reader stream from the rewind start offset returned by the coordinator."
                );
                LogStoreReadStateStreamRangeStart::LastPersistedEpoch(rewind_start_offset)
            }
            None => {
                // still consuming persisted state store data
                let persisted_epoch =
                    self.truncate_offset
                        .map(|truncate_offset| match truncate_offset {
                            TruncateOffset::Chunk { epoch, .. } => epoch.prev_epoch(),
                            TruncateOffset::Barrier { epoch } => epoch,
                        });

                match persisted_epoch {
                    Some(last_persisted_epoch) => {
                        LogStoreReadStateStreamRangeStart::LastPersistedEpoch(last_persisted_epoch)
                    }
                    None => LogStoreReadStateStreamRangeStart::Unbounded,
                }
            }
        };
        self.future_state =
            if let LogStoreReadStateStreamRangeStart::LastPersistedEpoch(persisted_epoch) =
                range_start
                && persisted_epoch >= self.first_write_epoch.unwrap()
            {
                KvLogStoreReaderFutureState::Empty
            } else {
                // Acquire a permit before starting to read historical data.
                // This limits the number of concurrent historical reads across
                // the compute node, preventing excessive state store I/O during
                // recovery or restart.
                //
                // Deadlock safety: The writer side of this log store runs as a
                // separate future in a `select` with this consumer. Even if this
                // consumer is blocked waiting for a permit, the writer can still
                // make progress processing barriers. The log store buffer is also
                // unbounded on the writer side, so the writer won't be blocked by
                // the reader not consuming. Therefore, blocking here does not
                // prevent barrier flow.
                let permit = if let Some(semaphore) = &self.historical_read_semaphore {
                    Some(
                        semaphore
                            .clone()
                            .acquire_owned()
                            .instrument_await("Wait for Historical Read Permit")
                            .await
                            .map_err(|_| anyhow!("historical read semaphore closed"))?,
                    )
                } else {
                    None
                };
                KvLogStoreReaderFutureState::ReadStateStoreStream(
                    self.read_persisted_log_store(range_start).await?,
                    permit,
                )
            };
        self.rx.rewind(start_offset);
        Ok(())
    }

    async fn init(&mut self) -> LogStoreResult<()> {
        if let Some(init_epoch_rx) = self.init_epoch_rx.take() {
            let init_epoch = init_epoch_rx
                .await
                .map_err(|_| anyhow!("should get the first epoch"))?;
            let first_write_epoch = init_epoch.curr;

            assert_eq!(
                self.first_write_epoch.replace(first_write_epoch),
                None,
                "should not init twice"
            );
        } else {
            let (new_vnode_bitmap, write_epoch) = self
                .update_vnode_bitmap_rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("failed to receive update vnode"))?;
            self.state.serde.update_vnode_bitmap(new_vnode_bitmap);
            self.first_write_epoch = Some(write_epoch);
        };

        self.future_state = KvLogStoreReaderFutureState::Reset;
        self.latest_offset = None;
        self.truncate_offset = None;
        self.rewind_delay = RewindDelay::new(&self.metrics);

        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        while *self.is_paused.borrow_and_update() {
            info!("next_item of {} get blocked by is_pause", self.identity);
            self.is_paused
                .changed()
                .instrument_await("Wait for Pause Resume")
                .await
                .map_err(|_| anyhow!("unable to subscribe resume"))?;
        }
        match &mut self.future_state {
            KvLogStoreReaderFutureState::ReadStateStoreStream(state_store_stream, _permit) => {
                match state_store_stream
                    .try_next()
                    .instrument_await("Try Next for Historical Stream")
                    .await?
                {
                    Some((epoch, item)) => {
                        if let Some(latest_offset) = &self.latest_offset {
                            latest_offset.check_next_item_epoch(epoch)?;
                        }
                        let item = match item {
                            KvLogStoreItem::StreamChunk { chunk, .. } => {
                                let chunk_id = if let Some(latest_offset) = self.latest_offset {
                                    latest_offset.next_chunk_id()
                                } else {
                                    0
                                };
                                self.latest_offset =
                                    Some(TruncateOffset::Chunk { epoch, chunk_id });
                                LogStoreReadItem::StreamChunk { chunk, chunk_id }
                            }
                            KvLogStoreItem::Barrier { is_checkpoint, .. } => {
                                self.latest_offset = Some(TruncateOffset::Barrier { epoch });
                                LogStoreReadItem::Barrier {
                                    is_checkpoint,
                                    new_vnode_bitmap: None,
                                    is_stop: false,
                                    schema_change: None,
                                }
                            }
                        };
                        return Ok((epoch, item));
                    }
                    None => {
                        // Historical stream fully consumed. Permit is dropped with the variant.
                        self.future_state = KvLogStoreReaderFutureState::Empty;
                    }
                }
            }
            KvLogStoreReaderFutureState::ReadFlushedChunk(future) => {
                let (chunk_id, chunk, item_epoch) = future.await?;
                self.future_state = KvLogStoreReaderFutureState::Empty;
                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                if let Some(latest_offset) = &self.latest_offset {
                    assert!(offset > *latest_offset);
                }
                self.latest_offset = Some(offset);
                return Ok((
                    item_epoch,
                    LogStoreReadItem::StreamChunk { chunk, chunk_id },
                ));
            }
            KvLogStoreReaderFutureState::Empty => {}
            KvLogStoreReaderFutureState::Reset => {
                unreachable!("Must call log_reader.start_from() for a Reset reader.")
            }
        }

        // Now the historical state store has been consumed.
        let (item_epoch, item) = self
            .rx
            .next_item()
            .instrument_await("Wait Next Item from Buffer")
            .await;
        if let Some(latest_offset) = &self.latest_offset {
            latest_offset.check_next_item_epoch(item_epoch)?;
        }
        Ok(match item {
            LogStoreBufferItem::StreamChunk {
                chunk, chunk_id, ..
            } => {
                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                if let Some(latest_offset) = &self.latest_offset {
                    assert!(offset > *latest_offset);
                }
                self.latest_offset = Some(offset);
                (
                    item_epoch,
                    LogStoreReadItem::StreamChunk { chunk, chunk_id },
                )
            }
            LogStoreBufferItem::Flushed {
                vnode_bitmap,
                start_seq_id,
                end_seq_id,
                chunk_id,
            } => {
                let read_flushed_chunk_future = {
                    let read_metrics = self.metrics.flushed_buffer_read_metrics.clone();
                    self.state
                        .read_flushed_chunk(
                            vnode_bitmap,
                            chunk_id,
                            start_seq_id,
                            end_seq_id,
                            item_epoch,
                            read_metrics,
                        )
                        .boxed()
                };

                let (_, chunk, _) = set_and_drive_future!(
                    &mut self.future_state,
                    ReadFlushedChunk,
                    read_flushed_chunk_future
                )
                .await?;

                let offset = TruncateOffset::Chunk {
                    epoch: item_epoch,
                    chunk_id,
                };
                if let Some(latest_offset) = &self.latest_offset {
                    assert!(offset > *latest_offset);
                }
                self.latest_offset = Some(offset);
                (
                    item_epoch,
                    LogStoreReadItem::StreamChunk { chunk, chunk_id },
                )
            }
            LogStoreBufferItem::Barrier {
                is_checkpoint,
                next_epoch,
                schema_change,
                is_stop,
            } => {
                assert!(
                    item_epoch < next_epoch,
                    "next epoch {} should be greater than current epoch {}",
                    next_epoch,
                    item_epoch
                );
                self.latest_offset = Some(TruncateOffset::Barrier { epoch: item_epoch });
                (
                    item_epoch,
                    LogStoreReadItem::Barrier {
                        is_checkpoint,
                        new_vnode_bitmap: None,
                        is_stop,
                        schema_change,
                    },
                )
            }
        })
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        if offset > self.latest_offset.expect("should exist before truncation") {
            return Err(anyhow!(
                "truncate at a later offset {:?} than the current latest offset {:?}",
                offset,
                self.latest_offset
            ));
        }
        if offset.epoch() >= self.first_write_epoch.expect("should have init") {
            if let Some(truncate_offset) = &self.truncate_offset
                && offset <= *truncate_offset
            {
                return Err(anyhow!(
                    "truncate offset {:?} earlier than prev truncate offset {:?}",
                    offset,
                    truncate_offset
                ));
            }
            self.rx.truncate_buffer(offset);
            self.truncate_offset = Some(offset);
        } else {
            // For historical data, no need to truncate at seq id level. Only truncate at barrier.
            if let TruncateOffset::Barrier { epoch } = &offset {
                if let Some(truncate_offset) = &self.truncate_offset
                    && offset <= *truncate_offset
                {
                    return Err(anyhow!(
                        "truncate offset {:?} earlier than prev truncate offset {:?}",
                        offset,
                        truncate_offset
                    ));
                }
                self.rx.truncate_historical(*epoch);
                self.truncate_offset = Some(offset);
            }
        }
        Ok(())
    }

    async fn rewind(&mut self) -> LogStoreResult<()> {
        self.rewind_delay.rewind_delay(self.truncate_offset).await;
        self.latest_offset = None;
        self.future_state = KvLogStoreReaderFutureState::Reset;
        Ok(())
    }
}

impl<S: StateStoreRead> LogStoreReadState<S> {
    pub(crate) fn read_flushed_chunk(
        &self,
        vnode_bitmap: Bitmap,
        chunk_id: ChunkId,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        item_epoch: u64,
        read_metrics: KvLogStoreReadMetrics,
    ) -> impl Future<Output = LogStoreResult<(ChunkId, StreamChunk, Epoch)>> + 'static {
        let state_store = self.state_store.clone();
        let serde = self.serde.clone();
        async move {
            tracing::trace!(
                start_seq_id,
                end_seq_id,
                chunk_id,
                item_epoch,
                "reading flushed chunk"
            );
            let iters = try_join_all(vnode_bitmap.iter_vnodes().map(|vnode| {
                let range_start =
                    serde.serialize_log_store_pk(vnode, item_epoch, Some(start_seq_id));
                let range_end = serde.serialize_log_store_pk(vnode, item_epoch, Some(end_seq_id));
                let state_store = &state_store;

                // Use MAX EPOCH here because the epoch to consume may be below the safe
                // epoch
                async move {
                    let iter = state_store
                        .iter(
                            (Included(range_start), Included(range_end)),
                            ReadOptions {
                                prefetch_options: PrefetchOptions::prefetch_for_large_range_scan(),
                                cache_policy: CachePolicy::Fill(Hint::Low),
                                ..Default::default()
                            },
                        )
                        .await?;
                    Ok::<_, anyhow::Error>((vnode, iter))
                }
            }))
            .instrument_await("Wait Create Iter Stream")
            .await?;

            let chunk = serde
                .deserialize_stream_chunk(
                    iters,
                    start_seq_id,
                    end_seq_id,
                    item_epoch,
                    &read_metrics,
                )
                .instrument_await("Deserialize Stream Chunk")
                .await?;

            Ok((chunk_id, chunk, item_epoch))
        }
        .instrument_await("Read Flushed Chunk")
    }
}

#[derive(Clone)]
pub(crate) enum LogStoreReadStateStreamRangeStart {
    Unbounded,
    LastPersistedEpoch(u64),
}

impl<S: StateStoreRead> LogStoreReadState<S> {
    pub(crate) fn read_persisted_log_store(
        &self,
        read_metrics: KvLogStoreReadMetrics,
        first_write_epoch: u64,
        range_start: LogStoreReadStateStreamRangeStart,
    ) -> impl Future<
        Output = LogStoreResult<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>,
    > + Send
    + 'static {
        let serde = self.serde.clone();
        let range_start = match range_start {
            LogStoreReadStateStreamRangeStart::Unbounded => Unbounded,
            LogStoreReadStateStreamRangeStart::LastPersistedEpoch(last_persisted_epoch) => {
                // start from the next epoch of last_persisted_epoch
                Included(serde.serialize_pk_epoch_prefix(last_persisted_epoch + 1))
            }
        };
        let range_end = serde.serialize_pk_epoch_prefix(first_write_epoch);

        let state_store = self.state_store.clone();
        let table_id = self.table_id;
        let streams_future = try_join_all(self.serde.vnodes().iter_vnodes().map(move |vnode| {
            let key_range = prefixed_range_with_vnode(
                (range_start.clone(), Excluded(range_end.clone())),
                vnode,
            );
            let state_store = state_store.clone();
            async move {
                // rebuild the iter every 10 minutes to avoid pinning hummock version for too long
                iter_with_timeout_rebuild(
                    state_store,
                    key_range,
                    table_id,
                    ReadOptions {
                        // This stream lives too long, the connection of prefetch object may break. So use a short connection prefetch.
                        prefetch_options: PrefetchOptions::prefetch_for_small_range_scan(),
                        cache_policy: CachePolicy::Fill(Hint::Low),
                        ..Default::default()
                    },
                    Duration::from_secs(10 * 60),
                )
                .await
                .map(|iter| (vnode, iter))
            }
        }));

        let chunk_size = self.chunk_size;
        streams_future.map_err(Into::into).map_ok(move |streams| {
            // TODO: set chunk size by config
            Box::pin(merge_log_store_item_stream(
                streams,
                serde,
                chunk_size,
                read_metrics,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::FutureExt;
    use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_connector::sink::log_store::{LogReader, LogStoreReadItem};
    use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
    use risingwave_storage::error::StorageResult;
    use risingwave_storage::store::{
        KeyValueFn, ReadOptions, StateStoreGet, StateStoreIter, StateStoreKeyedRowRef,
        StateStoreRead, StorageFuture,
    };
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::{Semaphore, oneshot, watch};
    use tokio::time::timeout;

    use super::KvLogStoreReader;
    use crate::common::log_store_impl::kv_log_store::buffer::new_log_store_buffer;
    use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
    use crate::common::log_store_impl::kv_log_store::state::LogStoreReadState;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        TEST_TABLE_ID, check_stream_chunk_eq, gen_stream_chunk_with_info, gen_test_log_store_table,
    };
    use crate::common::log_store_impl::kv_log_store::{
        KV_LOG_STORE_V2_INFO, KvLogStoreMetrics, SeqId,
    };

    struct ControlledIterPlan {
        release_rx: watch::Receiver<bool>,
    }

    struct ControlledIterHandle {
        release_tx: watch::Sender<bool>,
    }

    impl ControlledIterHandle {
        fn release(&self) {
            self.release_tx.send_replace(true);
        }
    }

    struct ControlledIter {
        release_rx: watch::Receiver<bool>,
        finished: bool,
    }

    impl StateStoreIter for ControlledIter {
        async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
            if self.finished {
                return Ok(None);
            }
            if !*self.release_rx.borrow() {
                self.release_rx
                    .changed()
                    .await
                    .expect("release sender should stay alive during the test");
            }
            self.finished = true;
            Ok(None)
        }
    }

    #[derive(Clone, Default)]
    struct MockStateStore {
        iter_plans: Arc<Mutex<VecDeque<ControlledIterPlan>>>,
    }

    impl MockStateStore {
        fn push_blocking_iter(&self) -> ControlledIterHandle {
            let (release_tx, release_rx) = watch::channel(false);
            self.iter_plans
                .lock()
                .expect("poisoned iter plans")
                .push_back(ControlledIterPlan { release_rx });
            ControlledIterHandle { release_tx }
        }
    }

    impl StateStoreGet for MockStateStore {
        fn on_key_value<'a, O: Send + 'a>(
            &'a self,
            _key: TableKey<Bytes>,
            _read_options: ReadOptions,
            _on_key_value_fn: impl KeyValueFn<'a, O>,
        ) -> impl StorageFuture<'a, Option<O>> {
            async { Ok(None) }
        }
    }

    impl StateStoreRead for MockStateStore {
        type Iter = ControlledIter;
        type RevIter = ControlledIter;

        fn iter(
            &self,
            _key_range: TableKeyRange,
            _read_options: ReadOptions,
        ) -> impl StorageFuture<'_, Self::Iter> {
            let plan = self
                .iter_plans
                .lock()
                .expect("poisoned iter plans")
                .pop_front()
                .expect("missing iter plan for test");
            async move {
                Ok(ControlledIter {
                    release_rx: plan.release_rx,
                    finished: false,
                })
            }
        }

        fn rev_iter(
            &self,
            _key_range: TableKeyRange,
            _read_options: ReadOptions,
        ) -> impl StorageFuture<'_, Self::RevIter> {
            async { unreachable!("rev_iter should not be called in this test") }
        }
    }

    fn single_vnode_bitmap() -> Arc<Bitmap> {
        let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
        builder.set(0, true);
        Arc::new(builder.finish())
    }

    fn new_reader(
        state_store: MockStateStore,
        semaphore: Arc<Semaphore>,
        identity: &str,
    ) -> (
        KvLogStoreReader<MockStateStore>,
        crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferSender,
    ) {
        let epoch = test_epoch(1);
        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);
        let vnodes = single_vnode_bitmap();
        let serde = LogStoreRowSerde::new(&table, Some(vnodes), pk_info);
        let read_state = LogStoreReadState {
            table_id: TEST_TABLE_ID,
            state_store: Arc::new(state_store),
            serde,
            chunk_size: 1024,
        };
        let (tx, rx) = new_log_store_buffer(1024, 1024, KvLogStoreMetrics::for_test());
        let (init_epoch_tx, init_epoch_rx) = oneshot::channel();
        let (_update_vnode_bitmap_tx, update_vnode_bitmap_rx) = unbounded_channel();
        let (_pause_tx, pause_rx) = watch::channel(false);
        init_epoch_tx
            .send(EpochPair::new_test_epoch(epoch))
            .expect("init epoch send should succeed");
        (
            KvLogStoreReader::new(
                read_state,
                rx,
                init_epoch_rx,
                update_vnode_bitmap_rx,
                KvLogStoreMetrics::for_test(),
                pause_rx,
                identity.to_owned(),
                Some(semaphore),
            ),
            tx,
        )
    }

    #[tokio::test]
    async fn test_historical_read_init_concurrency_limit_does_not_block_writer_buffer() {
        let mock_store = MockStateStore::default();
        let iter1 = mock_store.push_blocking_iter();
        let iter2 = mock_store.push_blocking_iter();
        let semaphore = Arc::new(Semaphore::new(1));

        let (mut reader1, tx1) = new_reader(mock_store.clone(), semaphore.clone(), "reader1");
        let (mut reader2, tx2) = new_reader(mock_store, semaphore, "reader2");

        reader1.init().await.unwrap();
        reader2.init().await.unwrap();
        reader1.start_from(None).await.unwrap();

        let mut start_reader2 = Box::pin(reader2.start_from(None));
        assert!(
            start_reader2.as_mut().now_or_never().is_none(),
            "second reader should wait for the historical read permit"
        );

        let epoch = test_epoch(1);
        let next_epoch = test_epoch(2);
        let expected_chunk = gen_stream_chunk_with_info(0, &KV_LOG_STORE_V2_INFO);
        let chunk_end_seq_id = expected_chunk.cardinality() as SeqId - 1;
        assert!(
            tx2.try_add_stream_chunk(epoch, expected_chunk.clone(), 0, chunk_end_seq_id)
                .is_none()
        );
        tx2.barrier(epoch, true, next_epoch, None, false);

        tx1.barrier(epoch, true, next_epoch, None, false);
        iter1.release();

        let (_, item) = reader1.next_item().await.unwrap();
        assert!(matches!(
            item,
            LogStoreReadItem::Barrier {
                is_checkpoint: true,
                ..
            }
        ));

        timeout(Duration::from_secs(1), start_reader2)
            .await
            .expect("second reader should start once the permit is released")
            .unwrap();

        iter2.release();

        let (_, item) = reader2.next_item().await.unwrap();
        let LogStoreReadItem::StreamChunk { chunk, chunk_id } = item else {
            panic!("expected stream chunk");
        };
        assert_eq!(chunk_id, 0);
        assert!(check_stream_chunk_eq(&expected_chunk, &chunk));

        let (_, item) = reader2.next_item().await.unwrap();
        assert!(matches!(
            item,
            LogStoreReadItem::Barrier {
                is_checkpoint: true,
                ..
            }
        ));
    }
}
