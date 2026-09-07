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

use std::sync::Arc;

use anyhow::anyhow;
use futures::FutureExt;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::sink::log_store::{
    FlushCurrentEpochOptions, LogStoreResult, LogWriter, LogWriterPostFlushCurrentEpoch,
};
use risingwave_storage::store::LocalStateStore;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, watch};

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferSender;
use crate::common::log_store_impl::kv_log_store::state::LogStoreWriteState;
use crate::common::log_store_impl::kv_log_store::{
    FIRST_SEQ_ID, KvLogStoreMetrics, LogStoreVnodeProgress, ReaderTruncationOffsetType, SeqId,
};

pub struct KvLogStoreWriter<LS: LocalStateStore> {
    seq_id: SeqId,

    state: LogStoreWriteState<LS>,

    tx: LogStoreBufferSender,
    init_epoch_tx: Option<oneshot::Sender<EpochPair>>,
    update_vnode_bitmap_tx: UnboundedSender<(Arc<Bitmap>, u64)>,

    metrics: KvLogStoreMetrics,

    paused_notifier: watch::Sender<bool>,

    is_paused: bool,

    identity: String,

    last_truncate_offset: Option<ReaderTruncationOffsetType>,
}

impl<LS: LocalStateStore> KvLogStoreWriter<LS> {
    pub(crate) fn new(
        state: LogStoreWriteState<LS>,
        tx: LogStoreBufferSender,
        init_epoch_tx: oneshot::Sender<EpochPair>,
        update_vnode_bitmap_tx: UnboundedSender<(Arc<Bitmap>, u64)>,
        metrics: KvLogStoreMetrics,
        paused_notifier: watch::Sender<bool>,
        identity: String,
    ) -> Self {
        Self {
            seq_id: FIRST_SEQ_ID,
            state,
            tx,
            init_epoch_tx: Some(init_epoch_tx),
            update_vnode_bitmap_tx,
            metrics,
            paused_notifier,
            identity,
            is_paused: false,
            last_truncate_offset: None,
        }
    }
}

impl<LS: LocalStateStore> LogWriter for KvLogStoreWriter<LS> {
    async fn init(
        &mut self,
        epoch: EpochPair,
        pause_read_on_bootstrap: bool,
    ) -> LogStoreResult<()> {
        self.state.init(epoch).await?;
        if pause_read_on_bootstrap {
            self.pause()?;
            info!("KvLogStore of {} paused on bootstrap", self.identity);
        }
        self.seq_id = FIRST_SEQ_ID;
        if let Err(e) = self
            .init_epoch_tx
            .take()
            .expect("should be Some in first init")
            .send(epoch)
        {
            error!("unable to send init epoch: {:?}", e);
        }
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
        // No data is expected when the stream is paused.
        assert!(!self.is_paused);

        if chunk.cardinality() == 0 {
            return Ok(());
        }
        let epoch = self.state.epoch().curr;
        let start_seq_id = self.seq_id;
        self.seq_id += chunk.cardinality() as SeqId;
        let end_seq_id = self.seq_id - 1;
        if let Some(chunk) = self
            .tx
            .try_add_stream_chunk(epoch, chunk, start_seq_id, end_seq_id)
        {
            // When enter this branch, the chunk cannot be added directly, and should be add to
            // state store and flush
            let mut writer = self.state.start_writer(true);
            writer.write_chunk(&chunk, epoch, start_seq_id, end_seq_id)?;
            let (flush_info, vnode_bitmap) = writer.finish().await?;
            flush_info.report(&self.metrics);

            self.tx.add_flushed(
                epoch,
                start_seq_id,
                end_seq_id,
                vnode_bitmap
                    .expect("should exist since we set record_vnodes as true when start_writer"),
            );
        }
        Ok(())
    }

    async fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        options: FlushCurrentEpochOptions,
    ) -> LogStoreResult<LogWriterPostFlushCurrentEpoch<'_>> {
        let epoch = self.state.epoch().curr;
        let mut writer = self.state.start_writer(false);

        // When the stream is paused, donot flush barrier to ensure there is no dirty data in state store.
        // Besides, barrier on a paused stream is useless in log store because it won't change the log store state.
        if !self.is_paused {
            writer.write_barrier(epoch, options.is_checkpoint)?;
        }
        self.tx
            .flush_all_unflushed(|chunk, epoch, start_seq_id, end_seq_id| {
                writer.write_chunk(chunk, epoch, start_seq_id, end_seq_id)?;

                Ok(())
            })?;

        let (flush_info, _) = writer.finish().await?;

        // No data is expected when the stream is paused.
        if self.is_paused {
            assert_eq!(flush_info.flush_count, 0);
            assert_eq!(flush_info.flush_size, 0);
        }
        flush_info.report(&self.metrics);

        let should_wait_log_store_flush =
            options.schema_change.is_some() || options.wait_log_store_flush;
        // Barrier's new_vnode_bitmap field does not need to be passed to log-reader, because when sink is decoupled, we
        // always rebuild sink when update vnode bitmap.
        self.tx.barrier(
            epoch,
            options.is_checkpoint,
            next_epoch,
            options.schema_change,
            options.is_stop,
        );
        if should_wait_log_store_flush {
            let truncate_offset = self.tx.wait_for_barrier_truncation(epoch).await?;
            assert_eq!(truncate_offset, (epoch, None));
            self.last_truncate_offset = Some(truncate_offset);
        }
        if let Some(truncate_offset) = self.tx.pop_truncation(epoch) {
            self.last_truncate_offset = Some(truncate_offset);
        }
        let post_seal_epoch = self.state.seal_current_epoch(
            next_epoch,
            self.last_truncate_offset
                .map(|(epoch, seq_id)| {
                    LogStoreVnodeProgress::Aligned(self.state.vnodes().clone(), epoch, seq_id)
                })
                .unwrap_or(LogStoreVnodeProgress::None),
        );
        let update_vnode_bitmap_tx = &mut self.update_vnode_bitmap_tx;
        let tx = &mut self.tx;
        self.seq_id = FIRST_SEQ_ID;
        Ok(LogWriterPostFlushCurrentEpoch::new(move || {
            async move {
                {
                    let new_vnodes = options.new_vnode_bitmap;

                    post_seal_epoch
                        .post_yield_barrier(new_vnodes.clone())
                        .await?;
                    if let Some(new_vnodes) = new_vnodes {
                        update_vnode_bitmap_tx
                            .send((new_vnodes, next_epoch))
                            .map_err(|_| {
                                anyhow!("failed to send the updated vnode bitmap to the reader")
                            })?;
                        tx.clear();
                    }
                    Ok(())
                }
            }
            .boxed()
        }))
    }

    fn pause(&mut self) -> LogStoreResult<()> {
        info!("KvLogStore of {} is paused", self.identity);
        assert!(!self.is_paused);
        self.is_paused = true;
        self.paused_notifier
            .send(true)
            .map_err(|_| anyhow!("unable to set pause"))
    }

    fn resume(&mut self) -> LogStoreResult<()> {
        info!("KvLogStore of {} is resumed", self.identity);
        assert!(self.is_paused);
        self.is_paused = false;
        self.paused_notifier
            .send(false)
            .map_err(|_| anyhow!("unable to set resume"))
    }
}
