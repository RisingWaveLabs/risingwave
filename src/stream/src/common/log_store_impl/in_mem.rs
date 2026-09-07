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

use anyhow::{Context, anyhow};
use await_tree::InstrumentAwait;
use futures::FutureExt;
use futures::future::BoxFuture;
use risingwave_common::array::StreamChunk;
use risingwave_common::util::epoch::{EpochExt, EpochPair, INVALID_EPOCH};
use risingwave_connector::sink::log_store::{
    FlushCurrentEpochOptions, LogReader, LogStoreFactory, LogStoreReadItem, LogStoreResult,
    LogWriter, LogWriterPostFlushCurrentEpoch, TruncateBarrierLogReader, TruncateOffset,
};
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
};
use tokio::sync::oneshot;

use crate::common::log_store_impl::in_mem::LogReaderEpochProgress::{AwaitingTruncate, Consuming};
use crate::executor::StreamExecutorResult;

enum InMemLogStoreItem {
    StreamChunk(StreamChunk),
    Barrier {
        next_epoch: u64,
        options: FlushCurrentEpochOptions,
    },
}

/// An in-memory log store that can buffer a bounded amount of stream chunk in memory via bounded
/// mpsc channel.
///
/// Since it is in-memory, when `flush_current_epoch` with checkpoint epoch, it should wait for the
/// reader to finish consuming all the data in current checkpoint epoch.
pub struct BoundedInMemLogStoreWriter {
    /// Current epoch. Should be `Some` after `init`
    curr_epoch: Option<u64>,

    /// Holder of oneshot channel to send the initial epoch to the associated log reader.
    init_epoch_tx: Option<oneshot::Sender<u64>>,

    /// Sending log store item to log reader
    item_tx: Sender<InMemLogStoreItem>,

    /// Receiver for the epoch consumed by log reader.
    truncated_epoch_rx: UnboundedReceiver<u64>,

    wait_init_epoch: Option<WaitInitEpochFn>,
}

#[derive(Eq, PartialEq, Debug)]
enum LogReaderEpochProgress {
    /// In progress of consuming data in current epoch.
    Consuming(u64),
    /// Finished emitting the data in checkpoint epoch, and waiting for a call on `truncate`.
    AwaitingTruncate { sealed_epoch: u64, next_epoch: u64 },
}

const UNINITIALIZED: LogReaderEpochProgress = LogReaderEpochProgress::Consuming(INVALID_EPOCH);

pub struct BoundedInMemLogStoreReader {
    /// Current progress of log reader. Can be either consuming an epoch, or has finished consuming
    /// an epoch and waiting to be truncated.
    epoch_progress: LogReaderEpochProgress,

    /// Holder for oneshot channel to receive the initial epoch
    init_epoch_rx: Option<oneshot::Receiver<u64>>,

    /// Receiver to fetch log store item
    item_rx: Receiver<InMemLogStoreItem>,

    /// Sender of consumed epoch to the log writer
    truncated_epoch_tx: UnboundedSender<u64>,

    /// Offset of the latest emitted item
    latest_offset: TruncateOffset,

    /// Offset of the latest truncated item
    truncate_offset: TruncateOffset,
}

type WaitInitEpochFn =
    Box<dyn FnOnce(EpochPair) -> BoxFuture<'static, StreamExecutorResult<()>> + Send + 'static>;

pub struct BoundedInMemLogStoreFactory {
    bound: usize,
    wait_init_epoch: WaitInitEpochFn,
}

impl BoundedInMemLogStoreFactory {
    pub fn new(
        bound: usize,
        wait_init_epoch: impl FnOnce(EpochPair) -> BoxFuture<'static, StreamExecutorResult<()>>
        + Send
        + 'static,
    ) -> Self {
        Self {
            bound,
            wait_init_epoch: Box::new(wait_init_epoch),
        }
    }

    #[cfg(test)]
    pub fn for_test(bound: usize) -> Self {
        Self {
            bound,
            wait_init_epoch: Box::new(|_x| std::future::ready(Ok(())).boxed()),
        }
    }
}

impl LogStoreFactory for BoundedInMemLogStoreFactory {
    type Reader = TruncateBarrierLogReader<BoundedInMemLogStoreReader>;
    type Writer = BoundedInMemLogStoreWriter;

    const ALLOW_REWIND: bool = false;
    const REBUILD_SINK_ON_UPDATE_VNODE_BITMAP: bool = false;

    async fn build(self) -> (Self::Reader, Self::Writer) {
        let (init_epoch_tx, init_epoch_rx) = oneshot::channel();
        let (item_tx, item_rx) = channel(self.bound);
        let (truncated_epoch_tx, truncated_epoch_rx) = unbounded_channel();
        let reader = BoundedInMemLogStoreReader {
            epoch_progress: UNINITIALIZED,
            init_epoch_rx: Some(init_epoch_rx),
            item_rx,
            truncated_epoch_tx,
            latest_offset: TruncateOffset::Barrier { epoch: 0 },
            truncate_offset: TruncateOffset::Barrier { epoch: 0 },
        };
        let writer = BoundedInMemLogStoreWriter {
            curr_epoch: None,
            init_epoch_tx: Some(init_epoch_tx),
            item_tx,
            truncated_epoch_rx,
            wait_init_epoch: Some(self.wait_init_epoch),
        };
        (TruncateBarrierLogReader::new(reader), writer)
    }
}

impl LogReader for BoundedInMemLogStoreReader {
    async fn init(&mut self) -> LogStoreResult<()> {
        let init_epoch_rx = self
            .init_epoch_rx
            .take()
            .expect("should not init for twice");
        let epoch = init_epoch_rx.await.context("unable to get init epoch")?;
        assert_eq!(self.epoch_progress, UNINITIALIZED);
        self.epoch_progress = LogReaderEpochProgress::Consuming(epoch);
        self.latest_offset = TruncateOffset::Barrier {
            epoch: epoch.prev_epoch(),
        };
        self.truncate_offset = TruncateOffset::Barrier {
            epoch: epoch.prev_epoch(),
        };
        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        match self.epoch_progress {
            Consuming(current_epoch) => match self.item_rx.recv().await {
                Some(item) => match item {
                    InMemLogStoreItem::StreamChunk(chunk) => {
                        let chunk_id = match self.latest_offset {
                            TruncateOffset::Chunk { epoch, chunk_id } => {
                                assert_eq!(epoch, current_epoch);
                                chunk_id + 1
                            }
                            TruncateOffset::Barrier { epoch } => {
                                assert!(
                                    epoch < current_epoch,
                                    "prev offset at barrier {} but current epoch {}",
                                    epoch,
                                    current_epoch
                                );
                                0
                            }
                        };
                        self.latest_offset = TruncateOffset::Chunk {
                            epoch: current_epoch,
                            chunk_id,
                        };
                        Ok((
                            current_epoch,
                            LogStoreReadItem::StreamChunk { chunk, chunk_id },
                        ))
                    }
                    InMemLogStoreItem::Barrier {
                        next_epoch,
                        options,
                    } => {
                        if options.is_checkpoint {
                            self.epoch_progress = AwaitingTruncate {
                                next_epoch,
                                sealed_epoch: current_epoch,
                            };
                        } else {
                            self.epoch_progress = Consuming(next_epoch);
                        }
                        self.latest_offset = TruncateOffset::Barrier {
                            epoch: current_epoch,
                        };
                        Ok((
                            current_epoch,
                            LogStoreReadItem::Barrier {
                                is_checkpoint: options.is_checkpoint,
                                new_vnode_bitmap: options.new_vnode_bitmap,
                                is_stop: options.is_stop,
                                schema_change: options.schema_change,
                            },
                        ))
                    }
                },
                None => Err(anyhow!("end of log stream")),
            },
            AwaitingTruncate { .. } => std::future::pending().await,
        }
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        // check the truncate offset is higher than prev truncate offset
        if self.truncate_offset >= offset {
            return Err(anyhow!(
                "truncate offset {:?} but prev truncate offset is {:?}",
                offset,
                self.truncate_offset
            ));
        }

        // check the truncate offset does not exceed the latest possible offset
        if offset > self.latest_offset {
            return Err(anyhow!(
                "truncate at {:?} but latest offset is {:?}",
                offset,
                self.latest_offset
            ));
        }

        if let AwaitingTruncate {
            sealed_epoch,
            next_epoch,
        } = &self.epoch_progress
            && let TruncateOffset::Barrier { epoch } = offset
            && epoch == *sealed_epoch
        {
            let sealed_epoch = *sealed_epoch;
            self.epoch_progress = Consuming(*next_epoch);
            self.truncated_epoch_tx
                .send(sealed_epoch)
                .map_err(|_| anyhow!("unable to send sealed epoch"))?;
        }
        self.truncate_offset = offset;
        Ok(())
    }

    async fn rewind(&mut self) -> LogStoreResult<()> {
        Err(anyhow!("should not call rewind on it"))
    }

    async fn start_from(&mut self, _start_offset: Option<u64>) -> LogStoreResult<()> {
        Ok(())
    }
}

impl LogWriter for BoundedInMemLogStoreWriter {
    async fn init(
        &mut self,
        epoch: EpochPair,
        _pause_read_on_bootstrap: bool,
    ) -> LogStoreResult<()> {
        let init_epoch_tx = self.init_epoch_tx.take().expect("cannot be init for twice");
        self.wait_init_epoch
            .take()
            .expect("cannot be init for in-mem log store")(epoch)
        .await?;
        init_epoch_tx
            .send(epoch.curr)
            .map_err(|_| anyhow!("unable to send init epoch"))?;
        self.curr_epoch = Some(epoch.curr);
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
        self.item_tx
            .send(InMemLogStoreItem::StreamChunk(chunk))
            .instrument_await("in_mem_send_item_chunk")
            .await
            .map_err(|_| anyhow!("unable to send stream chunk"))?;
        Ok(())
    }

    async fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        options: FlushCurrentEpochOptions,
    ) -> LogStoreResult<LogWriterPostFlushCurrentEpoch<'_>> {
        let is_checkpoint = options.is_checkpoint;
        self.item_tx
            .send(InMemLogStoreItem::Barrier {
                next_epoch,
                options,
            })
            .instrument_await("in_mem_send_item_barrier")
            .await
            .map_err(|_| anyhow!("unable to send barrier"))?;

        let prev_epoch = self
            .curr_epoch
            .replace(next_epoch)
            .expect("should have epoch");

        if is_checkpoint {
            let truncated_epoch = self
                .truncated_epoch_rx
                .recv()
                .instrument_await("in_mem_recv_truncated_epoch")
                .await
                .ok_or_else(|| anyhow!("cannot get truncated epoch"))?;
            assert_eq!(truncated_epoch, prev_epoch);
        }

        Ok(LogWriterPostFlushCurrentEpoch::new(move || {
            async move { Ok(()) }.boxed()
        }))
    }

    fn pause(&mut self) -> LogStoreResult<()> {
        // no-op when decouple is not enabled
        Ok(())
    }

    fn resume(&mut self) -> LogStoreResult<()> {
        // no-op when decouple is not enabled
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::task::Poll;

    use futures::FutureExt;
    use risingwave_common::array::{Op, StreamChunkBuilder};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_connector::sink::log_store::{
        LogReader, LogStoreFactory, LogStoreReadItem, LogWriter, TruncateOffset,
    };

    use crate::common::log_store_impl::in_mem::BoundedInMemLogStoreFactory;
    use crate::common::log_store_impl::kv_log_store::test_utils::LogWriterTestExt;

    #[tokio::test]
    async fn test_in_memory_log_store() {
        let factory = BoundedInMemLogStoreFactory::for_test(4);
        let (mut reader, mut writer) = factory.build().await;

        let init_epoch = test_epoch(1);
        let epoch1 = test_epoch(2);
        let epoch2 = test_epoch(3);

        let ops = vec![Op::Insert, Op::Delete, Op::UpdateInsert, Op::UpdateDelete];
        let mut builder =
            StreamChunkBuilder::unlimited(vec![DataType::Int64, DataType::Varchar], None);
        for (i, op) in ops.into_iter().enumerate() {
            assert!(
                builder
                    .append_row(
                        op,
                        [
                            Some(ScalarImpl::Int64(i as i64)),
                            Some(ScalarImpl::Utf8(format!("name_{}", i).into_boxed_str()))
                        ]
                    )
                    .is_none()
            );
        }
        let stream_chunk = builder.take().unwrap();
        let stream_chunk_clone = stream_chunk.clone();

        let mut join_handle = tokio::spawn(async move {
            writer
                .init(EpochPair::new_test_epoch(init_epoch), false)
                .await
                .unwrap();
            writer
                .write_chunk(stream_chunk_clone.clone())
                .await
                .unwrap();
            writer
                .write_chunk(stream_chunk_clone.clone())
                .await
                .unwrap();
            writer
                .flush_current_epoch_for_test(epoch1, false)
                .await
                .unwrap();
            writer.write_chunk(stream_chunk_clone).await.unwrap();
            writer
                .flush_current_epoch_for_test(epoch2, true)
                .await
                .unwrap();
        });

        reader.init().await.unwrap();
        let _chunk_id1_1 = match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, chunk_id }) => {
                assert_eq!(epoch, init_epoch);
                assert_eq!(&chunk, &stream_chunk);
                chunk_id
            }
            _ => unreachable!(),
        };

        let chunk_id1_2 = match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, chunk_id }) => {
                assert_eq!(epoch, init_epoch);
                assert_eq!(&chunk, &stream_chunk);
                chunk_id
            }
            _ => unreachable!(),
        };

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert!(!is_checkpoint);
                assert_eq!(epoch, init_epoch);
            }
            _ => unreachable!(),
        }

        let chunk_id2_1 = match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, chunk_id }) => {
                assert_eq!(&chunk, &stream_chunk);
                assert_eq!(epoch, epoch1);
                chunk_id
            }
            _ => unreachable!(),
        };

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert!(is_checkpoint);
                assert_eq!(epoch, epoch1);
            }
            _ => unreachable!(),
        }

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: init_epoch,
                chunk_id: chunk_id1_2,
            })
            .unwrap();
        assert!(
            poll_fn(|cx| Poll::Ready(join_handle.poll_unpin(cx)))
                .await
                .is_pending()
        );
        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch1,
                chunk_id: chunk_id2_1,
            })
            .unwrap();
        assert!(
            poll_fn(|cx| Poll::Ready(join_handle.poll_unpin(cx)))
                .await
                .is_pending()
        );
        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();
        join_handle.await.unwrap();
    }
}
