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

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;

use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{LogSinker, Result, SinkLogReader};

/// A sink writer that buffers rows across chunks and commits them in batches, driven by
/// [`BatchingLogSinker`].
#[async_trait]
pub trait BatchingSinkWriter: Send + 'static {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

    /// Commits buffered data if a batch is ready. Returning `true` means everything received so
    /// far is visible downstream, allowing the log store to truncate up to this point.
    async fn try_commit(&mut self) -> Result<bool>;

    /// Called at a barrier. Returns whether the barrier may be truncated, i.e. everything received
    /// so far is committed or was never buffered. Batching across barriers by returning `false`
    /// while data is pending is only safe for sinks guaranteed to run decoupled: on the in-memory
    /// log store, an untruncated checkpoint barrier blocks the checkpoint from completing. Sinks
    /// that may run non-decoupled must flush here and return `true`.
    async fn commit_on_barrier(&mut self) -> Result<bool>;
}

/// Log sinker for sinks that batch rows across chunks: an offset is truncated only once a commit
/// has made its rows visible downstream, preserving at-least-once delivery on restart.
pub struct BatchingLogSinker<W> {
    writer: W,
}

impl<W> BatchingLogSinker<W> {
    pub fn new(writer: W) -> Self {
        BatchingLogSinker { writer }
    }
}

#[async_trait]
impl<W: BatchingSinkWriter> LogSinker for BatchingLogSinker<W> {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        let mut sink_writer = self.writer;
        enum LogConsumerState {
            Uninitialized,
            EpochBegun { curr_epoch: u64 },
            BarrierReceived { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;
        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            state = match state {
                LogConsumerState::Uninitialized => {
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::EpochBegun { curr_epoch } => {
                    assert!(
                        epoch >= curr_epoch,
                        "new epoch {} should not be below the current epoch {}",
                        epoch,
                        curr_epoch
                    );
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::BarrierReceived { prev_epoch } => {
                    assert!(
                        epoch > prev_epoch,
                        "new epoch {} should be greater than prev epoch {}",
                        epoch,
                        prev_epoch
                    );
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    sink_writer.write_batch(chunk).await?;
                    if sink_writer.try_commit().await? {
                        // A chunk truncation also covers all preceding barriers.
                        log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                    }
                }
                LogStoreReadItem::Barrier { .. } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };

                    // Truncating in idle periods keeps barriers from accumulating in the log store.
                    if sink_writer.commit_on_barrier().await? {
                        log_reader.truncate(TruncateOffset::Barrier { epoch: prev_epoch })?;
                    }

                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
            }
        }
    }
}
