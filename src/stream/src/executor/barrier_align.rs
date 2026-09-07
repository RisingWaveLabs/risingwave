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

use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use await_tree::InstrumentAwait;
use enum_as_inner::EnumAsInner;
use futures::StreamExt;
use futures::future::{Either, select};
use futures_async_stream::try_stream;
use risingwave_common::bail;

use super::error::StreamExecutorError;
use super::{Barrier, BoxedMessageStream, Message, StreamChunk, StreamExecutorResult, Watermark};
use crate::executor::monitor::StreamingMetrics;
use crate::task::{ActorId, FragmentId};

pub type AlignedMessageStreamItem = StreamExecutorResult<AlignedMessage>;
pub trait AlignedMessageStream = futures::Stream<Item = AlignedMessageStreamItem> + Send;

#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
#[derive(Debug, EnumAsInner)]
pub enum AlignedMessage {
    Barrier(Barrier),
    WatermarkLeft(Watermark),
    WatermarkRight(Watermark),
    Left(StreamChunk),
    Right(StreamChunk),
}

#[try_stream(ok = AlignedMessage, error = StreamExecutorError)]
pub async fn barrier_align(
    mut left: BoxedMessageStream,
    mut right: BoxedMessageStream,
    actor_id: ActorId,
    fragment_id: FragmentId,
    metrics: Arc<StreamingMetrics>,
    executor_name: &str,
) {
    let actor_id = actor_id.to_string();
    let fragment_id = fragment_id.to_string();
    let left_barrier_align_duration = metrics.barrier_align_duration.with_guarded_label_values(&[
        actor_id.as_str(),
        fragment_id.as_str(),
        "left",
        executor_name,
    ]);
    let right_barrier_align_duration = metrics.barrier_align_duration.with_guarded_label_values(&[
        actor_id.as_str(),
        fragment_id.as_str(),
        "right",
        executor_name,
    ]);
    loop {
        let prefer_left: bool = rand::random();
        let select_result = if prefer_left {
            select(left.next(), right.next()).await
        } else {
            match select(right.next(), left.next()).await {
                Either::Left(x) => Either::Right(x),
                Either::Right(x) => Either::Left(x),
            }
        };
        match select_result {
            Either::Left((None, _)) => {
                // left stream end, passthrough right chunks
                while let Some(msg) = right.next().await {
                    match msg? {
                        Message::Watermark(watermark) => {
                            yield AlignedMessage::WatermarkRight(watermark)
                        }
                        Message::Chunk(chunk) => yield AlignedMessage::Right(chunk),
                        Message::Barrier(_) => {
                            bail!("right barrier received while left stream end");
                        }
                    }
                }
                break;
            }
            Either::Right((None, _)) => {
                // right stream end, passthrough left chunks
                while let Some(msg) = left.next().await {
                    match msg? {
                        Message::Watermark(watermark) => {
                            yield AlignedMessage::WatermarkLeft(watermark)
                        }
                        Message::Chunk(chunk) => yield AlignedMessage::Left(chunk),
                        Message::Barrier(_) => {
                            bail!("left barrier received while right stream end");
                        }
                    }
                }
                break;
            }
            Either::Left((Some(msg), _)) => match msg? {
                Message::Watermark(watermark) => yield AlignedMessage::WatermarkLeft(watermark),
                Message::Chunk(chunk) => yield AlignedMessage::Left(chunk),
                Message::Barrier(barrier) => loop {
                    let start_time = Instant::now();
                    // received left barrier, waiting for right barrier
                    match right
                        .next()
                        .instrument_await(await_tree::span!(
                            "barrier_align_wait_right epoch={}",
                            barrier.epoch.curr
                        ))
                        .await
                        .context("failed to poll right message, stream closed unexpectedly")??
                    {
                        Message::Watermark(watermark) => {
                            yield AlignedMessage::WatermarkRight(watermark)
                        }
                        Message::Chunk(chunk) => yield AlignedMessage::Right(chunk),
                        Message::Barrier(barrier) => {
                            yield AlignedMessage::Barrier(barrier);
                            right_barrier_align_duration
                                .inc_by(start_time.elapsed().as_nanos() as u64);
                            break;
                        }
                    }
                },
            },
            Either::Right((Some(msg), _)) => match msg? {
                Message::Watermark(watermark) => yield AlignedMessage::WatermarkRight(watermark),
                Message::Chunk(chunk) => yield AlignedMessage::Right(chunk),
                Message::Barrier(barrier) => loop {
                    let start_time = Instant::now();
                    // received right barrier, waiting for left barrier
                    match left
                        .next()
                        .instrument_await(await_tree::span!(
                            "barrier_align_wait_left epoch={}",
                            barrier.epoch.curr
                        ))
                        .await
                        .context("failed to poll left message, stream closed unexpectedly")??
                    {
                        Message::Watermark(watermark) => {
                            yield AlignedMessage::WatermarkLeft(watermark)
                        }
                        Message::Chunk(chunk) => yield AlignedMessage::Left(chunk),
                        Message::Barrier(barrier) => {
                            yield AlignedMessage::Barrier(barrier);
                            left_barrier_align_duration
                                .inc_by(start_time.elapsed().as_nanos() as u64);
                            break;
                        }
                    }
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_stream::try_stream;
    use futures::{Stream, TryStreamExt};
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use tokio::time::sleep;

    use super::*;

    fn barrier_align_for_test(
        left: BoxedMessageStream,
        right: BoxedMessageStream,
    ) -> impl Stream<Item = Result<AlignedMessage, StreamExecutorError>> {
        barrier_align(
            left,
            right,
            0.into(),
            0.into(),
            Arc::new(StreamingMetrics::unused()),
            "dummy_executor",
        )
    }

    #[tokio::test]
    async fn test_barrier_align() {
        let left = try_stream! {
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
            yield Message::Barrier(Barrier::new_test_barrier(test_epoch(1)));
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 2"));
            yield Message::Barrier(Barrier::new_test_barrier(test_epoch(2)));
        }
        .boxed();
        let right = try_stream! {
            sleep(Duration::from_millis(1)).await;
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
            yield Message::Barrier(Barrier::new_test_barrier(test_epoch(1)));
            yield Message::Barrier(Barrier::new_test_barrier(test_epoch(2)));
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 3"));
        }
        .boxed();
        let output: Vec<_> = barrier_align_for_test(left, right)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            output,
            vec![
                AlignedMessage::Left(StreamChunk::from_pretty("I\n + 1")),
                AlignedMessage::Right(StreamChunk::from_pretty("I\n + 1")),
                AlignedMessage::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                AlignedMessage::Left(StreamChunk::from_pretty("I\n + 2")),
                AlignedMessage::Barrier(Barrier::new_test_barrier(2 * test_epoch(1))),
                AlignedMessage::Right(StreamChunk::from_pretty("I\n + 3")),
            ]
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn left_barrier_right_end_1() {
        let left = try_stream! {
            sleep(Duration::from_millis(1)).await;
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
            yield Message::Barrier(Barrier::new_test_barrier(test_epoch(1)));
        }
        .boxed();
        let right = try_stream! {
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
        }
        .boxed();
        let _output: Vec<_> = barrier_align_for_test(left, right)
            .try_collect()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn left_barrier_right_end_2() {
        let left = try_stream! {
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
            yield Message::Barrier(Barrier::new_test_barrier(test_epoch(1)));
        }
        .boxed();
        let right = try_stream! {
            sleep(Duration::from_millis(1)).await;
            yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
        }
        .boxed();
        let _output: Vec<_> = barrier_align_for_test(left, right)
            .try_collect()
            .await
            .unwrap();
    }
}
