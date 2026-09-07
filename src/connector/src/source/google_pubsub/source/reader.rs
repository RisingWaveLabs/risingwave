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

use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use anyhow::Context;
use async_trait::async_trait;
use futures::{FutureExt, Stream, StreamExt};
use futures_async_stream::try_stream;
use google_cloud_pubsub::subscriber::SubscriberConfig;
use google_cloud_pubsub::subscription::{MessageStream, SubscribeConfig, Subscription};
use risingwave_common::ensure;

use super::TaggedReceivedMessage;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::google_pubsub::{PubsubProperties, PubsubSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

const MAX_BATCH_SIZE: usize = 1024;

/// Wrapper around [`MessageStream`] that calls [`MessageStream::dispose()`] on drop
/// instead of the default `Drop` impl which has a race condition: `MessageStream::drop()`
/// spawns a task to drain buffered messages, but `Subscriber::drop()` immediately aborts
/// the receive task, causing in-flight messages to be lost without being nacked back to
/// the subscription.
struct DisposableMessageStream(Option<MessageStream>);

impl Drop for DisposableMessageStream {
    fn drop(&mut self) {
        #[cfg(not(madsim))]
        if let Some(stream) = self.0.take() {
            tokio::spawn(async move {
                let count = stream.dispose().await;
                if count > 0 {
                    tracing::info!(
                        "disposed pubsub streaming pull, nacked {} in-flight messages",
                        count
                    );
                }
            });
        }
    }
}

impl Stream for DisposableMessageStream {
    type Item = <MessageStream as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match self.0.as_mut() {
            Some(inner) => Pin::new(inner).poll_next(cx),
            None => Poll::Ready(None),
        }
    }
}

pub struct PubsubSplitReader {
    subscription: Subscription,
    subscriber_config: SubscriberConfig,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl PubsubSplitReader {
    fn build_subscribe_config(&self) -> SubscribeConfig {
        SubscribeConfig::default().with_subscriber_config(self.subscriber_config.clone())
    }

    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        loop {
            let subscribe_config = self.build_subscribe_config();
            let raw_stream = self
                .subscription
                .subscribe(Some(subscribe_config))
                .await
                .context("failed to subscribe")?;
            let mut stream = DisposableMessageStream(Some(raw_stream));

            while let Some(first) = stream.next().await {
                let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
                batch.push(SourceMessage::from(TaggedReceivedMessage(
                    self.split_id.clone(),
                    first,
                )));

                // Drain remaining ready messages without blocking
                while batch.len() < MAX_BATCH_SIZE {
                    match stream.next().now_or_never() {
                        Some(Some(msg)) => {
                            batch.push(SourceMessage::from(TaggedReceivedMessage(
                                self.split_id.clone(),
                                msg,
                            )));
                        }
                        _ => break,
                    }
                }

                yield batch;
            }

            // Stream ended (all subscribers stopped). Reconnect.
            tracing::warn!("pubsub streaming pull ended, reconnecting...");
        }
    }
}

#[async_trait]
impl SplitReader for PubsubSplitReader {
    type Properties = PubsubProperties;
    type Split = PubsubSplit;

    async fn new(
        properties: PubsubProperties,
        splits: Vec<PubsubSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        ensure!(
            splits.len() == 1,
            "the pubsub reader only supports a single split"
        );
        let split = splits.into_iter().next().unwrap();

        let subscriber_config = properties.subscriber_config()?;
        let subscription = properties.subscription_client().await?;

        Ok(Self {
            subscription,
            subscriber_config,
            split_id: split.id(),
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}
