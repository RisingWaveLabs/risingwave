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

use anyhow::Context;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use google_cloud_pubsub::subscription::SeekTo;
use risingwave_common::bail;

use crate::error::ConnectorResult;
use crate::source::SourceEnumeratorContextRef;
use crate::source::base::SplitEnumerator;
use crate::source::google_pubsub::PubsubProperties;
use crate::source::google_pubsub::split::PubsubSplit;

pub struct PubsubSplitEnumerator {
    subscription: String,
}

#[async_trait]
impl SplitEnumerator for PubsubSplitEnumerator {
    type Properties = PubsubProperties;
    type Split = PubsubSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<PubsubSplitEnumerator> {
        if properties.parallelism.is_some() {
            tracing::warn!(
                "pubsub.parallelism is deprecated and will be ignored. \
                 Split count now adapts automatically to the number of actors."
            );
        }

        properties.subscriber_config()?;

        let sub = properties.subscription_client().await?;
        if !sub
            .exists(None)
            .await
            .context("error checking subscription validity")?
        {
            bail!(
                "subscription {} does not exist. \
                 If not using emulator, ensure Google ADC is configured: \
                 set `pubsub.credentials` parameter, or configure GOOGLE_APPLICATION_CREDENTIALS_JSON/GOOGLE_APPLICATION_CREDENTIALS environment variables, \
                 or run on Google Cloud with appropriate service account",
                &sub.id()
            )
        }

        let seek_to = match (properties.start_offset, properties.start_snapshot) {
            (None, None) => None,
            (Some(start_offset), None) => {
                let ts = start_offset
                    .parse::<i64>()
                    .context("error parsing start_offset")
                    .map(|nanos| Utc.timestamp_nanos(nanos).into())?;
                Some(SeekTo::Timestamp(ts))
            }
            (None, Some(snapshot)) => Some(SeekTo::Snapshot(snapshot)),
            (Some(_), Some(_)) => {
                bail!("specify at most one of start_offset or start_snapshot")
            }
        };

        if let Some(seek_to) = seek_to {
            sub.seek(seek_to, None)
                .await
                .context("error seeking subscription")?;
        }

        Ok(Self {
            subscription: properties.subscription,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<PubsubSplit>> {
        tracing::debug!("enumerating pubsub splits (adaptive mode, returning 1 template split)");
        Ok(vec![PubsubSplit {
            index: 0,
            subscription: self.subscription.clone(),
            __deprecated_start_offset: None,
            __deprecated_stop_offset: None,
        }])
    }
}
