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

use std::marker::PhantomData;

use anyhow::Context;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Azblob;

use super::opendal_enumerator::OpendalEnumerator;
use super::{AzblobProperties, OpendalSource};
use crate::error::ConnectorResult;
use crate::source::filesystem::s3::enumerator::get_prefix;

impl<Src: OpendalSource> OpendalEnumerator<Src> {
    /// create opendal azblob source.
    pub fn new_azblob_source(azblob_properties: AzblobProperties) -> ConnectorResult<Self> {
        // Create azblob builder.
        let mut builder = Azblob::default()
            .container(&azblob_properties.container_name)
            .endpoint(&azblob_properties.endpoint_url);

        if let Some(account_name) = azblob_properties.account_name {
            builder = builder.account_name(&account_name);
        } else {
            tracing::warn!(
                "account_name azblob is not set, container  {}",
                azblob_properties.container_name
            );
        }

        if let Some(account_key) = azblob_properties.account_key {
            builder = builder.account_key(&account_key);
        } else {
            tracing::warn!(
                "account_key azblob is not set, container  {}",
                azblob_properties.container_name
            );
        }
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default());

        let (prefix, matcher) = if let Some(pattern) = azblob_properties.match_pattern.as_ref() {
            let prefix = get_prefix(pattern);
            let matcher = glob::Pattern::new(pattern)
                .with_context(|| format!("Invalid match_pattern: {}", pattern))?;
            (Some(prefix), Some(matcher))
        } else {
            (None, None)
        };

        let compression_format = azblob_properties.compression_format;
        Ok(Self {
            op,
            prefix,
            matcher,
            marker: PhantomData,
            compression_format,
        })
    }
}
