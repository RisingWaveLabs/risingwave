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

use std::marker::PhantomData;

use anyhow::Context;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;

use super::OpendalSource;
use super::opendal_enumerator::OpendalEnumerator;
use crate::error::ConnectorResult;
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::s3::S3PropertiesCommon;
use crate::source::filesystem::s3::enumerator::get_prefix;

impl<Src: OpendalSource> OpendalEnumerator<Src> {
    /// create opendal s3 source.
    pub fn new_s3_source(
        s3_properties: &S3PropertiesCommon,
        assume_role: Option<String>,
        compression_format: CompressionFormat,
    ) -> ConnectorResult<Self> {
        // Create s3 builder.
        let mut builder = S3::default()
            .bucket(&s3_properties.bucket_name)
            .region(&s3_properties.region_name);

        if let Some(endpoint_url) = &s3_properties.endpoint_url {
            builder = builder.endpoint(endpoint_url);
        }

        if let Some(access) = &s3_properties.access {
            builder = builder.access_key_id(access);
        }

        if let Some(secret) = &s3_properties.secret {
            builder = builder.secret_access_key(secret);
        }

        if let Some(assume_role) = assume_role {
            builder = builder.role_arn(&assume_role);
        }

        let (prefix, matcher) = if let Some(pattern) = s3_properties.match_pattern.as_ref() {
            let prefix = get_prefix(pattern);
            let matcher = glob::Pattern::new(pattern)
                .with_context(|| format!("Invalid match_pattern: {}", pattern))?;
            (Some(prefix), Some(matcher))
        } else {
            (None, None)
        };

        // Default behavior is disable loading config from environment.
        if !s3_properties.enable_config_load() {
            builder = builder.disable_config_load();
        }
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default());

        Ok(Self {
            op,
            prefix,
            matcher,
            marker: PhantomData,
            compression_format,
        })
    }
}
