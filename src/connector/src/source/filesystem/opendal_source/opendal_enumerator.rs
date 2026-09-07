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
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use risingwave_common::types::Timestamptz;

use super::OpendalSource;
use crate::error::ConnectorResult;
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::{FsPageItem, OpendalFsSplit};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

#[inline]
fn to_timestamptz(ts: Option<opendal::raw::Timestamp>) -> Timestamptz {
    let system_time = ts
        .map(std::time::SystemTime::from)
        .unwrap_or(std::time::UNIX_EPOCH);
    Timestamptz::from(DateTime::<Utc>::from(system_time))
}

#[derive(Debug, Clone)]
pub struct OpendalEnumerator<Src: OpendalSource> {
    pub op: Operator,
    // prefix is used to reduce the number of objects to be listed
    pub(crate) prefix: Option<String>,
    pub(crate) matcher: Option<glob::Pattern>,
    pub(crate) marker: PhantomData<Src>,
    pub(crate) compression_format: CompressionFormat,
}

#[async_trait]
impl<Src: OpendalSource> SplitEnumerator for OpendalEnumerator<Src> {
    type Properties = Src::Properties;
    type Split = OpendalFsSplit<Src>;

    async fn new(
        properties: Src::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Src::new_enumerator(properties)
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<OpendalFsSplit<Src>>> {
        let empty_split: OpendalFsSplit<Src> = OpendalFsSplit::empty_split();
        let prefix = self.prefix.as_deref().unwrap_or("/");
        let list_prefix = Self::extract_list_prefix(prefix);

        let mut lister = self.op.lister(&list_prefix).await?;
        // fetch one item as validation, no need to get all
        lister
            .try_next()
            .await
            .context("failed to create the source; please check your configuration")?;
        Ok(vec![empty_split])
    }
}

impl<Src: OpendalSource> OpendalEnumerator<Src> {
    /// Extract the directory to list from a prefix.
    /// If prefix ends with "/", use it as-is (directory).
    /// Otherwise, extract parent directory.
    fn extract_list_prefix(prefix: &str) -> String {
        if prefix.ends_with("/") {
            prefix.to_owned()
        } else if let Some(parent_pos) = prefix.rfind('/') {
            prefix[..=parent_pos].to_owned()
        } else {
            "/".to_owned()
        }
    }

    pub async fn list(&self) -> ConnectorResult<ObjectMetadataIter> {
        let prefix = self.prefix.as_deref().unwrap_or("/");
        let list_prefix = Self::extract_list_prefix(prefix);
        let object_lister = self.op.lister_with(&list_prefix).recursive(true).await?;

        let op = self.op.clone();
        let stream = stream::unfold(object_lister, move |mut object_lister| {
            let op = op.clone();

            async move {
                match object_lister.next().await {
                    Some(Ok(object)) => {
                        let name = object.path().to_owned();

                        // OpenDAL 0.55 removed list metadata capability flags and reports
                        // unknown content length as 0. Use listed metadata first, and call
                        // stat() if timestamp is missing or size is 0 to avoid treating
                        // unknown sizes as real zero-byte objects.
                        let meta = object.metadata();
                        let mut t = meta.last_modified();
                        let mut size = meta.content_length() as i64;
                        if t.is_none() || size == 0 {
                            let stat_meta = match op
                                .stat(&name)
                                .await
                                .with_context(|| format!("failed to stat listed object {name}"))
                            {
                                Ok(stat_meta) => stat_meta,
                                Err(err) => return Some((Err(err.into()), object_lister)),
                            };
                            t = stat_meta.last_modified();
                            size = stat_meta.content_length() as i64;
                        }

                        let timestamp = to_timestamptz(t);
                        let metadata = FsPageItem {
                            name,
                            size,
                            timestamp,
                        };
                        Some((Ok(metadata), object_lister))
                    }
                    Some(Err(err)) => Some((Err(err.into()), object_lister)),
                    None => {
                        tracing::info!("finished listing objects");
                        None
                    }
                }
            }
        });

        Ok(stream.boxed())
    }

    pub fn get_matcher(&self) -> &Option<glob::Pattern> {
        &self.matcher
    }

    pub fn get_prefix(&self) -> &str {
        self.prefix.as_deref().unwrap_or("/")
    }
}
pub type ObjectMetadataIter = BoxStream<'static, ConnectorResult<FsPageItem>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::filesystem::opendal_source::OpendalS3;
    use crate::source::filesystem::s3::enumerator::get_prefix;

    fn calculate_list_prefix(prefix: &str) -> String {
        OpendalEnumerator::<OpendalS3>::extract_list_prefix(prefix)
    }

    #[test]
    fn test_prefix_logic() {
        let test_cases = vec![
            ("a/b/c/hello*/*.json", "a/b/c/hello", "a/b/c/"),
            ("a/b/c.json", "a/b/c.json", "a/b/"),
            ("a/b/c/", "a/b/c/", "a/b/c/"),
            ("a/b/c", "a/b/c", "a/b/"),
            ("file.json", "file.json", "/"),
            ("*.json", "", "/"),
            ("a/b/c/[h]ello*/*.json", "a/b/c/", "a/b/c/"),
        ];

        for (pattern, expected_prefix, expected_list_prefix) in test_cases {
            let prefix = get_prefix(pattern);
            let list_prefix = calculate_list_prefix(&prefix);

            assert_eq!(
                prefix, expected_prefix,
                "get_prefix failed for: {}",
                pattern
            );
            assert_eq!(
                list_prefix, expected_list_prefix,
                "list_prefix failed for: {}",
                pattern
            );
        }
    }

    #[test]
    fn test_bug_fix() {
        let problematic_pattern = "a/b/c/hello*/*.json";
        let prefix = get_prefix(problematic_pattern);
        let list_prefix = calculate_list_prefix(&prefix);

        // Before fix: would fallback to "/"
        // After fix: should use parent directory "a/b/c/"
        assert_eq!(prefix, "a/b/c/hello");
        assert_eq!(list_prefix, "a/b/c/");
    }
}
