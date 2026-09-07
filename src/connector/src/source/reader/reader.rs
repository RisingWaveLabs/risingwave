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

use std::sync::Arc;

use anyhow::Context;
use async_nats::jetstream::consumer::AckPolicy;
use futures::StreamExt;
use futures::stream::pending;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::ColumnId;
use thiserror_ext::AsReport as _;

use crate::WithOptionsSecResolved;
use crate::error::ConnectorResult;
use crate::parser::{CommonParserConfig, ParserConfig, SpecificParserConfig};
use crate::source::filesystem::opendal_source::opendal_enumerator::OpendalEnumerator;
use crate::source::filesystem::opendal_source::{
    DEFAULT_REFRESH_INTERVAL_SEC, OpendalAzblob, OpendalGcs, OpendalPosixFs, OpendalS3,
    OpendalSource,
};
use crate::source::filesystem::{FsPageItem, OpendalFsSplit};
use crate::source::{
    BoxSourceReaderEventStream, BoxTryStream, Column, ConnectorProperties, ConnectorState,
    CreateSplitReaderOpt, CreateSplitReaderResult, SourceColumnDesc, SourceContext,
    WaitCheckpointTask,
};

#[derive(Clone, Debug)]
pub struct SourceReader {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub parser_config: SpecificParserConfig,
    pub connector_message_buffer_size: usize,
}

fn needs_commit_cdc_offset_after_checkpoint(config: &ConnectorProperties) -> bool {
    matches!(
        config,
        ConnectorProperties::PostgresCdc(_) | ConnectorProperties::OracleCdc(_)
    )
}

impl SourceReader {
    pub fn new(
        properties: WithOptionsSecResolved,
        columns: Vec<SourceColumnDesc>,
        connector_message_buffer_size: usize,
        parser_config: SpecificParserConfig,
    ) -> ConnectorResult<Self> {
        let config = ConnectorProperties::extract(properties, false)?;

        Ok(Self {
            config,
            columns,
            parser_config,
            connector_message_buffer_size,
        })
    }

    fn get_target_columns(
        &self,
        column_ids: Vec<ColumnId>,
    ) -> ConnectorResult<Vec<SourceColumnDesc>> {
        column_ids
            .iter()
            .map(|id| {
                self.columns
                    .iter()
                    .find(|c| c.column_id == *id)
                    .with_context(|| {
                        format!("Failed to find column id: {} in source: {:?}", id, self)
                    })
                    .cloned()
            })
            .try_collect()
            .map_err(Into::into)
    }

    pub fn get_source_list(&self) -> ConnectorResult<BoxTryStream<FsPageItem>> {
        let config = self.config.clone();
        let list_interval_sec: u64;
        let get_list_interval_sec =
            |interval: Option<u64>| -> u64 { interval.unwrap_or(DEFAULT_REFRESH_INTERVAL_SEC) };
        match config {
            ConnectorProperties::Gcs(prop) => {
                list_interval_sec = get_list_interval_sec(prop.fs_common.refresh_interval_sec);
                let lister: OpendalEnumerator<OpendalGcs> =
                    OpendalEnumerator::new_gcs_source(*prop)?;
                Ok(build_opendal_fs_list_stream(lister, list_interval_sec))
            }
            ConnectorProperties::OpendalS3(prop) => {
                list_interval_sec = get_list_interval_sec(prop.fs_common.refresh_interval_sec);
                let lister: OpendalEnumerator<OpendalS3> = OpendalEnumerator::new_s3_source(
                    &prop.s3_properties,
                    prop.assume_role,
                    prop.fs_common.compression_format,
                )?;
                Ok(build_opendal_fs_list_stream(lister, list_interval_sec))
            }
            ConnectorProperties::Azblob(prop) => {
                list_interval_sec = get_list_interval_sec(prop.fs_common.refresh_interval_sec);
                let lister: OpendalEnumerator<OpendalAzblob> =
                    OpendalEnumerator::new_azblob_source(*prop)?;
                Ok(build_opendal_fs_list_stream(lister, list_interval_sec))
            }
            ConnectorProperties::PosixFs(prop) => {
                list_interval_sec = get_list_interval_sec(prop.fs_common.refresh_interval_sec);
                let lister: OpendalEnumerator<OpendalPosixFs> =
                    OpendalEnumerator::new_posix_fs_source(*prop)?;
                Ok(build_opendal_fs_list_stream(lister, list_interval_sec))
            }
            other => bail!("Unsupported source: {:?}", other),
        }
    }

    /// Refer to `WaitCheckpointWorker` for more details.
    pub async fn create_wait_checkpoint_task(&self) -> ConnectorResult<Option<WaitCheckpointTask>> {
        if needs_commit_cdc_offset_after_checkpoint(&self.config) {
            return Ok(Some(WaitCheckpointTask::CommitCdcOffset(None)));
        }
        Ok(match &self.config {
            ConnectorProperties::GooglePubsub(prop) => Some(WaitCheckpointTask::AckPubsubMessage(
                prop.subscription_client().await?,
                vec![],
            )),
            ConnectorProperties::Nats(prop) => {
                match prop.nats_properties_consumer.get_ack_policy()? {
                    a @ AckPolicy::Explicit | a @ AckPolicy::All => {
                        Some(WaitCheckpointTask::AckNatsJetStream(
                            prop.common.build_context().await?,
                            vec![],
                            a,
                        ))
                    }
                    AckPolicy::None | AckPolicy::FlowControl => None,
                }
            }
            ConnectorProperties::Pulsar(_) => Some(WaitCheckpointTask::AckPulsarMessage(vec![])),
            _ => None,
        })
    }

    /// Build `SplitReader`s and then `BoxSourceReaderEventStream` from the given `ConnectorState` (`SplitImpl`s).
    ///
    /// If `seek_to_latest` is true, will also return the latest splits after seek.
    pub async fn build_stream(
        &self,
        state: ConnectorState,
        column_ids: Vec<ColumnId>,
        source_ctx: Arc<SourceContext>,
        seek_to_latest: bool,
    ) -> ConnectorResult<(BoxSourceReaderEventStream, CreateSplitReaderResult)> {
        let Some(splits) = state else {
            return Ok((pending().boxed(), Default::default()));
        };
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;

        let data_gen_columns = Some(
            columns
                .iter()
                .map(|col| Column {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    is_visible: col.is_visible(),
                })
                .collect_vec(),
        );

        let parser_config = ParserConfig {
            specific: self.parser_config.clone(),
            common: CommonParserConfig {
                rw_columns: columns,
            },
        };

        config
            .create_split_reader(
                splits,
                parser_config,
                source_ctx,
                data_gen_columns,
                CreateSplitReaderOpt {
                    seek_to_latest,
                    ..Default::default()
                },
            )
            .await
    }
}

#[try_stream(boxed, ok = FsPageItem, error = crate::error::ConnectorError)]
async fn build_opendal_fs_list_stream<Src: OpendalSource>(
    lister: OpendalEnumerator<Src>,
    list_interval_sec: u64,
) {
    loop {
        let matcher = lister.get_matcher();
        let mut object_metadata_iter = lister.list().await?;
        while let Some(list_res) = object_metadata_iter.next().await {
            match list_res {
                Ok(res) => {
                    if matcher
                        .as_ref()
                        .map(|m| m.matches(&res.name) || m.to_string() == res.name)
                        .unwrap_or(true)
                    {
                        yield res
                    } else {
                        continue;
                    }
                }
                Err(err) => {
                    tracing::error!(error = %err.as_report(), "list object fail");
                    return Err(err);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(list_interval_sec)).await;
    }
}

#[try_stream(boxed, ok = OpendalFsSplit<Src>,  error = crate::error::ConnectorError)]
pub async fn build_opendal_fs_list_for_batch<Src: OpendalSource>(lister: OpendalEnumerator<Src>) {
    let matcher = lister.get_matcher();
    let mut object_metadata_iter = lister.list().await?;

    while let Some(list_res) = object_metadata_iter.next().await {
        match list_res {
            Ok(res) => {
                if matcher
                    .as_ref()
                    .map(|m| m.matches(&res.name))
                    .unwrap_or(true)
                {
                    let split = OpendalFsSplit::new(res.name, 0, res.size as usize);
                    yield split
                } else {
                    continue;
                }
            }
            Err(err) => {
                tracing::error!(error = %err.as_report(), "list object fail");
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_offsets_are_committed_after_checkpoint() {
        assert!(needs_commit_cdc_offset_after_checkpoint(
            &ConnectorProperties::OracleCdc(Box::default())
        ));
        assert!(needs_commit_cdc_offset_after_checkpoint(
            &ConnectorProperties::PostgresCdc(Box::default())
        ));
        assert!(!needs_commit_cdc_offset_after_checkpoint(
            &ConnectorProperties::MysqlCdc(Box::default())
        ));
    }
}
