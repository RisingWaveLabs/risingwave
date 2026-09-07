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

use std::future::IntoFuture;
use std::pin::Pin;

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use opendal::Operator;
use prometheus::core::GenericCounter;
use risingwave_common::array::StreamChunk;
use risingwave_common::metrics::LabelGuardedMetric;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;

use super::OpendalSource;
use super::opendal_enumerator::OpendalEnumerator;
use crate::error::ConnectorResult;
use crate::parser::{
    ByteStreamSourceParserImpl, EncodingProperties, ParserConfig, into_data_chunk_stream,
};
use crate::source::filesystem::OpendalFsSplit;
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::iceberg::read_parquet_file;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SourceMessageEvent, SourceMeta,
    SplitMetaData, SplitReader,
};

#[derive(Debug, Clone)]
pub struct OpendalReader<Src: OpendalSource> {
    connector: OpendalEnumerator<Src>,
    splits: Vec<OpendalFsSplit<Src>>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
}

#[async_trait]
impl<Src: OpendalSource> SplitReader for OpendalReader<Src> {
    type Properties = Src::Properties;
    type Split = OpendalFsSplit<Src>;

    async fn new(
        properties: Src::Properties,
        splits: Vec<OpendalFsSplit<Src>>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        let connector = Src::new_enumerator(properties)?;
        let opendal_reader = OpendalReader {
            connector,
            splits,
            parser_config,
            source_ctx,
            columns,
        };
        Ok(opendal_reader)
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        self.into_stream_inner()
    }
}

impl<Src: OpendalSource> OpendalReader<Src> {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in self.splits {
            let source_ctx = self.source_ctx.clone();

            let object_name = split.name.clone();
            let actor_id = source_ctx.actor_id.to_string();
            let fragment_id = source_ctx.fragment_id.to_string();
            let source_id = source_ctx.source_id.to_string();
            let source_name = source_ctx.source_name.clone();
            let file_source_input_row_count = self
                .source_ctx
                .metrics
                .file_source_input_row_count
                .with_guarded_label_values(&[&actor_id, &source_id, &source_name, &fragment_id]);
            let chunk_stream;
            if let EncodingProperties::Parquet(parquet_props) =
                &self.parser_config.specific.encoding_config
            {
                let actor_id = source_ctx.actor_id.to_string();
                let source_id = source_ctx.source_id.to_string();
                let split_id = split.id();
                let source_name = source_ctx.source_name.clone();
                let parquet_source_skip_row_count_metrics: LabelGuardedMetric<
                    GenericCounter<prometheus::core::AtomicU64>,
                > = self
                    .source_ctx
                    .metrics
                    .parquet_source_skip_row_count
                    .with_guarded_label_values(&[
                        actor_id.as_str(),
                        source_id.as_str(),
                        &split_id,
                        source_name.as_str(),
                    ]);
                chunk_stream = read_parquet_file(
                    self.connector.op.clone(),
                    object_name.clone(),
                    self.columns.clone(),
                    Some(self.parser_config.common.rw_columns.clone()),
                    parquet_props.case_insensitive,
                    self.source_ctx.source_ctrl_opts.chunk_size,
                    split.offset,
                    Some(file_source_input_row_count.clone()),
                    Some(parquet_source_skip_row_count_metrics),
                )
                .await?;
            } else {
                assert!(
                    need_nd_streaming(&self.parser_config.specific.encoding_config),
                    "except for parquet, file source only support split by newline for now"
                );

                let line_stream = Self::stream_read_lines(
                    self.connector.op.clone(),
                    split,
                    self.source_ctx.clone(),
                    self.connector.compression_format.clone(),
                    file_source_input_row_count.clone(),
                );

                let parser =
                    ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx)
                        .await?;
                chunk_stream = Box::pin(into_data_chunk_stream(parser.parse_stream_with_events(
                    line_stream.map_ok(SourceMessageEvent::Data).boxed(),
                )));
            }

            #[for_await]
            for chunk in chunk_stream {
                yield chunk?;
            }
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn stream_read_lines(
        op: Operator,
        split: OpendalFsSplit<Src>,
        source_ctx: SourceContextRef,
        compression_format: CompressionFormat,
        file_source_input_row_count_metrics: LabelGuardedMetric<
            GenericCounter<prometheus::core::AtomicU64>,
        >,
    ) {
        let actor_id = source_ctx.actor_id.to_string();
        let fragment_id = source_ctx.fragment_id.to_string();
        let source_id = source_ctx.source_id.to_string();
        let source_name = source_ctx.source_name.clone();
        let split_id = split.id();
        let object_name = split.name.clone();
        let start_offset = split.offset;
        // After a recovery occurs, for gzip-compressed files, it is necessary to read from the beginning each time,
        // other files can continue reading from the last read `start_offset`.
        let reader = match object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
            true => op.read_with(&object_name).into_future().await?,

            false => {
                op.read_with(&object_name)
                    .range(start_offset as u64..)
                    .into_future()
                    .await?
            }
        };

        let stream_reader = StreamReader::new(reader.map_err(std::io::Error::other));

        let mut buf_reader: Pin<Box<dyn AsyncBufRead + Send>> = match compression_format {
            CompressionFormat::Gzip => {
                let gzip_decoder = GzipDecoder::new(stream_reader);
                Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncBufRead + Send>>
            }
            CompressionFormat::None => {
                // todo: support automatic decompression of more compression types.
                if object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
                    let gzip_decoder = GzipDecoder::new(stream_reader);
                    Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncBufRead + Send>>
                } else {
                    Box::pin(BufReader::new(stream_reader)) as Pin<Box<dyn AsyncBufRead + Send>>
                }
            }
        };

        let mut offset = match object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
            true => 0,
            false => start_offset,
        };
        let partition_input_bytes_metrics = source_ctx
            .metrics
            .partition_input_bytes
            .with_guarded_label_values(&[
                actor_id.as_str(),
                source_id.as_str(),
                &split_id,
                source_name.as_str(),
                fragment_id.as_str(),
            ]);

        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let mut batch = Vec::with_capacity(max_chunk_size);
        let mut line_buf = String::new();

        loop {
            let n_read = buf_reader.read_line(&mut line_buf).await?;
            if n_read == 0 {
                // EOF
                break;
            }
            let msg_offset = (offset + n_read).to_string();
            // note that the buffer contains the newline character
            debug_assert_eq!(n_read, line_buf.len());
            if (object_name.ends_with(".gz") || object_name.ends_with(".gzip"))
                && offset + n_read <= start_offset
            {
                // For gzip compressed files, the reader needs to read from the beginning each time,
                // but it needs to skip the previously read part and start yielding chunks from a position greater than or equal to start_offset.
            } else {
                batch.push(SourceMessage {
                    key: None,
                    payload: Some(std::mem::take(&mut line_buf).into_bytes()),
                    offset: msg_offset,
                    split_id: split.id(),
                    meta: SourceMeta::Empty,
                });
            }

            offset += n_read;
            partition_input_bytes_metrics.inc_by(n_read as _);

            if batch.len() >= max_chunk_size {
                file_source_input_row_count_metrics.inc_by(max_chunk_size as _);
                yield std::mem::replace(&mut batch, Vec::with_capacity(max_chunk_size));
            }
        }

        if !batch.is_empty() {
            batch.shrink_to_fit();
            file_source_input_row_count_metrics.inc_by(batch.len() as _);
            yield batch;
        }
    }
}
