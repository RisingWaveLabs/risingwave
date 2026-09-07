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
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader};
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

/// The magic number at the beginning of a gzip member (RFC 1952).
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

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
                .with_guarded_label_values(&[&source_id, &source_name, &actor_id, &fragment_id]);
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
        let has_gzip_extension = object_name.ends_with(".gz") || object_name.ends_with(".gzip");
        // After a recovery occurs, for gzip-compressed files, it is necessary to read from the beginning each time,
        // other files can continue reading from the last read `start_offset`.
        let reader = match has_gzip_extension {
            true => op.read_with(&object_name).into_future().await?,

            false => {
                op.read_with(&object_name)
                    .range(start_offset as u64..)
                    .into_future()
                    .await?
            }
        };

        let mut stream_reader = StreamReader::new(reader.map_err(std::io::Error::other));

        // Whether the object is supposed to be gzip-compressed, either by the
        // explicit `compression_format` property or by its file extension.
        let expect_gzip =
            matches!(compression_format, CompressionFormat::Gzip) || has_gzip_extension;

        // Objects stored with `Content-Encoding: gzip` metadata (e.g. files delivered
        // by AWS Kinesis Data Firehose with GZIP compression enabled) may be
        // transparently decompressed by the HTTP client before reaching us, when the
        // client is built with auto-decompression support (reqwest's `gzip` feature,
        // which other dependencies enable transitively). Gzip-decoding such a payload
        // again fails with "Invalid gzip header". To be robust, peek the first two
        // bytes and only decompress when the payload actually starts with the gzip
        // magic number. Skip the sniffing when resuming a file without a gzip
        // extension from a non-zero offset, as the peeked bytes would come from the
        // middle of the file.
        let sniff_gzip_magic = expect_gzip && (has_gzip_extension || start_offset == 0);
        let mut magic = [0u8; GZIP_MAGIC.len()];
        let mut magic_len = 0;
        if sniff_gzip_magic {
            while magic_len < magic.len() {
                let n = stream_reader.read(&mut magic[magic_len..]).await?;
                if n == 0 {
                    break;
                }
                magic_len += n;
            }
        }
        let payload_is_gzip = magic[..magic_len] == GZIP_MAGIC;
        // Put the peeked bytes back in front of the remaining stream.
        let reader = std::io::Cursor::new(magic[..magic_len].to_vec()).chain(stream_reader);

        let mut buf_reader: Pin<Box<dyn AsyncBufRead + Send>> = if expect_gzip {
            if !sniff_gzip_magic || payload_is_gzip {
                Box::pin(BufReader::new(GzipDecoder::new(reader)))
                    as Pin<Box<dyn AsyncBufRead + Send>>
            } else {
                tracing::warn!(
                    source_name,
                    object_name,
                    "object is expected to be gzip-compressed but does not start with the gzip magic number, reading it as plain data; this typically happens when the object carries `Content-Encoding: gzip` metadata and has already been decompressed transparently by the HTTP client",
                );
                Box::pin(BufReader::new(reader)) as Pin<Box<dyn AsyncBufRead + Send>>
            }
        } else {
            // todo: support automatic decompression of more compression types.
            Box::pin(BufReader::new(reader)) as Pin<Box<dyn AsyncBufRead + Send>>
        };

        let mut offset = match has_gzip_extension {
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
            if has_gzip_extension && offset + n_read <= start_offset {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_compression::tokio::write::GzipEncoder;
    use futures::TryStreamExt;
    use opendal::Operator;
    use opendal::services::Memory;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::source::SourceContext;
    use crate::source::filesystem::opendal_source::OpendalS3;

    const CONTENT: &str = "line1\nline2\nline3\n";

    async fn gzipped(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(data).await.unwrap();
        encoder.shutdown().await.unwrap();
        encoder.into_inner()
    }

    async fn read_lines(
        op: &Operator,
        object_name: &str,
        compression_format: CompressionFormat,
    ) -> Vec<String> {
        let split = OpendalFsSplit::<OpendalS3>::new(object_name.to_owned(), 0, 0);
        let source_ctx = Arc::new(SourceContext::dummy());
        let metrics = source_ctx
            .metrics
            .file_source_input_row_count
            .with_guarded_label_values(&["0", "dummy", "0", "0"]);
        OpendalReader::<OpendalS3>::stream_read_lines(
            op.clone(),
            split,
            source_ctx,
            compression_format,
            metrics,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .map(|m| String::from_utf8(m.payload.unwrap()).unwrap())
        .collect()
    }

    fn expected_lines() -> Vec<String> {
        vec![
            "line1\n".to_owned(),
            "line2\n".to_owned(),
            "line3\n".to_owned(),
        ]
    }

    #[tokio::test]
    async fn test_gzip_payload_with_gzip_extension() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.gz", gzipped(CONTENT.as_bytes()).await)
            .await
            .unwrap();
        let lines = read_lines(&op, "data.gz", CompressionFormat::None).await;
        assert_eq!(lines, expected_lines());
    }

    #[tokio::test]
    async fn test_gzip_payload_with_compression_format() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.log", gzipped(CONTENT.as_bytes()).await)
            .await
            .unwrap();
        let lines = read_lines(&op, "data.log", CompressionFormat::Gzip).await;
        assert_eq!(lines, expected_lines());
    }

    /// A `.gz`-named object whose payload is actually plain data, which happens
    /// when the object carries `Content-Encoding: gzip` metadata and the HTTP
    /// client has already decompressed it transparently. This used to fail with
    /// "Invalid gzip header".
    #[tokio::test]
    async fn test_plain_payload_with_gzip_extension() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.gz", CONTENT.as_bytes()).await.unwrap();
        let lines = read_lines(&op, "data.gz", CompressionFormat::None).await;
        assert_eq!(lines, expected_lines());
    }

    #[tokio::test]
    async fn test_plain_payload_with_compression_format() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.log", CONTENT.as_bytes()).await.unwrap();
        let lines = read_lines(&op, "data.log", CompressionFormat::Gzip).await;
        assert_eq!(lines, expected_lines());
    }

    #[tokio::test]
    async fn test_plain_payload_plain_extension() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.log", CONTENT.as_bytes()).await.unwrap();
        let lines = read_lines(&op, "data.log", CompressionFormat::None).await;
        assert_eq!(lines, expected_lines());
    }

    /// An object shorter than the gzip magic number must not break the sniffing.
    #[tokio::test]
    async fn test_tiny_payload_with_gzip_extension() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.gz", "x".as_bytes()).await.unwrap();
        let lines = read_lines(&op, "data.gz", CompressionFormat::None).await;
        assert_eq!(lines, vec!["x".to_owned()]);
    }

    #[tokio::test]
    async fn test_empty_payload_with_gzip_extension() {
        let op = Operator::new(Memory::default()).unwrap();
        op.write("data.gz", Vec::<u8>::new()).await.unwrap();
        let lines = read_lines(&op, "data.gz", CompressionFormat::None).await;
        assert!(lines.is_empty());
    }
}
