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

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::Deref;
use std::pin::pin;
use std::time::Instant;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use await_tree::{InstrumentAwait, span};
use futures::TryStreamExt;
use futures::future::select;
use phf::phf_set;
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::global_jvm::Jvm;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::types::DataType;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_jni_core::{
    JniReceiverType, JniSenderType, JniSinkWriterStreamRequest, call_static_method, gen_class_name,
};
use risingwave_pb::connector_service::sink_coordinator_stream_request::StartCoordinator;
use risingwave_pb::connector_service::sink_writer_stream_request::{
    Request as SinkRequest, StartSink,
};
use risingwave_pb::connector_service::{
    PbSinkParam, SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse, SinkMetadata,
    SinkWriterStreamRequest, SinkWriterStreamResponse, TableSchema, ValidateSinkRequest,
    ValidateSinkResponse, sink_coordinator_stream_request, sink_coordinator_stream_response,
    sink_writer_stream_response,
};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::{
    BidiStreamReceiver, BidiStreamSender, DEFAULT_BUFFER_SIZE, SinkCoordinatorStreamHandle,
    SinkWriterStreamHandle,
};
use rw_futures_util::drop_either_future;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, UnboundedSender, unbounded_channel};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use super::elasticsearch_opensearch::elasticsearch_converter::{
    StreamChunkConverter, is_remote_es_sink,
};
use super::elasticsearch_opensearch::elasticsearch_opensearch_config::ES_OPTION_DELIMITER;
use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::log_store::{LogStoreReadItem, LogStoreResult, TruncateOffset};
use crate::sink::writer::SinkWriter;
use crate::sink::{
    LogSinker, Result, SinglePhaseCommitCoordinator, Sink, SinkCommitCoordinator, SinkError,
    SinkLogReader, SinkParam, SinkWriterMetrics, SinkWriterParam,
};

macro_rules! def_remote_sink {
    () => {
        def_remote_sink! {
            //todo!, delete java impl
            // { ElasticSearchJava, ElasticSearchJavaSink, "elasticsearch_v1" }
            // { OpensearchJava, OpenSearchJavaSink, "opensearch_v1"}
            { Cassandra, CassandraSink, "cassandra", [ "cassandra.url" ] }
            { Jdbc, JdbcSink, "jdbc", [ "jdbc.url" ] }
        }
    };
    ({ $variant_name:ident, $sink_type_name:ident, $sink_name:expr, [ $($enforce_secret_prop:expr),* ] }) => {
        #[derive(Debug)]
        pub struct $variant_name;
        impl RemoteSinkTrait for $variant_name {
            const SINK_NAME: &'static str = $sink_name;
        }
        impl EnforceSecret for $variant_name {
            const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf_set! {
                $($enforce_secret_prop),*
            };
        }
        pub type $sink_type_name = RemoteSink<$variant_name>;
    };
    ({ $variant_name:ident, $sink_type_name:ident, $sink_name:expr, [ $($enforce_secret_prop:expr),* ], |$desc:ident| $body:expr }) => {
        #[derive(Debug)]
        pub struct $variant_name;
        impl RemoteSinkTrait for $variant_name {
            const SINK_NAME: &'static str = $sink_name;
            fn default_sink_decouple($desc: &SinkDesc) -> bool {
                $body
            }
        }
        impl EnforceSecret for $variant_name {
            const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf_set! {
                $($enforce_secret_prop),*
            };
        }
        pub type $sink_type_name = RemoteSink<$variant_name>;
    };
    ({ $($first:tt)+ } $({$($rest:tt)+})*) => {
        def_remote_sink! {
            {$($first)+}
        }
        def_remote_sink! {
            $({$($rest)+})*
        }
    };
    ($($invalid:tt)*) => {
        compile_error! {concat! {"invalid `", stringify!{$($invalid)*}, "`"}}
    }
}

def_remote_sink!();

pub trait RemoteSinkTrait: EnforceSecret + Send + Sync + 'static {
    const SINK_NAME: &'static str;
    fn default_sink_decouple() -> bool {
        true
    }
}

#[derive(Debug)]
pub struct RemoteSink<R: RemoteSinkTrait> {
    param: SinkParam,
    _phantom: PhantomData<R>,
}

impl<R: RemoteSinkTrait> EnforceSecret for RemoteSink<R> {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = R::ENFORCE_SECRET_PROPERTIES;
}

impl<R: RemoteSinkTrait> TryFrom<SinkParam> for RemoteSink<R> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            param,
            _phantom: PhantomData,
        })
    }
}

impl<R: RemoteSinkTrait> Sink for RemoteSink<R> {
    type LogSinker = RemoteLogSinker;

    const SINK_NAME: &'static str = R::SINK_NAME;

    fn is_sink_decouple(user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default => Ok(R::default_sink_decouple()),
            SinkDecouple::Enable => Ok(true),
            SinkDecouple::Disable => Ok(false),
        }
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        RemoteLogSinker::new(self.param.clone(), writer_param, Self::SINK_NAME).await
    }

    async fn validate(&self) -> Result<()> {
        validate_remote_sink(&self.param, Self::SINK_NAME).await?;
        Ok(())
    }
}

async fn validate_remote_sink(param: &SinkParam, sink_name: &str) -> ConnectorResult<()> {
    // if sink_name == OpenSearchJavaSink::SINK_NAME {
    //     risingwave_common::license::Feature::OpenSearchSink
    //         .check_available()
    //         .map_err(|e| anyhow::anyhow!(e))?;
    // }
    if is_remote_es_sink(sink_name)
        && param.downstream_pk_or_empty().len() > 1
        && !param.properties.contains_key(ES_OPTION_DELIMITER)
    {
        bail!("Es sink only supports single pk or pk with delimiter option");
    }
    // FIXME: support struct and array in stream sink
    param.columns.iter().try_for_each(|col| {
        match &col.data_type {
            DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Decimal
                    | DataType::Timestamp
                    | DataType::Timestamptz
                    | DataType::Varchar
                    | DataType::Date
                    | DataType::Time
                    | DataType::Interval
                    | DataType::Jsonb
                    | DataType::Bytea => Ok(()),
            DataType::List(list) => {
                if is_remote_es_sink(sink_name) || matches!(list.elem(), DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 | DataType::Varchar){
                    Ok(())
                } else{
                    Err(SinkError::Remote(anyhow!(
                        "Remote sink only supports list<int16, int32, int64, float, double, varchar>, got {:?}: {:?}",
                        col.name,
                        col.data_type,
                    )))
                }
            },
            DataType::Struct(_) => {
                if is_remote_es_sink(sink_name){
                    Ok(())
                }else{
                    Err(SinkError::Remote(anyhow!(
                        "Only Es sink supports struct, got {:?}: {:?}",
                        col.name,
                        col.data_type,
                    )))
                }
            },
            DataType::Vector(_) |
            DataType::Serial | DataType::Int256 | DataType::Map(_) | DataType::Variant => Err(SinkError::Remote(anyhow!(
                            "remote sink supports Int16, Int32, Int64, Float32, Float64, Boolean, Decimal, Time, Date, Interval, Jsonb, Timestamp, Timestamptz, Bytea, List and Varchar, (Es sink support Struct) got {:?}: {:?}",
                            col.name,
                            col.data_type,
                        )))}})?;

    let jvm = Jvm::get_or_init()?;
    let sink_param = param.to_proto();

    spawn_blocking(move || -> anyhow::Result<()> {
        execute_with_jni_env(jvm, |env| {
            let validate_sink_request = ValidateSinkRequest {
                sink_param: Some(sink_param),
            };
            let validate_sink_request_bytes =
                env.byte_array_from_slice(&Message::encode_to_vec(&validate_sink_request))?;

            let validate_sink_response_bytes = call_static_method!(
                env,
                {com.risingwave.connector.JniSinkValidationHandler},
                {byte[] validate(byte[] validateSourceRequestBytes)},
                &validate_sink_request_bytes
            )?;

            let validate_sink_response: ValidateSinkResponse = Message::decode(
                risingwave_jni_core::to_guarded_slice(&validate_sink_response_bytes, env)?.deref(),
            )?;

            validate_sink_response.error.map_or_else(
                || Ok(()), // If there is no error message, return Ok here.
                |err| bail!("sink validation failed: {}", err.error_message),
            )
        })
    })
    .await
    .context("join handle returned an error")??;

    Ok(())
}

pub struct RemoteLogSinker {
    request_sender: BidiStreamSender<JniSinkWriterStreamRequest>,
    response_stream: BidiStreamReceiver<SinkWriterStreamResponse>,
    stream_chunk_converter: StreamChunkConverter,
    sink_writer_metrics: SinkWriterMetrics,
}

impl RemoteLogSinker {
    async fn new(
        sink_param: SinkParam,
        writer_param: SinkWriterParam,
        sink_name: &str,
    ) -> Result<Self> {
        let sink_proto = sink_param.to_proto();
        let payload_schema = if is_remote_es_sink(sink_name) {
            let columns = vec![
                ColumnDesc::unnamed(ColumnId::from(0), DataType::Varchar).to_protobuf(),
                ColumnDesc::unnamed(ColumnId::from(1), DataType::Varchar).to_protobuf(),
                ColumnDesc::unnamed(ColumnId::from(2), DataType::Jsonb).to_protobuf(),
                ColumnDesc::unnamed(ColumnId::from(2), DataType::Varchar).to_protobuf(),
            ];
            Some(TableSchema {
                columns,
                pk_indices: vec![],
            })
        } else {
            sink_proto.table_schema.clone()
        };

        let SinkWriterStreamHandle {
            request_sender,
            response_stream,
        } = EmbeddedConnectorClient::new()?
            .start_sink_writer_stream(payload_schema, sink_proto)
            .await?;

        Ok(RemoteLogSinker {
            request_sender,
            response_stream,
            sink_writer_metrics: SinkWriterMetrics::new(&writer_param),
            stream_chunk_converter: StreamChunkConverter::new(
                sink_name,
                sink_param.schema(),
                &sink_param.downstream_pk_or_empty(),
                &sink_param.properties,
                sink_param.sink_type.is_append_only(),
            )?,
        })
    }
}

#[async_trait]
impl LogSinker for RemoteLogSinker {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        let mut request_tx = self.request_sender;
        let mut response_err_stream_rx = self.response_stream;
        let sink_writer_metrics = self.sink_writer_metrics;

        let (response_tx, mut response_rx) = unbounded_channel();

        let poll_response_stream = async move {
            loop {
                let result = response_err_stream_rx
                    .stream
                    .try_next()
                    .instrument_await("log_sinker_wait_next_response")
                    .await;
                match result {
                    Ok(Some(response)) => {
                        response_tx.send(response).map_err(|err| {
                            SinkError::Remote(anyhow!("unable to send response: {:?}", err.0))
                        })?;
                    }
                    Ok(None) => return Err(SinkError::Remote(anyhow!("end of response stream"))),
                    Err(e) => return Err(SinkError::Remote(anyhow!(e))),
                }
            }
        };

        let poll_consume_log_and_sink = async move {
            fn truncate_matched_offset(
                queue: &mut VecDeque<(TruncateOffset, Option<Instant>)>,
                persisted_offset: TruncateOffset,
                log_reader: &mut impl SinkLogReader,
                sink_writer_metrics: &SinkWriterMetrics,
            ) -> Result<()> {
                while let Some((sent_offset, _)) = queue.front()
                    && sent_offset < &persisted_offset
                {
                    queue.pop_front();
                }

                let (sent_offset, start_time) = queue.pop_front().ok_or_else(|| {
                    anyhow!("get unsent offset {:?} in response", persisted_offset)
                })?;
                if sent_offset != persisted_offset {
                    bail!(
                        "new response offset {:?} does not match the buffer offset {:?}",
                        persisted_offset,
                        sent_offset
                    );
                }

                if let (TruncateOffset::Barrier { .. }, Some(start_time)) =
                    (persisted_offset, start_time)
                {
                    sink_writer_metrics
                        .sink_commit_duration
                        .observe(start_time.elapsed().as_secs_f64());
                }

                log_reader.truncate(persisted_offset)?;
                Ok(())
            }

            let mut prev_offset: Option<TruncateOffset> = None;
            // Push from back and pop from front
            let mut sent_offset_queue: VecDeque<(TruncateOffset, Option<Instant>)> =
                VecDeque::new();

            loop {
                let either_result: futures::future::Either<
                    Option<SinkWriterStreamResponse>,
                    LogStoreResult<(u64, LogStoreReadItem)>,
                > = drop_either_future(
                    select(pin!(response_rx.recv()), pin!(log_reader.next_item())).await,
                );
                match either_result {
                    futures::future::Either::Left(opt) => {
                        let response = opt.context("end of response stream")?;
                        match response {
                            SinkWriterStreamResponse {
                                response:
                                    Some(sink_writer_stream_response::Response::Batch(
                                        sink_writer_stream_response::BatchWrittenResponse {
                                            epoch,
                                            batch_id,
                                        },
                                    )),
                            } => {
                                truncate_matched_offset(
                                    &mut sent_offset_queue,
                                    TruncateOffset::Chunk {
                                        epoch,
                                        chunk_id: batch_id as _,
                                    },
                                    &mut log_reader,
                                    &sink_writer_metrics,
                                )?;
                            }
                            SinkWriterStreamResponse {
                                response:
                                    Some(sink_writer_stream_response::Response::Commit(
                                        sink_writer_stream_response::CommitResponse {
                                            epoch,
                                            metadata,
                                        },
                                    )),
                            } => {
                                if let Some(metadata) = metadata {
                                    warn!("get unexpected non-empty metadata: {:?}", metadata);
                                }
                                truncate_matched_offset(
                                    &mut sent_offset_queue,
                                    TruncateOffset::Barrier { epoch },
                                    &mut log_reader,
                                    &sink_writer_metrics,
                                )?;
                            }
                            response => {
                                return Err(SinkError::Remote(anyhow!(
                                    "get unexpected response: {:?}",
                                    response
                                )));
                            }
                        }
                    }
                    futures::future::Either::Right(result) => {
                        let (epoch, item): (u64, LogStoreReadItem) = result?;

                        match item {
                            LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                                let offset = TruncateOffset::Chunk { epoch, chunk_id };
                                if let Some(prev_offset) = &prev_offset {
                                    prev_offset.check_next_offset(offset)?;
                                }
                                let cardinality = chunk.cardinality();
                                sink_writer_metrics
                                    .connector_sink_rows_received
                                    .inc_by(cardinality as _);

                                let chunk = self.stream_chunk_converter.convert_chunk(chunk)?;
                                request_tx
                                    .send_request(JniSinkWriterStreamRequest::Chunk {
                                        epoch,
                                        batch_id: chunk_id as u64,
                                        chunk,
                                    })
                                    .instrument_await(span!(
                                        "log_sinker_send_chunk (chunk {chunk_id})"
                                    ))
                                    .await?;
                                prev_offset = Some(offset);
                                sent_offset_queue
                                    .push_back((TruncateOffset::Chunk { epoch, chunk_id }, None));
                            }
                            LogStoreReadItem::Barrier { is_checkpoint, .. } => {
                                let offset = TruncateOffset::Barrier { epoch };
                                if let Some(prev_offset) = &prev_offset {
                                    prev_offset.check_next_offset(offset)?;
                                }
                                let start_time = if is_checkpoint {
                                    let start_time = Instant::now();
                                    request_tx
                                        .barrier(epoch, true)
                                        .instrument_await(span!(
                                            "log_sinker_commit_checkpoint (epoch {epoch})"
                                        ))
                                        .await?;
                                    Some(start_time)
                                } else {
                                    request_tx
                                        .barrier(epoch, false)
                                        .instrument_await(span!(
                                            "log_sinker_send_barrier (epoch {epoch})"
                                        ))
                                        .await?;
                                    None
                                };
                                prev_offset = Some(offset);
                                sent_offset_queue
                                    .push_back((TruncateOffset::Barrier { epoch }, start_time));
                            }
                        }
                    }
                }
            }
        };

        select(pin!(poll_response_stream), pin!(poll_consume_log_and_sink))
            .await
            .factor_first()
            .0
    }
}

#[derive(Debug)]
pub struct CoordinatedRemoteSink<R: RemoteSinkTrait> {
    param: SinkParam,
    _phantom: PhantomData<R>,
}

impl<R: RemoteSinkTrait> EnforceSecret for CoordinatedRemoteSink<R> {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = R::ENFORCE_SECRET_PROPERTIES;
}

impl<R: RemoteSinkTrait> TryFrom<SinkParam> for CoordinatedRemoteSink<R> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            param,
            _phantom: PhantomData,
        })
    }
}

impl<R: RemoteSinkTrait> Sink for CoordinatedRemoteSink<R> {
    type LogSinker = CoordinatedLogSinker<CoordinatedRemoteSinkWriter>;

    const SINK_NAME: &'static str = R::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        validate_remote_sink(&self.param, Self::SINK_NAME).await?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let metrics = SinkWriterMetrics::new(&writer_param);
        CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            CoordinatedRemoteSinkWriter::new(self.param.clone(), metrics.clone()).await?,
            NonZero::new(1).unwrap(),
        )
        .await
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let coordinator = RemoteCoordinator::new::<R>(self.param.clone()).await?;
        Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
    }
}

pub struct CoordinatedRemoteSinkWriter {
    epoch: Option<u64>,
    batch_id: u64,
    stream_handle: SinkWriterStreamHandle<JniSinkWriterStreamRequest>,
    metrics: SinkWriterMetrics,
}

impl CoordinatedRemoteSinkWriter {
    pub async fn new(param: SinkParam, metrics: SinkWriterMetrics) -> Result<Self> {
        let sink_proto = param.to_proto();
        let stream_handle = EmbeddedConnectorClient::new()?
            .start_sink_writer_stream(sink_proto.table_schema.clone(), sink_proto)
            .await?;

        Ok(Self {
            epoch: None,
            batch_id: 0,
            stream_handle,
            metrics,
        })
    }

    #[cfg(test)]
    fn for_test(
        response_receiver: Receiver<ConnectorResult<SinkWriterStreamResponse>>,
        request_sender: tokio::sync::mpsc::Sender<JniSinkWriterStreamRequest>,
    ) -> CoordinatedRemoteSinkWriter {
        use futures::StreamExt;

        let stream_handle = SinkWriterStreamHandle::for_test(
            request_sender,
            ReceiverStream::new(response_receiver)
                .map_err(RpcError::from)
                .boxed(),
        );

        CoordinatedRemoteSinkWriter {
            epoch: None,
            batch_id: 0,
            stream_handle,
            metrics: SinkWriterMetrics::for_test(),
        }
    }
}

#[async_trait]
impl SinkWriter for CoordinatedRemoteSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let cardinality = chunk.cardinality();
        self.metrics
            .connector_sink_rows_received
            .inc_by(cardinality as _);

        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote(anyhow!("epoch has not been initialize, call `begin_epoch`"))
        })?;
        let batch_id = self.batch_id;
        self.stream_handle
            .request_sender
            .send_request(JniSinkWriterStreamRequest::Chunk {
                chunk,
                epoch,
                batch_id,
            })
            .await?;
        self.batch_id += 1;
        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = Some(epoch);
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote(anyhow!("epoch has not been initialize, call `begin_epoch`"))
        })?;
        if is_checkpoint {
            // TODO: add metrics to measure commit time
            let rsp = self.stream_handle.commit(epoch).await?;
            rsp.metadata
                .ok_or_else(|| {
                    SinkError::Remote(anyhow!(
                        "get none metadata in commit response for coordinated sink writer"
                    ))
                })
                .map(Some)
        } else {
            self.stream_handle.barrier(epoch).await?;
            Ok(None)
        }
    }
}

pub struct RemoteCoordinator {
    stream_handle: SinkCoordinatorStreamHandle,
}

impl RemoteCoordinator {
    pub async fn new<R: RemoteSinkTrait>(param: SinkParam) -> Result<Self> {
        let stream_handle = EmbeddedConnectorClient::new()?
            .start_sink_coordinator_stream(param.clone())
            .await?;

        tracing::trace!("{:?} RemoteCoordinator started", R::SINK_NAME,);

        Ok(RemoteCoordinator { stream_handle })
    }
}

#[async_trait]
impl SinglePhaseCommitCoordinator for RemoteCoordinator {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        Ok(self.stream_handle.commit(epoch, metadata).await?)
    }
}

struct EmbeddedConnectorClient {
    jvm: Jvm,
}

impl EmbeddedConnectorClient {
    fn new() -> Result<Self> {
        let jvm = Jvm::get_or_init().context("failed to create EmbeddedConnectorClient")?;
        Ok(EmbeddedConnectorClient { jvm })
    }

    async fn start_sink_writer_stream(
        &self,
        payload_schema: Option<TableSchema>,
        sink_proto: PbSinkParam,
    ) -> Result<SinkWriterStreamHandle<JniSinkWriterStreamRequest>> {
        let (handle, first_rsp) = SinkWriterStreamHandle::initialize(
            SinkWriterStreamRequest {
                request: Some(SinkRequest::Start(StartSink {
                    sink_param: Some(sink_proto),
                    payload_schema,
                })),
            },
            |rx| async move {
                let rx = self.start_jvm_worker_thread(
                    gen_class_name!(com.risingwave.connector.JniSinkWriterHandler),
                    "runJniSinkWriterThread",
                    rx,
                );
                Ok(ReceiverStream::new(rx).map_err(RpcError::from))
            },
        )
        .await?;

        match first_rsp {
            SinkWriterStreamResponse {
                response: Some(sink_writer_stream_response::Response::Start(_)),
            } => Ok(handle),
            msg => Err(SinkError::Internal(anyhow!(
                "should get start response but get {:?}",
                msg
            ))),
        }
    }

    async fn start_sink_coordinator_stream(
        &self,
        param: SinkParam,
    ) -> Result<SinkCoordinatorStreamHandle> {
        let (handle, first_rsp) = SinkCoordinatorStreamHandle::initialize(
            SinkCoordinatorStreamRequest {
                request: Some(sink_coordinator_stream_request::Request::Start(
                    StartCoordinator {
                        param: Some(param.to_proto()),
                    },
                )),
            },
            |rx| async move {
                let rx = self.start_jvm_worker_thread(
                    gen_class_name!(com.risingwave.connector.JniSinkCoordinatorHandler),
                    "runJniSinkCoordinatorThread",
                    rx,
                );
                Ok(ReceiverStream::new(rx).map_err(RpcError::from))
            },
        )
        .await?;

        match first_rsp {
            SinkCoordinatorStreamResponse {
                response: Some(sink_coordinator_stream_response::Response::Start(_)),
            } => Ok(handle),
            msg => Err(SinkError::Internal(anyhow!(
                "should get start response but get {:?}",
                msg
            ))),
        }
    }

    fn start_jvm_worker_thread<REQ: Send + 'static, RSP: Send + 'static>(
        &self,
        class_name: &'static str,
        method_name: &'static str,
        mut request_rx: JniReceiverType<REQ>,
    ) -> Receiver<std::result::Result<RSP, anyhow::Error>> {
        let (mut response_tx, response_rx): (JniSenderType<RSP>, _) =
            mpsc::channel(DEFAULT_BUFFER_SIZE);

        let jvm = self.jvm;
        std::thread::spawn(move || {
            let result = execute_with_jni_env(jvm, |env| {
                let result = call_static_method!(
                    env,
                    class_name,
                    method_name,
                    {{void}, {long requestRx, long responseTx}},
                    &mut request_rx as *mut JniReceiverType<REQ>,
                    &mut response_tx as *mut JniSenderType<RSP>
                );

                match result {
                    Ok(_) => {
                        tracing::debug!("end of jni call {}::{}", class_name, method_name);
                    }
                    Err(e) => {
                        tracing::error!(error = %e.as_report(), "jni call error");
                    }
                };

                Ok(())
            });

            if let Err(e) = result {
                let _ = response_tx.blocking_send(Err(e));
            }
        });
        response_rx
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_jni_core::JniSinkWriterStreamRequest;
    use risingwave_pb::connector_service::sink_writer_stream_request::{Barrier, Request};
    use risingwave_pb::connector_service::sink_writer_stream_response::{CommitResponse, Response};
    use risingwave_pb::connector_service::{SinkWriterStreamRequest, SinkWriterStreamResponse};
    use tokio::sync::mpsc;

    use crate::sink::remote::CoordinatedRemoteSinkWriter;
    use crate::sink::writer::SinkWriter;

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::channel(16);
        let (_, resp_recv) = mpsc::channel(16);

        let mut sink = CoordinatedRemoteSinkWriter::for_test(resp_recv, request_sender);
        let chunk = StreamChunk::from_pretty(
            " i T
            + 1 Ripper
        ",
        );

        // test epoch check
        assert!(
            tokio::time::timeout(Duration::from_secs(10), sink.barrier(true))
                .await
                .expect("test failed: should not commit without epoch")
                .is_err(),
            "test failed: no epoch check for commit()"
        );
        assert!(
            request_recv.try_recv().is_err(),
            "test failed: unchecked epoch before request"
        );

        assert!(
            tokio::time::timeout(Duration::from_secs(1), sink.write_batch(chunk))
                .await
                .expect("test failed: should not write without epoch")
                .is_err(),
            "test failed: no epoch check for write_batch()"
        );
        assert!(
            request_recv.try_recv().is_err(),
            "test failed: unchecked epoch before request"
        );
    }

    #[tokio::test]
    async fn test_remote_sink() {
        let (request_sender, mut request_receiver) = mpsc::channel(16);
        let (response_sender, response_receiver) = mpsc::channel(16);
        let mut sink = CoordinatedRemoteSinkWriter::for_test(response_receiver, request_sender);

        let chunk_a = StreamChunk::from_pretty(
            " i T
            + 1 Alice
            + 2 Bob
            + 3 Clare
        ",
        );
        let chunk_b = StreamChunk::from_pretty(
            " i T
            + 4 David
            + 5 Eve
            + 6 Frank
        ",
        );

        // test write batch
        sink.begin_epoch(2022).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));

        sink.write_batch(chunk_a.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));
        assert_eq!(sink.batch_id, 1);
        match request_receiver.recv().await.unwrap() {
            JniSinkWriterStreamRequest::Chunk {
                epoch,
                batch_id,
                chunk,
            } => {
                assert_eq!(epoch, 2022);
                assert_eq!(batch_id, 0);
                assert_eq!(chunk, chunk_a);
            }
            _ => panic!("test failed: failed to construct write request"),
        }

        // test commit
        response_sender
            .send(Ok(SinkWriterStreamResponse {
                response: Some(Response::Commit(CommitResponse {
                    epoch: 2022,
                    metadata: None,
                })),
            }))
            .await
            .expect("test failed: failed to sync epoch");
        sink.barrier(false).await.unwrap();
        let commit_request = request_receiver.recv().await.unwrap();
        match commit_request {
            JniSinkWriterStreamRequest::PbRequest(SinkWriterStreamRequest {
                request:
                    Some(Request::Barrier(Barrier {
                        epoch,
                        is_checkpoint: false,
                    })),
            }) => {
                assert_eq!(epoch, 2022);
            }
            _ => panic!("test failed: failed to construct sync request "),
        };

        // begin another epoch
        sink.begin_epoch(2023).await.unwrap();
        assert_eq!(sink.epoch, Some(2023));

        // test another write
        sink.write_batch(chunk_b.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2023));
        assert_eq!(sink.batch_id, 2);
        match request_receiver.recv().await.unwrap() {
            JniSinkWriterStreamRequest::Chunk {
                epoch,
                batch_id,
                chunk,
            } => {
                assert_eq!(epoch, 2023);
                assert_eq!(batch_id, 1);
                assert_eq!(chunk, chunk_b);
            }
            _ => panic!("test failed: failed to construct write request"),
        }
    }
}
