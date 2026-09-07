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

use std::str::FromStr;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures_async_stream::try_stream;
use itertools::Itertools;
use prost::Message;
use risingwave_common::bail;
use risingwave_common::global_jvm::Jvm;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_jni_core::{JniReceiverType, OwnedPointer, call_static_method};
use risingwave_pb::connector_service::{GetEventStreamRequest, GetEventStreamResponse};
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::cdc::{CdcProperties, CdcSourceType, CdcSourceTypeTrait, DebeziumCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SplitId, SplitMetaData, SplitReader,
    into_chunk_stream,
};

pub struct CdcSplitReader<T: CdcSourceTypeTrait> {
    source_id: u64,
    #[expect(dead_code)]
    start_offset: Option<String>,
    // host address of worker node for a Citus cluster
    #[expect(dead_code)]
    server_addr: Option<String>,
    #[expect(dead_code)]
    conn_props: CdcProperties<T>,
    #[expect(dead_code)]
    split_id: SplitId,
    // whether the full snapshot phase is done
    #[expect(dead_code)]
    snapshot_done: bool,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    rx: JniReceiverType<anyhow::Result<GetEventStreamResponse>>,
}

const DEFAULT_CHANNEL_SIZE: usize = 16;

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitReader for CdcSplitReader<T> {
    type Properties = CdcProperties<T>;
    type Split = DebeziumCdcSplit<T>;

    async fn new(
        conn_props: CdcProperties<T>,
        splits: Vec<DebeziumCdcSplit<T>>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.id();

        let mut properties = conn_props.properties.clone();

        let mut citus_server_addr = None;
        // For citus, we need to rewrite the `table.name` to capture sharding tables
        if matches!(T::source_type(), CdcSourceType::Citus)
            && let Some(ref citus_split) = split.citus_split
            && let Some(ref server_addr) = citus_split.server_addr
        {
            citus_server_addr = Some(server_addr.clone());
            let host_addr =
                HostAddr::from_str(server_addr).context("invalid server address for cdc split")?;
            properties.insert("hostname".to_owned(), host_addr.host);
            properties.insert("port".to_owned(), host_addr.port.to_string());
            // rewrite table name with suffix to capture all shards in the split
            let mut table_name = properties
                .remove("table.name")
                .ok_or_else(|| anyhow!("missing field 'table.name'"))?;
            table_name.push_str("_[0-9]+");
            properties.insert("table.name".into(), table_name);
        }

        let source_id = split.split_id() as u64;
        let source_type = conn_props.get_source_type_pb();
        let (tx, mut rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let jvm = Jvm::get_or_init()?;
        let get_event_stream_request = GetEventStreamRequest {
            source_id,
            source_type: source_type as _,
            start_offset: split.start_offset().clone().unwrap_or_default(),
            properties,
            snapshot_done: split.snapshot_done(),
            is_source_job: conn_props.is_cdc_source_job,
        };

        std::thread::spawn(move || {
            execute_with_jni_env(jvm, |env| {
                let result: anyhow::Result<_> = try {
                    let get_event_stream_request_bytes = env
                        .byte_array_from_slice(&Message::encode_to_vec(&get_event_stream_request))
                        .map_err(anyhow::Error::from)?;
                    (env, get_event_stream_request_bytes)
                };

                let (env, get_event_stream_request_bytes) = match result {
                    Ok(inner) => inner,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(
                            e.context("err before calling runJniDbzSourceThread")
                        ));
                        return Ok(());
                    }
                };

                // `runJniDbzSourceThread` will take ownership of `tx`, and release it later in
                // `Java_com_risingwave_java_binding_Binding_cdcSourceSenderClose` via ref cleaner.
                let tx: OwnedPointer<_> = tx.into();

                let result = call_static_method!(
                    env,
                    {com.risingwave.connector.source.core.JniDbzSourceHandler},
                    {void runJniDbzSourceThread(byte[] getEventStreamRequestBytes, long channelPtr)},
                    &get_event_stream_request_bytes,
                    tx.into_pointer()
                );

                match result {
                    Ok(_) => {
                        tracing::info!(?source_id, "end of jni call runJniDbzSourceThread");
                    }
                    Err(e) => {
                        tracing::error!(?source_id, error = %e.as_report(), "jni call error");
                    }
                }

                Ok(())
            })
        });

        // wait for the handshake message
        if let Some(res) = rx.recv().await {
            let resp: GetEventStreamResponse = res?;
            let inited = match resp.control {
                Some(info) => info.handshake_ok,
                None => {
                    tracing::error!(
                        ?source_id,
                        "did not receive the CDC handshake message: {:?}",
                        resp
                    );
                    false
                }
            };
            if !inited {
                bail!(
                    "failed to start the CDC connector before the timeout (default: 60s).\n\
                    HINT: You can increase the timeout by:\n\
                    - Alter the running source: ALTER SOURCE <source_name> CONNECTOR WITH (cdc.source.wait.streaming.start.timeout = '<larger_value>');\n\
                    - Set the session variable and recreate the source: SET cdc_source_wait_streaming_start_timeout = <larger_value>;"
                );
            }
        }
        tracing::info!(?source_id, "CDC connector started");

        let instance = match T::source_type() {
            CdcSourceType::Mysql
            | CdcSourceType::Postgres
            | CdcSourceType::Mongodb
            | CdcSourceType::SqlServer
            | CdcSourceType::Oracle => Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: None,
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
                rx,
            },
            CdcSourceType::Citus => Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: citus_server_addr,
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
                rx,
            },
            CdcSourceType::Unspecified => {
                unreachable!();
            }
        };
        Ok(instance)
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl<T: CdcSourceTypeTrait> CdcSplitReader<T> {
    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        let mut rx = self.rx;
        let source_id = self.source_id.to_string();

        while let Some(result) = rx.recv().await {
            match result {
                Ok(GetEventStreamResponse { events, .. }) => {
                    tracing::trace!("receive {} cdc events ", events.len());
                    let msgs = events.into_iter().map(SourceMessage::from).collect_vec();
                    yield msgs;
                }
                Err(e) => {
                    GLOBAL_ERROR_METRICS.user_source_error.report([
                        "cdc_source".to_owned(),
                        source_id.clone(),
                        self.source_ctx.source_name.clone(),
                        self.source_ctx.fragment_id.to_string(),
                    ]);
                    Err(e)?;
                }
            }
        }

        bail!("all senders are dropped");
    }
}
