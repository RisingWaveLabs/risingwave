// Copyright 2026 RisingWave Labs
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

//! WebSocket-based streaming DML ingest endpoint.
//!
//! Clients open a `WebSocket` connection per table. The first text frame authenticates the session
//! and later frames carry unsigned batches of upsert / delete DML messages. Each websocket
//! payload carries a monotonically increasing `dml_batch_id`.
//!
//! Wire format (JSON over text `WebSocket` frames):
//!
//! **Client → Server** (authenticated init):
//! ```json
//! {"type": "init", "timestamp": 1760000000000}
//! ```
//!
//! **Client → Server** (DML batch):
//! ```json
//! {
//!   "dml_batch_id": 1,
//!   "items": [
//!     {"op": "upsert", "data": {"id": 1, "name": "foo"}},
//!     {"op": "delete", "data": {"id": 1, "name": "foo"}}
//!   ]
//! }
//! ```
//!
//! **Server → Client** (ack):
//! ```json
//! {"ack": 1}
//! ```
//!
//! **Server → Client** (fatal — the connection will close):
//! ```json
//! {"fatal": "DML channel closed, please reconnect"}
//! ```
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use futures::SinkExt;
use futures::stream::StreamExt;
use jsonbb::Value;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::license::Feature;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, JsonbVal};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::task_service::{
    IngestDmlInitRequest, IngestDmlPayloadRequest, IngestDmlRequest, ingest_dml_request,
    ingest_dml_response,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror_ext::AsReport;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

use crate::session::SESSION_MANAGER;
use crate::webhook::payload::{build_json_access_builder, owned_row_from_payload_row};
use crate::webhook::utils::{authenticate_webhook_payload, err, header_map_to_json};
use crate::webhook::{PayloadSchema, acquire_table_info};

const INIT_MESSAGE_TYPE: &str = "init";
const INGEST_DML_REQUEST_BUFFER_SIZE: usize = 64;
const WEBSOCKET_INIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared state for the ingest service.
pub struct IngestService {
    counter: AtomicU32,
}

impl IngestService {
    pub fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }
}

pub type ServiceRef = Arc<IngestService>;

#[derive(Debug, Deserialize)]
struct InitRequest {
    #[serde(rename = "type")]
    msg_type: String,
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct RawDmlBatchRequest {
    pub dml_batch_id: u64,
    pub items: Vec<RawDmlRequest>,
}

#[derive(Debug, Deserialize)]
pub struct RawDmlRequest {
    pub op: Option<String>,
    // `RawValue` is unsized; `Box<RawValue>` is serde's owned form that preserves the original JSON text.
    pub data: Box<RawValue>,
}

#[derive(Debug, Clone, Copy)]
enum DmlOp {
    Upsert,
    Delete,
}

#[derive(Debug)]
pub struct DmlRequest {
    op: DmlOp,
    data: Box<RawValue>,
}

impl TryFrom<RawDmlRequest> for DmlRequest {
    type Error = String;

    fn try_from(raw: RawDmlRequest) -> Result<Self, Self::Error> {
        let op = match raw.op.as_deref() {
            None => {
                return Err(
                    "missing `op`; expected `upsert`, `insert`, `update`, or `delete`".to_owned(),
                );
            }
            Some("upsert" | "insert" | "update") => DmlOp::Upsert,
            Some("delete") => DmlOp::Delete,
            Some(other) => {
                return Err(format!(
                    "unknown `op` value `{other}`; expected `upsert`, `insert`, `update`, or `delete`"
                ));
            }
        };

        Ok(Self { op, data: raw.data })
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ServerMessage {
    Ack { ack: u64 },
    Fatal { fatal: String },
}

#[derive(Debug)]
struct PreparedDmlBatch {
    dml_batch_id: u64,
    payload: IngestDmlPayloadRequest,
}

type WsTx = futures::stream::SplitSink<WebSocket, Message>;
type WsRx = futures::stream::SplitStream<WebSocket>;

// Keep licensing at the WebSocket entry point so webhook ingestion remains unlicensed.
fn check_websocket_ingest_license() -> crate::webhook::utils::Result<()> {
    Feature::WebSocketIngest
        .check_available()
        .map_err(|e| err(e, StatusCode::FORBIDDEN))
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(svc): Extension<ServiceRef>,
    headers: HeaderMap,
    Path((database, schema, table)): Path<(String, String, String)>,
) -> crate::webhook::utils::Result<impl IntoResponse> {
    check_websocket_ingest_license()?;

    let request_id = svc.counter.fetch_add(1, Ordering::Relaxed);
    let headers_jsonb = header_map_to_json(&headers);
    Ok(ws.on_upgrade(move |socket| {
        handle_connection(
            socket,
            database,
            schema,
            table,
            request_id,
            headers,
            headers_jsonb,
        )
    }))
}

async fn handle_connection(
    socket: WebSocket,
    database: String,
    schema: String,
    table: String,
    request_id: u32,
    headers: HeaderMap,
    headers_jsonb: JsonbVal,
) {
    let (mut ws_tx, ws_rx) = socket.split();

    if let Err(e) = try_handle_connection(
        &mut ws_tx,
        ws_rx,
        database,
        schema,
        table,
        request_id,
        headers,
        headers_jsonb,
    )
    .await
    {
        let _ = send_fatal(&mut ws_tx, e).await;
    }
}

async fn try_handle_connection(
    ws_tx: &mut WsTx,
    mut ws_rx: WsRx,
    database: String,
    schema: String,
    table: String,
    request_id: u32,
    headers: HeaderMap,
    headers_jsonb: JsonbVal,
) -> Result<(), String> {
    let table_info = acquire_table_info(request_id, &database, &schema, &table)
        .await
        .map_err(|e| format!("table lookup failed: {}", e.as_report()))?;

    let session_mgr = SESSION_MANAGER.get().expect("session manager initialized");
    let max_clock_skew_ms = session_mgr
        .env()
        .frontend_config()
        .webhook_auth_max_clock_skew_ms;
    let webhook_source_info = table_info.webhook_source_info;
    let table_id = table_info.table_id;
    let table_version_id = table_info.table_version_id;
    let row_id_index = table_info.row_id_index;
    let compute_client = table_info.compute_client;
    let payload_schema = table_info.payload_schema;

    let init_text = match timeout(WEBSOCKET_INIT_TIMEOUT, ws_rx.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => text,
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) => return Ok(()),
        Ok(Some(Ok(_))) => {
            return Err("the first WebSocket frame must be a text `init` message".to_owned());
        }
        Ok(Some(Err(e))) => {
            return Err(format!(
                "failed to read WebSocket init message: {}",
                e.as_report()
            ));
        }
        Err(_) => {
            return Err(format!(
                "timed out waiting for WebSocket init message after {}s",
                WEBSOCKET_INIT_TIMEOUT.as_secs()
            ));
        }
    };

    if let Err(e) =
        authenticate_webhook_payload(headers_jsonb, init_text.as_bytes(), &webhook_source_info)
            .await
    {
        return Err(format!("{}", e.as_report()));
    }

    parse_and_validate_init_request(&init_text, max_clock_skew_ms)?;

    let (ingest_req_tx, ingest_req_rx) = tokio::sync::mpsc::channel(INGEST_DML_REQUEST_BUFFER_SIZE);
    if ingest_req_tx
        .send(IngestDmlRequest {
            request: Some(ingest_dml_request::Request::Init(IngestDmlInitRequest {
                table_id,
                table_version_id,
                request_id,
                row_id_index,
            })),
        })
        .await
        .is_err()
    {
        return Err("failed to enqueue init request for ingest stream".to_owned());
    }

    let mut ingest_resp_stream = compute_client
        .ingest_dml(ReceiverStream::new(ingest_req_rx))
        .await
        .map_err(|e| format!("failed to open ingest stream: {}", e.as_report()))?;

    match ingest_resp_stream.message().await {
        Ok(Some(resp)) => match resp.response {
            Some(ingest_dml_response::Response::Init(_)) => {}
            _ => {
                return Err("unexpected init response from ingest stream".to_owned());
            }
        },
        Ok(None) => return Err("ingest stream closed during init".to_owned()),
        Err(e) => {
            return Err(format!("ingest stream init error: {}", e.as_report()));
        }
    }

    let mut last_seen_dml_batch_id = 0_u64;
    let mut last_forwarded_dml_batch_id = 0_u64;
    let mut last_acked_dml_batch_id = 0_u64;

    loop {
        tokio::select! {
            biased;
            ingest_resp = ingest_resp_stream.message() => {
                match ingest_resp {
                    Ok(Some(resp)) => match resp.response {
                        Some(ingest_dml_response::Response::Ack(ack)) => {
                            if ack.dml_batch_id <= last_acked_dml_batch_id
                                || ack.dml_batch_id > last_forwarded_dml_batch_id
                            {
                                return Err(format!(
                                    "received an unexpected ack for dml_batch_id {}",
                                    ack.dml_batch_id
                                ));
                            }
                            last_acked_dml_batch_id = ack.dml_batch_id;
                            send_server_message(
                                ws_tx,
                                ServerMessage::Ack {
                                    ack: ack.dml_batch_id,
                                },
                            )
                            .await
                            .map_err(|_| "WebSocket connection closed while sending an ack".to_owned())?;
                        }
                        Some(ingest_dml_response::Response::Init(_)) => {
                            return Err("unexpected extra init response from ingest stream".to_owned());
                        }
                        None => return Err("empty response from ingest stream".to_owned()),
                    },
                    Ok(None) => return Err("ingest stream closed".to_owned()),
                    Err(e) => return Err(format!("ingest stream error: {}", e.as_report())),
                }
            }

            ws_msg = ws_rx.next() => {
                let text = match ws_msg {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => {
                        return Err(format!(
                            "failed to read WebSocket message: {}",
                            e.as_report()
                        ));
                    }
                };

                let raw_dml_batch = match serde_json::from_str::<RawDmlBatchRequest>(&text) {
                    Ok(batch) => batch,
                    Err(e) => return Err(format!("malformed payload: {}", e.as_report())),
                };

                last_seen_dml_batch_id = validate_monotonic_dml_batch_id(
                    raw_dml_batch.dml_batch_id,
                    last_seen_dml_batch_id,
                )?;

                if raw_dml_batch.items.is_empty() {
                    send_server_message(
                        ws_tx,
                        ServerMessage::Ack {
                            ack: raw_dml_batch.dml_batch_id,
                        },
                    )
                    .await
                    .map_err(|_| "WebSocket connection closed while sending an ack".to_owned())?;
                    continue;
                }

                let prepared_batch = prepare_dml_batch_payload(
                    &headers,
                    raw_dml_batch,
                    &payload_schema,
                )
                .map_err(|e| format!("failed to prepare DML batch: {e}"))?;

                let PreparedDmlBatch {
                    dml_batch_id,
                    payload,
                } = prepared_batch;

                ingest_req_tx
                    .send(IngestDmlRequest {
                        request: Some(ingest_dml_request::Request::Payload(payload)),
                    })
                    .await
                    .map_err(|_| "ingest stream request channel closed".to_owned())?;
                last_forwarded_dml_batch_id = dml_batch_id;
            }
        }
    }

    Ok(())
}

fn parse_and_validate_init_request(
    text: &str,
    max_clock_skew_ms: u64,
) -> Result<InitRequest, String> {
    let init_req: InitRequest = serde_json::from_str(text)
        .map_err(|e| format!("malformed init message: {}", e.as_report()))?;

    if init_req.msg_type != INIT_MESSAGE_TYPE {
        return Err(format!(
            "invalid init message type '{}', expected '{}'",
            init_req.msg_type, INIT_MESSAGE_TYPE
        ));
    }

    validate_timestamp_skew(init_req.timestamp, max_clock_skew_ms)?;
    Ok(init_req)
}

fn validate_timestamp_skew(timestamp_ms: i64, max_clock_skew_ms: u64) -> Result<(), String> {
    if timestamp_ms < 0 {
        return Err("timestamp must be a non-negative Unix timestamp in milliseconds".to_owned());
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i128;
    let diff_ms = (now_ms - i128::from(timestamp_ms)).abs();

    if diff_ms > i128::from(max_clock_skew_ms) {
        return Err(format!(
            "timestamp skew {}ms exceeds the allowed {}ms window",
            diff_ms, max_clock_skew_ms
        ));
    }

    Ok(())
}

fn validate_monotonic_dml_batch_id(
    dml_batch_id: u64,
    last_seen_dml_batch_id: u64,
) -> Result<u64, String> {
    if dml_batch_id <= last_seen_dml_batch_id {
        return Err(format!(
            "dml_batch_id must increase monotonically: received {} after {}",
            dml_batch_id, last_seen_dml_batch_id
        ));
    }
    Ok(dml_batch_id)
}

fn prepare_dml_batch_payload(
    headers: &HeaderMap,
    raw_dml_batch: RawDmlBatchRequest,
    payload_schema: &PayloadSchema,
) -> Result<PreparedDmlBatch, String> {
    let dml_batch_id = raw_dml_batch.dml_batch_id;
    let raw_dml_reqs = raw_dml_batch.items;

    match payload_schema {
        PayloadSchema::SingleJsonb => {
            let mut chunk_builder = DataChunkBuilder::new(
                vec![DataType::Jsonb],
                raw_dml_reqs.len().saturating_add(1).max(1),
            );
            let mut ops = Vec::with_capacity(raw_dml_reqs.len());

            for (index, raw_dml_req) in raw_dml_reqs.into_iter().enumerate() {
                let item_index = index + 1;
                let dml_req = DmlRequest::try_from(raw_dml_req)
                    .map_err(|e| format!("dml_batch_id {dml_batch_id} item {item_index}: {e}"))?;

                let row = Value::from_text(dml_req.data.get().as_bytes())
                    .map(|json_value| OwnedRow::new(vec![Some(JsonbVal::from(json_value).into())]))
                    .map_err(|e| {
                        format!(
                            "dml_batch_id {dml_batch_id} item {item_index}: failed to parse request body: {}",
                            e.as_report()
                        )
                    })?;

                let output = chunk_builder.append_one_row(row);
                debug_assert!(output.is_none());
                ops.push(match dml_req.op {
                    DmlOp::Upsert => Op::Insert,
                    DmlOp::Delete => Op::Delete,
                });
            }

            let data_chunk = chunk_builder
                .consume_all()
                .expect("buffered rows should produce a chunk");
            let chunk = StreamChunk::from_parts(ops, data_chunk);
            let payload = IngestDmlPayloadRequest {
                dml_batch_id,
                chunk: Some(chunk.to_protobuf()),
            };

            Ok(PreparedDmlBatch {
                dml_batch_id,
                payload,
            })
        }
        PayloadSchema::FullSchema { columns } => {
            let mut access_builder =
                build_json_access_builder(headers).map_err(|e| format!("{}", e.as_report()))?;
            let mut chunk_builder = DataChunkBuilder::new(
                columns
                    .iter()
                    .map(|column| column.data_type.clone())
                    .collect(),
                raw_dml_reqs.len().saturating_add(1).max(1),
            );
            let mut ops = Vec::with_capacity(raw_dml_reqs.len());

            for (index, raw_dml_req) in raw_dml_reqs.into_iter().enumerate() {
                let item_index = index + 1;
                let dml_req = DmlRequest::try_from(raw_dml_req)
                    .map_err(|e| format!("dml_batch_id {dml_batch_id} item {item_index}: {e}"))?;

                let row = owned_row_from_payload_row(
                    &mut access_builder,
                    columns,
                    dml_req.data.get().as_bytes(),
                )
                .map_err(|e| {
                    format!(
                        "dml_batch_id {dml_batch_id} item {item_index}: {}",
                        e.as_report()
                    )
                })?;

                let output = chunk_builder.append_one_row(row);
                debug_assert!(output.is_none());
                ops.push(match dml_req.op {
                    DmlOp::Upsert => Op::Insert,
                    DmlOp::Delete => Op::Delete,
                });
            }

            let data_chunk = chunk_builder
                .consume_all()
                .expect("buffered rows should produce a chunk");
            let chunk = StreamChunk::from_parts(ops, data_chunk);
            let payload = IngestDmlPayloadRequest {
                dml_batch_id,
                chunk: Some(chunk.to_protobuf()),
            };

            Ok(PreparedDmlBatch {
                dml_batch_id,
                payload,
            })
        }
    }
}

async fn send_server_message(ws_tx: &mut WsTx, msg: ServerMessage) -> Result<(), String> {
    let text = serde_json::to_string(&msg).map_err(|e| format!("{}", e.as_report()))?;
    ws_tx
        .send(Message::Text(text.into()))
        .await
        .map_err(|e| format!("{}", e.as_report()))
}

async fn send_fatal(ws_tx: &mut WsTx, fatal: String) -> Result<(), String> {
    send_server_message(ws_tx, ServerMessage::Fatal { fatal }).await
}

pub fn build_router(svc: ServiceRef) -> Router {
    Router::new()
        .route("/{database}/{schema}/{table}", get(ws_handler))
        .layer(
            ServiceBuilder::new()
                .layer(AddExtensionLayer::new(svc))
                .into_inner(),
        )
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};
    use risingwave_common::license::{LicenseKey, LicenseManager};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use super::*;
    use crate::webhook::WebhookTableColumnDesc;

    fn raw_json(text: &str) -> Box<RawValue> {
        serde_json::from_str(text).unwrap()
    }

    fn raw_req(op: Option<&str>, data: &str) -> RawDmlRequest {
        RawDmlRequest {
            op: op.map(str::to_owned),
            data: raw_json(data),
        }
    }

    fn raw_batch(dml_batch_id: u64, items: Vec<RawDmlRequest>) -> RawDmlBatchRequest {
        RawDmlBatchRequest {
            dml_batch_id,
            items,
        }
    }

    struct RestoreDefaultLicense;

    impl Drop for RestoreDefaultLicense {
        fn drop(&mut self) {
            LicenseManager::get().refresh(LicenseKey::default().as_ref());
        }
    }

    #[test]
    fn test_websocket_ingest_requires_license() {
        let _restore = RestoreDefaultLicense;
        LicenseManager::get().refresh(LicenseKey::default().as_ref());
        check_websocket_ingest_license().unwrap();

        LicenseManager::get().refresh(LicenseKey::empty().as_ref());

        let err = check_websocket_ingest_license().unwrap_err();

        assert_eq!(err.code(), StatusCode::FORBIDDEN);
        assert!(err.to_string().contains("WebSocketIngest"));
    }

    fn test_columns(columns: &[(&str, DataType, bool)]) -> Vec<WebhookTableColumnDesc> {
        columns
            .iter()
            .map(|(name, data_type, is_pk)| WebhookTableColumnDesc {
                name: (*name).to_owned(),
                data_type: data_type.clone(),
                is_pk: *is_pk,
            })
            .collect()
    }

    #[test]
    fn test_validate_monotonic_dml_batch_id() {
        assert_eq!(validate_monotonic_dml_batch_id(5, 1).unwrap(), 5);
    }

    #[test]
    fn test_validate_monotonic_dml_batch_id_rejects_non_monotonic_sequence() {
        let err = validate_monotonic_dml_batch_id(3, 3).unwrap_err();
        assert!(err.contains("dml_batch_id must increase monotonically"));
    }

    #[test]
    fn test_parse_and_validate_init_request() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64;
        let init = format!(r#"{{"type":"init","timestamp":{now_ms}}}"#);
        assert_eq!(
            parse_and_validate_init_request(&init, 300_000)
                .unwrap()
                .timestamp,
            now_ms
        );
    }

    #[test]
    fn test_parse_and_validate_init_request_rejects_stale_timestamp() {
        let init = r#"{"type":"init","timestamp":0}"#;
        let err = parse_and_validate_init_request(init, 1).unwrap_err();
        assert!(err.contains("timestamp skew"));
    }

    #[test]
    fn test_dml_request_requires_op() {
        let err = DmlRequest::try_from(raw_req(None, r#"{"id":1}"#)).unwrap_err();
        assert_eq!(
            err,
            "missing `op`; expected `upsert`, `insert`, `update`, or `delete`"
        );
    }

    #[test]
    fn test_empty_batch_is_valid_json_protocol_input() {
        let raw_dml_batch = raw_batch(42, vec![]);
        assert_eq!(
            validate_monotonic_dml_batch_id(raw_dml_batch.dml_batch_id, 41).unwrap(),
            42
        );
        assert!(raw_dml_batch.items.is_empty());
    }

    #[test]
    fn test_prepare_dml_batch_payload_builds_single_chunk_for_batch() {
        let raw_dml_batch = raw_batch(
            10,
            vec![
                raw_req(
                    Some("upsert"),
                    r#"{"id":1,"price":"19.99","created_at":"2026-04-15 10:00:00","name":"alice"}"#,
                ),
                raw_req(
                    Some("delete"),
                    r#"{"id":2,"price":"29.99","created_at":"2026-04-16 10:00:00","name":"bob"}"#,
                ),
            ],
        );
        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("price", DataType::Decimal, false),
                ("created_at", DataType::Timestamp, false),
                ("name", DataType::Varchar, false),
            ]),
        };

        let prepared_batch =
            prepare_dml_batch_payload(&HeaderMap::new(), raw_dml_batch, &payload_schema).unwrap();

        assert_eq!(prepared_batch.dml_batch_id, 10);

        let payload = prepared_batch.payload;
        assert_eq!(payload.dml_batch_id, 10);
        let chunk = StreamChunk::from_protobuf(payload.chunk.as_ref().unwrap()).unwrap();

        assert_eq!(chunk.ops(), &[Op::Insert, Op::Delete]);
        let mut rows = chunk.rows();
        let row = rows.next().unwrap().1;
        assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
        assert!(matches!(
            row.datum_at(1).to_owned_datum(),
            Some(ScalarImpl::Decimal(_))
        ));
        assert!(matches!(
            row.datum_at(2).to_owned_datum(),
            Some(ScalarImpl::Timestamp(_))
        ));
        assert_eq!(
            row.datum_at(3).to_owned_datum(),
            Some(ScalarImpl::Utf8("alice".into()))
        );

        let row = rows.next().unwrap().1;
        assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(2)));
    }

    #[test]
    fn test_prepare_dml_batch_payload_returns_first_item_error() {
        let raw_dml_batch = raw_batch(
            11,
            vec![
                raw_req(Some("delete"), r#"{"id":1,"name":"alice"}"#),
                raw_req(Some("upsert"), r#"{"name":"bob"}"#),
            ],
        );
        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("name", DataType::Varchar, false),
            ]),
        };

        let err = prepare_dml_batch_payload(&HeaderMap::new(), raw_dml_batch, &payload_schema)
            .unwrap_err();

        assert!(err.contains("dml_batch_id 11 item 2"));
        assert!(err.contains("failed to decode webhook JSON payload"));
    }

    #[test]
    fn test_prepare_dml_batch_payload_allows_delete_with_only_pk() {
        let raw_dml_batch = raw_batch(
            12,
            vec![
                raw_req(Some("upsert"), r#"{"id":7,"name":"alice"}"#),
                raw_req(Some("delete"), r#"{"id":7}"#),
            ],
        );
        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("name", DataType::Varchar, false),
            ]),
        };

        let prepared_batch =
            prepare_dml_batch_payload(&HeaderMap::new(), raw_dml_batch, &payload_schema).unwrap();
        let chunk =
            StreamChunk::from_protobuf(prepared_batch.payload.chunk.as_ref().unwrap()).unwrap();

        assert_eq!(prepared_batch.dml_batch_id, 12);
        assert_eq!(chunk.ops(), &[Op::Insert, Op::Delete]);
        let mut rows = chunk.rows();
        assert_eq!(
            rows.next().unwrap().1.datum_at(1).to_owned_datum(),
            Some(ScalarImpl::Utf8("alice".into()))
        );
        assert_eq!(rows.next().unwrap().1.datum_at(1).to_owned_datum(), None);
    }

    #[test]
    fn test_prepare_dml_batch_payload_rejects_incomplete_composite_pk_insert() {
        let raw_dml_batch = raw_batch(
            51,
            vec![raw_req(Some("upsert"), r#"{"id":1,"name":"alice"}"#)],
        );
        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("tenant_id", DataType::Int32, true),
                ("id", DataType::Int32, true),
                ("name", DataType::Varchar, false),
            ]),
        };

        let err = prepare_dml_batch_payload(&HeaderMap::new(), raw_dml_batch, &payload_schema)
            .unwrap_err();

        assert!(err.contains("dml_batch_id 51 item 1"));
        assert!(err.contains("failed to decode webhook JSON payload"));
    }

    #[test]
    fn test_prepare_dml_batch_payload_rejects_incomplete_composite_pk_delete() {
        let raw_dml_batch = raw_batch(52, vec![raw_req(Some("delete"), r#"{"tenant_id":1}"#)]);
        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("tenant_id", DataType::Int32, true),
                ("id", DataType::Int32, true),
                ("name", DataType::Varchar, false),
            ]),
        };

        let err = prepare_dml_batch_payload(&HeaderMap::new(), raw_dml_batch, &payload_schema)
            .unwrap_err();

        assert!(err.contains("dml_batch_id 52 item 1"));
        assert!(err.contains("failed to decode webhook JSON payload"));
    }

    #[test]
    fn test_prepare_dml_batch_payload_applies_supported_decoder_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-rw-webhook-json-timestamp-handling-mode",
            HeaderValue::from_static("milli"),
        );
        headers.insert(
            "x-rw-webhook-json-time-handling-mode",
            HeaderValue::from_static("milli"),
        );
        headers.insert(
            "x-rw-webhook-json-bigint-unsigned-handling-mode",
            HeaderValue::from_static("precise"),
        );

        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("event_time", DataType::Timestamp, false),
            ]),
        };
        let prepared_timestamp_batch = prepare_dml_batch_payload(
            &headers,
            raw_batch(
                13,
                vec![raw_req(
                    Some("upsert"),
                    r#"{"id":1,"event_time":1712800800123}"#,
                )],
            ),
            &payload_schema,
        )
        .unwrap();
        let timestamp_chunk =
            StreamChunk::from_protobuf(prepared_timestamp_batch.payload.chunk.as_ref().unwrap())
                .unwrap();
        assert!(matches!(
            timestamp_chunk
                .rows()
                .next()
                .unwrap()
                .1
                .datum_at(1)
                .to_owned_datum(),
            Some(ScalarImpl::Timestamp(_))
        ));

        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("event_time", DataType::Time, false),
            ]),
        };
        let prepared_time_batch = prepare_dml_batch_payload(
            &headers,
            raw_batch(
                14,
                vec![raw_req(Some("upsert"), r#"{"id":2,"event_time":3723123}"#)],
            ),
            &payload_schema,
        )
        .unwrap();
        let time_chunk =
            StreamChunk::from_protobuf(prepared_time_batch.payload.chunk.as_ref().unwrap())
                .unwrap();
        assert!(matches!(
            time_chunk
                .rows()
                .next()
                .unwrap()
                .1
                .datum_at(1)
                .to_owned_datum(),
            Some(ScalarImpl::Time(_))
        ));

        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("amount", DataType::Decimal, false),
            ]),
        };
        let prepared_decimal_batch = prepare_dml_batch_payload(
            &headers,
            raw_batch(
                15,
                vec![raw_req(Some("upsert"), r#"{"id":3,"amount":"AeJA"}"#)],
            ),
            &payload_schema,
        )
        .unwrap();
        let decimal_chunk =
            StreamChunk::from_protobuf(prepared_decimal_batch.payload.chunk.as_ref().unwrap())
                .unwrap();
        assert!(matches!(
            decimal_chunk
                .rows()
                .next()
                .unwrap()
                .1
                .datum_at(1)
                .to_owned_datum(),
            Some(ScalarImpl::Decimal(_))
        ));
    }

    #[test]
    fn test_prepare_dml_batch_payload_rejects_invalid_decoder_headers() {
        for (header, value, expected) in [
            (
                "x-rw-webhook-json-timestamp-handling-mode",
                "invalid",
                "unrecognized value `invalid` for `x-rw-webhook-json-timestamp-handling-mode`",
            ),
            (
                "x-rw-webhook-json-timestamptz-handling-mode",
                "invalid",
                "invalid webhook JSON decoder option",
            ),
            (
                "x-rw-webhook-json-time-handling-mode",
                "invalid",
                "unrecognized value `invalid` for `x-rw-webhook-json-time-handling-mode`",
            ),
            (
                "x-rw-webhook-json-bigint-unsigned-handling-mode",
                "invalid",
                "unrecognized value `invalid` for `x-rw-webhook-json-bigint-unsigned-handling-mode`",
            ),
            (
                "x-rw-webhook-json-handle-toast-columns",
                "invalid",
                "unrecognized value `invalid` for `x-rw-webhook-json-handle-toast-columns`",
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header, HeaderValue::from_static(value));
            let raw_dml_batch = raw_batch(
                21,
                vec![raw_req(Some("upsert"), r#"{"id":1,"name":"alice"}"#)],
            );
            let payload_schema = PayloadSchema::FullSchema {
                columns: test_columns(&[
                    ("id", DataType::Int32, true),
                    ("name", DataType::Varchar, false),
                ]),
            };

            let err =
                prepare_dml_batch_payload(&headers, raw_dml_batch, &payload_schema).unwrap_err();
            assert!(err.contains(expected), "{header}");
        }
    }

    #[test]
    fn test_prepare_dml_batch_payload_returns_first_type_error_with_item_index() {
        let raw_dml_batch = raw_batch(
            31,
            vec![
                raw_req(Some("upsert"), r#"{"id":1,"name":"alice"}"#),
                raw_req(Some("upsert"), r#"{"id":"not-an-int","name":"bob"}"#),
            ],
        );
        let payload_schema = PayloadSchema::FullSchema {
            columns: test_columns(&[
                ("id", DataType::Int32, true),
                ("name", DataType::Varchar, false),
            ]),
        };

        let err = prepare_dml_batch_payload(&HeaderMap::new(), raw_dml_batch, &payload_schema)
            .unwrap_err();

        assert!(err.contains("dml_batch_id 31 item 2"));
        assert!(err.contains("failed to decode webhook JSON payload"));
    }

    #[test]
    fn test_prepare_dml_batch_payload_single_jsonb_accepts_scalar_json() {
        let raw_dml_batch = raw_batch(41, vec![raw_req(Some("upsert"), "123")]);

        let prepared_batch = prepare_dml_batch_payload(
            &HeaderMap::new(),
            raw_dml_batch,
            &PayloadSchema::SingleJsonb,
        )
        .unwrap();
        let payload = prepared_batch.payload;
        let chunk = StreamChunk::from_protobuf(payload.chunk.as_ref().unwrap()).unwrap();

        assert_eq!(prepared_batch.dml_batch_id, 41);
        assert_eq!(payload.dml_batch_id, 41);
        assert_eq!(chunk.ops(), &[Op::Insert]);
        assert!(matches!(
            chunk.rows().next().unwrap().1.datum_at(0).to_owned_datum(),
            Some(ScalarImpl::Jsonb(_))
        ));
    }
}
