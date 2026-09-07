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

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::{Context, anyhow};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
#[cfg(not(madsim))]
use axum_server::tls_openssl::OpenSSLConfig;
use itertools::Itertools;
use pgwire::pg_protocol::TlsConfig;
use risingwave_common::array::{Array, ArrayBuilder, DataChunk};
use risingwave_common::catalog::TableId;
use risingwave_common::session_config::SearchPath;
use risingwave_common::types::{DataType, JsonbVal, Scalar};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::catalog::WebhookSourceInfo;
use risingwave_pb::task_service::{FastInsertRequest, FastInsertResponse};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::catalog::root_catalog::SchemaPath;
use crate::scheduler::choose_fast_insert_client;
use crate::session::SESSION_MANAGER;
use crate::webhook::payload::{build_json_access_builder, owned_row_from_payload_row};
use crate::webhook::utils::{Result, authenticate_webhook_payload, err, header_map_to_json};
pub(crate) mod payload;
pub(crate) mod utils;
pub(crate) mod websocket;
use risingwave_rpc_client::ComputeClient;

pub type Service = Arc<WebhookService>;

// We always use the `root` user to connect to the database to allow the webhook service to access all tables.
const USER: &str = "root";

#[derive(Clone, Debug)]
pub(crate) struct WebhookTableColumnDesc {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) is_pk: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum PayloadSchema {
    SingleJsonb,
    FullSchema {
        columns: Vec<WebhookTableColumnDesc>,
    },
}

impl PayloadSchema {
    fn new(columns: Vec<WebhookTableColumnDesc>) -> Self {
        if columns.len() == 1 && columns[0].data_type == DataType::Jsonb {
            Self::SingleJsonb
        } else {
            Self::FullSchema { columns }
        }
    }
}

#[derive(Clone)]
pub(crate) struct WebhookTableInsertContext {
    pub(crate) webhook_source_info: WebhookSourceInfo,
    pub(crate) table_id: TableId,
    pub(crate) table_version_id: u64,
    pub(crate) row_id_index: Option<u32>,
    pub(crate) compute_client: ComputeClient,
    pub(crate) payload_schema: PayloadSchema,
}

pub struct WebhookService {
    webhook_addr: SocketAddr,
    tls_config: Option<TlsConfig>,
    counter: AtomicU32,
}

pub(super) mod handlers {
    use jsonbb::Value;
    use risingwave_common::array::JsonbArrayBuilder;
    use risingwave_pb::task_service::fast_insert_response;

    use super::*;

    pub async fn handle_post_request(
        Extension(srv): Extension<Service>,
        headers: HeaderMap,
        Path((database, schema, table)): Path<(String, String, String)>,
        body: Bytes,
    ) -> Result<()> {
        let request_id = srv
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let WebhookTableInsertContext {
            webhook_source_info,
            table_id,
            table_version_id,
            row_id_index,
            compute_client,
            payload_schema,
        } = acquire_table_info(request_id, &database, &schema, &table).await?;
        authenticate_webhook_payload(
            header_map_to_json(&headers),
            body.as_ref(),
            &webhook_source_info,
        )
        .await?;

        let data_chunk = match &payload_schema {
            PayloadSchema::SingleJsonb => {
                generate_data_chunk(webhook_source_info.is_batched, &body)?
            }
            PayloadSchema::FullSchema { columns } => {
                let rows: Vec<_> = if webhook_source_info.is_batched {
                    body.split(|&byte| byte == b'\n')
                        .filter(|b| !b.is_empty())
                        .collect()
                } else {
                    vec![body.as_ref()]
                };
                let mut access_builder = build_json_access_builder(&headers)?;
                let mut chunk_builder = DataChunkBuilder::new(
                    columns
                        .iter()
                        .map(|column| column.data_type.clone())
                        .collect_vec(),
                    rows.len().saturating_add(1).max(1),
                );

                for row in rows {
                    let owned_row = owned_row_from_payload_row(&mut access_builder, columns, row)?;
                    assert!(chunk_builder.append_one_row(owned_row).is_none());
                }

                let Some(chunk) = chunk_builder.consume_all() else {
                    return Ok(());
                };

                chunk
            }
        };

        let fast_insert_request = FastInsertRequest {
            table_id,
            table_version_id,
            data_chunk: Some(data_chunk.to_protobuf()),
            row_id_index,
            request_id,
            wait_for_persistence: webhook_source_info.wait_for_persistence,
        };
        // execute on the compute node
        let res = execute(fast_insert_request, compute_client).await?;

        if res.status == fast_insert_response::Status::Succeeded as i32 {
            Ok(())
        } else {
            Err(err(
                anyhow!("Fast insert failed: {}", res.error_message),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }

    fn generate_data_chunk(is_batched: bool, body: &Bytes) -> Result<DataChunk> {
        let mut builder = JsonbArrayBuilder::with_type(1, DataType::Jsonb);

        if !is_batched {
            // Use builder to obtain a single column & single row DataChunk
            let json_value = Value::from_text(body).map_err(|e| {
                err(
                    anyhow!(e).context("failed to parse request body"),
                    StatusCode::UNPROCESSABLE_ENTITY,
                )
            })?;

            let jsonb_val = JsonbVal::from(json_value);
            builder.append(Some(jsonb_val.as_scalar_ref()));

            Ok(DataChunk::new(vec![builder.finish().into_ref()], 1))
        } else {
            let rows: Vec<_> = body
                .split(|&b| b == b'\n')
                .filter(|b| !b.is_empty())
                .collect();

            for row in &rows {
                let json_value = Value::from_text(row).map_err(|e| {
                    err(
                        anyhow!(e).context("failed to parse request body"),
                        StatusCode::UNPROCESSABLE_ENTITY,
                    )
                })?;
                let jsonb_val = JsonbVal::from(json_value);

                builder.append(Some(jsonb_val.as_scalar_ref()));
            }

            Ok(DataChunk::new(
                vec![builder.finish().into_ref()],
                rows.len(),
            ))
        }
    }

    pub(crate) async fn acquire_table_info(
        request_id: u32,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<WebhookTableInsertContext> {
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        let frontend_env = session_mgr.env();

        let search_path = SearchPath::default();
        let schema_path = SchemaPath::new(Some(schema), &search_path, USER);

        let (webhook_source_info, table_id, table_version_id, row_id_index, payload_schema) = {
            let reader = frontend_env.catalog_reader().read_guard();
            let (table_catalog, _schema) = reader
                .get_any_table_by_name(database, schema_path, table)
                .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

            let (columns_to_insert, row_id_index) = table_catalog.columns_to_insert();
            let payload_schema = PayloadSchema::new(
                columns_to_insert
                    .map(|(column, is_pk)| WebhookTableColumnDesc {
                        is_pk,
                        name: column.column_desc.name.clone(),
                        data_type: column.column_desc.data_type.clone(),
                    })
                    .collect(),
            );
            let row_id_index = row_id_index.map(|row_id_index| row_id_index as u32);

            let webhook_source_info = table_catalog
                .webhook_info
                .as_ref()
                .ok_or_else(|| {
                    err(
                        anyhow!("Table `{}` is not backed by a webhook source", table),
                        StatusCode::FORBIDDEN,
                    )
                })?
                .clone();
            (
                webhook_source_info,
                table_catalog.id(),
                table_catalog.version_id().expect("table must be versioned"),
                row_id_index,
                payload_schema,
            )
        };

        let compute_client = choose_fast_insert_client(table_id, frontend_env, request_id)
            .await
            .map_err(|e| {
                err(
                    anyhow!(e).context("Failed to choose a compute node for fast insert"),
                    StatusCode::SERVICE_UNAVAILABLE,
                )
            })?;

        Ok(WebhookTableInsertContext {
            webhook_source_info,
            table_id,
            table_version_id,
            row_id_index,
            compute_client,
            payload_schema,
        })
    }

    async fn execute(
        request: FastInsertRequest,
        client: ComputeClient,
    ) -> Result<FastInsertResponse> {
        let response = client.fast_insert(request).await.map_err(|e| {
            err(
                anyhow!(e).context("failed to execute on the compute node"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        Ok(response)
    }
}

pub(crate) use handlers::acquire_table_info;

impl WebhookService {
    pub fn new(webhook_addr: SocketAddr, tls_config: Option<TlsConfig>) -> Self {
        Self {
            webhook_addr,
            tls_config,
            counter: AtomicU32::new(0),
        }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);

        let webhook_router: Router = Router::new()
            .route("/{database}/{schema}/{table}", post(handle_post_request))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        // The ingest WebSocket endpoint shares the same listener as the webhook service.
        let ingest_svc = Arc::new(websocket::IngestService::new());
        let ingest_router = websocket::build_router(ingest_svc);

        let app: Router = Router::new()
            .nest("/webhook", webhook_router)
            .nest("/ingest", ingest_router)
            .layer(CompressionLayer::new());

        #[cfg(not(madsim))]
        {
            if let Some(tls_config) = &srv.tls_config {
                let config = OpenSSLConfig::from_pem_file(&tls_config.cert, &tls_config.key)
                    .context("failed to load the TLS config for the webhook service")?;
                axum_server::bind_openssl(srv.webhook_addr, config)
                    .serve(app.into_make_service())
                    .await
                    .context("failed to serve the webhook service over TLS")?;
            } else {
                let listener = TcpListener::bind(&srv.webhook_addr)
                    .await
                    .context("failed to bind the webhook address")?;
                axum::serve(listener, app)
                    .await
                    .context("failed to serve the webhook service")?;
            }
        }

        Ok(())
    }
}
