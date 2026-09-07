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

use std::sync::Arc;
use std::time::Duration;

use opendal::layers::LoggingLayer;
use opendal::services::S3;
use opendal::{HttpTransporter, OperationContext, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use risingwave_common::config::ObjectStoreConfig;
use thiserror_ext::AsReport as _;

use super::{MediaType, OpendalObjectStore, new_operator};
use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::{ObjectError, ObjectResult};

impl OpendalObjectStore {
    /// create opendal s3 engine.
    pub fn new_s3_engine(
        bucket: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        // Create s3 builder.
        let mut builder = S3::default().bucket(&bucket);
        // For AWS S3, there is no need to set an endpoint; for other S3 compatible object stores, it is necessary to set this field.
        if let Ok(endpoint_url) = std::env::var("RW_S3_ENDPOINT") {
            builder = builder.endpoint(&endpoint_url);
        }

        if std::env::var("RW_IS_FORCE_PATH_STYLE").is_err() {
            builder = builder.enable_virtual_host_style();
        }

        let http_client = Self::new_http_client(&config)?;
        let transport = HttpTransporter::new(ReqwestTransport::new(http_client));

        let op = new_operator(
            &config,
            Operator::new(builder)?
                .with_context(OperationContext::new().with_http_transport(transport))
                .layer(LoggingLayer::default()),
        );

        Ok(Self {
            op,
            media_type: MediaType::S3,
            config,
            metrics,
        })
    }

    /// Creates a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub fn new_minio_engine(
        server: &str,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, mut rest) = rest.split_once('@').unwrap();

        let endpoint_prefix = if let Some(rest_stripped) = rest.strip_prefix("https://") {
            rest = rest_stripped;
            "https://"
        } else if let Some(rest_stripped) = rest.strip_prefix("http://") {
            rest = rest_stripped;
            "http://"
        } else {
            "http://"
        };
        let (address, bucket) = rest.split_once('/').unwrap();
        let builder = S3::default()
            .bucket(bucket)
            .region("custom")
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key)
            .endpoint(&format!("{}{}", endpoint_prefix, address))
            .disable_config_load();

        let http_client = Self::new_http_client(&config)?;
        let transport = HttpTransporter::new(ReqwestTransport::new(http_client));

        let op = new_operator(
            &config,
            Operator::new(builder)?
                .with_context(OperationContext::new().with_http_transport(transport))
                .layer(LoggingLayer::default()),
        );

        Ok(Self {
            op,
            media_type: MediaType::Minio,
            config,
            metrics,
        })
    }

    pub fn new_http_client(config: &ObjectStoreConfig) -> ObjectResult<reqwest::Client> {
        let mut client_builder = reqwest::ClientBuilder::new();

        if let Some(keepalive_ms) = config.s3.keepalive_ms.as_ref() {
            client_builder = client_builder.tcp_keepalive(Duration::from_millis(*keepalive_ms));
        }

        if let Some(nodelay) = config.s3.nodelay.as_ref() {
            client_builder = client_builder.tcp_nodelay(*nodelay);
        }
        client_builder.build().map_err(|e| {
            ObjectError::internal(format!("failed to build HTTP client: {}", e.as_report()))
        })
    }
}
