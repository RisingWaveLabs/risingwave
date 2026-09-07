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

use std::borrow::BorrowMut;
use std::cmp;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use await_tree::{InstrumentAwait, SpanExt};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, CompletedMultipartUpload,
    CompletedPart, Delete, ExpirationStatus, LifecycleRule, LifecycleRuleFilter, ObjectIdentifier,
};
use aws_smithy_http::futures_stream_adapter::FuturesStreamCompatByteStream;
use aws_smithy_http_client::{
    Builder as SmithyHttpClientBuilder, Connector as SmithyConnector, tls,
};
use aws_smithy_runtime_api::client::http::HttpClient;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::BytesMut;
use fail::fail_point;
use futures::future::{BoxFuture, FutureExt, try_join_all};
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::range::RangeBoundsExt;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;

use super::object_metrics::ObjectStoreMetrics;
use super::{
    Bytes, ObjectError, ObjectErrorInner, ObjectMetadata, ObjectRangeBounds, ObjectResult,
    ObjectStore, StreamingUploader, prefix, retry_request,
};
use crate::object::{
    ObjectDataStream, ObjectMetadataIter, OperationType, try_update_failure_metric,
};

type PartId = i32;

/// MinIO and S3 share the same minimum part ID and part size.
const MIN_PART_ID: PartId = 1;
/// Stop multipart uploads that don't complete within a specified number of days after being
/// initiated. (Day is the smallest granularity)
const S3_INCOMPLETE_MULTIPART_UPLOAD_RETENTION_DAYS: i32 = 1;

/// S3 multipart upload handle. The multipart upload is not initiated until the first part is
/// available for upload.
///
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html>
pub struct S3StreamingUploader {
    client: Client,
    part_size: usize,
    bucket: String,
    /// The key of the object.
    key: String,
    /// The identifier of multipart upload task for S3.
    upload_id: Option<String>,
    /// Next part ID.
    next_part_id: PartId,
    /// Join handles for part uploads.
    join_handles: Vec<JoinHandle<ObjectResult<(PartId, UploadPartOutput)>>>,
    /// Buffer for data. It will store at least `part_size` bytes of data before wrapping itself
    /// into a stream and upload to object store as a part.
    buf: Vec<Bytes>,
    /// Length of the data that have not been uploaded to S3.
    not_uploaded_len: usize,
    /// To record metrics for uploading part.
    metrics: Arc<ObjectStoreMetrics>,

    config: Arc<ObjectStoreConfig>,
}

impl S3StreamingUploader {
    const MEDIA_TYPE: &'static str = "s3";

    pub fn new(
        client: Client,
        bucket: String,
        key: String,
        metrics: Arc<ObjectStoreMetrics>,
        config: Arc<ObjectStoreConfig>,
    ) -> S3StreamingUploader {
        /// The minimum number of bytes that is buffered before they are uploaded as a part.
        /// Its value must be greater than the minimum part size of 5MiB.
        ///
        /// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
        const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;
        let part_size = config.upload_part_size.clamp(MIN_PART_SIZE, MAX_PART_SIZE);

        Self {
            client,
            bucket,
            part_size,
            key,
            upload_id: None,
            next_part_id: MIN_PART_ID,
            join_handles: Default::default(),
            buf: Default::default(),
            not_uploaded_len: 0,
            metrics,
            config,
        }
    }

    async fn upload_next_part(&mut self) -> ObjectResult<()> {
        let operation_type = OperationType::StreamingUpload;
        let operation_type_str = operation_type.as_str();

        // Lazily create multipart upload.
        if self.upload_id.is_none() {
            let builder = || async {
                self.client
                    .create_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .send()
                    .await
                    .map_err(|err| {
                        set_error_should_retry::<CreateMultipartUploadError>(
                            self.config.clone(),
                            err.into(),
                        )
                    })
            };

            let resp = retry_request(
                builder,
                &self.config,
                OperationType::StreamingUploadInit,
                self.metrics.clone(),
                Self::MEDIA_TYPE,
            )
            .await;

            try_update_failure_metric(
                &self.metrics,
                &resp,
                OperationType::StreamingUploadInit.as_str(),
            );

            self.upload_id = Some(resp?.upload_id.unwrap());
        }

        // Get the data to upload for the next part.
        let data = self.buf.drain(..).collect_vec();
        let len = self.not_uploaded_len;
        debug_assert_eq!(
            data.iter().map(|b| b.len()).sum::<usize>(),
            self.not_uploaded_len
        );

        // Update part id.
        let part_id = self.next_part_id;
        self.next_part_id += 1;

        // Clone the variables to be passed into the upload join handle.
        let client_cloned = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone().unwrap();

        let metrics = self.metrics.clone();
        metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(len as f64);
        let config = self.config.clone();

        self.join_handles.push(tokio::spawn(async move {
            let _timer = metrics
                .operation_latency
                .with_label_values(&["s3", operation_type_str])
                .start_timer();

            let builder = || async {
                client_cloned
                    .upload_part()
                    .bucket(bucket.clone())
                    .key(key.clone())
                    .upload_id(upload_id.clone())
                    .part_number(part_id)
                    .body(get_upload_body(data.clone()))
                    .content_length(len as i64)
                    .send()
                    .await
                    .map_err(|err| {
                        set_error_should_retry::<CreateMultipartUploadError>(
                            config.clone(),
                            err.into(),
                        )
                    })
            };

            let res = retry_request(
                builder,
                &config,
                operation_type,
                metrics.clone(),
                Self::MEDIA_TYPE,
            )
            .await;
            try_update_failure_metric(&metrics, &res, operation_type_str);
            Ok((part_id, res?))
        }));

        Ok(())
    }

    async fn flush_multipart_and_complete(&mut self) -> ObjectResult<()> {
        let operation_type = OperationType::StreamingUploadFinish;

        if !self.buf.is_empty() {
            self.upload_next_part().await?;
        }

        // If any part fails to upload, abort the upload.
        let join_handles = self.join_handles.drain(..).collect_vec();

        let mut uploaded_parts = Vec::with_capacity(join_handles.len());
        for result in try_join_all(join_handles)
            .await
            .map_err(ObjectError::internal)?
        {
            uploaded_parts.push(result?);
        }

        let completed_parts = Some(
            uploaded_parts
                .iter()
                .map(|(part_id, output)| {
                    CompletedPart::builder()
                        .set_e_tag(output.e_tag.clone())
                        .set_part_number(Some(*part_id))
                        .build()
                })
                .collect_vec(),
        );

        let builder = || async {
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(self.upload_id.as_ref().unwrap())
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(completed_parts.clone())
                        .build(),
                )
                .send()
                .await
                .map_err(|err| {
                    set_error_should_retry::<CompleteMultipartUploadError>(
                        self.config.clone(),
                        err.into(),
                    )
                })
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.metrics.clone(),
            Self::MEDIA_TYPE,
        )
        .await;
        try_update_failure_metric(&self.metrics, &res, operation_type.as_str());
        let _res = res?;

        Ok(())
    }

    async fn abort_multipart_upload(&self) -> ObjectResult<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(self.upload_id.as_ref().unwrap())
            .send()
            .await
            .map_err(|err| {
                set_error_should_retry::<AbortMultipartUploadError>(self.config.clone(), err.into())
            })?;
        Ok(())
    }
}

impl StreamingUploader for S3StreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        fail_point!("s3_write_bytes_err", |_| Err(ObjectError::internal(
            "s3 write bytes error"
        )));
        let data_len = data.len();
        self.not_uploaded_len += data_len;
        self.buf.push(data);

        if self.not_uploaded_len >= self.part_size {
            self.upload_next_part()
                .instrument_await("s3_upload_next_part".verbose())
                .await?;
            self.not_uploaded_len = 0;
        }
        Ok(())
    }

    /// If the multipart upload has not been initiated, we can use `PutObject` instead to save the
    /// `CreateMultipartUpload` and `CompleteMultipartUpload` requests. Otherwise flush the
    /// remaining data of the buffer to S3 as a new part.
    async fn finish(mut self) -> ObjectResult<()> {
        fail_point!("s3_finish_streaming_upload_err", |_| Err(
            ObjectError::internal("s3 finish streaming upload error")
        ));

        if self.upload_id.is_none() {
            debug_assert!(self.join_handles.is_empty());
            if self.buf.is_empty() {
                debug_assert_eq!(self.not_uploaded_len, 0);
                Err(ObjectError::internal("upload empty object"))
            } else {
                let operation_type = OperationType::Upload;
                let builder = || async {
                    self.client
                        .put_object()
                        .bucket(&self.bucket)
                        .body(get_upload_body(self.buf.clone()))
                        .content_length(self.not_uploaded_len as i64)
                        .key(&self.key)
                        .send()
                        .instrument_await("s3_put_object".verbose())
                        .await
                        .map_err(|err| {
                            set_error_should_retry::<PutObjectError>(
                                self.config.clone(),
                                err.into(),
                            )
                        })
                };

                let res = retry_request(
                    builder,
                    &self.config,
                    operation_type,
                    self.metrics.clone(),
                    Self::MEDIA_TYPE,
                )
                .await;
                try_update_failure_metric(&self.metrics, &res, operation_type.as_str());
                res?;
                Ok(())
            }
        } else {
            match self
                .flush_multipart_and_complete()
                .instrument_await("s3_flush_multipart_and_complete".verbose())
                .await
            {
                Err(e) => {
                    tracing::warn!(key = self.key, error = %e.as_report(), "Failed to upload object");
                    self.abort_multipart_upload().await?;
                    Err(e)
                }
                _ => Ok(()),
            }
        }
    }

    fn get_memory_usage(&self) -> u64 {
        self.part_size as u64
    }
}

fn get_upload_body(data: Vec<Bytes>) -> ByteStream {
    // `ByteStream` is retryable when created from in-memory data.
    // This code path is used for non-multipart uploads, so a copy is acceptable.
    let total_len: usize = data.iter().map(|b| b.len()).sum();
    let mut buf = BytesMut::with_capacity(total_len);
    for chunk in data {
        buf.extend_from_slice(&chunk);
    }
    ByteStream::from(buf.freeze())
}

/// Object store with S3 backend
/// The full path to a file on S3 would be `s3://bucket/<data_directory>/prefix/file`
#[derive(Clone)]
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    /// For S3 specific metrics.
    metrics: Arc<ObjectStoreMetrics>,

    config: Arc<ObjectStoreConfig>,
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    type StreamingUploader = S3StreamingUploader;

    fn get_object_prefix(&self, obj_id: u64, _use_new_object_prefix_strategy: bool) -> String {
        // Delegate to static method to avoid creating an `S3ObjectStore` in unit test.
        // Using aws s3 sdk as object storage, the object prefix will be divided by default.
        prefix::s3::get_object_prefix(obj_id)
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("s3_upload_err", |_| Err(ObjectError::internal(
            "s3 upload error"
        )));
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .body(ByteStream::from(obj))
                .key(path)
                .send()
                .await
                .map_err(|err| {
                    set_error_should_retry::<PutObjectError>(self.config.clone(), err.into())
                })?;
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        fail_point!("s3_streaming_upload_err", |_| Err(ObjectError::internal(
            "s3 streaming upload error"
        )));
        Ok(S3StreamingUploader::new(
            self.client.clone(),
            self.bucket.clone(),
            path.to_owned(),
            self.metrics.clone(),
            self.config.clone(),
        ))
    }

    /// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        fail_point!("s3_read_err", |_| Err(ObjectError::internal(
            "s3 read error"
        )));

        let val = match self.obj_store_request(path, range.clone()).send().await {
            Ok(resp) => resp
                .body
                .collect()
                .await
                .map_err(|err| {
                    set_error_should_retry::<GetObjectError>(self.config.clone(), err.into())
                })?
                .into_bytes(),
            Err(sdk_err) => {
                return Err(set_error_should_retry::<GetObjectError>(
                    self.config.clone(),
                    sdk_err.into(),
                ));
            }
        };

        if let Some(len) = range.len()
            && len != val.len()
        {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                len,
                val.len(),
                path,
                range,
            )));
        }

        Ok(val)
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        fail_point!("s3_metadata_err", |_| Err(ObjectError::internal(
            "s3 metadata error"
        )));
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|err| {
                set_error_should_retry::<HeadObjectError>(self.config.clone(), err.into())
            })?;
        Ok(ObjectMetadata {
            key: path.to_owned(),
            last_modified: resp
                .last_modified()
                .expect("last_modified required")
                .as_secs_f64(),
            total_size: resp.content_length.unwrap_or_default() as usize,
        })
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> ObjectResult<ObjectDataStream> {
        fail_point!("s3_streaming_read_init_err", |_| Err(
            ObjectError::internal("s3 streaming read init error")
        ));

        let resp = match self.obj_store_request(path, range.clone()).send().await {
            Ok(resp) => resp,
            Err(sdk_err) => {
                return Err(set_error_should_retry::<GetObjectError>(
                    self.config.clone(),
                    sdk_err.into(),
                ));
            }
        };

        let reader = FuturesStreamCompatByteStream::new(resp.body);

        Ok(Box::pin(
            reader
                .into_stream()
                .map(|item| item.map_err(ObjectError::from)),
        ))
    }

    /// Permanently deletes the whole object.
    /// According to Amazon S3, this will simply return Ok if the object does not exist.
    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("s3_delete_err", |_| Err(ObjectError::internal(
            "s3 delete error"
        )));
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|err| {
                set_error_should_retry::<DeleteObjectError>(self.config.clone(), err.into())
            })?;
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    ///
    /// Uses AWS' `DeleteObjects` API. See [AWS Docs](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) for more details.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        // AWS restricts the number of objects per request to 1000.
        const MAX_LEN: usize = 1000;
        let mut all_errors = Vec::new();

        // If needed, split given set into subsets of size with no more than `MAX_LEN` objects.
        for start_idx /* inclusive */ in (0..paths.len()).step_by(MAX_LEN) {
            let end_idx /* exclusive */ = cmp::min(paths.len(), start_idx + MAX_LEN);
            let slice = &paths[start_idx..end_idx];
            // Create identifiers from paths.
            let mut obj_ids = Vec::with_capacity(slice.len());
            for path in slice {
                obj_ids.push(ObjectIdentifier::builder().key(path).build().unwrap());
            }

            // Build and submit request to delete objects.
            let delete_builder = Delete::builder().set_objects(Some(obj_ids));
            let delete_output = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete_builder.build().unwrap()).send()
                .await.map_err(|err| {
                    set_error_should_retry::<DeleteObjectsError>(self.config.clone(),err.into())
                })?;

            // Check if there were errors.
            if !delete_output.errors().is_empty() {
                all_errors.append(&mut delete_output.errors().to_owned());
            }
        }
        if !all_errors.is_empty() {
            return Err(ObjectError::internal(format!(
                "DeleteObjects request returned exception for some objects: {:?}",
                all_errors
            )));
        }

        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        Ok(Box::pin(
            S3ObjectIter::new(
                self.client.clone(),
                self.bucket.clone(),
                prefix.to_owned(),
                self.config.clone(),
                start_after,
            )
            .take(limit.unwrap_or(usize::MAX)),
        ))
    }

    fn store_media_type(&self) -> &'static str {
        "s3"
    }
}

impl S3ObjectStore {
    pub fn new_http_client(config: &ObjectStoreConfig) -> impl HttpClient + use<> {
        let nodelay = config.s3.nodelay;
        let pool_idle_timeout = config.s3.keepalive_ms.map(Duration::from_millis);

        // Use the Smithy default (hyper 1.x) client stack. This avoids the deprecated hyper 0.14
        // connector path (`aws-smithy-runtime/tls-rustls`).
        let tls_provider = tls::Provider::Rustls(tls::rustls_provider::CryptoMode::AwsLc);
        SmithyHttpClientBuilder::new().build_with_connector_fn(move |settings, _components| {
            let mut builder = SmithyConnector::builder();

            if let Some(settings) = settings {
                builder = builder.connector_settings(settings.clone());
            }

            if let Some(nodelay) = nodelay {
                builder = builder.enable_tcp_nodelay(nodelay);
            }

            if let Some(pool_idle_timeout) = pool_idle_timeout {
                builder = builder.pool_idle_timeout(pool_idle_timeout);
            }

            builder.tls_provider(tls_provider.clone()).build()
        })
    }

    /// Creates an S3 object store from environment variable.
    ///
    /// See [AWS Docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) on how to provide credentials and region from env variable. If you are running compute-node on EC2, no configuration is required.
    pub async fn new_with_config(
        bucket: String,
        metrics: Arc<ObjectStoreMetrics>,
        config: Arc<ObjectStoreConfig>,
    ) -> Self {
        let sdk_config_loader = aws_config::from_env().http_client(Self::new_http_client(&config));

        // Retry 3 times if we get server-side errors or throttling errors
        let client = match std::env::var("RW_S3_ENDPOINT") {
            Ok(endpoint) => {
                // s3 compatible storage
                let is_force_path_style = match std::env::var("RW_IS_FORCE_PATH_STYLE") {
                    Ok(value) => value == "true",
                    Err(_) => false,
                };

                let sdk_config = sdk_config_loader.load().await;
                #[cfg(madsim)]
                let client = Client::new(&sdk_config);
                #[cfg(not(madsim))]
                let client = Client::from_conf(
                    aws_sdk_s3::config::Builder::from(&sdk_config)
                        .endpoint_url(endpoint)
                        .force_path_style(is_force_path_style)
                        .identity_cache(
                            aws_sdk_s3::config::IdentityCache::lazy()
                                .load_timeout(Duration::from_secs(
                                    config.s3.identity_resolution_timeout_s,
                                ))
                                .build(),
                        )
                        .stalled_stream_protection(
                            aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
                        )
                        .build(),
                );
                client
            }
            Err(_) => {
                // s3
                let sdk_config = sdk_config_loader.load().await;
                #[cfg(madsim)]
                let client = Client::new(&sdk_config);
                #[cfg(not(madsim))]
                let client = Client::from_conf(
                    aws_sdk_s3::config::Builder::from(&sdk_config)
                        .identity_cache(
                            aws_sdk_s3::config::IdentityCache::lazy()
                                .load_timeout(Duration::from_secs(
                                    config.s3.identity_resolution_timeout_s,
                                ))
                                .build(),
                        )
                        .stalled_stream_protection(
                            aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
                        )
                        .build(),
                );
                client
            }
        };

        Self {
            client,
            bucket,
            metrics,
            config,
        }
    }

    /// Creates a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub async fn new_minio_engine(
        server: &str,
        metrics: Arc<ObjectStoreMetrics>,
        object_store_config: Arc<ObjectStoreConfig>,
    ) -> Self {
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

        #[cfg(madsim)]
        let builder = aws_sdk_s3::config::Builder::new().credentials_provider(
            Credentials::from_keys(access_key_id, secret_access_key, None),
        );
        #[cfg(not(madsim))]
        let builder = aws_sdk_s3::config::Builder::from(
            &aws_config::ConfigLoader::default()
                // FIXME: https://github.com/awslabs/aws-sdk-rust/issues/973
                .credentials_provider(Credentials::from_keys(
                    access_key_id,
                    secret_access_key,
                    None,
                ))
                .load()
                .await,
        )
        .force_path_style(true)
        .identity_cache(
            aws_sdk_s3::config::IdentityCache::lazy()
                .load_timeout(Duration::from_secs(
                    object_store_config.s3.identity_resolution_timeout_s,
                ))
                .build(),
        )
        .http_client(Self::new_http_client(&object_store_config))
        .behavior_version_latest()
        .stalled_stream_protection(aws_sdk_s3::config::StalledStreamProtectionConfig::disabled());
        let config = builder
            .region(Region::new("custom"))
            .endpoint_url(format!("{}{}", endpoint_prefix, address))
            .build();
        let client = Client::from_conf(config);

        Self {
            client,
            bucket: bucket.to_owned(),
            metrics,
            config: object_store_config,
        }
    }

    /// Generates an HTTP GET request to download the object specified in `path`. If given,
    /// `start_pos` and `end_pos` specify the first and last byte to download, respectively. Both
    /// are inclusive and 0-based. For example, set `start_pos = 0` and `end_pos = 7` to download
    /// the first 8 bytes. If neither is given, the request will download the whole object.
    fn obj_store_request(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> GetObjectFluentBuilder {
        let req = self.client.get_object().bucket(&self.bucket).key(path);
        if range.is_full() {
            return req;
        }

        let start = range.start().map(|v| v.to_string()).unwrap_or_default();
        let end = range.end().map(|v| (v - 1).to_string()).unwrap_or_default(); // included

        req.range(format!("bytes={}-{}", start, end))
    }

    // When multipart upload is aborted, if any part uploads are in progress, those part uploads
    // might or might not succeed. As a result, these parts will remain in the bucket and be
    // charged for part storage. Therefore, we need to configure the bucket to purge stale
    // parts.
    //
    /// Note: This configuration only works for S3. MinIO automatically enables this feature, and it
    /// is not configurable with S3 sdk. To verify that this feature is enabled, use `mc admin
    /// config get <alias> api`.
    ///
    /// Reference:
    /// - S3
    ///   - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html>
    ///   - <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html>
    /// - MinIO
    ///   - <https://github.com/minio/minio/issues/15681#issuecomment-1245126561>
    pub async fn configure_bucket_lifecycle(&self, data_directory: &str) -> bool {
        // Check if lifecycle is already configured to avoid overriding existing configuration.
        let bucket = self.bucket.as_str();
        let mut configured_rules = vec![];
        let get_config_result = self
            .client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await;
        let mut is_expiration_configured = false;

        if let Ok(config) = &get_config_result {
            for rule in config.rules() {
                if rule.expiration().is_some() {
                    // When both of the conditions are met, it is considered that there is a risk of data deletion.
                    //
                    // 1. expiration status rule is enabled
                    // 2. (a) prefix filter is not set
                    // or (b) prefix filter is set to the data directory of RisingWave.
                    //
                    // P.S. 1 && (2a || 2b)
                    is_expiration_configured |= rule.status == ExpirationStatus::Enabled // 1
                    && match rule.filter().as_ref() {
                        // 2a
                        None => true,
                        // 2b
                        Some(filter) => {
                            // `LifecycleRuleFilter` is a struct in newer aws-sdk-s3 versions.
                            //
                            // Treat an "empty filter" as applying to the entire bucket, which is
                            // considered risky for RisingWave data deletion.
                            let is_empty = filter.prefix().is_none()
                                && filter.tag().is_none()
                                && filter.and().is_none()
                                && filter.object_size_greater_than().is_none()
                                && filter.object_size_less_than().is_none();
                            if is_empty {
                                true
                            } else {
                                filter
                                    .prefix()
                                    .is_some_and(|prefix| data_directory.starts_with(prefix))
                            }
                        }
                    };

                    if matches!(rule.status(), ExpirationStatus::Enabled)
                        && rule.abort_incomplete_multipart_upload().is_some()
                    {
                        configured_rules.push(rule);
                    }
                }
            }
        }

        if !configured_rules.is_empty() {
            tracing::info!(
                "S3 bucket {} has already configured AbortIncompleteMultipartUpload: {:?}",
                bucket,
                configured_rules,
            );
        } else {
            let bucket_lifecycle_rule = LifecycleRule::builder()
                .id("abort-incomplete-multipart-upload")
                .status(ExpirationStatus::Enabled)
                // Empty prefix means "all objects".
                .filter(LifecycleRuleFilter::builder().prefix("").build())
                .abort_incomplete_multipart_upload(
                    AbortIncompleteMultipartUpload::builder()
                        .days_after_initiation(S3_INCOMPLETE_MULTIPART_UPLOAD_RETENTION_DAYS)
                        .build(),
                )
                .build()
                .unwrap();
            let bucket_lifecycle_config = BucketLifecycleConfiguration::builder()
                .rules(bucket_lifecycle_rule)
                .build()
                .unwrap();
            if self
                .client
                .put_bucket_lifecycle_configuration()
                .bucket(bucket)
                .lifecycle_configuration(bucket_lifecycle_config)
                .send()
                .await
                .is_ok()
            {
                tracing::info!(
                    "S3 bucket {:?} is configured to automatically purge abandoned MultipartUploads after {} days",
                    bucket,
                    S3_INCOMPLETE_MULTIPART_UPLOAD_RETENTION_DAYS,
                );
            } else {
                tracing::warn!(
                    "Failed to configure life cycle rule for S3 bucket: {:?}. It is recommended to configure it manually to avoid unnecessary storage cost.",
                    bucket
                );
            }
        }
        if is_expiration_configured {
            tracing::info!(
                "S3 bucket {} has already configured the expiration for the lifecycle.",
                bucket,
            );
        }
        is_expiration_configured
    }
}

struct S3ObjectIter {
    buffer: VecDeque<ObjectMetadata>,
    client: Client,
    bucket: String,
    prefix: String,
    next_continuation_token: Option<String>,
    is_truncated: Option<bool>,
    #[expect(clippy::type_complexity)]
    send_future: Option<
        BoxFuture<
            'static,
            Result<(Vec<ObjectMetadata>, Option<String>, Option<bool>), ObjectError>,
        >,
    >,

    config: Arc<ObjectStoreConfig>,
    start_after: Option<String>,
}

impl S3ObjectIter {
    fn new(
        client: Client,
        bucket: String,
        prefix: String,
        config: Arc<ObjectStoreConfig>,
        start_after: Option<String>,
    ) -> Self {
        Self {
            buffer: VecDeque::default(),
            client,
            bucket,
            prefix,
            next_continuation_token: None,
            is_truncated: Some(true),
            send_future: None,
            config,
            start_after,
        }
    }
}

impl Stream for S3ObjectIter {
    type Item = ObjectResult<ObjectMetadata>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(e) = self.buffer.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        if let Some(f) = self.send_future.as_mut() {
            return match ready!(f.poll_unpin(cx)) {
                Ok((more, next_continuation_token, is_truncated)) => {
                    self.next_continuation_token = next_continuation_token;
                    self.is_truncated = is_truncated;
                    self.buffer.extend(more);
                    self.send_future = None;
                    // only the first request may set start_after
                    self.start_after = None;
                    self.poll_next(cx)
                }
                Err(e) => {
                    self.send_future = None;
                    Poll::Ready(Some(Err(e)))
                }
            };
        }
        if !self.is_truncated.unwrap_or_default() {
            return Poll::Ready(None);
        }
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix);
        #[cfg(not(madsim))]
        if let Some(start_after) = self.start_after.as_ref() {
            request = request.start_after(start_after);
        }
        if let Some(continuation_token) = self.next_continuation_token.as_ref() {
            request = request.continuation_token(continuation_token);
        }
        let config = self.config.clone();
        let f = async move {
            match request.send().await {
                Ok(r) => {
                    let more = r
                        .contents()
                        .iter()
                        .map(|obj| ObjectMetadata {
                            key: obj.key().expect("key required").to_owned(),
                            last_modified: obj
                                .last_modified()
                                .map(|l| l.as_secs_f64())
                                .unwrap_or(0f64),
                            total_size: obj.size().unwrap_or_default() as usize,
                        })
                        .collect_vec();
                    let is_truncated = r.is_truncated;
                    let next_continuation_token = r.next_continuation_token;
                    Ok((more, next_continuation_token, is_truncated))
                }
                Err(e) => Err(set_error_should_retry::<ListObjectsV2Error>(
                    config,
                    e.into(),
                )),
            }
        };
        self.send_future = Some(Box::pin(f));
        self.poll_next(cx)
    }
}

fn set_error_should_retry<E>(config: Arc<ObjectStoreConfig>, object_err: ObjectError) -> ObjectError
where
    E: ProvideErrorMetadata + Into<BoxError> + Sync + Send + std::error::Error + 'static,
{
    let not_found = object_err.is_object_not_found_error();

    if not_found {
        return object_err;
    }

    let mut inner = object_err.into_inner();
    match inner.borrow_mut() {
        ObjectErrorInner::S3 {
            should_retry,
            inner,
        } => {
            let sdk_err = inner
                .as_ref()
                .downcast_ref::<SdkError<E, aws_smithy_runtime_api::http::Response<SdkBody>>>();

            let err_should_retry = match sdk_err {
                Some(SdkError::DispatchFailure(e)) => {
                    if e.is_timeout() {
                        tracing::warn!(target: "http_timeout_retry", "{e:?} occurs, retry S3 get_object request.");
                        true
                    } else {
                        false
                    }
                }

                Some(SdkError::ServiceError(e)) => match e.err().code() {
                    None => {
                        if config.s3.developer.retry_unknown_service_error
                            || config.s3.retry_unknown_service_error
                        {
                            tracing::warn!(target: "unknown_service_error", "{e:?} occurs, retry S3 get_object request.");
                            true
                        } else {
                            false
                        }
                    }
                    Some(code) => {
                        if config
                            .s3
                            .developer
                            .retryable_service_error_codes
                            .iter()
                            .any(|s| s.as_str().eq_ignore_ascii_case(code))
                        {
                            tracing::warn!(target: "retryable_service_error", "{e:?} occurs, retry S3 get_object request.");
                            true
                        } else {
                            false
                        }
                    }
                },

                Some(SdkError::TimeoutError(_err)) => true,

                _ => false,
            };

            *should_retry = err_should_retry;
        }

        _ => unreachable!(),
    }

    ObjectError::from(inner)
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use crate::object::prefix::s3::{NUM_BUCKET_PREFIXES, get_object_prefix};

    fn get_hash_of_object(obj_id: u64) -> u32 {
        let crc_hash = crc32fast::hash(&obj_id.to_be_bytes());
        crc_hash % NUM_BUCKET_PREFIXES
    }

    #[tokio::test]
    async fn test_get_object_prefix() {
        for obj_id in 0..99999 {
            let hash = get_hash_of_object(obj_id);
            let prefix = get_object_prefix(obj_id);
            assert_eq!(format!("{}/", hash), prefix);
        }

        let obj_prefix = String::default();
        let path = format!("{}/{}{}.data", "hummock_001", obj_prefix, 101);
        assert_eq!("hummock_001/101.data", path);
    }
}
