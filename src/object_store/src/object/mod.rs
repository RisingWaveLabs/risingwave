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

#![expect(
    unexpected_cfgs,
    reason = "feature(hdfs-backend) is banned https://github.com/risingwavelabs/risingwave/pull/7875"
)]

pub mod sim;
use std::io;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::{Buf, Bytes};

pub mod mem;
pub use mem::*;

pub mod opendal_engine;
pub use opendal_engine::*;

pub mod s3;
use await_tree::{InstrumentAwait, SpanExt};
use futures::stream::BoxStream;
use futures::{Future, Stream, StreamExt};
use futures_async_stream::try_stream;
use pin_project_lite::pin_project;
pub use risingwave_common::config::ObjectStoreConfig;
pub use s3::*;

pub mod error;
pub mod object_metrics;

pub mod prefix;

pub use error::*;
use object_metrics::ObjectStoreMetrics;
use risingwave_common::util::retry::exponential_backoff;
use thiserror_ext::AsReport;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_retry::strategy::jitter;

#[cfg(madsim)]
use self::sim::SimObjectStore;

pub type ObjectStoreRef = Arc<ObjectStoreImpl>;
pub type ObjectStreamingUploader = StreamingUploaderImpl;

pub trait ObjectRangeBounds = RangeBounds<usize> + Clone + Send + Sync + std::fmt::Debug + 'static;

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMetadata {
    // Full path
    pub key: String,
    // Seconds since unix epoch.
    pub last_modified: f64,
    pub total_size: usize,
}

pub trait StreamingUploader: Send {
    #[expect(async_fn_in_trait)]
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()>;

    #[expect(async_fn_in_trait)]
    async fn finish(self) -> ObjectResult<()>;

    fn get_memory_usage(&self) -> u64;
}

/// The implementation must be thread-safe.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    type StreamingUploader: StreamingUploader;
    /// Get the key prefix for object, the prefix is determined by the type of object store and `devise_object_prefix`.
    fn get_object_prefix(&self, obj_id: u64, use_new_object_prefix_strategy: bool) -> String;

    /// Uploads the object to `ObjectStore`.
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()>;

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader>;

    /// If objects are PUT using a multipart upload, it's a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// <https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2>
    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes>;

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        read_range: impl ObjectRangeBounds,
    ) -> ObjectResult<ObjectDataStream>;

    /// Obtains the object metadata.
    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata>;

    /// Deletes blob permanently.
    async fn delete(&self, path: &str) -> ObjectResult<()>;

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()>;

    fn monitored(
        self,
        metrics: Arc<ObjectStoreMetrics>,
        config: Arc<ObjectStoreConfig>,
    ) -> MonitoredObjectStore<Self>
    where
        Self: Sized,
    {
        MonitoredObjectStore::new(self, metrics, config)
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter>;

    fn store_media_type(&self) -> &'static str;
}

#[cfg(not(madsim))]
macro_rules! for_all_object_store {
    ($macro:ident $($args:tt)*) => {
        $macro! {
            {
                { InMem, InMemObjectStore },
                { Opendal, OpendalObjectStore },
                { S3, S3ObjectStore }
            }
            $($args)*
        }
    }
}

#[cfg(madsim)]
macro_rules! for_all_object_store {
    ($macro:ident $($args:tt)*) => {
        $macro! {
            {
                { InMem, InMemObjectStore },
                { Opendal, OpendalObjectStore },
                { S3, S3ObjectStore },
                { Sim, SimObjectStore }
            }
            $($args)*
        }
    }
}

macro_rules! enum_map {
    (
        {
            $(
                {$variant:ident, $_type_name:ty}
            ),*
        },
        $object_store:expr,
        $var_name:ident,
        $func:expr
    ) => {
        match $object_store {
            $(
                ObjectStoreEnum::$variant($var_name) => ObjectStoreEnum::$variant({
                    $func
                }),
            )*
        }
    };
    ($object_store:expr, |$var_name:ident| $func:expr) => {
        for_all_object_store! {
            enum_map, $object_store, $var_name, $func
        }
    };
}

macro_rules! dispatch_object_store_enum {
    (
        {
            $(
                {$variant:ident, $_type_name:ty}
            ),*
        },
        $object_store:expr,
        $var_name:ident,
        $func:expr
    ) => {
        match $object_store {
            $(
                ObjectStoreEnum::$variant($var_name) => {
                    $func
                },
            )*
        }
    };
    ($object_store:expr, |$var_name:ident| $func:expr) => {
        for_all_object_store! {
            dispatch_object_store_enum, $object_store, $var_name, $func
        }
    };
}

macro_rules! define_object_store_impl {
    () => {
        for_all_object_store! {
            define_object_store_impl
        }
    };
    (
        {$(
            {$variant:ident, $type_name:ty}
        ),*}
    ) => {
        pub enum ObjectStoreEnum<
            $($variant),*
        > {
            $(
                $variant($variant),
            )*
        }

        pub type ObjectStoreImpl = ObjectStoreEnum<
            $(
                MonitoredObjectStore<$type_name>,
            )*
        >;

        pub type StreamingUploaderImpl = ObjectStoreEnum<
            $(
                MonitoredStreamingUploader<<$type_name as ObjectStore>::StreamingUploader>
            ),*
        >;
    };
}

define_object_store_impl!();

/// This macro routes the object store operation to the real implementation by the `ObjectStoreImpl`
/// enum type and the `path`.
///
/// Except for `InMem`,the operation should be performed on remote object store.
macro_rules! object_store_impl_method_body {
    // with await
    ($object_store:expr, $method_name:ident ($($args:expr),*).await) => {
        {
            dispatch_object_store_enum! {$object_store, |os| {
                os.$method_name($($args),*).await
            }}
        }
    };
    // no await
    ($object_store:expr, $method_name:ident ($(, $args:expr)*)) => {
        {
            dispatch_object_store_enum! {$object_store, |os| {
                os.$method_name($($args),*)
            }}
        }
    };
}

impl StreamingUploaderImpl {
    pub async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        object_store_impl_method_body!(self, write_bytes(data).await)
    }

    pub async fn finish(self) -> ObjectResult<()> {
        object_store_impl_method_body!(self, finish().await)
    }

    pub fn get_memory_usage(&self) -> u64 {
        object_store_impl_method_body!(self, get_memory_usage())
    }
}

impl ObjectStoreImpl {
    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        object_store_impl_method_body!(self, upload(path, obj).await)
    }

    pub async fn streaming_upload(&self, path: &str) -> ObjectResult<ObjectStreamingUploader> {
        Ok(enum_map!(self, |store| {
            store.streaming_upload(path).await?
        }))
    }

    pub async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        object_store_impl_method_body!(self, read(path, range).await)
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        object_store_impl_method_body!(self, metadata(path).await)
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    pub async fn streaming_read(
        &self,
        path: &str,
        start_loc: impl ObjectRangeBounds,
    ) -> ObjectResult<MonitoredStreamingReader> {
        object_store_impl_method_body!(self, streaming_read(path, start_loc).await)
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        object_store_impl_method_body!(self, delete(path).await)
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    ///
    /// If a hybrid storage is used, the method will first attempt to delete objects in local
    /// storage. Only if that is successful, it will remove objects from remote storage.
    pub async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        object_store_impl_method_body!(self, delete_objects(paths).await)
    }

    pub async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        object_store_impl_method_body!(self, list(prefix, start_after, limit).await)
    }

    pub fn get_object_prefix(&self, obj_id: u64, use_new_object_prefix_strategy: bool) -> String {
        dispatch_object_store_enum!(self, |store| store
            .inner
            .get_object_prefix(obj_id, use_new_object_prefix_strategy))
    }

    pub fn media_type(&self) -> &'static str {
        object_store_impl_method_body!(self, media_type())
    }
}

fn try_update_failure_metric<T>(
    metrics: &Arc<ObjectStoreMetrics>,
    result: &ObjectResult<T>,
    operation_type: &'static str,
) {
    if let Err(e) = &result {
        tracing::error!(error = %e.as_report(), "{} failed", operation_type);
        metrics
            .failure_count
            .with_label_values(&[operation_type])
            .inc();
    }
}

/// `MonitoredStreamingUploader` will report the following metrics.
/// - `write_bytes`: The number of bytes uploaded from the uploader's creation to finish.
/// - `operation_size`:
///   - `streaming_upload_write_bytes`: The number of bytes written for each call to `write_bytes`.
///   - `streaming_upload`: Same as `write_bytes`.
/// - `operation_latency`:
///   - `streaming_upload_start`: The time spent creating the uploader.
///   - `streaming_upload_write_bytes`: The time spent on each call to `write_bytes`.
///   - `streaming_upload_finish`: The time spent calling `finish`.
/// - `failure_count`: `streaming_upload_start`, `streaming_upload_write_bytes`,
///   `streaming_upload_finish`
pub struct MonitoredStreamingUploader<U: StreamingUploader> {
    inner: U,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    /// Length of data uploaded with this uploader.
    operation_size: usize,
}

impl<U: StreamingUploader> MonitoredStreamingUploader<U> {
    pub fn new(handle: U, object_store_metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self {
            inner: handle,
            object_store_metrics,
            operation_size: 0,
        }
    }
}

/// NOTICE: after #16231, streaming uploader implemented via aws-sdk-s3 will maintain metrics internally in s3.rs
/// so `MonitoredStreamingUploader` will only be used when the inner object store is opendal.
impl<U: StreamingUploader> MonitoredStreamingUploader<U> {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        let operation_type = OperationType::StreamingUpload;
        let operation_type_str = operation_type.as_str();
        let data_len = data.len();

        let res = self
            .inner
            .write_bytes(data)
            .instrument_await(operation_type_str.verbose())
            .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);

        // duration metrics is collected and reported inside the specific implementation of the streaming uploader.
        self.object_store_metrics
            .write_bytes
            .inc_by(data_len as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(data_len as f64);
        self.operation_size += data_len;

        res
    }

    async fn finish(self) -> ObjectResult<()> {
        let operation_type = OperationType::StreamingUploadFinish;
        let operation_type_str = operation_type.as_str();

        let res =
            // TODO: we should avoid this special case after fully migrating to opeandal for s3.
            self.inner
                .finish()
                .instrument_await(operation_type_str.verbose())
                .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);

        // duration metrics is collected and reported inside the specific implementation of the streaming uploader.
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(self.operation_size as f64);
        res
    }

    fn get_memory_usage(&self) -> u64 {
        self.inner.get_memory_usage()
    }
}

pub struct MonitoredStreamingReader {
    inner: ObjectDataStream,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    operation_size: usize,
    media_type: &'static str,
    streaming_read_timeout: Option<Duration>,
    operation_type_str: &'static str,
}

impl MonitoredStreamingReader {
    pub fn new(
        media_type: &'static str,
        handle: ObjectDataStream,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        streaming_read_timeout: Option<Duration>,
    ) -> Self {
        Self {
            inner: handle,
            object_store_metrics,
            operation_size: 0,
            media_type,
            streaming_read_timeout,
            operation_type_str: OperationType::StreamingRead.as_str(),
        }
    }

    pub async fn read_bytes(&mut self) -> Option<ObjectResult<Bytes>> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, self.operation_type_str])
            .start_timer();
        let future = async {
            self.inner
                .next()
                .instrument_await(self.operation_type_str.verbose())
                .await
        };
        let res = match self.streaming_read_timeout.as_ref() {
            None => future.await,
            Some(timeout_duration) => tokio::time::timeout(*timeout_duration, future)
                .await
                .unwrap_or_else(|_| {
                    Some(Err(ObjectError::timeout(format!(
                        "Retry attempts exhausted for {}. Please modify {}_attempt_timeout_ms (current={:?}) under [storage.object_store.retry] in the config accordingly if needed.",
                        self.operation_type_str, self.operation_type_str, timeout_duration.as_millis()
                    ))))
                }),
        };

        if let Some(ret) = &res {
            try_update_failure_metric(&self.object_store_metrics, ret, self.operation_type_str);
        }
        if let Some(Ok(data)) = &res {
            let data_len = data.len();
            self.object_store_metrics.read_bytes.inc_by(data_len as u64);
            self.object_store_metrics
                .operation_size
                .with_label_values(&[self.operation_type_str])
                .observe(data_len as f64);
            self.operation_size += data_len;
        }
        res
    }

    pub fn into_stream(self) -> ObjectDataStream {
        Self::into_stream_inner(self).boxed()
    }

    #[try_stream(ok = Bytes, error = ObjectError)]
    async fn into_stream_inner(mut self) {
        while let Some(bytes) = self.read_bytes().await.transpose()? {
            yield bytes;
        }
    }
}

impl Drop for MonitoredStreamingReader {
    fn drop(&mut self) {
        self.object_store_metrics
            .operation_size
            .with_label_values(&[self.operation_type_str])
            .observe(self.operation_size as f64);
    }
}

pub struct MonitoredObjectStore<OS: ObjectStore> {
    inner: OS,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    config: Arc<ObjectStoreConfig>,
}

/// Manually dispatch trait methods.
///
/// The metrics are updated in the following order:
/// - Write operations
///   - `write_bytes`
///   - `operation_size`
///   - start `operation_latency` timer
///   - `failure_count`
/// - Read operations
///   - start `operation_latency` timer
///   - `failure-count`
///   - `read_bytes`
///   - `operation_size`
/// - Other
///   - start `operation_latency` timer
///   - `failure-count`
impl<OS: ObjectStore> MonitoredObjectStore<OS> {
    pub fn new(
        store: OS,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        config: Arc<ObjectStoreConfig>,
    ) -> Self {
        Self {
            object_store_metrics,
            inner: store,
            config,
        }
    }

    fn media_type(&self) -> &'static str {
        self.inner.store_media_type()
    }

    pub fn inner(&self) -> &OS {
        &self.inner
    }

    pub fn mut_inner(&mut self) -> &mut OS {
        &mut self.inner
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        let operation_type = OperationType::Upload;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();

        self.object_store_metrics
            .write_bytes
            .inc_by(obj.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(obj.len() as f64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .upload(path, obj.clone())
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    pub async fn streaming_upload(
        &self,
        path: &str,
    ) -> ObjectResult<MonitoredStreamingUploader<OS::StreamingUploader>> {
        let operation_type = OperationType::StreamingUploadInit;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let res = self
            .inner
            .streaming_upload(path)
            .instrument_await(operation_type_str.verbose())
            .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);

        Ok(MonitoredStreamingUploader::new(
            res?,
            self.object_store_metrics.clone(),
        ))
    }

    pub async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let operation_type = OperationType::Read;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();

        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .read(path, range.clone())
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        if let Err(e) = &res
            && e.is_object_not_found_error()
            && !path.ends_with(".data")
        {
            // Some not_found_error is expected, e.g. metadata backup's manifest.json.
            // This is a quick fix that'll only log error in `try_update_failure_metric` in state store usage.
        } else {
            try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        }

        let data = res?;
        self.object_store_metrics
            .read_bytes
            .inc_by(data.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(data.len() as f64);
        Ok(data)
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> ObjectResult<MonitoredStreamingReader> {
        let operation_type = OperationType::StreamingReadInit;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .streaming_read(path, range.clone())
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);

        Ok(MonitoredStreamingReader::new(
            media_type,
            res?,
            self.object_store_metrics.clone(),
            Some(Duration::from_millis(
                self.config.retry.streaming_read_attempt_timeout_ms,
            )),
        ))
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let operation_type = OperationType::Metadata;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .metadata(path)
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        let operation_type = OperationType::Delete;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();

        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .delete(path)
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let operation_type = OperationType::DeleteObjects;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();

        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .delete_objects(paths)
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    pub async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        let operation_type = OperationType::List;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();

        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .list(prefix, start_after.clone(), limit)
                .instrument_await(operation_type_str.verbose())
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
            media_type,
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }
}

/// Creates a new [`ObjectStore`] from the given `url`. Credentials are configured via environment
/// variables.
///
/// # Panics
/// If the `url` is invalid. Therefore, it is only suitable to be used during startup.
pub async fn build_remote_object_store(
    url: &str,
    metrics: Arc<ObjectStoreMetrics>,
    ident: &str,
    config: Arc<ObjectStoreConfig>,
    probe_prefix: &str,
) -> ObjectStoreImpl {
    tracing::debug!(config=?config, "object store {ident}");
    match url {
        s3 if s3.starts_with("s3://") => {
            let bucket = s3.strip_prefix("s3://").unwrap();
            let store = if config.s3.developer.use_opendal {
                tracing::info!("Using OpenDAL to access s3, bucket is {}", bucket);
                ObjectStoreImpl::Opendal(
                    OpendalObjectStore::new_s3_engine(
                        bucket.to_owned(),
                        config.clone(),
                        metrics.clone(),
                    )
                    .unwrap()
                    .monitored(metrics.clone(), config.clone()),
                )
            } else {
                ObjectStoreImpl::S3(
                    S3ObjectStore::new_with_config(bucket.to_owned(), metrics.clone(), config.clone())
                        .await
                        .monitored(metrics.clone(), config.clone()),
                )
            };
            // Eagerly probe the bucket with a single, cheap `list` call so that a
            // misconfigured bucket (wrong name, wrong region, missing permissions, ...)
            // is reported clearly at startup instead of surfacing as an opaque error on
            // first real access. Using `list` (rather than reimplementing bucket naming
            // rules) also works for S3 Access Point aliases and S3-compatible services
            // (e.g. Ceph) that don't follow AWS's own bucket naming rules.
            // The probe is scoped to `probe_prefix` (typically the configured data
            // directory) rather than the bucket root, since cloud deployments commonly
            // grant `s3:prefix`-restricted permissions that don't allow listing the
            // bucket root.
            match store.list(probe_prefix, None, Some(1)).await {
                Ok(mut stream) => {
                    if let Some(Err(e)) = stream.next().await {
                        panic!(
                            "Invalid object store configuration for bucket {bucket:?}: {}",
                            e.as_report()
                        );
                    }
                }
                Err(e) => {
                    panic!(
                        "Invalid object store configuration for bucket {bucket:?}: {}",
                        e.as_report()
                    );
                }
            }
            store
        }
        #[cfg(feature = "hdfs-backend")]
        hdfs if hdfs.starts_with("hdfs://") => {
            let hdfs = hdfs.strip_prefix("hdfs://").unwrap();
            let (namenode, root) = hdfs.split_once('@').unwrap_or((hdfs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_hdfs_engine(
                    namenode.to_string(),
                    root.to_string(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        gcs if gcs.starts_with("gcs://") => {
            let gcs = gcs.strip_prefix("gcs://").unwrap();
            let (bucket, root) = gcs.split_once('@').unwrap_or((gcs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_gcs_engine(
                    bucket.to_owned(),
                    root.to_owned(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        obs if obs.starts_with("obs://") => {
            let obs = obs.strip_prefix("obs://").unwrap();
            let (bucket, root) = obs.split_once('@').unwrap_or((obs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_obs_engine(
                    bucket.to_owned(),
                    root.to_owned(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }

        oss if oss.starts_with("oss://") => {
            let oss = oss.strip_prefix("oss://").unwrap();
            let (bucket, root) = oss.split_once('@').unwrap_or((oss, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_oss_engine(
                    bucket.to_owned(),
                    root.to_owned(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        webhdfs if webhdfs.starts_with("webhdfs://") => {
            let webhdfs = webhdfs.strip_prefix("webhdfs://").unwrap();
            let (namenode, root) = webhdfs.split_once('@').unwrap_or((webhdfs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_webhdfs_engine(
                    namenode.to_owned(),
                    root.to_owned(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        azblob if azblob.starts_with("azblob://") => {
            let azblob = azblob.strip_prefix("azblob://").unwrap();
            let (container_name, root) = azblob.split_once('@').unwrap_or((azblob, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_azblob_engine(
                    container_name.to_owned(),
                    root.to_owned(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        fs if fs.starts_with("fs://") => {
            let fs = fs.strip_prefix("fs://").unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_fs_engine(fs.to_owned(), config.clone(), metrics.clone())
                    .unwrap()
                    .monitored(metrics, config),
            )
        }

        s3_compatible if s3_compatible.starts_with("s3-compatible://") => {
            tracing::error!("The s3 compatible mode has been unified with s3.");
            tracing::error!("If you want to use s3 compatible storage, please set your access_key, secret_key and region to the environment variable AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
            set your endpoint to the environment variable RW_S3_ENDPOINT.");
            panic!(
                "Passing s3-compatible is not supported, please modify the environment variable and pass in s3."
            );
        }
        minio if minio.starts_with("minio://") => {
            if config.s3.developer.use_opendal {
                tracing::info!("Using OpenDAL to access minio.");
                ObjectStoreImpl::Opendal(
                    OpendalObjectStore::new_minio_engine(minio, config.clone(), metrics.clone())
                        .unwrap()
                        .monitored(metrics, config),
                )
            } else {
                ObjectStoreImpl::S3(
                    S3ObjectStore::new_minio_engine(minio, metrics.clone(), config.clone())
                        .await
                        .monitored(metrics, config),
                )
            }
        }
        "memory" | "memory-shared" /* backward compatible, memory is always shared now */ => {
            if ident == "Meta Backup" {
                tracing::warn!(
                    "You're using in-memory remote object store for {}. This is not recommended for production environment.",
                    ident
                );
            } else {
                tracing::warn!(
                    "You're using in-memory remote object store for {}. This should never be used in benchmarks and production environment.",
                    ident
                );
            }
            ObjectStoreImpl::InMem(InMemObjectStore::shared().monitored(metrics, config))
        }
        #[cfg(debug_assertions)]
        "memory-isolated-for-test" /* isolated memory is only available for tests */ => {
            ObjectStoreImpl::InMem(InMemObjectStore::for_test().monitored(metrics, config))
        }
        #[cfg(madsim)]
        sim if sim.starts_with("sim://") => {
            ObjectStoreImpl::Sim(SimObjectStore::new(url).monitored(metrics, config))
        }
        other => {
            unimplemented!(
                "{} remote object store only supports s3, minio, gcs, oss, cos, azure blob, hdfs, disk, memory.",
                other
            )
        }
    }
}

#[inline(always)]
fn get_retry_strategy(
    config: &ObjectStoreConfig,
    operation_type: OperationType,
) -> impl Iterator<Item = Duration> + use<> {
    let attempts = get_retry_attempts_by_type(config, operation_type);
    exponential_backoff(
        Duration::from_millis(config.retry.req_backoff_interval_ms),
        config.retry.req_backoff_factor,
        Duration::from_millis(config.retry.req_backoff_max_delay_ms),
    )
    .take(attempts)
    .map(jitter)
}

pub type ObjectMetadataIter = BoxStream<'static, ObjectResult<ObjectMetadata>>;
pub type ObjectDataStream = BoxStream<'static, ObjectResult<Bytes>>;

pin_project! {
    pub struct ObjectDataStreamReader<S> {
        #[pin]
        stream: S,
        pending: Bytes,
    }
}

impl<S> ObjectDataStreamReader<S>
where
    S: Stream<Item = ObjectResult<Bytes>>,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            pending: Bytes::new(),
        }
    }
}

impl ObjectDataStreamReader<ObjectDataStream> {
    pub fn into_bytes_stream(self) -> ObjectDataStream {
        Self::into_bytes_stream_inner(self).boxed()
    }

    #[try_stream(ok = Bytes, error = ObjectError)]
    async fn into_bytes_stream_inner(mut self) {
        if self.pending.has_remaining() {
            yield std::mem::take(&mut self.pending);
        }
        while let Some(bytes) = self.stream.next().await.transpose()? {
            yield bytes;
        }
    }
}

impl<S> AsyncRead for ObjectDataStreamReader<S>
where
    S: Stream<Item = ObjectResult<Bytes>>,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut this = self.project();
        loop {
            if this.pending.has_remaining() {
                let len = this.pending.remaining().min(buf.remaining());
                buf.put_slice(&this.pending[..len]);
                this.pending.advance(len);
                return Poll::Ready(Ok(()));
            }

            match ready!(this.stream.as_mut().poll_next(cx)) {
                Some(Ok(bytes)) => {
                    *this.pending = bytes;
                }
                Some(Err(err)) => return Poll::Ready(Err(io::Error::other(err))),
                None => return Poll::Ready(Ok(())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use tokio::io::AsyncReadExt;

    use super::{Bytes, ObjectDataStreamReader, ObjectError};

    #[tokio::test]
    async fn test_object_data_stream_reader_reads_across_chunks() {
        let stream = stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cdef")),
            Ok(Bytes::from_static(b"g")),
        ]);
        let mut reader = ObjectDataStreamReader::new(stream);

        let mut first = [0; 3];
        reader.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"abc");

        let mut rest = vec![];
        reader.read_to_end(&mut rest).await.unwrap();
        assert_eq!(&rest, b"defg");
    }

    #[tokio::test]
    async fn test_object_data_stream_reader_returns_stream_error() {
        let stream = stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Err(ObjectError::internal("injected stream error")),
        ]);
        let mut reader = ObjectDataStreamReader::new(stream);

        let mut output = vec![];
        let err = reader.read_to_end(&mut output).await.unwrap_err();
        assert!(err.to_string().contains("injected stream error"));
    }
}

#[derive(Debug, Clone, Copy)]
enum OperationType {
    Upload,
    StreamingUploadInit,
    StreamingUpload,
    StreamingUploadFinish,
    Read,
    StreamingReadInit,
    StreamingRead,
    Metadata,
    Delete,
    DeleteObjects,
    List,
}

impl OperationType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::StreamingUploadInit => "streaming_upload_init",
            Self::StreamingUpload => "streaming_upload",
            Self::StreamingUploadFinish => "streaming_upload_finish",
            Self::Read => "read",
            Self::StreamingReadInit => "streaming_read_init",
            Self::StreamingRead => "streaming_read",
            Self::Metadata => "metadata",
            Self::Delete => "delete",
            Self::DeleteObjects => "delete_objects",
            Self::List => "list",
        }
    }
}

fn get_retry_attempts_by_type(config: &ObjectStoreConfig, operation_type: OperationType) -> usize {
    match operation_type {
        OperationType::Upload => config.retry.upload_retry_attempts,
        OperationType::StreamingUploadInit
        | OperationType::StreamingUpload
        | OperationType::StreamingUploadFinish => config.retry.streaming_upload_retry_attempts,
        OperationType::Read => config.retry.read_retry_attempts,
        OperationType::StreamingReadInit | OperationType::StreamingRead => {
            config.retry.streaming_read_retry_attempts
        }
        OperationType::Metadata => config.retry.metadata_retry_attempts,
        OperationType::Delete => config.retry.delete_retry_attempts,
        OperationType::DeleteObjects => config.retry.delete_objects_retry_attempts,
        OperationType::List => config.retry.list_retry_attempts,
    }
}

fn get_attempt_timeout_by_type(config: &ObjectStoreConfig, operation_type: OperationType) -> u64 {
    match operation_type {
        OperationType::Upload => config.retry.upload_attempt_timeout_ms,
        OperationType::StreamingUploadInit
        | OperationType::StreamingUpload
        | OperationType::StreamingUploadFinish => config.retry.streaming_upload_attempt_timeout_ms,
        OperationType::Read => config.retry.read_attempt_timeout_ms,
        OperationType::StreamingReadInit | OperationType::StreamingRead => {
            config.retry.streaming_read_attempt_timeout_ms
        }
        OperationType::Metadata => config.retry.metadata_attempt_timeout_ms,
        OperationType::Delete => config.retry.delete_attempt_timeout_ms,
        OperationType::DeleteObjects => config.retry.delete_objects_attempt_timeout_ms,
        OperationType::List => config.retry.list_attempt_timeout_ms,
    }
}

struct RetryCondition {
    operation_type: OperationType,
    retry_count: usize,
    metrics: Arc<ObjectStoreMetrics>,
    retry_opendal_s3_unknown_error: bool,
}

impl RetryCondition {
    fn new(
        operation_type: OperationType,
        metrics: Arc<ObjectStoreMetrics>,
        retry_opendal_s3_unknown_error: bool,
    ) -> Self {
        Self {
            operation_type,
            retry_count: 0,
            metrics,
            retry_opendal_s3_unknown_error,
        }
    }

    #[inline(always)]
    fn should_retry_inner(&mut self, err: &ObjectError) -> bool {
        let should_retry = err.should_retry(self.retry_opendal_s3_unknown_error);
        if should_retry {
            self.retry_count += 1;
        }

        should_retry
    }
}

impl tokio_retry::Condition<ObjectError> for RetryCondition {
    fn should_retry(&mut self, err: &ObjectError) -> bool {
        self.should_retry_inner(err)
    }
}

impl Drop for RetryCondition {
    fn drop(&mut self) {
        if self.retry_count > 0 {
            self.metrics
                .request_retry_count
                .with_label_values(&[self.operation_type.as_str()])
                .inc_by(self.retry_count as _);
        }
    }
}

async fn retry_request<F, T, B>(
    builder: B,
    config: &ObjectStoreConfig,
    operation_type: OperationType,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    media_type: &'static str,
) -> ObjectResult<T>
where
    B: Fn() -> F,
    F: Future<Output = ObjectResult<T>>,
{
    let backoff = get_retry_strategy(config, operation_type);
    let timeout_duration =
        Duration::from_millis(get_attempt_timeout_by_type(config, operation_type));
    let operation_type_str = operation_type.as_str();

    let retry_condition = RetryCondition::new(
        operation_type,
        object_store_metrics,
        (config.s3.developer.retry_unknown_service_error || config.s3.retry_unknown_service_error)
            && (media_type == opendal_engine::MediaType::S3.as_str()
                || media_type == opendal_engine::MediaType::Minio.as_str()),
    );

    let f = || async {
        let future = builder();
        if timeout_duration.is_zero() {
            future.await
        } else {
            tokio::time::timeout(timeout_duration, future)
                .await
                .unwrap_or_else(|_| {
                    Err(ObjectError::timeout(format!(
                        "Retry attempts exhausted for {}. Please modify {}_attempt_timeout_ms (current={:?}) and {}_retry_attempts (current={}) under [storage.object_store.retry] in the config accordingly if needed.",
                        operation_type_str, operation_type_str, timeout_duration.as_millis(), operation_type_str, get_retry_attempts_by_type(config, operation_type)
                    )))
                })
        }
    };

    tokio_retry::RetryIf::spawn(backoff, f, retry_condition).await
}
