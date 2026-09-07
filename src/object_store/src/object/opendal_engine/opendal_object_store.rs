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

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fail::fail_point;
use futures::{StreamExt, stream};
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::raw::BoxedStaticFuture;
use opendal::services::Memory;
use opendal::{Execute, Executor, Operator, Writer};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::range::RangeBoundsExt;
use thiserror_ext::AsReport;

use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::{
    MonitoredObjectStore, ObjectDataStream, ObjectError, ObjectMetadata, ObjectMetadataIter,
    ObjectRangeBounds, ObjectResult, ObjectStore, OperationType, StreamingUploader, prefix,
};

/// Opendal object storage.
#[derive(Clone)]
pub struct OpendalObjectStore {
    pub(crate) op: Operator,
    pub(crate) media_type: MediaType,
    pub(crate) config: Arc<ObjectStoreConfig>,
    pub(crate) metrics: Arc<ObjectStoreMetrics>,
}

#[derive(Clone)]
pub enum MediaType {
    Memory,
    Hdfs,
    Gcs,
    Minio,
    S3,
    Obs,
    Oss,
    Webhdfs,
    Azblob,
    Fs,
}

impl MediaType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MediaType::Memory => "Memory",
            MediaType::Hdfs => "Hdfs",
            MediaType::Gcs => "Gcs",
            MediaType::Minio => "Minio",
            MediaType::S3 => "S3",
            MediaType::Obs => "Obs",
            MediaType::Oss => "Oss",
            MediaType::Webhdfs => "Webhdfs",
            MediaType::Azblob => "Azblob",
            MediaType::Fs => "Fs",
        }
    }
}

impl OpendalObjectStore {
    /// create opendal memory engine, used for unit tests.
    pub fn test_new_memory_engine() -> ObjectResult<Self> {
        // Create memory backend builder.
        let builder = Memory::default();
        let op: Operator = Operator::new(builder)?;

        Ok(Self {
            op,
            media_type: MediaType::Memory,
            config: Arc::new(ObjectStoreConfig::default()),
            metrics: Arc::new(ObjectStoreMetrics::unused()),
        })
    }
}

#[inline]
fn timestamp_to_secs(t: opendal::raw::Timestamp) -> f64 {
    t.into_inner().as_second() as f64
}

#[async_trait::async_trait]
impl ObjectStore for OpendalObjectStore {
    type StreamingUploader = OpendalStreamingUploader;

    fn get_object_prefix(&self, obj_id: u64, use_new_object_prefix_strategy: bool) -> String {
        match self.media_type {
            MediaType::S3 => prefix::s3::get_object_prefix(obj_id),
            MediaType::Minio => prefix::s3::get_object_prefix(obj_id),
            MediaType::Memory => String::default(),
            MediaType::Hdfs
            | MediaType::Gcs
            | MediaType::Obs
            | MediaType::Oss
            | MediaType::Webhdfs
            | MediaType::Azblob
            | MediaType::Fs => {
                prefix::opendal_engine::get_object_prefix(obj_id, use_new_object_prefix_strategy)
            }
        }
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            self.op.write(path, obj).await?;
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        if self.op.info().capability().write_can_multi {
            Ok(OpendalStreamingUploader::Native(Box::new(
                OpendalNativeStreamingUploader::new(
                    self.op.clone(),
                    path.to_owned(),
                    self.config.clone(),
                    self.metrics.clone(),
                    self.store_media_type(),
                )
                .await?,
            )))
        } else {
            Ok(OpendalStreamingUploader::Buffered(
                OpendalBufferedStreamingUploader::new(
                    self.clone()
                        .monitored(self.metrics.clone(), self.config.clone()),
                    path.to_owned(),
                ),
            ))
        }
    }

    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let data = if range.is_full() {
            self.op.read(path).await?
        } else {
            self.op
                .read_with(path)
                .range(range.map(|v| *v as u64))
                .await?
        };

        if let Some(len) = range.len()
            && len != data.len()
        {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                len,
                data.len(),
                path,
                range,
            )));
        }

        Ok(data.to_bytes())
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> ObjectResult<ObjectDataStream> {
        fail_point!("opendal_streaming_read_err", |_| Err(
            ObjectError::internal("opendal streaming read error")
        ));
        let range = range.map(|v| *v as u64);

        // The layer specified first will be executed first.
        // `TimeoutLayer` must be specified before `RetryLayer`.
        // Otherwise, it will lead to bad state inside OpenDAL and panic.
        // See https://docs.rs/opendal/latest/opendal/layers/struct.RetryLayer.html#panics
        let reader = self
            .op
            .clone()
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(
                self.config.retry.streaming_read_attempt_timeout_ms,
            )))
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(
                        self.config.retry.req_backoff_interval_ms,
                    ))
                    .with_max_delay(Duration::from_millis(
                        self.config.retry.req_backoff_max_delay_ms,
                    ))
                    .with_max_times(self.config.retry.streaming_read_retry_attempts)
                    .with_factor(self.config.retry.req_backoff_factor as f32)
                    .with_jitter(),
            )
            .reader_with(path)
            .await?;
        let stream = reader.into_bytes_stream(range).await?.map(|item| {
            item.map(|b| Bytes::copy_from_slice(b.as_ref()))
                .map_err(|e| {
                    ObjectError::internal(format!("reader into_stream fail {}", e.as_report()))
                })
        });

        Ok(Box::pin(stream))
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let opendal_metadata = self.op.stat(path).await?;
        let key = path.to_owned();
        let last_modified = match opendal_metadata.last_modified() {
            Some(t) => timestamp_to_secs(t),
            None => 0_f64,
        };

        let total_size = opendal_metadata.content_length() as usize;
        let metadata = ObjectMetadata {
            key,
            last_modified,
            total_size,
        };
        Ok(metadata)
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        self.op.delete(path).await?;
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        self.op.delete_iter(paths.to_vec()).await?;
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        let mut object_lister = self.op.lister_with(prefix).recursive(true);
        if let Some(start_after) = start_after {
            object_lister = object_lister.start_after(&start_after);
        }
        let object_lister = object_lister.await?;

        let op = self.op.clone();
        let stream = stream::unfold(object_lister, move |mut object_lister| {
            let op = op.clone();

            async move {
                match object_lister.next().await {
                    Some(Ok(object)) => {
                        let key = object.path().to_owned();

                        // OpenDAL 0.55 removed list metadata capability flags and reports
                        // unknown content length as 0. Use listed metadata first, and call
                        // stat() if timestamp is missing or size is 0 to avoid treating
                        // unknown sizes as real zero-byte objects.
                        let meta = object.metadata();
                        let mut last_modified = meta.last_modified().map(timestamp_to_secs);
                        let mut total_size = meta.content_length() as usize;
                        if last_modified.is_none() || total_size == 0 {
                            let stat_meta = op.stat(&key).await.ok()?;
                            last_modified = stat_meta.last_modified().map(timestamp_to_secs);
                            total_size = stat_meta.content_length() as usize;
                        }

                        let metadata = ObjectMetadata {
                            key,
                            last_modified: last_modified.unwrap_or(0_f64),
                            total_size,
                        };
                        Some((Ok(metadata), object_lister))
                    }
                    Some(Err(err)) => Some((Err(err.into()), object_lister)),
                    None => None,
                }
            }
        });

        Ok(stream.take(limit.unwrap_or(usize::MAX)).boxed())
    }

    fn store_media_type(&self) -> &'static str {
        self.media_type.as_str()
    }
}

impl OpendalObjectStore {
    pub async fn copy(&self, from_path: &str, to_path: &str) -> ObjectResult<()> {
        self.op.copy(from_path, to_path).await?;
        Ok(())
    }
}

struct OpendalStreamingUploaderExecute {
    /// To record metrics for uploading part.
    metrics: Arc<ObjectStoreMetrics>,
    media_type: &'static str,
}

impl OpendalStreamingUploaderExecute {
    const STREAMING_UPLOAD_TYPE: OperationType = OperationType::StreamingUpload;

    fn new(metrics: Arc<ObjectStoreMetrics>, media_type: &'static str) -> Self {
        Self {
            metrics,
            media_type,
        }
    }
}

impl Execute for OpendalStreamingUploaderExecute {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        let operation_type_str = Self::STREAMING_UPLOAD_TYPE.as_str();
        let media_type = self.media_type;

        let metrics = self.metrics.clone();
        let _handle = tokio::spawn(async move {
            let _timer = metrics
                .operation_latency
                .with_label_values(&[media_type, operation_type_str])
                .start_timer();

            f.await
        });
    }
}

pub enum OpendalStreamingUploader {
    Native(Box<OpendalNativeStreamingUploader>),
    Buffered(OpendalBufferedStreamingUploader),
}

/// Store multiple parts in a map, and concatenate them on finish.
pub struct OpendalNativeStreamingUploader {
    writer: Writer,
    /// Buffer for data. It will store at least `UPLOAD_BUFFER_SIZE` bytes of data before wrapping itself
    /// into a stream and upload to object store as a part.
    buf: Vec<Bytes>,
    /// Length of the data that have not been uploaded to object store.
    not_uploaded_len: usize,
    /// Whether the writer is valid. The writer is invalid after abort/close.
    is_valid: bool,

    abort_on_err: bool,

    upload_part_size: usize,
}

impl OpendalNativeStreamingUploader {
    pub async fn new(
        op: Operator,
        path: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
        media_type: &'static str,
    ) -> ObjectResult<Self> {
        let monitored_execute = OpendalStreamingUploaderExecute::new(metrics, media_type);
        let executor = Executor::with(monitored_execute);
        // Start from the base context so existing layers are replayed exactly once by
        // `with_context`. Using the composed context here would double-wrap resources such as
        // the HTTP concurrency semaphore.
        let ctx = op.base_context().with_executor(executor);
        let op = op.with_context(ctx);

        let writer = op
            .clone()
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(
                config.retry.streaming_upload_attempt_timeout_ms,
            )))
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(config.retry.req_backoff_interval_ms))
                    .with_max_delay(Duration::from_millis(config.retry.req_backoff_max_delay_ms))
                    .with_max_times(config.retry.streaming_upload_retry_attempts)
                    .with_factor(config.retry.req_backoff_factor as f32)
                    .with_jitter(),
            )
            .writer_with(&path)
            .concurrent(config.opendal_upload_concurrency)
            .await?;
        Ok(Self {
            writer,
            buf: vec![],
            not_uploaded_len: 0,
            is_valid: true,
            abort_on_err: config.opendal_writer_abort_on_err,
            upload_part_size: config.upload_part_size,
        })
    }

    async fn flush(&mut self) -> ObjectResult<()> {
        let data = std::mem::take(&mut self.buf);
        debug_assert_eq!(
            data.iter().map(|b| b.len()).sum::<usize>(),
            self.not_uploaded_len
        );
        if let Err(err) = self.writer.write(data).await {
            self.is_valid = false;
            if self.abort_on_err {
                self.writer.abort().await?;
            }
            return Err(err.into());
        }
        self.not_uploaded_len = 0;
        Ok(())
    }
}

impl StreamingUploader for OpendalNativeStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        assert!(self.is_valid);
        self.not_uploaded_len += data.len();
        self.buf.push(data);
        if self.not_uploaded_len >= self.upload_part_size {
            self.flush().await?;
        }
        Ok(())
    }

    async fn finish(mut self) -> ObjectResult<()> {
        assert!(self.is_valid);
        if self.not_uploaded_len > 0 {
            self.flush().await?;
        }

        assert!(self.buf.is_empty());
        assert_eq!(self.not_uploaded_len, 0);

        self.is_valid = false;
        match self.writer.close().await {
            Ok(_) => (),
            Err(err) => {
                if self.abort_on_err {
                    self.writer.abort().await?;
                }
                return Err(err.into());
            }
        };

        Ok(())
    }

    // Not absolutely accurate. Some bytes may be in the infight request.
    fn get_memory_usage(&self) -> u64 {
        self.not_uploaded_len as u64
    }
}

pub struct OpendalBufferedStreamingUploader {
    store: MonitoredObjectStore<OpendalObjectStore>,
    path: String,
    buf: BytesMut,
}

impl OpendalBufferedStreamingUploader {
    fn new(store: MonitoredObjectStore<OpendalObjectStore>, path: String) -> Self {
        Self {
            store,
            path,
            buf: BytesMut::new(),
        }
    }
}

impl StreamingUploader for OpendalBufferedStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.buf.extend_from_slice(&data);
        Ok(())
    }

    async fn finish(self) -> ObjectResult<()> {
        self.store.upload(&self.path, self.buf.freeze()).await
    }

    fn get_memory_usage(&self) -> u64 {
        self.buf.len() as u64
    }
}

impl StreamingUploader for OpendalStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        match self {
            Self::Native(uploader) => uploader.write_bytes(data).await,
            Self::Buffered(uploader) => uploader.write_bytes(data).await,
        }
    }

    async fn finish(self) -> ObjectResult<()> {
        match self {
            Self::Native(uploader) => (*uploader).finish().await,
            Self::Buffered(uploader) => uploader.finish().await,
        }
    }

    fn get_memory_usage(&self) -> u64 {
        match self {
            Self::Native(uploader) => uploader.get_memory_usage(),
            Self::Buffered(uploader) => uploader.get_memory_usage(),
        }
    }
}

#[cfg(test)]
mod tests {
    use stream::TryStreamExt;

    use super::*;

    async fn list_all(prefix: &str, store: &OpendalObjectStore) -> Vec<ObjectMetadata> {
        store
            .list(prefix, None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_memory_upload() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("/abc", block).await.unwrap();

        // No such object.
        store.read("/ab", 0..3).await.unwrap_err();

        let bytes = store.read("/abc", 4..6).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_owned());

        store.delete("/abc").await.unwrap();

        // No such object.
        store.read("/abc", 0..3).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_memory_read_out_of_range_returns_error() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("/abc", block).await.unwrap();

        // OpenDAL 0.58 reports an out-of-range read instead of panicking.
        store.read("/abc", 4..44).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_memory_metadata() {
        let block = Bytes::from("123456");
        let path = "/abc".to_owned();
        let obj_store = OpendalObjectStore::test_new_memory_engine().unwrap();
        obj_store.upload("/abc", block).await.unwrap();

        let err = obj_store.metadata("/not_exist").await.unwrap_err();
        assert!(err.is_object_not_found_error());
        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
        obj_store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_buffered_streaming_upload() {
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        let mut uploader = OpendalBufferedStreamingUploader::new(
            store
                .clone()
                .monitored(store.metrics.clone(), store.config.clone()),
            "/abc".to_owned(),
        );

        uploader.write_bytes(Bytes::from("123")).await.unwrap();
        assert_eq!(uploader.get_memory_usage(), 3);
        uploader.write_bytes(Bytes::from("456")).await.unwrap();
        assert_eq!(uploader.get_memory_usage(), 6);
        uploader.finish().await.unwrap();

        let read_obj = store.read("/abc", ..).await.unwrap();
        assert_eq!(read_obj, Bytes::from("123456"));
    }

    #[tokio::test]
    async fn test_empty_buffered_streaming_upload() {
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        let uploader = OpendalBufferedStreamingUploader::new(
            store
                .clone()
                .monitored(store.metrics.clone(), store.config.clone()),
            "/abc".to_owned(),
        );

        uploader.finish().await.unwrap_err();
    }

    #[tokio::test]
    async fn test_memory_delete_objects_and_list_object() {
        let block1 = Bytes::from("123456");
        let block2 = Bytes::from("987654");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("abc", Bytes::from("123456")).await.unwrap();
        store.upload("prefix/abc", block1).await.unwrap();

        store.upload("prefix/xyz", block2).await.unwrap();

        assert_eq!(list_all("", &store).await.len(), 3);
        assert_eq!(list_all("prefix/", &store).await.len(), 2);
        let str_list = [String::from("prefix/abc"), String::from("prefix/xyz")];

        store.delete_objects(&str_list).await.unwrap();

        assert!(store.read("prefix/abc/", ..).await.is_err());
        assert!(store.read("prefix/xyz/", ..).await.is_err());
        assert_eq!(list_all("", &store).await.len(), 1);
        assert_eq!(list_all("prefix/", &store).await.len(), 0);
    }
}
