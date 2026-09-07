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

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use fail::fail_point;
use futures::Stream;
use itertools::Itertools;
use risingwave_common::range::RangeBoundsExt;
use thiserror::Error;
use tokio::sync::Mutex;

use super::{
    ObjectError, ObjectMetadata, ObjectRangeBounds, ObjectResult, ObjectStore, StreamingUploader,
};
use crate::object::{ObjectDataStream, ObjectMetadataIter};

#[derive(Error, Debug)]
pub enum Error {
    #[error("NotFound error: {0}")]
    NotFound(String),
    #[error("Other error: {0}")]
    Other(String),
}

impl Error {
    pub fn is_object_not_found_error(&self) -> bool {
        matches!(self, Error::NotFound(_))
    }
}

impl Error {
    fn not_found(msg: impl ToString) -> Self {
        Error::NotFound(msg.to_string())
    }

    fn other(msg: impl ToString) -> Self {
        Error::Other(msg.to_string())
    }
}

/// Store multiple parts in a map, and concatenate them on finish.
pub struct InMemStreamingUploader {
    path: String,
    buf: BytesMut,
    objects: Arc<Mutex<HashMap<String, (ObjectMetadata, Bytes)>>>,
}

impl StreamingUploader for InMemStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        fail_point!("mem_write_bytes_err", |_| Err(ObjectError::internal(
            "mem write bytes error"
        )));
        self.buf.put(data);
        Ok(())
    }

    async fn finish(self) -> ObjectResult<()> {
        fail_point!("mem_finish_streaming_upload_err", |_| Err(
            ObjectError::internal("mem finish streaming upload error")
        ));
        let obj = self.buf.freeze();
        if obj.is_empty() {
            Err(Error::other("upload empty object").into())
        } else {
            let metadata = get_obj_meta(&self.path, &obj)?;
            self.objects.lock().await.insert(self.path, (metadata, obj));
            Ok(())
        }
    }

    fn get_memory_usage(&self) -> u64 {
        self.buf.capacity() as u64
    }
}

/// In-memory object storage, useful for testing.
#[derive(Default, Clone)]
pub struct InMemObjectStore {
    objects: Arc<Mutex<HashMap<String, (ObjectMetadata, Bytes)>>>,
}

#[async_trait::async_trait]
impl ObjectStore for InMemObjectStore {
    type StreamingUploader = InMemStreamingUploader;

    fn get_object_prefix(&self, _obj_id: u64, _use_new_object_prefix_strategy: bool) -> String {
        String::default()
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("mem_upload_err", |_| Err(ObjectError::internal(
            "mem upload error"
        )));
        if obj.is_empty() {
            Err(Error::other("upload empty object").into())
        } else {
            let metadata = get_obj_meta(path, &obj)?;
            self.objects
                .lock()
                .await
                .insert(path.into(), (metadata, obj));
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        Ok(InMemStreamingUploader {
            path: path.to_owned(),
            buf: BytesMut::new(),
            objects: self.objects.clone(),
        })
    }

    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        fail_point!("mem_read_err", |_| Err(ObjectError::internal(
            "mem read error"
        )));
        self.get_object(path, range).await
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        read_range: impl ObjectRangeBounds,
    ) -> ObjectResult<ObjectDataStream> {
        fail_point!("mem_streaming_read_err", |_| Err(ObjectError::internal(
            "mem streaming read error"
        )));
        let bytes = self.get_object(path, read_range).await?;

        Ok(Box::pin(InMemDataIterator::new(bytes)))
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        self.objects
            .lock()
            .await
            .get(path)
            .map(|(metadata, _)| metadata)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("no object at path '{}'", path)).into())
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("mem_delete_err", |_| Err(ObjectError::internal(
            "mem delete error"
        )));
        self.objects.lock().await.remove(path);
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let mut guard = self.objects.lock().await;

        for path in paths {
            guard.remove(path);
        }

        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        let list_result = self
            .objects
            .lock()
            .await
            .iter()
            .filter_map(|(path, (metadata, _))| {
                if let Some(ref start_after) = start_after
                    && metadata.key.le(start_after)
                {
                    return None;
                }
                if path.starts_with(prefix) {
                    return Some(metadata.clone());
                }
                None
            })
            .sorted_by(|a, b| Ord::cmp(&a.key, &b.key))
            .take(limit.unwrap_or(usize::MAX))
            .collect_vec();
        Ok(Box::pin(InMemObjectIter::new(list_result)))
    }

    fn store_media_type(&self) -> &'static str {
        "mem"
    }
}

pub struct InMemDataIterator {
    data: Bytes,
    offset: usize,
}

impl InMemDataIterator {
    pub fn new(data: Bytes) -> Self {
        Self { data, offset: 0 }
    }
}

impl Stream for InMemDataIterator {
    type Item = ObjectResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const MAX_PACKET_SIZE: usize = 128 * 1024;
        if self.offset >= self.data.len() {
            return Poll::Ready(None);
        }
        let read_len = std::cmp::min(self.data.len() - self.offset, MAX_PACKET_SIZE);
        let data = self.data.slice(self.offset..(self.offset + read_len));
        self.offset += read_len;
        Poll::Ready(Some(Ok(data)))
    }
}

static SHARED: LazyLock<spin::Mutex<InMemObjectStore>> =
    LazyLock::new(|| spin::Mutex::new(InMemObjectStore::new()));

impl InMemObjectStore {
    fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new in-memory object store for testing, isolated with others.
    pub fn for_test() -> Self {
        Self::new()
    }

    /// Get a reference to the in-memory object store shared in this process.
    ///
    /// Note: Should only be used for `risedev playground`, when there're multiple compute-nodes or
    /// compactors in the same process.
    pub(super) fn shared() -> Self {
        SHARED.lock().clone()
    }

    /// Reset the shared in-memory object store.
    pub fn reset_shared() {
        *SHARED.lock() = InMemObjectStore::new();
    }

    async fn get_object(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let objects = self.objects.lock().await;

        let obj = objects
            .get(path)
            .map(|(_, obj)| obj)
            .ok_or_else(|| Error::not_found(format!("no object at path '{}'", path)))?;

        if let Some(end) = range.end()
            && end > obj.len()
        {
            return Err(Error::other("bad block offset and size").into());
        }

        Ok(obj.slice(range))
    }
}

fn get_obj_meta(path: &str, obj: &Bytes) -> ObjectResult<ObjectMetadata> {
    Ok(ObjectMetadata {
        key: path.to_owned(),
        last_modified: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(ObjectError::internal)?
            .as_secs_f64(),
        total_size: obj.len(),
    })
}

struct InMemObjectIter {
    list_result: VecDeque<ObjectMetadata>,
}

impl InMemObjectIter {
    fn new(list_result: Vec<ObjectMetadata>) -> Self {
        Self {
            list_result: list_result.into(),
        }
    }
}

impl Stream for InMemObjectIter {
    type Item = ObjectResult<ObjectMetadata>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(i) = self.list_result.pop_front() {
            return Poll::Ready(Some(Ok(i)));
        }
        Poll::Ready(None)
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use itertools::enumerate;

    use super::*;

    #[tokio::test]
    async fn test_upload() {
        let block = Bytes::from("123456");

        let s3 = InMemObjectStore::for_test();
        s3.upload("/abc", block).await.unwrap();

        // No such object.
        let err = s3.read("/ab", 0..3).await.unwrap_err();
        assert!(err.is_object_not_found_error());

        let bytes = s3.read("/abc", 4..6).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_owned());

        // Overflow.
        s3.read("/abc", 4..8).await.unwrap_err();

        s3.delete("/abc").await.unwrap();

        // No such object.
        s3.read("/abc", 0..3).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_streaming_upload() {
        let blocks = vec![Bytes::from("123"), Bytes::from("456"), Bytes::from("789")];
        let obj = Bytes::from("123456789");

        let store = InMemObjectStore::for_test();
        let mut uploader = store.streaming_upload("/abc").await.unwrap();

        for block in blocks {
            uploader.write_bytes(block).await.unwrap();
        }
        uploader.finish().await.unwrap();

        // Read whole object.
        let read_obj = store.read("/abc", ..).await.unwrap();
        assert!(read_obj.eq(&obj));

        // Read part of the object.
        let read_obj = store.read("/abc", 4..6).await.unwrap();
        assert_eq!(
            String::from_utf8(read_obj.to_vec()).unwrap(),
            "56".to_owned()
        );
    }

    #[tokio::test]
    async fn test_metadata() {
        let block = Bytes::from("123456");

        let obj_store = InMemObjectStore::for_test();
        obj_store.upload("/abc", block).await.unwrap();

        let err = obj_store.metadata("/not_exist").await.unwrap_err();
        assert!(err.is_object_not_found_error());

        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
    }

    async fn list_all(prefix: &str, store: &InMemObjectStore) -> Vec<ObjectMetadata> {
        store
            .list(prefix, None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_list() {
        let payload = Bytes::from("123456");
        let store = InMemObjectStore::for_test();
        assert!(list_all("", &store).await.is_empty());

        let paths = vec!["001/002/test.obj", "001/003/test.obj"];
        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(list_all("", &store).await.len(), i);
            store.upload(path, payload.clone()).await.unwrap();
            assert_eq!(list_all("", &store).await.len(), i + 1);
        }

        let list_path = list_all("", &store)
            .await
            .iter()
            .map(|p| p.key.clone())
            .collect_vec();
        assert_eq!(list_path, paths);

        for i in 0..=5 {
            assert_eq!(list_all(&paths[0][0..=i], &store).await.len(), 2);
        }
        for i in 6..=paths[0].len() - 1 {
            assert_eq!(list_all(&paths[0][0..=i], &store).await.len(), 1)
        }
        assert!(list_all("003", &store).await.is_empty());

        for (i, path) in enumerate(paths.clone()) {
            assert_eq!(list_all("", &store).await.len(), paths.len() - i);
            store.delete(path).await.unwrap();
            assert_eq!(list_all("", &store).await.len(), paths.len() - i - 1);
        }
    }

    #[tokio::test]
    async fn test_delete_objects() {
        let block1 = Bytes::from("123456");
        let block2 = Bytes::from("987654");

        let store = InMemObjectStore::for_test();
        store.upload("/abc", block1).await.unwrap();
        store.upload("/klm", block2).await.unwrap();

        assert_eq!(list_all("", &store).await.len(), 2);

        let str_list = [
            String::from("/abc"),
            String::from("/klm"),
            String::from("/xyz"),
        ];

        store.delete_objects(&str_list).await.unwrap();

        assert_eq!(list_all("", &store).await.len(), 0);
    }
}
