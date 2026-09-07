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

#![cfg(madsim)]

mod client;
mod error;
use bytes::{BufMut, BytesMut};
pub use error::SimError;
use futures::Stream;
mod rpc_server;
pub use rpc_server::SimServer;
mod service;

use std::net::SocketAddr;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::task::{Context, Poll};

use madsim::rand::{RngCore, thread_rng};
use madsim::time::{Duration, sleep};
use risingwave_common::range::RangeBoundsExt;

use self::client::Client;
use self::service::Response;
use super::{
    Bytes, ObjectDataStream, ObjectError, ObjectMetadata, ObjectMetadataIter, ObjectRangeBounds,
    ObjectResult, ObjectStore, StreamingUploader,
};

pub struct SimStreamingUploader {
    client: Client,
    key: String,
    buf: BytesMut,
}

impl SimStreamingUploader {
    fn new(client: Client, key: String) -> Self {
        Self {
            client,
            key,
            buf: Default::default(),
        }
    }
}

impl StreamingUploader for SimStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.buf.put(data);
        Ok(())
    }

    async fn finish(mut self) -> ObjectResult<()> {
        if self.buf.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            let resp = self
                .client
                .send_request(service::Request::Upload {
                    path: self.key,
                    obj: self.buf.freeze(),
                })
                .await?;
            let Response::Upload = resp else {
                panic!("expect Response::Upload");
            };
            Ok(())
        }
    }

    fn get_memory_usage(&self) -> u64 {
        self.buf.len() as u64
    }
}

pub struct SimDataIterator {
    data: Bytes,
    offset: usize,
}

impl SimDataIterator {
    pub fn new(data: Bytes) -> Self {
        Self { data, offset: 0 }
    }
}

impl Stream for SimDataIterator {
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

pub struct SimObjectStore {
    client: Client,
}

#[async_trait::async_trait]
impl ObjectStore for SimObjectStore {
    type StreamingUploader = SimStreamingUploader;

    fn get_object_prefix(&self, _obj_id: u64, _use_new_object_prefix_strategy: bool) -> String {
        String::default()
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            let path = path.to_string();
            let resp = self
                .client
                .send_request(service::Request::Upload { path, obj })
                .await?;
            if let Response::Upload = resp {
                Ok(())
            } else {
                panic!("expect Response::Upload");
            }
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        Ok(SimStreamingUploader::new(
            self.client.clone(),
            path.to_string(),
        ))
    }

    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::Read { path })
            .await?;
        if let Response::Read(obj) = resp {
            if let Some(end) = range.end()
                && end > obj.len()
            {
                return Err(SimError::other("bad block offset and size").into());
            }
            Ok(obj.slice(range))
        } else {
            panic!("expect Response::Read");
        }
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let path = path.to_string();
        if let Response::Metadata(m) = self
            .client
            .send_request(service::Request::Metadata { path })
            .await?
        {
            Ok(m)
        } else {
            panic!("expect Response::Metadata");
        }
    }

    async fn streaming_read(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> ObjectResult<ObjectDataStream> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::Read { path })
            .await?;
        let Response::Read(body) = resp else {
            panic!("expect Response::Read");
        };

        Ok(Box::pin(SimDataIterator::new(body.slice(range))))
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::Delete { path })
            .await?;
        if let Response::Delete = resp {
            Ok(())
        } else {
            panic!("expect Response::Delete");
        }
    }

    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let resp = self
            .client
            .send_request(service::Request::DeleteObjects {
                paths: paths.to_vec(),
            })
            .await?;
        if let Response::DeleteObjects = resp {
            Ok(())
        } else {
            panic!("expect Response::DeleteObjects");
        }
    }

    async fn list(
        &self,
        path: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        if let Some(start_after) = start_after {
            tracing::warn!(start_after, "start_after is ignored by SimObjectStore");
        }
        if let Some(limit) = limit {
            tracing::warn!(limit, "limit is ignored by SimObjectStore");
        }
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::List { path })
            .await?;
        if let Response::List(o) = resp {
            Ok(Box::pin(o))
        } else {
            panic!("expect Response::List");
        }
    }

    fn store_media_type(&self) -> &'static str {
        "sim"
    }
}

impl SimObjectStore {
    pub fn new(addr: &str) -> Self {
        let addr = addr.strip_prefix("sim://").unwrap();
        let (_access_key_id, rest) = addr.split_once(':').unwrap();
        let (_secret_access_key, rest) = rest.split_once('@').unwrap();
        let (address, _bucket) = rest.split_once('/').unwrap();

        Self {
            client: Client::new(
                address
                    .parse::<SocketAddr>()
                    .expect(&format!("parse SockAddr failed: {}", address)),
            ),
        }
    }
}
