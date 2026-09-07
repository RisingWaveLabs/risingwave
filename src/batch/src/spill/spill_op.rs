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

use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use anyhow::anyhow;
use futures_async_stream::try_stream;
use futures_util::AsyncReadExt;
use opendal::Operator;
use opendal::layers::RetryLayer;
use opendal::services::{Fs, Memory};
use risingwave_common::array::DataChunk;
use risingwave_common::util::batch_spill_config::batch_spill_base_dir;
use risingwave_pb::Message;
use risingwave_pb::data::DataChunk as PbDataChunk;
use thiserror_ext::AsReport;
use tokio::sync::Mutex;
use twox_hash::XxHash64;

use crate::error::{BatchError, Result};
use crate::monitor::BatchSpillMetrics;

pub const DEFAULT_SPILL_PARTITION_NUM: usize = 20;
const RW_MANAGED_SPILL_DIR: &str = "rw_batch_spill/";
const DEFAULT_IO_BUFFER_SIZE: usize = 256 * 1024;
const DEFAULT_IO_CONCURRENT_TASK: usize = 8;

#[derive(Clone)]
pub enum SpillBackend {
    Disk,
    /// Only for testing purpose
    Memory,
}

/// `SpillOp` is used to manage the spill directory of the spilling executor and it will drop the directory with a RAII style.
pub struct SpillOp {
    pub op: Operator,
}

impl SpillOp {
    fn batch_spill_root() -> PathBuf {
        batch_spill_base_dir().join(RW_MANAGED_SPILL_DIR)
    }

    pub fn create(path: impl AsRef<Path>, spill_backend: SpillBackend) -> Result<SpillOp> {
        let path = path.as_ref();
        if !path.is_relative() {
            bail!("Spill path must be relative, but got {:?}", path);
        }

        let root = Self::batch_spill_root().join(path);

        let op = match spill_backend {
            SpillBackend::Disk => {
                let builder = Fs::default().root(&root.to_string_lossy());
                Operator::new(builder)?.layer(RetryLayer::default())
            }
            SpillBackend::Memory => {
                let builder = Memory::default().root(&root.to_string_lossy());
                Operator::new(builder)?.layer(RetryLayer::default())
            }
        };
        Ok(SpillOp { op })
    }

    pub async fn clean_spill_directory() -> opendal::Result<()> {
        static LOCK: LazyLock<Mutex<usize>> = LazyLock::new(|| Mutex::new(0));
        let _guard = LOCK.lock().await;

        let root = Self::batch_spill_root();

        let builder = Fs::default().root(&root.to_string_lossy());

        let op: Operator = Operator::new(builder)?.layer(RetryLayer::default());

        op.delete_with("/").recursive(true).await
    }

    pub async fn writer_with(&self, name: &str) -> Result<opendal::Writer> {
        Ok(self
            .op
            .writer_with(name)
            .concurrent(DEFAULT_IO_CONCURRENT_TASK)
            .chunk(DEFAULT_IO_BUFFER_SIZE)
            .await?)
    }

    pub async fn reader_with(&self, name: &str) -> Result<opendal::Reader> {
        Ok(self
            .op
            .reader_with(name)
            .chunk(DEFAULT_IO_BUFFER_SIZE)
            .await?)
    }

    /// spill file content will look like the below.
    ///
    /// ```text
    /// [proto_len]
    /// [proto_bytes]
    /// ...
    /// [proto_len]
    /// [proto_bytes]
    /// ```
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    pub async fn read_stream(reader: opendal::Reader, spill_metrics: Arc<BatchSpillMetrics>) {
        let mut reader = reader.into_futures_async_read(..).await?;
        let mut buf = [0u8; 4];
        loop {
            if let Err(err) = reader.read_exact(&mut buf).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(anyhow!(err).into());
                }
            }
            let len = u32::from_le_bytes(buf) as usize;
            spill_metrics.batch_spill_read_bytes.inc_by(len as u64 + 4);
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await.map_err(|e| anyhow!(e))?;
            let chunk_pb: PbDataChunk = Message::decode(buf.as_slice()).map_err(|e| anyhow!(e))?;
            let chunk = DataChunk::from_protobuf(&chunk_pb)?;
            yield chunk;
        }
    }
}

impl Drop for SpillOp {
    fn drop(&mut self) {
        let op = self.op.clone();
        tokio::task::spawn(async move {
            let result = op.delete_with("/").recursive(true).await;
            if let Err(error) = result {
                error!(
                    error = %error.as_report(),
                    "Failed to remove spill directory"
                );
            }
        });
    }
}

impl DerefMut for SpillOp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}

impl Deref for SpillOp {
    type Target = Operator;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

#[derive(Default, Clone, Copy)]
pub struct SpillBuildHasher(pub u64);

impl BuildHasher for SpillBuildHasher {
    type Hasher = XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash64::with_seed(self.0)
    }
}

pub const SPILL_AT_LEAST_MEMORY: u64 = 1024 * 1024;
