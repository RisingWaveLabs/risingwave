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

use std::sync::Arc;

use bytes::Bytes;
use fail::fail_point;
use foyer::HybridCacheProperties;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::ObjectStreamingUploader;
use tokio::task::JoinHandle;
use zstd::zstd_safe::WriteBuf;

use super::multi_builder::UploadJoinHandle;
use super::{Block, BlockMeta};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    CachePolicy, HummockResult, RecentFilterTrait, SstableBlockIndex, SstableBuilderOptions,
    SstableMeta, SstableStore, SstableStoreRef,
};

/// A consumer of SST data.
#[async_trait::async_trait]
pub trait SstableWriter: Send {
    type Output;

    /// Write an SST block to the writer.
    async fn write_block(&mut self, block: &[u8], meta: &BlockMeta) -> HummockResult<()>;

    async fn write_block_bytes(&mut self, block: Bytes, meta: &BlockMeta) -> HummockResult<()>;

    /// Finish writing the SST.
    async fn finish(self, meta: SstableMeta) -> HummockResult<Self::Output>;

    /// Get the length of data that has already been written.
    fn data_len(&self) -> usize;
}

/// Append SST data to a buffer. Used for tests and benchmarks.
pub struct InMemWriter {
    buf: Vec<u8>,
}

impl InMemWriter {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }
}

impl From<&SstableBuilderOptions> for InMemWriter {
    fn from(options: &SstableBuilderOptions) -> Self {
        Self::new(options.capacity + options.block_capacity)
    }
}

#[async_trait::async_trait]
impl SstableWriter for InMemWriter {
    type Output = (Bytes, SstableMeta);

    async fn write_block(&mut self, block: &[u8], _meta: &BlockMeta) -> HummockResult<()> {
        self.buf.extend_from_slice(block);
        Ok(())
    }

    async fn write_block_bytes(&mut self, block: Bytes, _meta: &BlockMeta) -> HummockResult<()> {
        self.buf.extend_from_slice(&block);
        Ok(())
    }

    async fn finish(mut self, meta: SstableMeta) -> HummockResult<Self::Output> {
        meta.encode_to(&mut self.buf);
        Ok((Bytes::from(self.buf), meta))
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

pub struct SstableWriterOptions {
    /// Total length of SST data.
    pub capacity_hint: Option<usize>,
    pub tracker: Option<MemoryTracker>,
    pub policy: CachePolicy,
}

impl Default for SstableWriterOptions {
    fn default() -> Self {
        Self {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::NotFill,
        }
    }
}
#[async_trait::async_trait]
pub trait SstableWriterFactory: Send {
    type Writer: SstableWriter<Output = UploadJoinHandle>;

    async fn create_sst_writer(
        &mut self,
        object_id: impl Into<HummockSstableObjectId> + Send,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer>;
}

pub struct BatchSstableWriterFactory {
    sstable_store: SstableStoreRef,
}

impl BatchSstableWriterFactory {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        BatchSstableWriterFactory { sstable_store }
    }
}

#[async_trait::async_trait]
impl SstableWriterFactory for BatchSstableWriterFactory {
    type Writer = BatchUploadWriter;

    async fn create_sst_writer(
        &mut self,
        object_id: impl Into<HummockSstableObjectId> + Send,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer> {
        Ok(BatchUploadWriter::new(
            object_id,
            self.sstable_store.clone(),
            options,
        ))
    }
}

/// Buffer SST data and upload it as a whole on `finish`.
/// The upload is finished when the returned `JoinHandle` is joined.
pub struct BatchUploadWriter {
    object_id: HummockSstableObjectId,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
    buf: Vec<u8>,
    block_info: Vec<Block>,
    tracker: Option<MemoryTracker>,
}

impl BatchUploadWriter {
    pub fn new(
        object_id: impl Into<HummockSstableObjectId>,
        sstable_store: Arc<SstableStore>,
        options: SstableWriterOptions,
    ) -> Self {
        Self {
            object_id: object_id.into(),
            sstable_store,
            policy: options.policy,
            buf: Vec::with_capacity(options.capacity_hint.unwrap_or(0)),
            block_info: Vec::new(),
            tracker: options.tracker,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter for BatchUploadWriter {
    type Output = JoinHandle<HummockResult<()>>;

    async fn write_block(&mut self, block: &[u8], meta: &BlockMeta) -> HummockResult<()> {
        self.buf.extend_from_slice(block);
        if let CachePolicy::Fill(_) = self.policy {
            self.block_info.push(Block::decode(
                Bytes::from(block.to_vec()),
                meta.uncompressed_size as usize,
            )?);
        }
        Ok(())
    }

    async fn write_block_bytes(&mut self, block: Bytes, meta: &BlockMeta) -> HummockResult<()> {
        self.buf.extend_from_slice(&block);
        if let CachePolicy::Fill(_) = self.policy {
            self.block_info
                .push(Block::decode(block, meta.uncompressed_size as usize)?);
        }
        Ok(())
    }

    async fn finish(mut self, meta: SstableMeta) -> HummockResult<Self::Output> {
        fail_point!("data_upload_err");
        let join_handle = tokio::spawn(async move {
            meta.encode_to(&mut self.buf);
            let data = Bytes::from(self.buf);
            let _tracker = self.tracker.map(|mut t| {
                if !t.try_increase_memory(data.capacity() as u64) {
                    tracing::debug!("failed to allocate increase memory for data file, sst object id: {}, file size: {}",
                                    self.object_id, data.capacity());
                }
                t
            });

            // Upload data to object store.
            self.sstable_store
                .clone()
                .put_sst_data(self.object_id, data)
                .await?;
            self.sstable_store.insert_meta_cache(self.object_id, meta);

            // Only update recent filter with sst obj id is okay here, for l0 is only filter by sst obj id with recent filter.
            self.sstable_store
                .recent_filter()
                .insert((self.object_id, usize::MAX));

            // Add block cache.
            if let CachePolicy::Fill(hint) = self.policy {
                // The `block_info` may be empty when there is only range-tombstones, because we
                //  store them in meta-block.
                for (block_idx, block) in self.block_info.into_iter().enumerate() {
                    self.sstable_store.block_cache().insert_with_properties(
                        SstableBlockIndex {
                            sst_id: self.object_id,
                            block_idx: block_idx as _,
                        },
                        Box::new(block),
                        HybridCacheProperties::default().with_hint(hint),
                    );
                }
            }
            Ok(())
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

pub struct StreamingUploadWriter {
    object_id: HummockSstableObjectId,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
    /// Data are uploaded block by block, except for the size footer.
    object_uploader: ObjectStreamingUploader,
    /// Compressed blocks to refill block or meta cache. Keep the uncompressed capacity for decode.
    blocks: Vec<Block>,
    data_len: usize,
    tracker: Option<MemoryTracker>,
}

impl StreamingUploadWriter {
    pub fn new(
        object_id: HummockSstableObjectId,
        sstable_store: SstableStoreRef,
        object_uploader: ObjectStreamingUploader,
        options: SstableWriterOptions,
    ) -> Self {
        Self {
            object_id,
            sstable_store,
            policy: options.policy,
            object_uploader,
            blocks: Vec::new(),
            data_len: 0,
            tracker: options.tracker,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter for StreamingUploadWriter {
    type Output = JoinHandle<HummockResult<()>>;

    async fn write_block(&mut self, block_data: &[u8], meta: &BlockMeta) -> HummockResult<()> {
        self.data_len += block_data.len();
        let block_data = Bytes::from(block_data.to_vec());
        if let CachePolicy::Fill(_) = self.policy {
            let block = Block::decode(block_data.clone(), meta.uncompressed_size as usize)?;
            self.blocks.push(block);
        }
        self.object_uploader
            .write_bytes(block_data)
            .await
            .map_err(Into::into)
    }

    async fn write_block_bytes(&mut self, block: Bytes, meta: &BlockMeta) -> HummockResult<()> {
        self.data_len += block.len();
        if let CachePolicy::Fill(_) = self.policy {
            let block = Block::decode(block.clone(), meta.uncompressed_size as usize)?;
            self.blocks.push(block);
        }
        self.object_uploader
            .write_bytes(block)
            .await
            .map_err(Into::into)
    }

    async fn finish(mut self, meta: SstableMeta) -> HummockResult<UploadJoinHandle> {
        let metadata = Bytes::from(meta.encode_to_bytes());

        self.object_uploader.write_bytes(metadata).await?;
        let join_handle = tokio::spawn(async move {
            let uploader_memory_usage = self.object_uploader.get_memory_usage();
            let _tracker = self.tracker.map(|mut t| {
                    if !t.try_increase_memory(uploader_memory_usage) {
                        tracing::debug!("failed to allocate increase memory for data file, sst object id: {}, file size: {}",
                                        self.object_id, uploader_memory_usage);
                    }
                    t
                });

            assert!(!meta.block_metas.is_empty());

            // Upload data to object store.
            self.object_uploader.finish().await?;
            // Add meta cache.
            self.sstable_store.insert_meta_cache(self.object_id, meta);

            // Add block cache.
            if let CachePolicy::Fill(hint) = self.policy
                && !self.blocks.is_empty()
            {
                for (block_idx, block) in self.blocks.into_iter().enumerate() {
                    self.sstable_store.block_cache().insert_with_properties(
                        SstableBlockIndex {
                            sst_id: self.object_id,
                            block_idx: block_idx as _,
                        },
                        Box::new(block),
                        HybridCacheProperties::default().with_hint(hint),
                    );
                }
            }
            Ok(())
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.data_len
    }
}

pub struct StreamingSstableWriterFactory {
    sstable_store: SstableStoreRef,
}

impl StreamingSstableWriterFactory {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        StreamingSstableWriterFactory { sstable_store }
    }
}

#[async_trait::async_trait]
impl SstableWriterFactory for StreamingSstableWriterFactory {
    type Writer = StreamingUploadWriter;

    async fn create_sst_writer(
        &mut self,
        object_id: impl Into<HummockSstableObjectId> + Send,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer> {
        let object_id = object_id.into();
        let path = self.sstable_store.get_sst_data_path(object_id);
        let uploader = self.sstable_store.create_streaming_uploader(&path).await?;
        Ok(StreamingUploadWriter::new(
            object_id,
            self.sstable_store.clone(),
            uploader,
            options,
        ))
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use rand::{Rng, SeedableRng};
    use risingwave_common::util::iter_util::ZipEqFast;

    use crate::hummock::sstable::VERSION;
    use crate::hummock::{BlockMeta, InMemWriter, SstableMeta, SstableWriter};

    fn get_sst() -> (Bytes, Vec<Bytes>, SstableMeta) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let mut buffer: Vec<u8> = vec![0; 5000];
        rng.fill(&mut buffer[..]);
        buffer.extend((5_u32).to_le_bytes());
        let data = Bytes::from(buffer);

        let mut blocks = Vec::with_capacity(5);
        let mut block_metas = Vec::with_capacity(5);
        for i in 0..5 {
            block_metas.push(BlockMeta {
                smallest_key: Vec::new(),
                len: 1000,
                offset: i * 1000,
                ..Default::default()
            });
            blocks.push(data.slice((i * 1000) as usize..((i + 1) * 1000) as usize));
        }
        #[expect(deprecated)]
        let meta = SstableMeta {
            block_metas,
            bloom_filter: vec![],
            estimated_size: 0,
            key_count: 0,
            smallest_key: Vec::new(),
            largest_key: Vec::new(),
            meta_offset: data.len() as u64,
            monotonic_tombstone_events: vec![],
            version: VERSION,
        };

        (data, blocks, meta)
    }

    #[tokio::test]
    async fn test_in_mem_writer() {
        let (data, blocks, meta) = get_sst();
        let mut writer = Box::new(InMemWriter::new(0));
        for (block, meta) in blocks.iter().zip_eq_fast(meta.block_metas.iter()) {
            writer.write_block(&block[..], meta).await.unwrap();
        }

        let meta_offset = meta.meta_offset as usize;
        let (output_data, _) = writer.finish(meta).await.unwrap();
        assert_eq!(output_data.slice(0..meta_offset), data);
    }
}
