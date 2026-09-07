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

use std::collections::{HashMap, HashSet};
use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::BytesMut;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::version_object_size_map;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockObjectId, HummockVersionId, get_stale_object_ids};
use risingwave_pb::hummock::hummock_version_checkpoint::{PbStaleObjects, StaleObjects};
use risingwave_pb::hummock::{
    CheckpointCompressionAlgorithm, PbHummockVersion, PbHummockVersionArchive,
    PbHummockVersionCheckpoint, PbHummockVersionCheckpointEnvelope, PbVectorIndexObject,
    PbVectorIndexObjectType,
};
use thiserror_ext::AsReport;
use tracing::warn;

use crate::hummock::HummockManager;
use crate::hummock::error::Result;
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::metrics_utils::{gc_stale_object_stats, trigger_gc_stat, trigger_split_stat};

/// Computes xxhash64 checksum of the given data, using seed 0.
/// This matches the xxhash64 used in block checksum (see `sstable/utils.rs`).
pub(crate) fn xxhash64_checksum(data: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish()
}

#[derive(Default)]
pub struct HummockVersionCheckpoint {
    pub version: Arc<HummockVersion>,

    /// stale objects of versions before the current checkpoint.
    ///
    /// Previously we stored the stale object of each single version.
    /// Currently we will merge the stale object between two checkpoints, and only the
    /// id of the checkpointed hummock version are included in the map.
    pub stale_objects: HashMap<HummockVersionId, PbStaleObjects>,
}

impl HummockVersionCheckpoint {
    pub fn from_protobuf(checkpoint: &PbHummockVersionCheckpoint) -> Self {
        let version = checkpoint.version.as_ref().unwrap();
        warn_if_legacy_table_change_logs_are_present(version);
        Self {
            version: Arc::new(HummockVersion::from_persisted_protobuf(version)),
            stale_objects: checkpoint
                .stale_objects
                .iter()
                .map(|(version_id, objects)| (*version_id, objects.clone()))
                .collect(),
        }
    }

    /// Convert an owned `PbHummockVersionCheckpoint` to `HummockVersionCheckpoint`,
    /// moving data instead of cloning for better performance on large checkpoints.
    pub fn from_protobuf_owned(checkpoint: PbHummockVersionCheckpoint) -> Self {
        let version = checkpoint.version.unwrap();
        warn_if_legacy_table_change_logs_are_present(&version);
        Self {
            version: HummockVersion::from_persisted_protobuf_owned(version).into(),
            stale_objects: checkpoint.stale_objects,
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionCheckpoint {
        PbHummockVersionCheckpoint {
            version: Some(PbHummockVersion::from(self.version.as_ref())),
            stale_objects: self
                .stale_objects
                .iter()
                .map(|(version_id, objects)| (*version_id, objects.clone()))
                .collect(),
        }
    }
}

fn warn_if_legacy_table_change_logs_are_present(version: &PbHummockVersion) {
    if !version.table_change_logs.is_empty() {
        warn!(
            version_id = ?version.id,
            table_count = version.table_change_logs.len(),
            "deprecated table change logs found in hummock version checkpoint; ignoring them"
        );
    }
}

/// Decodes checkpoint data, supporting both envelope (compressed) and legacy (raw) formats.
///
/// Format detection: our writer always sets `checksum` in the envelope, so
/// `checksum.is_some()` reliably distinguishes envelope from legacy format.
/// Legacy bytes may happen to decode as an envelope (field 1 wire-type mismatch
/// is skipped, field 2 LEN matches `payload`), but will never have a checksum.
///
/// Decoding logic:
/// 1. Try to decode as `HummockVersionCheckpointEnvelope`
/// 2. If `checksum.is_some()`:
///    - Verify xxhash64 checksum
///    - Decompress payload according to `compression_algorithm`
///    - Decode decompressed bytes as `PbHummockVersionCheckpoint`
/// 3. If decode fails or `checksum.is_none()`:
///    - Decode bytes directly as legacy `PbHummockVersionCheckpoint`
fn decode_checkpoint_data(data: bytes::Bytes) -> Result<PbHummockVersionCheckpoint> {
    use anyhow::Context;
    use prost::Message;

    let data_size = data.len();

    if let Ok(envelope) = PbHummockVersionCheckpointEnvelope::decode(data.clone())
        && let Some(expected) = envelope.checksum
    {
        let actual = xxhash64_checksum(&envelope.payload);
        if actual != expected {
            return Err(anyhow::anyhow!(
                "checkpoint checksum mismatch: expected {:#x}, got {:#x}",
                expected,
                actual
            )
            .into());
        }

        let algo = CheckpointCompressionAlgorithm::try_from(envelope.compression_algorithm)
            .with_context(|| {
                format!(
                    "unknown checkpoint compression algorithm: {}",
                    envelope.compression_algorithm
                )
            })?;

        let decompressed = decompress_payload(algo, &envelope.payload)?;
        let ckpt = PbHummockVersionCheckpoint::decode(decompressed.as_ref())
            .context("failed to decode checkpoint envelope payload")?;
        if ckpt.version.is_none() {
            return Err(anyhow::anyhow!("checkpoint missing required field `version`").into());
        }

        tracing::info!(
            compression = ?algo,
            compressed_size = envelope.payload.len(),
            decompressed_size = decompressed.len(),
            compression_ratio =
                format!("{:.2}x", decompressed.len() as f64 / envelope.payload.len().max(1) as f64),
            checksum = format!("{expected:#x}"),
            "decoded compressed checkpoint"
        );
        return Ok(ckpt);
    }

    // Legacy uncompressed format
    tracing::info!(
        data_size,
        "decoding checkpoint in legacy uncompressed format"
    );
    let ckpt =
        PbHummockVersionCheckpoint::decode(data).context("failed to decode legacy checkpoint")?;
    if ckpt.version.is_none() {
        return Err(anyhow::anyhow!("legacy checkpoint missing required field `version`").into());
    }
    Ok(ckpt)
}

fn decompress_payload(
    algo: CheckpointCompressionAlgorithm,
    payload: &[u8],
) -> Result<std::borrow::Cow<'_, [u8]>> {
    use anyhow::Context;

    match algo {
        CheckpointCompressionAlgorithm::CheckpointCompressionUnspecified => Ok(payload.into()),
        CheckpointCompressionAlgorithm::CheckpointCompressionZstd => {
            Ok(zstd::stream::decode_all(payload)
                .map(std::borrow::Cow::Owned)
                .context("zstd decompression failed")?)
        }
        CheckpointCompressionAlgorithm::CheckpointCompressionLz4 => {
            let mut decoder = lz4::Decoder::new(payload).context("lz4 decoder init failed")?;
            let mut decompressed = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)
                .context("lz4 decompression failed")?;
            Ok(decompressed.into())
        }
    }
}

/// Compresses checkpoint data using the specified algorithm.
pub(crate) fn compress_payload(
    algo: risingwave_common::config::CheckpointCompression,
    data: &[u8],
) -> Result<Vec<u8>> {
    use anyhow::Context;
    use risingwave_common::config::CheckpointCompression;

    match algo {
        CheckpointCompression::None => Ok(data.to_vec()),
        CheckpointCompression::Zstd => {
            // Level 3: good balance between compression ratio and speed
            Ok(zstd::stream::encode_all(data, 3).context("zstd compression failed")?)
        }
        CheckpointCompression::Lz4 => {
            let mut compressed = Vec::new();
            let mut encoder = lz4::EncoderBuilder::new()
                .level(4)
                .build(&mut compressed)
                .context("lz4 encoder init failed")?;
            std::io::Write::write_all(&mut encoder, data)
                .context("lz4 compression write failed")?;
            let (_writer, result) = encoder.finish();
            result.context("lz4 compression finish failed")?;
            Ok(compressed)
        }
    }
}

async fn read_bytes_in_chunks<F, Fut>(
    total_size: usize,
    chunk_size: usize,
    max_in_flight_chunks: usize,
    mut read_range: F,
) -> anyhow::Result<bytes::Bytes>
where
    F: FnMut(std::ops::Range<usize>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bytes::Bytes>>,
{
    use anyhow::Context;
    use futures::StreamExt;

    let num_chunks = total_size.div_ceil(chunk_size);
    let mut buf = BytesMut::with_capacity(total_size);

    let mut chunk_stream = futures::stream::iter((0..total_size).step_by(chunk_size))
        .enumerate()
        .map(|(chunk_idx, offset)| {
            let end = std::cmp::min(offset + chunk_size, total_size);
            let range = offset..end;
            let fut = read_range(range.clone());
            async move {
                fut.await.with_context(|| {
                    format!(
                        "read checkpoint chunk {}/{} range {}..{}",
                        chunk_idx + 1,
                        num_chunks,
                        range.start,
                        range.end
                    )
                })
            }
        })
        .buffered(max_in_flight_chunks);

    while let Some(chunk) = chunk_stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
    }

    Ok(buf.freeze())
}

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl HummockManager {
    /// Returns Ok(None) if not found.
    ///
    /// Reads large checkpoints using bounded parallel chunked reads to avoid
    /// single-request timeout issues. Chunk size and concurrency are configurable
    /// via `checkpoint_read_chunk_size` and `checkpoint_read_max_in_flight_chunks`.
    /// Supports both compressed (envelope) and uncompressed (legacy) checkpoint formats.
    pub async fn try_read_checkpoint(&self) -> Result<Option<HummockVersionCheckpoint>> {
        let object_metadata = match self
            .object_store
            .metadata(&self.version_checkpoint_path)
            .await
        {
            Ok(metadata) => metadata,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let total_size = object_metadata.total_size;

        let chunk_size = self.env.opts.checkpoint_read_chunk_size;
        let max_in_flight_chunks = self.env.opts.checkpoint_read_max_in_flight_chunks;

        let download_start = std::time::Instant::now();
        let data = if total_size <= chunk_size {
            self.object_store
                .read(&self.version_checkpoint_path, 0..total_size)
                .await?
        } else {
            let num_chunks = total_size.div_ceil(chunk_size);
            let data = read_bytes_in_chunks(
                total_size,
                chunk_size,
                max_in_flight_chunks,
                |range| async {
                    Ok(self
                        .object_store
                        .read(&self.version_checkpoint_path, range)
                        .await?)
                },
            )
            .await?;

            tracing::info!(
                total_size,
                num_chunks,
                chunk_size,
                max_in_flight_chunks,
                "chunked read complete"
            );
            data
        };
        let download_duration = download_start.elapsed();

        let decode_start = std::time::Instant::now();
        let ckpt = decode_checkpoint_data(data)?;
        let decode_duration = decode_start.elapsed();

        tracing::info!(
            total_size,
            download_ms = download_duration.as_millis() as u64,
            decode_ms = decode_duration.as_millis() as u64,
            "checkpoint read complete"
        );

        Ok(Some(HummockVersionCheckpoint::from_protobuf_owned(ckpt)))
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let raw_bytes = checkpoint.to_protobuf().encode_to_vec();
        let raw_size = raw_bytes.len();

        let compression = self.env.opts.checkpoint_compression_algorithm;
        let compressed = compress_payload(compression, &raw_bytes)?;
        let checksum = xxhash64_checksum(&compressed);

        tracing::info!(
            raw_size,
            compressed_size = compressed.len(),
            compression_ratio =
                format!("{:.2}x", raw_size as f64 / compressed.len().max(1) as f64),
            compression = ?compression,
            checksum = format!("{:#x}", checksum),
            "writing compressed checkpoint"
        );

        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: compression as i32,
            payload: compressed,
            checksum: Some(checksum),
        };

        let buf = envelope.encode_to_vec();
        self.object_store
            .upload(&self.version_checkpoint_path, buf.into())
            .await?;
        Ok(())
    }

    pub(super) async fn write_version_archive(
        &self,
        archive: &PbHummockVersionArchive,
    ) -> Result<()> {
        use prost::Message;
        let buf = archive.encode_to_vec();
        let archive_path = format!(
            "{}/{}",
            self.version_archive_dir,
            archive.version.as_ref().unwrap().id
        );
        self.object_store.upload(&archive_path, buf.into()).await?;
        Ok(())
    }

    /// Creates a hummock version checkpoint.
    /// Returns the diff between new and old checkpoint id.
    /// Note that this method must not be called concurrently, because internally it doesn't hold
    /// lock throughout the method.
    pub async fn create_version_checkpoint(&self, min_delta_log_num: u64) -> Result<u64> {
        let timer = self.metrics.version_checkpoint_latency.start_timer();
        // 1. hold read lock briefly and snapshot checkpoint inputs.
        let (
            current_version,
            old_checkpoint_version,
            mut stale_objects,
            version_deltas,
            current_table_change_log,
            new_checkpoint_id,
            old_checkpoint_id,
        ) = {
            let versioning_guard = self
                .versioning
                .read_with_process_name("version_checkpoint_build")
                .await;
            let versioning: &Versioning = versioning_guard.deref();
            let current_version = versioning.current_version.clone();
            let old_checkpoint: &HummockVersionCheckpoint = &versioning.checkpoint;
            let new_checkpoint_id = current_version.id;
            let old_checkpoint_id = old_checkpoint.version.id;
            if new_checkpoint_id < old_checkpoint_id + min_delta_log_num {
                return Ok(0);
            }
            if cfg!(test) && new_checkpoint_id == old_checkpoint_id {
                drop(versioning_guard);
                let versioning = self
                    .versioning
                    .read_with_process_name("version_checkpoint_build")
                    .await;
                let context_info = self
                    .context_info
                    .read_with_process_name("version_checkpoint_build")
                    .await;
                let min_pinned_version_id = context_info.min_pinned_version_id();
                let stale_object_stats = gc_stale_object_stats(
                    &versioning.checkpoint.stale_objects,
                    min_pinned_version_id,
                );
                trigger_gc_stat(
                    &self.metrics,
                    versioning.checkpoint.version.as_ref(),
                    &versioning.table_change_log,
                    stale_object_stats,
                );
                return Ok(0);
            }
            let old_checkpoint_version = old_checkpoint.version.clone();
            let version_deltas = versioning
                .hummock_version_deltas
                .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
                .map(|(_, version_delta)| version_delta.clone())
                .collect::<Vec<_>>();
            (
                current_version,
                old_checkpoint_version,
                old_checkpoint.stale_objects.clone(),
                version_deltas,
                versioning.table_change_log.clone(),
                new_checkpoint_id,
                old_checkpoint_id,
            )
        };
        assert!(new_checkpoint_id > old_checkpoint_id);
        let mut archive: Option<PbHummockVersionArchive> = None;
        // `object_sizes` is used to calculate size of stale objects.
        let mut object_sizes = version_object_size_map(old_checkpoint_version.as_ref());
        // The set of object ids that once exist in any hummock version
        let mut versions_object_ids: HashSet<_> = old_checkpoint_version.get_object_ids().collect();
        for version_delta in &version_deltas {
            // DO NOT REMOVE THIS LINE
            // This is to ensure that when adding new variant to `HummockObjectId`,
            // the compiler will warn us if we forget to handle it here.
            match HummockObjectId::Sstable(0.into()) {
                HummockObjectId::Sstable(_) => {}
                HummockObjectId::VectorFile(_) => {}
                HummockObjectId::HnswGraphFile(_) => {}
            };
            for (object_id, file_size) in version_delta
                .newly_added_sst_infos()
                .map(|sst| (HummockObjectId::Sstable(sst.object_id), sst.file_size))
                .chain(
                    version_delta
                        .vector_index_delta
                        .values()
                        .flat_map(|delta| delta.newly_added_objects()),
                )
            {
                object_sizes.insert(object_id, file_size);
                versions_object_ids.insert(object_id);
            }
        }

        // Object ids that once exist in any hummock version but not exist in the latest hummock version
        let current_version_object_ids = current_version
            .get_object_ids()
            .chain(
                current_table_change_log
                    .values()
                    .flat_map(|l| l.get_object_ids()),
            )
            .collect();
        let removed_object_ids = &versions_object_ids - &current_version_object_ids;
        let total_file_size = removed_object_ids
            .iter()
            .map(|t| {
                object_sizes.get(t).copied().unwrap_or_else(|| {
                    warn!(object_id = ?t, "unable to get size of removed object id");
                    0
                })
            })
            .sum::<u64>();
        stale_objects.insert(current_version.id, {
            let mut sst_ids = vec![];
            let mut vector_files = vec![];
            for object_id in removed_object_ids {
                match object_id {
                    HummockObjectId::Sstable(sst_id) => sst_ids.push(sst_id),
                    HummockObjectId::VectorFile(vector_file_id) => {
                        vector_files.push(PbVectorIndexObject {
                            id: vector_file_id.as_raw(),
                            object_type: PbVectorIndexObjectType::VectorIndexObjectVector as _,
                        })
                    }
                    HummockObjectId::HnswGraphFile(graph_file_id) => {
                        vector_files.push(PbVectorIndexObject {
                            id: graph_file_id.as_raw(),
                            object_type: PbVectorIndexObjectType::VectorIndexObjectHnswGraph as _,
                        });
                    }
                }
            }
            StaleObjects {
                id: sst_ids,
                total_file_size,
                vector_files,
            }
        });
        if self.env.opts.enable_hummock_data_archive {
            archive = Some(PbHummockVersionArchive {
                version: Some(PbHummockVersion::from(old_checkpoint_version.as_ref())),
                version_deltas: version_deltas
                    .iter()
                    .map(|version_delta| version_delta.into())
                    .collect(),
            });
        }
        let min_pinned_version_id = self
            .context_info
            .read_with_process_name("version_checkpoint_build")
            .await
            .min_pinned_version_id();
        let may_delete_object = stale_objects
            .iter()
            .filter_map(|(version_id, object_ids)| {
                if *version_id >= min_pinned_version_id {
                    return None;
                }
                Some(get_stale_object_ids(object_ids))
            })
            .flatten();
        self.gc_manager.add_may_delete_object_ids(may_delete_object);
        stale_objects.retain(|version_id, _| *version_id >= min_pinned_version_id);
        let new_checkpoint = HummockVersionCheckpoint {
            version: current_version.clone(),
            stale_objects,
        };
        // 2. persist the new checkpoint without holding lock
        self.write_checkpoint(&new_checkpoint).await?;
        if let Some(archive) = archive
            && let Err(e) = self.write_version_archive(&archive).await
        {
            tracing::warn!(
                error = %e.as_report(),
                "failed to write version archive {}",
                archive.version.as_ref().unwrap().id
            );
        }
        let min_pinned_version_id = self
            .context_info
            .read_with_process_name("version_checkpoint_build")
            .await
            .min_pinned_version_id();
        let stale_object_stats =
            gc_stale_object_stats(&new_checkpoint.stale_objects, min_pinned_version_id);
        // 3. hold write lock briefly and update in memory state
        let current_version_for_metrics = {
            let mut versioning_guard = self
                .versioning
                .write_with_process_name("version_checkpoint_install")
                .await;
            let versioning = versioning_guard.deref_mut();
            assert!(new_checkpoint.version.id > versioning.checkpoint.version.id);
            versioning.checkpoint = new_checkpoint;
            versioning.current_version.clone()
        };
        trigger_gc_stat(
            &self.metrics,
            current_version.as_ref(),
            &current_table_change_log,
            stale_object_stats,
        );
        trigger_split_stat(&self.metrics, current_version_for_metrics.as_ref());
        timer.observe_duration();
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id.as_i64_id());

        Ok(new_checkpoint_id - old_checkpoint_id)
    }

    pub fn pause_version_checkpoint(&self) {
        self.pause_version_checkpoint.store(true, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is paused.");
    }

    pub fn resume_version_checkpoint(&self) {
        self.pause_version_checkpoint
            .store(false, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is resumed.");
    }

    pub fn is_version_checkpoint_paused(&self) -> bool {
        self.pause_version_checkpoint.load(Ordering::Relaxed)
    }

    pub async fn get_checkpoint_version(&self) -> Arc<HummockVersion> {
        let versioning_guard = self
            .versioning
            .read_with_process_name("get_checkpoint_version")
            .await;
        versioning_guard.checkpoint.version.clone()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;
    use risingwave_common::config::CheckpointCompression;
    use risingwave_pb::hummock::hummock_version_checkpoint::StaleObjects;
    use risingwave_pb::hummock::{
        PbHummockVersion, PbHummockVersionCheckpoint, PbHummockVersionCheckpointEnvelope,
        PbTableChangeLog,
    };

    use super::{
        HummockVersionCheckpoint, compress_payload, decode_checkpoint_data, read_bytes_in_chunks,
        xxhash64_checksum,
    };

    #[expect(deprecated)]
    fn make_version(id: u64) -> PbHummockVersion {
        PbHummockVersion {
            id: id.into(),
            levels: Default::default(),
            max_committed_epoch: 0,
            table_watermarks: Default::default(),
            table_change_logs: Default::default(),
            state_table_info: Default::default(),
            vector_indexes: Default::default(),
        }
    }

    fn make_checkpoint(version_id: u64) -> PbHummockVersionCheckpoint {
        let stale = StaleObjects {
            id: vec![1u64.into(), 2u64.into(), 3u64.into()],
            total_file_size: 123,
            vector_files: vec![],
        };

        PbHummockVersionCheckpoint {
            version: Some(make_version(version_id)),
            stale_objects: [(1u64.into(), stale)].into_iter().collect(),
        }
    }

    #[test]
    fn deprecated_table_change_logs_are_ignored() {
        let mut checkpoint = make_checkpoint(42);
        checkpoint
            .version
            .as_mut()
            .unwrap()
            .table_change_logs
            .insert(1.into(), PbTableChangeLog::default());

        let checkpoint = HummockVersionCheckpoint::from_protobuf(&checkpoint);
        assert!(
            checkpoint
                .to_protobuf()
                .version
                .unwrap()
                .table_change_logs
                .is_empty()
        );
    }

    fn make_envelope_bytes(
        checkpoint: &PbHummockVersionCheckpoint,
        compression: CheckpointCompression,
        checksum: Option<u64>,
    ) -> Bytes {
        let raw = checkpoint.encode_to_vec();
        let payload = compress_payload(compression, &raw)
            .expect("compress checkpoint payload should succeed");
        let checksum = checksum.unwrap_or_else(|| xxhash64_checksum(&payload));
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: compression as i32,
            payload,
            checksum: Some(checksum),
        };
        Bytes::from(envelope.encode_to_vec())
    }

    #[test]
    fn decode_checkpoint_data_falls_back_to_legacy_format() {
        let checkpoint = make_checkpoint(42);
        let raw = Bytes::from(checkpoint.encode_to_vec());
        let decoded = decode_checkpoint_data(raw).expect("legacy checkpoint should decode");
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn decode_checkpoint_data_roundtrips_envelope_with_checksum() {
        let checkpoint = make_checkpoint(42);
        for compression in [
            CheckpointCompression::None,
            CheckpointCompression::Zstd,
            CheckpointCompression::Lz4,
        ] {
            let data = make_envelope_bytes(&checkpoint, compression, None);
            let decoded = decode_checkpoint_data(data).expect("envelope checkpoint should decode");
            assert_eq!(decoded, checkpoint);
        }
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_checksum_mismatch() {
        let checkpoint = make_checkpoint(42);
        let raw = checkpoint.encode_to_vec();
        let mut payload = compress_payload(CheckpointCompression::Zstd, &raw)
            .expect("compress checkpoint payload should succeed");
        let expected = xxhash64_checksum(&payload);
        payload[0] ^= 0x01;
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: CheckpointCompression::Zstd as i32,
            payload,
            checksum: Some(expected),
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let err = decode_checkpoint_data(data).expect_err("checksum mismatch should error");
        assert!(err.to_string().contains("checksum mismatch"), "{err:?}");
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_unknown_compression_algorithm() {
        let checkpoint = make_checkpoint(42);
        let payload = checkpoint.encode_to_vec();
        let checksum = xxhash64_checksum(&payload);
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: 123,
            payload,
            checksum: Some(checksum),
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let err =
            decode_checkpoint_data(data).expect_err("unknown compression algorithm should error");
        assert!(
            err.to_string()
                .contains("unknown checkpoint compression algorithm"),
            "{err:?}"
        );
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_legacy_missing_version() {
        let checkpoint = PbHummockVersionCheckpoint {
            version: None,
            stale_objects: Default::default(),
        };
        let data = Bytes::from(checkpoint.encode_to_vec());
        let err = decode_checkpoint_data(data).expect_err("missing version should error");
        assert!(
            err.to_string()
                .contains("legacy checkpoint missing required field `version`"),
            "{err:?}"
        );
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_corrupt_envelope_payload() {
        let garbage = b"not a valid protobuf";
        let checksum = xxhash64_checksum(garbage);
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: CheckpointCompression::None as i32,
            payload: garbage.to_vec(),
            checksum: Some(checksum),
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let err = decode_checkpoint_data(data).expect_err("corrupt envelope payload should error");
        assert!(
            err.to_string()
                .contains("failed to decode checkpoint envelope payload"),
            "{err:?}"
        );
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_empty_input() {
        let err = decode_checkpoint_data(Bytes::new()).expect_err("empty checkpoint should fail");
        assert!(
            err.to_string()
                .contains("legacy checkpoint missing required field `version`"),
            "{err:?}"
        );
    }

    #[test]
    fn decode_checkpoint_data_returns_error_on_envelope_missing_version() {
        let checkpoint = PbHummockVersionCheckpoint {
            version: None,
            stale_objects: Default::default(),
        };
        let raw = checkpoint.encode_to_vec();
        let checksum = xxhash64_checksum(&raw);
        let envelope = PbHummockVersionCheckpointEnvelope {
            compression_algorithm: CheckpointCompression::None as i32,
            payload: raw,
            checksum: Some(checksum),
        };
        let data = Bytes::from(envelope.encode_to_vec());
        let err =
            decode_checkpoint_data(data).expect_err("envelope with missing version should error");
        assert!(
            err.to_string()
                .contains("checkpoint missing required field `version`"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn read_bytes_in_chunks_respects_concurrency_limit_and_reassembles() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use tokio::time::{Duration, sleep};

        let total_size = 100usize;
        let chunk_size = 10usize;
        let max_in_flight = 3usize;

        let data: Arc<Vec<u8>> = Arc::new((0..total_size).map(|i| (i % 256) as u8).collect());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let out = read_bytes_in_chunks(total_size, chunk_size, max_in_flight, {
            let data = data.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            move |range: std::ops::Range<usize>| {
                let data = data.clone();
                let in_flight = in_flight.clone();
                let max_seen = max_seen.clone();
                async move {
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(cur, Ordering::SeqCst);

                    // Add a small delay to simulate real I/O and allow multiple reads
                    // to be in-flight concurrently. This tests that max_in_flight limit
                    // is respected (should not exceed 3 concurrent reads).
                    sleep(Duration::from_millis(30)).await;

                    let bytes = Bytes::copy_from_slice(&data[range]);
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(bytes)
                }
            }
        })
        .await
        .expect("chunked read should succeed");

        assert_eq!(out.as_ref(), data.as_slice());
        let max_seen = max_seen.load(Ordering::SeqCst);
        assert!(max_seen <= max_in_flight, "max_seen={max_seen}");
        assert!(
            max_seen > 1,
            "expected some concurrency, max_seen={max_seen}"
        );
    }

    #[tokio::test]
    async fn read_bytes_in_chunks_adds_range_context_on_error() {
        let total_size = 30usize;
        let chunk_size = 10usize;
        let max_in_flight = 2usize;

        let err = read_bytes_in_chunks(total_size, chunk_size, max_in_flight, |range| async move {
            if range.start == 10 {
                anyhow::bail!("boom");
            }
            Ok(Bytes::copy_from_slice(&vec![0u8; range.len()]))
        })
        .await
        .expect_err("should fail");

        let msg = err.to_string();
        assert!(
            msg.contains("read checkpoint chunk 2/3 range 10..20"),
            "unexpected error message: {msg}"
        );
        let msg_with_chain = format!("{err:#}");
        assert!(
            msg_with_chain.contains("boom"),
            "unexpected error message: {msg_with_chain}"
        );
    }
}
