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

use anyhow::{Context, anyhow};
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use iceberg::delete_vector::DeleteVector;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType,
    SerializedDataFile,
};
use iceberg::table::Table;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::id::SinkId;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergPositionDeleteCommitResult, read_position_deletes_from_file,
    resolve_partition_type, serialize_data_files_default_spec, write_dv_puffin_file,
    write_parquet_position_delete_file,
};
use risingwave_connector::sink::{Result as SinkResult, SinkError};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::ActorId;
use risingwave_rpc_client::MetaClient;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::load_table_at_least;
use super::position_delete_merger::PositionDeleteHandler;
use super::position_delete_staging::StagingVersion;

/// Real implementation of [`PositionDeleteHandler`] using the iceberg-rust crate.
///
/// The iceberg table is NOT loaded at construction. `start_seed` (called right
/// after the executor's first barrier) spawns a background task that first waits
/// for meta to have committed through the first barrier's `prev` epoch, then
/// loads the table from the catalog (retrying until it reflects the committed
/// snapshot) and seeds the per-shard delete state in [`StagingVersion`] from the
/// delete manifests. The seeded state is awaited lazily at the first flush; the
/// table handle is kept for object-store I/O and cached metadata.
///
/// `start_seed` is also called again after each compaction commit. That restart drops the whole
/// [`SeededState`] — table handle, resident delete vectors and all — and repeats the sequence
/// against the post-compaction snapshot.
pub struct PositionDeleteHandlerImpl {
    config: IcebergConfig,
    actor_id: ActorId,
    vnode_bitmap: Option<Bitmap>,
    sink_id: SinkId,
    meta_client: MetaClient,
    inner: HandlerInner,
    /// New positions accumulated since the last flush, keyed by data file path. Buffered regardless
    /// of seed state — `write` never touches the table.
    pending: HashMap<String, DeleteVector>,
}

/// Everything that only exists once the table has been loaded and staging seeded.
struct SeededState {
    table: Table,
    location_generator: DefaultLocationGenerator,
    /// File-name generator for V3 Puffin deletion vector files.
    puffin_file_name_generator: DefaultFileNameGenerator,
    /// File-name generator for V2 Parquet position-delete files.
    parquet_file_name_generator: DefaultFileNameGenerator,
    schema_id: i32,
    partition_spec_id: i32,
    format_version: FormatVersion,
    staging: StagingVersion,
}

enum HandlerInner {
    /// Before `start_seed`.
    Unseeded,
    /// Background task: wait for the commit epoch, load the table, seed staging.
    Seeding(JoinHandle<SinkResult<SeededState>>),
    Ready(SeededState),
}

impl Drop for PositionDeleteHandlerImpl {
    fn drop(&mut self) {
        // Abort the background seed task if it is still running, so a merger actor dropped mid-seed
        // does not leave a detached task pinning the meta wait RPC + catalog-reload retries.
        if let HandlerInner::Seeding(handle) = &self.inner {
            handle.abort();
        }
    }
}

impl PositionDeleteHandlerImpl {
    pub fn new(
        config: IcebergConfig,
        actor_id: ActorId,
        vnode_bitmap: Option<Bitmap>,
        sink_id: SinkId,
        meta_client: MetaClient,
    ) -> Self {
        Self {
            config,
            actor_id,
            vnode_bitmap,
            sink_id,
            meta_client,
            inner: HandlerInner::Unseeded,
            pending: HashMap::new(),
        }
    }

    /// Await the background seed, transitioning `Seeding -> Ready`. Errors if called before
    /// `start_seed`.
    async fn await_ready(&mut self) -> SinkResult<&mut SeededState> {
        if matches!(self.inner, HandlerInner::Seeding(_)) {
            // Take the handle out before awaiting so an errored seed leaves `Unseeded` (a clean error
            // on any later flush) rather than a `Seeding` holding a consumed `JoinHandle` that would
            // panic if re-polled.
            let HandlerInner::Seeding(handle) =
                std::mem::replace(&mut self.inner, HandlerInner::Unseeded)
            else {
                unreachable!("guarded by matches! above");
            };
            let state = handle.await.map_err(|e| SinkError::Iceberg(anyhow!(e)))??;
            self.inner = HandlerInner::Ready(state);
        }
        match &mut self.inner {
            HandlerInner::Ready(state) => Ok(state),
            HandlerInner::Unseeded => Err(SinkError::Iceberg(anyhow!(
                "pk-index merger flush before start_seed"
            ))),
            HandlerInner::Seeding(_) => unreachable!("just transitioned out of Seeding"),
        }
    }

    async fn flush_inner(&mut self) -> SinkResult<Option<SinkMetadata>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let config = self.config.clone();
        let pending = std::mem::take(&mut self.pending);
        let state = self.await_ready().await?;

        let mut delete_files = Vec::with_capacity(pending.len());
        let mut overwrite_files = Vec::new();

        for (data_file_path, new_positions) in pending {
            // Merge the new positions with the resident delete vector, lazily loading
            // it on first touch of this path.
            let plan = {
                let entry = state.staging.entry_mut(&data_file_path);
                if entry.needs_load() {
                    let existing_file = entry
                        .current_file()
                        .expect("needs_load implies current_file is Some");
                    let dv = read_position_deletes_from_file(state.table.file_io(), existing_file)
                        .await
                        .map_err(SinkError::Iceberg)?;
                    entry.set_loaded_delete_vector(dv);
                }
                entry.plan_write(new_positions)
            };
            if let Some(overwrite) = plan.overwrite {
                overwrite_files.push(overwrite);
            }

            // V3 tables use Puffin deletion vectors; V2 tables use file-scoped Parquet
            // position-delete files. The internal representation is a roaring bitmap
            // (`DeleteVector`) in both cases; only the on-disk format differs.
            let use_puffin = state.format_version >= FormatVersion::V3;
            let new_file = if use_puffin {
                write_dv_puffin_file(
                    &state.table,
                    &state.location_generator,
                    &state.puffin_file_name_generator,
                    data_file_path.clone(),
                    &plan.merged,
                    None,
                )
                .await
                .map_err(SinkError::Iceberg)?
            } else {
                write_parquet_position_delete_file(
                    &state.table,
                    &state.location_generator,
                    &state.parquet_file_name_generator,
                    &config,
                    data_file_path.clone(),
                    &plan.merged,
                    None,
                )
                .await
                .map_err(SinkError::Iceberg)?
            };

            state
                .staging
                .entry_mut(&data_file_path)
                .record_written(new_file.clone(), plan.merged);
            delete_files.push(new_file);
        }

        if delete_files.is_empty() {
            return Ok(None);
        }

        let delete_files = serialize_delete_files(&state.table, delete_files)?;
        let overwrite_files = serialize_overwrite_files(&state.table, overwrite_files)?;

        let sink_metadata = SinkMetadata::try_from(&IcebergPositionDeleteCommitResult {
            schema_id: state.schema_id,
            partition_spec_id: state.partition_spec_id,
            delete_files,
            overwrite_files,
        })?;
        Ok(Some(sink_metadata))
    }
}

#[async_trait::async_trait]
impl PositionDeleteHandler for PositionDeleteHandlerImpl {
    fn start_seed(&mut self, wait_epoch: u64) {
        // Restart (post-compaction re-seed): drop everything the previous seed produced. Replacing
        // `self.inner` below drops any `SeededState`, and with it the whole resident delete-vector
        // cache; abort a still-running previous seed first so it does not linger as a detached task
        // pinning the meta wait RPC and catalog-reload retries.
        if let HandlerInner::Seeding(handle) =
            std::mem::replace(&mut self.inner, HandlerInner::Unseeded)
        {
            handle.abort();
        }

        let config = self.config.clone();
        let actor_id = self.actor_id;
        let vnode_bitmap = self.vnode_bitmap.clone();
        let sink_id = self.sink_id;
        let meta_client = self.meta_client.clone();
        self.inner = HandlerInner::Seeding(tokio::spawn(async move {
            let result: SinkResult<SeededState> = async move {
                // 1. Block until meta has committed through `wait_epoch`; get the committed snapshot
                //    lower bound.
                let expected_snapshot = meta_client
                    .wait_iceberg_pk_index_sink_epoch(sink_id, wait_epoch)
                    .await
                    .map_err(|e| {
                        SinkError::Iceberg(anyhow!(e).context(
                            "wait for pk-index sink commit epoch before seeding merger staging",
                        ))
                    })?;

                // 2. Load the table, retrying until the catalog reflects at least `expected_snapshot`.
                let table = load_table_at_least(&config, expected_snapshot).await?;

                // 3. Derive per-table state + seed staging (shard-filtered).
                let location_generator = DefaultLocationGenerator::new(table.metadata())?;
                let uuid_suffix = Uuid::now_v7();
                let puffin_file_name_generator = DefaultFileNameGenerator::new(
                    actor_id.to_string(),
                    Some(format!("delvec-{}", uuid_suffix)),
                    DataFileFormat::Puffin,
                );
                let parquet_file_name_generator = DefaultFileNameGenerator::new(
                    actor_id.to_string(),
                    Some(format!("pos-del-{}", uuid_suffix)),
                    DataFileFormat::Parquet,
                );
                let schema_id = table.metadata().current_schema_id();
                let partition_spec_id = table.metadata().default_partition_spec_id();
                let format_version = table.metadata().format_version();

                let mut staging = StagingVersion::new(vnode_bitmap);
                seed_from_delete_manifests(&table, &mut staging).await?;

                Ok(SeededState {
                    table,
                    location_generator,
                    puffin_file_name_generator,
                    parquet_file_name_generator,
                    schema_id,
                    partition_spec_id,
                    format_version,
                    staging,
                })
            }
            .await;
            if let Err(e) = &result {
                // The handle is only awaited at the first flush with pending deletes, which may be far
                // in the future (or never on an idle shard); log here so a failed seed is visible.
                tracing::warn!(%sink_id, error = ?e.as_report(), "iceberg pk-index merger background seed failed; will surface at next flush");
            }
            result
        }));
    }

    fn write(&mut self, path: &str, pos: i64) -> SinkResult<()> {
        let pos: u64 = pos.try_into().context("position should be non-negative")?;
        self.pending.entry_ref(path).or_default().insert(pos);
        Ok(())
    }

    async fn flush(&mut self) -> SinkResult<Option<SinkMetadata>> {
        self.flush_inner().await
    }
}

/// Scan the table's current-snapshot delete manifests once and seed `staging` with
/// every live position-delete file whose referenced data file belongs to this
/// actor's shard. Contents are not read here; `StagedEntry::delete_vector` stays
/// `None` and is lazily loaded on first touch.
async fn seed_from_delete_manifests(table: &Table, staging: &mut StagingVersion) -> SinkResult<()> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(());
    };
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await?;

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if !entry.is_alive()
                || entry.content_type() != DataContentType::PositionDeletes
                || !matches!(
                    entry.file_format(),
                    DataFileFormat::Puffin | DataFileFormat::Parquet
                )
            {
                continue;
            }
            let data_file = entry.data_file();
            let Some(referenced_data_file) = data_file.referenced_data_file() else {
                continue;
            };
            if !staging.owns(&referenced_data_file) {
                continue;
            }
            staging.seed(referenced_data_file, data_file.clone());
        }
    }
    Ok(())
}

/// Serializes newly written deletion vector files against the current default
/// partition spec, after truncating oversized column statistics.
fn serialize_delete_files(
    table: &Table,
    delete_files: Vec<DataFile>,
) -> SinkResult<Vec<SerializedDataFile>> {
    serialize_data_files_default_spec(table, delete_files)
}

/// Serializes the original DV puffin files that are being overwritten. Each
/// existing DV puffin file was written under its own partition spec (potentially
/// older than the current default), so we resolve its partition type via the
/// file's `partition_spec_id` so the serialized form round-trips correctly,
/// instead of forcing the current default partition type.
fn serialize_overwrite_files(
    table: &Table,
    overwrite_files: Vec<DataFile>,
) -> SinkResult<Vec<SerializedDataFile>> {
    let format_version = table.metadata().format_version();
    let schema = table.metadata().current_schema();
    let mut partition_type_cache: HashMap<i32, iceberg::spec::StructType> = HashMap::new();
    overwrite_files
        .into_iter()
        .map(|f| {
            let spec_id = f.partition_spec_id();
            let pt = match partition_type_cache.entry(spec_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    // Resolve against the file's own (potentially older) partition spec, not the
                    // table default, so the serialized form round-trips correctly.
                    let pt = resolve_partition_type(table, spec_id, schema)?;
                    entry.insert(pt)
                }
            };

            Ok(SerializedDataFile::try_from(f, pt, format_version)?)
        })
        .collect()
}
