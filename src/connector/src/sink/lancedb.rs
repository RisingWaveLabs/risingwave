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

use core::num::NonZeroU64;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::CommitBuilder;
use lance::dataset::fragment::FileFragment;
use lance::dataset::transaction::{Operation, TransactionBuilder};
use lance::dataset::write::WriteParams;
use lance::io::ObjectStoreParams;
use lance_table::format::Fragment;
use lancedb::arrow::{
    SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream,
};
use lancedb::connection::ConnectBuilder;
use lancedb::{Connection as LanceDbConnection, Table as LanceDbTable};
use risingwave_common::array::StreamChunk;
use risingwave_common::array::arrow::LanceDbConvert;
use risingwave_common::catalog::Schema;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_USER_FORCE_APPEND_ONLY_OPTION,
    SinglePhaseCommitCoordinator, Sink, SinkCommitCoordinator, SinkError, SinkParam,
    SinkWriterParam, TwoPhaseCommitCoordinator,
};

pub const LANCEDB_SINK: &str = "lancedb";

const RW_EPOCH_TRANSACTION_PROPERTY: &str = "risingwave.epoch";
const RW_SINK_ID_TRANSACTION_PROPERTY: &str = "risingwave.sink_id";
const WRITE_CHANNEL_CAPACITY: usize = 16;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct LanceDbCommon {
    /// URI of the `LanceDB` database (e.g., `/tmp/lancedb`, `<s3://bucket/path/db>`)
    #[serde(rename = "lancedb.uri")]
    pub uri: String,

    /// Table name in the `LanceDB` database
    #[serde(rename = "lancedb.table")]
    pub table: String,

    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,
}

impl LanceDbCommon {
    pub async fn create_connection(&self) -> Result<LanceDbConnection> {
        let conn = ConnectBuilder::new(&self.uri)
            .execute()
            .await
            .context("failed to connect to LanceDB")
            .map_err(SinkError::LanceDb)?;
        Ok(conn)
    }

    async fn open_table(&self, conn: &LanceDbConnection) -> Result<LanceDbTable> {
        conn.open_table(&self.table)
            .execute()
            .await
            .context("failed to open LanceDB table")
            .map_err(SinkError::LanceDb)
    }

    /// Get the Lance dataset URI from the opened native `LanceDB` table.
    async fn dataset_uri(&self, table: &LanceDbTable) -> Result<String> {
        let dataset_wrapper = table.dataset().ok_or_else(|| {
            SinkError::LanceDb(anyhow!(
                "failed to get underlying lance Dataset (table may be remote)"
            ))
        })?;
        let dataset_guard = dataset_wrapper
            .get()
            .await
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;
        Ok(dataset_guard.uri().to_owned())
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct LanceDbConfig {
    #[serde(flatten)]
    pub common: LanceDbCommon,

    pub r#type: String,

    /// Whether to use RisingWave's two-phase commit framework for exactly-once commits.
    /// Defaults to true. Set to false to use single-phase commits.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub is_exactly_once: Option<bool>,
}

impl LanceDbConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<LanceDbConfig>(
            serde_json::to_value(properties).map_err(|e| SinkError::LanceDb(e.into()))?,
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LanceDbSink {
    pub config: LanceDbConfig,
    param: SinkParam,
}

impl EnforceSecret for LanceDbSink {
    fn enforce_secret<'a>(
        _prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        // LanceDB currently has no secret properties.
        Ok(())
    }
}

impl LanceDbSink {
    pub fn new(config: LanceDbConfig, param: SinkParam) -> Result<Self> {
        Ok(Self { config, param })
    }
}

impl Sink for LanceDbSink {
    type LogSinker = CoordinatedLogSinker<LanceDbSinkWriter>;

    const SINK_NAME: &'static str = LANCEDB_SINK;

    fn is_exactly_once(properties: &BTreeMap<String, String>) -> Result<bool> {
        let Some(value) = properties.get("is_exactly_once") else {
            return Ok(true);
        };
        value.parse::<bool>().map_err(|_| {
            SinkError::Config(anyhow!(
                "invalid value for `is_exactly_once`: expected `true` or `false`, got `{value}`"
            ))
        })
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let inner = LanceDbSinkWriter::new(self.config.clone(), self.param.schema()).await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.common.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        let writer = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            inner,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(writer)
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        LanceDbConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn validate(&self) -> Result<()> {
        // Only append-only is supported
        if self.config.r#type != SINK_TYPE_APPEND_ONLY
            && self.config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only LanceDB sink is supported",
            )));
        }

        if self.config.common.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }

        // Validate connection
        let conn = self.config.common.create_connection().await?;

        // Validate table exists and schema is compatible
        let table = self.config.common.open_table(&conn).await?;

        // Get the Lance table schema (Arrow schema)
        let lance_schema = table
            .schema()
            .await
            .context("failed to get LanceDB table schema")
            .map_err(SinkError::LanceDb)?;

        // Convert RW schema to arrow schema and compare
        let rw_schema = self.param.schema();
        let rw_arrow_schema = LanceDbConvert
            .rw_schema_to_arrow_schema(&rw_schema)
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;

        validate_ordered_schema(&rw_arrow_schema, lance_schema.as_ref())?;

        Ok(())
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let committer =
            LanceDbSinkCommitter::new(self.config.clone(), self.param.sink_id.to_string()).await?;
        if Self::is_exactly_once(&self.param.properties)? {
            Ok(SinkCommitCoordinator::TwoPhase(Box::new(committer)))
        } else {
            Ok(SinkCommitCoordinator::SinglePhase(Box::new(committer)))
        }
    }
}

impl TryFrom<SinkParam> for LanceDbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = LanceDbConfig::from_btreemap(param.properties.clone())?;
        LanceDbSink::new(config, param)
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

// Re-export arrow types from the LanceDb arrow module so they're used consistently.
use risingwave_common::array::arrow::{
    arrow_array_lancedb as arrow_array, arrow_schema_lancedb as arrow_schema,
};

fn validate_ordered_schema(
    rw_arrow_schema: &arrow_schema::Schema,
    lance_schema: &arrow_schema::Schema,
) -> Result<()> {
    if rw_arrow_schema.fields().len() != lance_schema.fields().len() {
        return Err(SinkError::LanceDb(anyhow!(
            "Columns mismatch. RisingWave schema has {} fields, LanceDB table has {} fields",
            rw_arrow_schema.fields().len(),
            lance_schema.fields().len()
        )));
    }

    for (idx, (rw_field, lance_field)) in rw_arrow_schema
        .fields()
        .iter()
        .zip_eq_fast(lance_schema.fields().iter())
        .enumerate()
    {
        if rw_field.name() != lance_field.name() {
            return Err(SinkError::LanceDb(anyhow!(
                "column order mismatch at position {}: LanceDB column is '{}', RisingWave column is '{}'",
                idx,
                lance_field.name(),
                rw_field.name()
            )));
        }

        if rw_field.data_type() != lance_field.data_type() {
            return Err(SinkError::LanceDb(anyhow!(
                "column '{}' type mismatch: LanceDB type is {:?}, RisingWave type is {:?}",
                rw_field.name(),
                lance_field.data_type(),
                rw_field.data_type()
            )));
        }
    }

    Ok(())
}

/// The writer writes data files directly to the Lance dataset storage using the
/// low-level `FileFragment::create_fragments()` API. On checkpoint, it returns
/// lightweight `Fragment` metadata instead of the actual data payload. A fragment
/// is a logical row segment that references one or more files containing columns
/// for those rows. The coordinator then commits these fragments atomically.
///
/// This follows the same pattern as the Iceberg sink, where writers handle I/O
/// and the coordinator only performs a metadata-only commit.
pub struct LanceDbSinkWriter {
    pub config: LanceDbConfig,
    #[expect(dead_code)]
    schema: Schema,
    arrow_schema: Arc<arrow_schema::Schema>,
    /// Dataset URI for the target Lance table (e.g., `/tmp/lancedb/my_table.lance`)
    dataset_uri: String,
    /// Lance write parameters matching the target dataset format.
    write_params: WriteParams,
    fragment_write: Option<FragmentWrite>,
}

struct FragmentWrite {
    sender: Option<mpsc::Sender<arrow_array::RecordBatch>>,
    task: Option<JoinHandle<Result<Vec<Fragment>>>>,
}

impl Drop for FragmentWrite {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl LanceDbSinkWriter {
    pub async fn new(config: LanceDbConfig, schema: Schema) -> Result<Self> {
        let arrow_schema = LanceDbConvert
            .rw_schema_to_arrow_schema(&schema)
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;

        let conn = config.common.create_connection().await?;
        let table = config.common.open_table(&conn).await?;
        let dataset_wrapper = table.dataset().ok_or_else(|| {
            SinkError::LanceDb(anyhow!(
                "failed to get underlying lance Dataset (table may be remote)"
            ))
        })?;
        let dataset_guard = dataset_wrapper
            .get()
            .await
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;
        let data_storage_version = dataset_guard
            .manifest
            .data_storage_format
            .lance_file_version()
            .context("failed to get LanceDB table storage version")
            .map_err(SinkError::LanceDb)?;
        let store_params =
            dataset_guard
                .storage_options_accessor()
                .map(|storage_options_accessor| ObjectStoreParams {
                    storage_options_accessor: Some(storage_options_accessor),
                    ..Default::default()
                });
        drop(dataset_guard);
        let dataset_uri = config.common.dataset_uri(&table).await?;

        Ok(Self {
            config,
            schema,
            arrow_schema: Arc::new(arrow_schema),
            dataset_uri,
            write_params: WriteParams {
                // Reuse the opened dataset's storage options so fragment writes use the same
                // credentials and object-store configuration as table access.
                store_params,
                // Lance otherwise chooses its latest stable format. Detached fragments must use
                // the existing table's format so they remain compatible when committed later.
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            },
            fragment_write: None,
        })
    }

    fn start_fragment_write(&self) -> FragmentWrite {
        let (sender, receiver) = mpsc::channel(WRITE_CHANNEL_CAPACITY);
        let stream = ReceiverStream::new(receiver).map(Ok::<_, lancedb::Error>);
        let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream::new(
            stream,
            self.arrow_schema.clone(),
        ));
        let stream = stream.into_df_stream();
        let dataset_uri = self.dataset_uri.clone();
        let write_params = self.write_params.clone();
        let task = tokio::spawn(async move {
            FileFragment::create_fragments(&dataset_uri, stream, Some(write_params))
                .await
                .context("failed to write lance data files")
                .map_err(SinkError::LanceDb)
        });

        FragmentWrite {
            sender: Some(sender),
            task: Some(task),
        }
    }

    async fn finish_fragment_write(&mut self) -> Result<Vec<Fragment>> {
        let Some(mut fragment_write) = self.fragment_write.take() else {
            return Ok(Vec::new());
        };

        drop(fragment_write.sender.take());
        fragment_write
            .task
            .take()
            .expect("fragment write task should be initialized")
            .await
            .context("Lance fragment write task failed")
            .map_err(SinkError::LanceDb)?
    }
}

#[async_trait]
impl SinkWriter for LanceDbSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let record_batch = LanceDbConvert
            .to_record_batch(self.arrow_schema.clone(), &chunk)
            .context("failed to convert DataChunk to RecordBatch for LanceDB")
            .map_err(SinkError::LanceDb)?;

        if self.fragment_write.is_none() {
            self.fragment_write = Some(self.start_fragment_write());
        }
        self.fragment_write
            .as_ref()
            .expect("fragment write should be initialized")
            .sender
            .as_ref()
            .expect("fragment write sender should be initialized")
            .send(record_batch)
            .await
            .map_err(|_| {
                SinkError::LanceDb(anyhow!(
                    "Lance fragment write task stopped before accepting a record batch"
                ))
            })?;
        Ok(())
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        let Some(mut fragment_write) = self.fragment_write.take() else {
            return Ok(());
        };

        drop(fragment_write.sender.take());
        match fragment_write
            .task
            .take()
            .expect("fragment write task should be initialized")
            .await
        {
            Ok(Ok(fragments)) => {
                tracing::debug!(
                    fragment_count = fragments.len(),
                    "Left uncommitted Lance fragments for table-level orphan cleanup after sink writer abort"
                );
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    error = %error.as_report(),
                    "Lance fragment write failed while aborting sink writer"
                );
            }
            Err(error) => {
                tracing::warn!(
                    error = %error.as_report(),
                    "Lance fragment write task failed while aborting sink writer"
                );
            }
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if !is_checkpoint {
            return Ok(None);
        }

        let fragments = self.finish_fragment_write().await?;

        // Serialize fragment metadata as JSON — this is lightweight (file paths + row counts).
        let metadata = serde_json::to_vec(&fragments)
            .context("failed to serialize fragment metadata")
            .map_err(SinkError::LanceDb)?;

        Ok(Some(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata })),
        }))
    }
}

// ---------------------------------------------------------------------------
// Committer (Coordinator)
// ---------------------------------------------------------------------------

/// The coordinator collects lightweight `Fragment` metadata from all writers
/// and commits them atomically via `CommitBuilder`.
/// No data payload flows through the coordinator — only file-level metadata.
pub struct LanceDbSinkCommitter {
    config: LanceDbConfig,
    conn: LanceDbConnection,
    sink_id: String,
}

impl LanceDbSinkCommitter {
    pub async fn new(config: LanceDbConfig, sink_id: String) -> Result<Self> {
        let conn = config.common.create_connection().await?;
        Ok(Self {
            config,
            conn,
            sink_id,
        })
    }

    fn collect_fragments(metadata: &[SinkMetadata]) -> Result<Vec<Fragment>> {
        let mut all_fragments = Vec::new();
        for meta in metadata {
            if let Some(Serialized(s)) = &meta.metadata {
                let fragments: Vec<Fragment> = serde_json::from_slice(&s.metadata)
                    .context("failed to deserialize fragment metadata")
                    .map_err(SinkError::LanceDb)?;
                all_fragments.extend(fragments);
            }
        }
        Ok(all_fragments)
    }

    async fn is_epoch_committed(&self, dataset: &Dataset, target_epoch: u64) -> Result<bool> {
        let mut dataset = dataset.clone();

        loop {
            if let Some(transaction) = dataset
                .read_transaction()
                .await
                .context("failed to read Lance transaction history")
                .map_err(SinkError::LanceDb)?
                && let Some(properties) = transaction.transaction_properties
                && properties
                    .get(RW_SINK_ID_TRANSACTION_PROPERTY)
                    .is_some_and(|sink_id| sink_id == &self.sink_id)
                && let Some(committed_epoch) = properties.get(RW_EPOCH_TRANSACTION_PROPERTY)
            {
                let committed_epoch = committed_epoch
                    .parse::<u64>()
                    .context("invalid RisingWave epoch in Lance transaction history")
                    .map_err(SinkError::LanceDb)?;
                return Ok(committed_epoch >= target_epoch);
            }

            let version = dataset.version().version;
            if version <= 1 {
                return Ok(false);
            }

            dataset = match dataset.checkout_version(version - 1).await {
                Ok(dataset) => dataset,
                // Lance cleanup removes a contiguous prefix of old versions. Reaching a
                // missing previous version therefore means that all retained history has
                // already been inspected.
                Err(lance::Error::DatasetNotFound { .. }) => return Ok(false),
                Err(error) => {
                    return Err(SinkError::LanceDb(
                        anyhow!(error).context("failed to read Lance transaction history"),
                    ));
                }
            };
        }
    }

    async fn commit_fragments(
        &mut self,
        epoch: u64,
        fragments: Vec<Fragment>,
        transaction_properties: Option<HashMap<String, String>>,
    ) -> Result<()> {
        if fragments.is_empty() {
            tracing::debug!("No fragments to commit in epoch {epoch}, skipping.");
            return Ok(());
        }

        // Open the table to get the underlying lance Dataset.
        let table = self
            .conn
            .open_table(&self.config.common.table)
            .execute()
            .await
            .context("failed to open LanceDB table for commit")
            .map_err(SinkError::LanceDb)?;

        let dataset_wrapper = table.dataset().ok_or_else(|| {
            SinkError::LanceDb(anyhow!(
                "failed to get underlying lance Dataset (table may be remote)"
            ))
        })?;

        let dataset_guard = dataset_wrapper
            .get()
            .await
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;
        let dataset = (*dataset_guard).clone();
        drop(dataset_guard);

        let data_storage_version = dataset
            .manifest
            .data_storage_format
            .lance_file_version()
            .context("failed to get LanceDB table storage version")
            .map_err(SinkError::LanceDb)?;

        if transaction_properties.is_some() && self.is_epoch_committed(&dataset, epoch).await? {
            tracing::info!(
                "LanceDB epoch {epoch} has already been committed, skipping duplicate commit."
            );
            return Ok(());
        }

        // Commit fragments via low-level Lance transaction with Operation::Append.
        // This is a metadata-only operation — the data files were already written by workers.
        let operation = Operation::Append { fragments };

        let mut transaction_builder = TransactionBuilder::new(dataset.version().version, operation);
        if let Some(transaction_properties) = transaction_properties {
            transaction_builder =
                transaction_builder.transaction_properties(Some(Arc::new(transaction_properties)));
        }
        let transaction = transaction_builder.build();

        let new_dataset = CommitBuilder::new(Arc::new(dataset))
            .with_storage_format(data_storage_version)
            .execute(transaction)
            .await
            .context("failed to commit fragments to lance dataset")
            .map_err(SinkError::LanceDb)?;

        // Update the lancedb Table's internal dataset to the new version.
        dataset_wrapper.update(new_dataset);

        tracing::debug!("Succeeded to commit fragments to LanceDB table in epoch {epoch}.");
        Ok(())
    }
}

#[async_trait::async_trait]
impl SinglePhaseCommitCoordinator for LanceDbSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            "LanceDB commit coordinator initialized for table '{}'",
            self.config.common.table
        );
        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::debug!("Starting LanceDB single-phase commit in epoch {epoch}.");

        let fragments = Self::collect_fragments(&metadata)?;
        self.commit_fragments(epoch, fragments, None).await
    }
}

#[async_trait::async_trait]
impl TwoPhaseCommitCoordinator for LanceDbSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            "LanceDB commit coordinator initialized for table '{}'",
            self.config.common.table
        );
        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        schema_change: Option<PbSinkSchemaChange>,
    ) -> Result<Option<Vec<u8>>> {
        if schema_change.is_some() {
            return Err(SinkError::LanceDb(anyhow!(
                "LanceDB sink does not support schema change"
            )));
        }

        let fragments = Self::collect_fragments(&metadata)?;
        if fragments.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            LanceDbPreCommitMetadata {
                sink_id: self.sink_id.clone(),
                epoch,
                fragments,
            }
            .try_into_bytes()?,
        ))
    }

    async fn commit_data(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::debug!("Starting LanceDB two-phase commit in epoch {epoch}.");

        if commit_metadata.is_empty() {
            return Ok(());
        }

        let pre_commit_metadata = LanceDbPreCommitMetadata::try_from_bytes(&commit_metadata)?;
        if pre_commit_metadata.epoch != epoch {
            return Err(SinkError::LanceDb(anyhow!(
                "LanceDB pre-commit epoch {} does not match commit epoch {}",
                pre_commit_metadata.epoch,
                epoch
            )));
        }
        if pre_commit_metadata.sink_id != self.sink_id {
            return Err(SinkError::LanceDb(anyhow!(
                "LanceDB pre-commit sink id {} does not match coordinator sink id {}",
                pre_commit_metadata.sink_id,
                self.sink_id
            )));
        }

        let transaction_properties = pre_commit_metadata.transaction_properties();
        self.commit_fragments(
            epoch,
            pre_commit_metadata.fragments,
            Some(transaction_properties),
        )
        .await
    }

    async fn abort(&mut self, epoch: u64, _commit_metadata: Vec<u8>) {
        // Unreferenced files are reclaimed by Lance's old-version/orphan cleanup. This is
        // intentionally not an eager delete: the commit may have succeeded even if its result
        // was lost, and process crashes can bypass this callback entirely.
        tracing::debug!("Left LanceDB epoch {epoch} for table-level orphan cleanup");
    }
}

#[derive(Serialize, Deserialize)]
struct LanceDbPreCommitMetadata {
    sink_id: String,
    epoch: u64,
    fragments: Vec<Fragment>,
}

impl LanceDbPreCommitMetadata {
    fn try_into_bytes(self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self)
            .context("cannot serialize LanceDB pre-commit metadata")
            .map_err(SinkError::LanceDb)
    }

    fn try_from_bytes(value: &[u8]) -> Result<Self> {
        serde_json::from_slice(value)
            .context("cannot deserialize LanceDB pre-commit metadata")
            .map_err(SinkError::LanceDb)
    }

    fn transaction_properties(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                RW_SINK_ID_TRANSACTION_PROPERTY.to_owned(),
                self.sink_id.clone(),
            ),
            (
                RW_EPOCH_TRANSACTION_PROPERTY.to_owned(),
                self.epoch.to_string(),
            ),
        ])
    }
}

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

impl From<lancedb::Error> for SinkError {
    fn from(value: lancedb::Error) -> Self {
        SinkError::LanceDb(anyhow!(value))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(madsim)))]
mod tests {
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::id::SinkId;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::sink::catalog::SinkType;
    use crate::sink::writer::SinkWriter;
    use crate::sink::{SinglePhaseCommitCoordinator, TwoPhaseCommitCoordinator};

    #[tokio::test]
    async fn test_lancedb_sink_roundtrip() {
        // 1. Create a temp directory for LanceDB
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        // 2. Create a LanceDB table with a schema
        let conn = lancedb::connect(uri).execute().await.unwrap();

        // Create a schema-only table, matching the e2e test setup.
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        conn.create_table(
            "test_table",
            arrow_array::RecordBatch::new_empty(arrow_schema.clone()),
        )
        .execute()
        .await
        .unwrap();

        // 3. Create a LanceDB sink writer
        let properties: BTreeMap<String, String> = [
            ("connector".to_owned(), "lancedb".to_owned()),
            ("type".to_owned(), "append-only".to_owned()),
            ("lancedb.uri".to_owned(), uri.to_owned()),
            ("lancedb.table".to_owned(), "test_table".to_owned()),
        ]
        .into();

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
            },
        ]);

        let config = LanceDbConfig::from_btreemap(properties).unwrap();
        assert_eq!(config.is_exactly_once, None);
        let mut writer = LanceDbSinkWriter::new(config.clone(), schema.clone())
            .await
            .unwrap();
        let table = conn.open_table("test_table").execute().await.unwrap();
        assert_eq!(writer.dataset_uri, table.uri().await.unwrap());

        // 4. Abort one streamed batch, then write two batches for the checkpoint.
        let chunk = StreamChunk::new(
            vec![Op::Insert],
            vec![
                I32Array::from_iter(vec![0]).into_ref(),
                Utf8Array::from_iter(vec!["Aborted"]).into_ref(),
            ],
        );
        writer.write_batch(chunk).await.unwrap();
        writer.abort().await.unwrap();

        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob"]).into_ref(),
            ],
        );
        writer.write_batch(chunk).await.unwrap();
        let chunk = StreamChunk::new(
            vec![Op::Insert],
            vec![
                I32Array::from_iter(vec![3]).into_ref(),
                Utf8Array::from_iter(vec!["Clare"]).into_ref(),
            ],
        );
        writer.write_batch(chunk).await.unwrap();

        // 5. Barrier → get SinkMetadata (lightweight fragment metadata, not data payload)
        let metadata = writer.barrier(true).await.unwrap().unwrap();

        // Verify the metadata is lightweight JSON (not Arrow IPC payload)
        if let Some(Serialized(s)) = &metadata.metadata {
            let fragments: Vec<Fragment> = serde_json::from_slice(&s.metadata).unwrap();
            assert!(!fragments.is_empty(), "should have at least one fragment");
            // Fragment metadata should be much smaller than actual data
            assert!(
                s.metadata.len() < 4096,
                "fragment metadata should be lightweight, got {} bytes",
                s.metadata.len()
            );
        } else {
            panic!("expected serialized metadata");
        }

        // 6. Commit via two-phase coordinator (metadata-only commit)
        let mut committer = LanceDbSinkCommitter::new(config.clone(), "test-sink".to_owned())
            .await
            .unwrap();
        TwoPhaseCommitCoordinator::init(&mut committer)
            .await
            .unwrap();
        let commit_metadata =
            TwoPhaseCommitCoordinator::pre_commit(&mut committer, 1, vec![metadata], None)
                .await
                .unwrap()
                .unwrap();
        TwoPhaseCommitCoordinator::commit_data(&mut committer, 1, commit_metadata.clone())
            .await
            .unwrap();

        // 7. Verify data was written by reading back
        let table = conn.open_table("test_table").execute().await.unwrap();
        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 3);
        let dataset_wrapper = table.dataset().unwrap();
        let dataset_guard = dataset_wrapper.get().await.unwrap();
        let transaction = dataset_guard.read_transaction().await.unwrap().unwrap();
        let transaction_properties = transaction.transaction_properties.unwrap();
        assert_eq!(
            transaction_properties.get(RW_SINK_ID_TRANSACTION_PROPERTY),
            Some(&"test-sink".to_owned())
        );
        assert!(
            committer
                .is_epoch_committed(&dataset_guard, 0)
                .await
                .unwrap()
        );
        assert!(
            committer
                .is_epoch_committed(&dataset_guard, 1)
                .await
                .unwrap()
        );
        assert!(
            !committer
                .is_epoch_committed(&dataset_guard, 2)
                .await
                .unwrap()
        );

        // 8. An independent sink must not use another sink's epoch marker.
        let replacement_committer =
            LanceDbSinkCommitter::new(config.clone(), "replacement-sink".to_owned())
                .await
                .unwrap();
        assert!(
            !replacement_committer
                .is_epoch_committed(&dataset_guard, 1)
                .await
                .unwrap()
        );
        drop(dataset_guard);

        // 9. Retrying the same committed epoch should be idempotent in two-phase mode.
        TwoPhaseCommitCoordinator::commit_data(&mut committer, 1, commit_metadata)
            .await
            .unwrap();
        let table = conn.open_table("test_table").execute().await.unwrap();
        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 3);

        // 10. Single-phase commit remains available when exactly-once is disabled.
        let mut writer = LanceDbSinkWriter::new(config.clone(), schema)
            .await
            .unwrap();
        let chunk = StreamChunk::new(
            vec![Op::Insert],
            vec![
                I32Array::from_iter(vec![4]).into_ref(),
                Utf8Array::from_iter(vec!["David"]).into_ref(),
            ],
        );
        writer.write_batch(chunk).await.unwrap();
        let metadata = writer.barrier(true).await.unwrap().unwrap();

        let mut single_phase_committer = LanceDbSinkCommitter::new(config, "test-sink".to_owned())
            .await
            .unwrap();
        SinglePhaseCommitCoordinator::init(&mut single_phase_committer)
            .await
            .unwrap();
        SinglePhaseCommitCoordinator::commit_data(&mut single_phase_committer, 2, vec![metadata])
            .await
            .unwrap();
        let table = conn.open_table("test_table").execute().await.unwrap();
        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn test_epoch_lookup_stops_at_pruned_history() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![0i32]))],
        )
        .unwrap();

        let conn = lancedb::connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("test_table", batch.clone())
            .execute()
            .await
            .unwrap();
        table.add(batch).execute().await.unwrap();

        let dataset_wrapper = table.dataset().unwrap();
        let dataset_guard = dataset_wrapper.get().await.unwrap();
        dataset_guard
            .cleanup_old_versions(chrono::Duration::zero(), Some(true), Some(false))
            .await
            .unwrap();
        drop(dataset_guard);
        drop(table);
        drop(conn);

        // Reopen with a fresh session so a cached old manifest cannot hide the pruning boundary.
        let conn = lancedb::connect(uri).execute().await.unwrap();
        let table = conn.open_table("test_table").execute().await.unwrap();
        let dataset_wrapper = table.dataset().unwrap();
        let dataset_guard = dataset_wrapper.get().await.unwrap();
        assert!(dataset_guard.checkout_version(1).await.is_err());

        let properties: BTreeMap<String, String> = [
            ("connector".to_owned(), "lancedb".to_owned()),
            ("type".to_owned(), "append-only".to_owned()),
            ("lancedb.uri".to_owned(), uri.to_owned()),
            ("lancedb.table".to_owned(), "test_table".to_owned()),
        ]
        .into();
        let config = LanceDbConfig::from_btreemap(properties).unwrap();
        let committer = LanceDbSinkCommitter::new(config, "test-sink".to_owned())
            .await
            .unwrap();

        assert!(
            !committer
                .is_epoch_committed(&dataset_guard, 1)
                .await
                .unwrap()
        );
    }

    #[test]
    fn test_validate_ordered_schema_rejects_reordered_columns() {
        let rw_schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]);
        let reordered_lance_schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        ]);

        let err = validate_ordered_schema(&rw_schema, &reordered_lance_schema).unwrap_err();
        assert!(
            format!("{err:?}").contains("column order mismatch"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_validate_rejects_reordered_lancedb_table_columns() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let conn = lancedb::connect(uri).execute().await.unwrap();
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["init"])),
                Arc::new(arrow_array::Int32Array::from(vec![0i32])),
            ],
        )
        .unwrap();
        conn.create_table("reordered_table", batch)
            .execute()
            .await
            .unwrap();

        let properties: BTreeMap<String, String> = [
            ("connector".to_owned(), "lancedb".to_owned()),
            ("type".to_owned(), "append-only".to_owned()),
            ("lancedb.uri".to_owned(), uri.to_owned()),
            ("lancedb.table".to_owned(), "reordered_table".to_owned()),
        ]
        .into();
        let config = LanceDbConfig::from_btreemap(properties.clone()).unwrap();
        let param = SinkParam {
            sink_id: SinkId::from(1u32),
            sink_name: "test_sink".to_owned(),
            properties,
            columns: vec![
                ColumnDesc::named("id", ColumnId::new(1), DataType::Int32),
                ColumnDesc::named("name", ColumnId::new(2), DataType::Varchar),
            ],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "dev".to_owned(),
            sink_from_name: "test_sink".to_owned(),
        };

        let sink = LanceDbSink::new(config, param).unwrap();
        let err = sink.validate().await.unwrap_err();
        assert!(
            format!("{err:?}").contains("column order mismatch"),
            "unexpected error: {err:?}"
        );
    }
}
