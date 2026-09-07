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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, FormatVersion, Operation, SerializedDataFile, TableMetadata};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, FastAppendAction, Transaction};
use itertools::Itertools;
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::bail;
use risingwave_common::catalog::{Field, RISINGWAVE_ICEBERG_COMMIT_EPOCH};
use risingwave_common::error::IcebergError;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use serde::{Deserialize, Serialize};
use serde_json::from_value;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

use super::commit_retry::{self, CommitError, CommitRetryLogContext};
use super::{GLOBAL_SINK_METRICS, IcebergConfig, SinkError, commit_branch, resolve_partition_type};
use crate::connector_common::{IcebergCommittedSnapshot, IcebergSinkCompactionUpdate};
use crate::sink::catalog::SinkId;
use crate::sink::{Result, SinglePhaseCommitCoordinator, SinkParam, TwoPhaseCommitCoordinator};

const SCHEMA_ID: &str = "schema_id";
const PARTITION_SPEC_ID: &str = "partition_spec_id";
const DATA_FILES: &str = "data_files";

#[derive(Default, Clone)]
pub struct IcebergCommitResult {
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub data_files: Vec<SerializedDataFile>,
}

impl IcebergCommitResult {
    pub fn try_from(value: &SinkMetadata) -> Result<Self> {
        let Some(Serialized(value)) = &value.metadata else {
            bail!("Can't create iceberg sink write result from empty data!");
        };

        Self::try_from_serialized_bytes(&value.metadata)
    }

    pub fn try_from_serialized_bytes(value: &[u8]) -> Result<Self> {
        let mut values = if let serde_json::Value::Object(value) =
            serde_json::from_slice::<serde_json::Value>(value)
                .context("Can't parse iceberg sink metadata")?
        {
            value
        } else {
            bail!("iceberg sink metadata should be an object");
        };

        let schema_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
            schema_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have schema_id");
        }

        let partition_spec_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
            partition_spec_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have partition_spec_id");
        }

        let data_files: Vec<SerializedDataFile>;
        if let serde_json::Value::Array(values) = values
            .remove(DATA_FILES)
            .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
        {
            data_files = values
                .into_iter()
                .map(from_value::<SerializedDataFile>)
                .collect::<std::result::Result<_, _>>()
                .unwrap();
        } else {
            bail!("iceberg sink metadata should have data_files object");
        }

        Ok(Self {
            schema_id: schema_id as i32,
            partition_spec_id: partition_spec_id as i32,
            data_files,
        })
    }
}

impl<'a> TryFrom<&'a IcebergCommitResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a IcebergCommitResult) -> std::result::Result<SinkMetadata, Self::Error> {
        let bytes = <Vec<u8>>::try_from(value)?;
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata: bytes })),
        })
    }
}

impl<'a> TryFrom<&'a IcebergCommitResult> for Vec<u8> {
    type Error = SinkError;

    fn try_from(value: &'a IcebergCommitResult) -> std::result::Result<Vec<u8>, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
            ]
            .into_iter()
            .collect(),
        );
        Ok(serde_json::to_vec(&json_value).context("Can't serialize iceberg sink metadata")?)
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct IcebergPositionDeleteCommitResult {
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub delete_files: Vec<SerializedDataFile>,
    pub overwrite_files: Vec<SerializedDataFile>,
}

impl<'a> TryFrom<&'a SinkMetadata> for IcebergPositionDeleteCommitResult {
    type Error = SinkError;

    fn try_from(value: &'a SinkMetadata) -> Result<Self> {
        let Some(Serialized(value)) = &value.metadata else {
            bail!("Can't create iceberg dv merger commit result from empty data!");
        };
        let value = serde_json::from_slice(&value.metadata)
            .context("Can't deserialize iceberg dv merger commit result from metadata")?;
        Ok(value)
    }
}

impl<'a> TryFrom<&'a IcebergPositionDeleteCommitResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a IcebergPositionDeleteCommitResult) -> Result<SinkMetadata> {
        let bytes = serde_json::to_vec(value)
            .context("Can't serialize iceberg dv merger commit result to metadata")?;
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata: bytes })),
        })
    }
}

fn arrow_data_type_compatible(current: &ArrowDataType, expected: &ArrowDataType) -> bool {
    use ArrowDataType::*;

    match (current, expected) {
        // RW Decimal has no precision/scale. Sink creation already accepts any
        // table Decimal128 precision/scale, while expected schemas here are
        // generated from RW columns using the same canonical mapping. Ignoring
        // both values prevents false schema-state ambiguity and cannot mask an
        // auto schema change, which only supports add/drop columns.
        (Decimal128(_, _), Decimal128(_, _)) => true,
        (Binary, LargeBinary) | (LargeBinary, Binary) => true,
        (List(current_field), List(expected_field)) => {
            arrow_data_type_compatible(current_field.data_type(), expected_field.data_type())
        }
        (Map(current_field, current_sorted), Map(expected_field, expected_sorted)) => {
            current_sorted == expected_sorted
                && arrow_data_type_compatible(current_field.data_type(), expected_field.data_type())
        }
        (Struct(current_fields), Struct(expected_fields)) => {
            let expected_fields = expected_fields
                .iter()
                .map(|field| field.as_ref().clone())
                .collect_vec();
            schema_contains_same_fields(current_fields, &expected_fields)
        }
        _ => current == expected,
    }
}

fn schema_contains_same_fields(current: &ArrowFields, expected: &[ArrowField]) -> bool {
    if current.len() != expected.len() {
        return false;
    }

    let mut unmatched_current = current.iter().collect_vec();
    expected.iter().all(|expected_field| {
        let Some(pos) = unmatched_current.iter().position(|current_field| {
            current_field.name() == expected_field.name()
                && arrow_data_type_compatible(current_field.data_type(), expected_field.data_type())
        }) else {
            return false;
        };
        unmatched_current.swap_remove(pos);
        true
    })
}

pub struct IcebergSinkCommitter {
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) table: Table,
    pub last_commit_epoch: u64,
    pub(crate) sink_id: SinkId,
    pub(crate) config: IcebergConfig,
    pub(crate) param: SinkParam,
    pub(super) commit_retry_num: u32,
    pub(crate) iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
}

impl IcebergSinkCommitter {
    fn latest_observed_snapshot(&self) -> Option<IcebergCommittedSnapshot> {
        let branch = commit_branch(self.config.r#type.as_str(), self.config.write_mode);
        let metadata = self.table.metadata();
        metadata
            .snapshot_for_ref(&branch)
            .map(|snapshot| IcebergCommittedSnapshot {
                branch,
                snapshot_id: snapshot.snapshot_id(),
                timestamp_ms: snapshot.timestamp_ms(),
                max_file_sequence_number: (metadata.format_version() >= FormatVersion::V2)
                    .then_some(snapshot.sequence_number()),
            })
    }

    fn notify_iceberg_compaction_scheduler(&self, force_compaction: bool) {
        let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender else {
            return;
        };
        let branch = commit_branch(self.config.r#type.as_str(), self.config.write_mode);

        let Some(observed_snapshot) = self.latest_observed_snapshot() else {
            warn!(
                iceberg_component = "sink_committer",
                iceberg_operation = "notify_compaction",
                sink_id = %self.sink_id,
                sink_name = %self.param.sink_name,
                table = %self.table.identifier(),
                branch = %branch,
                force_compaction,
                "iceberg_sink_compaction_update_skipped",
            );
            return;
        };

        let observed_snapshot_id = observed_snapshot.snapshot_id;
        let observed_snapshot_timestamp_ms = observed_snapshot.timestamp_ms;
        let observed_snapshot_branch = observed_snapshot.branch.clone();

        if iceberg_compact_stat_sender
            .send(IcebergSinkCompactionUpdate {
                sink_id: self.sink_id,
                force_compaction,
                observed_snapshot,
            })
            .is_err()
        {
            warn!(
                iceberg_component = "sink_committer",
                iceberg_operation = "notify_compaction",
                sink_id = %self.sink_id,
                sink_name = %self.param.sink_name,
                table = %self.table.identifier(),
                force_compaction,
                observed_snapshot_id,
                observed_snapshot_timestamp_ms,
                observed_snapshot_branch = %observed_snapshot_branch,
                "iceberg_sink_compaction_update_send_failed",
            );
        }
    }
}

#[async_trait]
impl SinglePhaseCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "init",
            sink_id = %self.param.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            "iceberg_sink_committer_initialized",
        );

        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "direct_commit",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            epoch,
            metadata_count = metadata.len(),
            "iceberg_sink_commit_started",
        );

        if metadata.is_empty() {
            tracing::debug!(
                iceberg_component = "sink_committer",
                iceberg_operation = "direct_commit",
                sink_id = %self.sink_id,
                table = %self.table.identifier(),
                epoch,
                "iceberg_sink_commit_skipped_empty_metadata",
            );
            return Ok(());
        }

        // Commit data if present
        if let Some((write_results, snapshot_id)) = self.pre_commit_inner(epoch, metadata)? {
            self.commit_data_impl(epoch, write_results, snapshot_id)
                .await?;
        }

        Ok(())
    }

    async fn commit_schema_change(
        &mut self,
        epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "schema_change",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            epoch,
            schema_change = ?schema_change,
            "iceberg_sink_schema_change_commit_started",
        );
        self.commit_schema_change_impl(schema_change).await?;
        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "schema_change",
            sink_id = %self.sink_id,
            table = %self.table.identifier(),
            epoch,
            "iceberg_sink_schema_change_commit_succeeded",
        );

        Ok(())
    }
}

#[async_trait]
impl TwoPhaseCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "init",
            sink_id = %self.param.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            "iceberg_sink_committer_initialized",
        );

        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        _schema_change: Option<PbSinkSchemaChange>,
    ) -> Result<Option<Vec<u8>>> {
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "pre_commit",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            epoch,
            metadata_count = metadata.len(),
            "iceberg_sink_pre_commit_started",
        );

        let (write_results, snapshot_id) = match self.pre_commit_inner(epoch, metadata)? {
            Some((write_results, snapshot_id)) => (write_results, snapshot_id),
            None => {
                tracing::debug!(
                    iceberg_component = "sink_committer",
                    iceberg_operation = "pre_commit",
                    sink_id = %self.sink_id,
                    table = %self.table.identifier(),
                    epoch,
                    "iceberg_sink_pre_commit_skipped_no_data",
                );
                return Ok(None);
            }
        };

        let mut write_results_bytes = Vec::new();
        for each_parallelism_write_result in write_results {
            let each_parallelism_write_result_bytes =
                <Vec<u8>>::try_from(&each_parallelism_write_result)?;
            write_results_bytes.push(each_parallelism_write_result_bytes);
        }

        let snapshot_id_bytes: Vec<u8> = snapshot_id.to_le_bytes().to_vec();
        write_results_bytes.push(snapshot_id_bytes);

        let pre_commit_metadata_bytes: Vec<u8> = serialize_metadata(write_results_bytes);
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "pre_commit",
            sink_id = %self.sink_id,
            table = %self.table.identifier(),
            epoch,
            snapshot_id,
            pre_commit_metadata_bytes = pre_commit_metadata_bytes.len(),
            "iceberg_sink_pre_commit_metadata_encoded",
        );
        Ok(Some(pre_commit_metadata_bytes))
    }

    async fn commit_data(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "commit",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            epoch,
            commit_metadata_bytes = commit_metadata.len(),
            "iceberg_sink_commit_started",
        );

        if commit_metadata.is_empty() {
            tracing::debug!(
                iceberg_component = "sink_committer",
                iceberg_operation = "commit",
                sink_id = %self.sink_id,
                table = %self.table.identifier(),
                epoch,
                "iceberg_sink_commit_skipped_empty_metadata",
            );
            return Ok(());
        }

        // Deserialize commit metadata
        let mut payload = deserialize_metadata(commit_metadata);
        if payload.is_empty() {
            return Err(SinkError::Iceberg(anyhow!(
                "Invalid commit metadata: empty payload"
            )));
        }

        // Last element is snapshot_id
        let snapshot_id_bytes = payload.pop().ok_or_else(|| {
            SinkError::Iceberg(anyhow!("Invalid commit metadata: missing snapshot_id"))
        })?;
        let snapshot_id = i64::from_le_bytes(
            snapshot_id_bytes
                .try_into()
                .map_err(|_| SinkError::Iceberg(anyhow!("Invalid snapshot id bytes")))?,
        );

        // Remaining elements are write_results
        let write_results = payload
            .into_iter()
            .map(|p| IcebergCommitResult::try_from_serialized_bytes(&p))
            .collect::<Result<Vec<_>>>()?;

        let snapshot_committed = self.is_snapshot_id_in_iceberg(snapshot_id).await?;

        if snapshot_committed {
            tracing::info!(
                iceberg_component = "sink_committer",
                iceberg_operation = "commit",
                sink_id = %self.sink_id,
                sink_name = %self.param.sink_name,
                table = %self.table.identifier(),
                epoch,
                snapshot_id,
                "iceberg_sink_commit_skipped_snapshot_already_committed",
            );
            return Ok(());
        }

        self.commit_data_impl(epoch, write_results, snapshot_id)
            .await
    }

    async fn commit_schema_change(
        &mut self,
        epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        let schema_updated = self.check_schema_change_applied(&schema_change)?;
        if schema_updated {
            tracing::info!(
                iceberg_component = "sink_committer",
                iceberg_operation = "schema_change",
                sink_id = %self.sink_id,
                table = %self.table.identifier(),
                epoch,
                "iceberg_sink_schema_change_skipped_already_applied",
            );
            return Ok(());
        }

        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "schema_change",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            epoch,
            schema_change = ?schema_change,
            "iceberg_sink_schema_change_commit_started",
        );
        self.commit_schema_change_impl(schema_change).await?;
        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "schema_change",
            sink_id = %self.sink_id,
            table = %self.table.identifier(),
            epoch,
            "iceberg_sink_schema_change_commit_succeeded",
        );

        Ok(())
    }

    async fn abort(&mut self, epoch: u64, _commit_metadata: Vec<u8>) {
        // TODO: Files that have been written but not committed should be deleted.
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "abort",
            sink_id = %self.sink_id,
            table = %self.table.identifier(),
            epoch,
            "iceberg_sink_commit_abort_unimplemented",
        );
    }
}

/// Methods Required to Achieve Exactly Once Semantics
impl IcebergSinkCommitter {
    fn pre_commit_inner(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
    ) -> Result<Option<(Vec<IcebergCommitResult>, i64)>> {
        let write_results: Vec<IcebergCommitResult> = metadata
            .iter()
            .map(IcebergCommitResult::try_from)
            .collect::<Result<Vec<IcebergCommitResult>>>()?;
        let data_file_count: usize = write_results.iter().map(|r| r.data_files.len()).sum();
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "pre_commit",
            sink_id = %self.sink_id,
            table = %self.table.identifier(),
            epoch,
            writer_result_count = write_results.len(),
            data_file_count,
            "iceberg_sink_pre_commit_metadata_decoded",
        );

        // Skip if no data to commit
        if write_results.is_empty() || write_results.iter().all(|r| r.data_files.is_empty()) {
            return Ok(None);
        }

        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;

        // guarantee that all write results has same schema_id and partition_spec_id
        if write_results
            .iter()
            .any(|r| r.schema_id != expect_schema_id)
            || write_results
                .iter()
                .any(|r| r.partition_spec_id != expect_partition_spec_id)
        {
            return Err(SinkError::Iceberg(anyhow!(
                "schema_id and partition_spec_id should be the same in all write results"
            )));
        }

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);
        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "pre_commit",
            sink_id = %self.sink_id,
            table = %self.table.identifier(),
            epoch,
            snapshot_id,
            schema_id = expect_schema_id,
            partition_spec_id = expect_partition_spec_id,
            data_file_count,
            "iceberg_sink_pre_commit_snapshot_assigned",
        );

        Ok(Some((write_results, snapshot_id)))
    }

    async fn commit_data_impl(
        &mut self,
        epoch: u64,
        write_results: Vec<IcebergCommitResult>,
        snapshot_id: i64,
    ) -> Result<()> {
        // Empty write results should be handled before calling this function.
        assert!(
            !write_results.is_empty() && !write_results.iter().all(|r| r.data_files.is_empty())
        );

        // Check snapshot limit before proceeding with commit
        self.wait_for_snapshot_limit().await?;

        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;
        let data_file_count: usize = write_results.iter().map(|r| r.data_files.len()).sum();
        let target_branch = commit_branch(self.config.r#type.as_str(), self.config.write_mode);
        tracing::info!(
            iceberg_component = "sink_committer",
            iceberg_operation = "commit",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            branch = %target_branch,
            epoch,
            snapshot_id,
            schema_id = expect_schema_id,
            partition_spec_id = expect_partition_spec_id,
            data_file_count,
            retry_num = self.commit_retry_num,
            "iceberg_sink_commit_applying",
        );

        // Load the latest table to avoid concurrent modification with the best effort.
        self.table = commit_retry::reload_table(
            self.catalog.as_ref(),
            self.table.identifier(),
            expect_schema_id,
            expect_partition_spec_id,
        )
        .await
        .map_err(SinkError::Iceberg)?;

        let Some(schema) = self.table.metadata().schema_by_id(expect_schema_id) else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find schema by id {}",
                expect_schema_id
            )));
        };
        let partition_type = resolve_partition_type(&self.table, expect_partition_spec_id, schema)?;

        let data_files = write_results
            .into_iter()
            .flat_map(|r| {
                r.data_files.into_iter().map(|f| {
                    f.try_into(expect_partition_spec_id, &partition_type, schema)
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))
                })
            })
            .collect::<Result<Vec<DataFile>>>()?;

        // # TODO:
        // This retry behavior should be revert and do in iceberg-rust when it supports retry(Track in: https://github.com/apache/iceberg-rust/issues/964)
        // because retry logic involved reapply the commit metadata.
        // For now, we just retry the commit operation.
        let catalog = self.catalog.clone();
        let sink_id = self.sink_id;
        let table_ident = self.table.identifier().clone();
        let table_name = table_ident.to_string();
        let retry_log_context = CommitRetryLogContext::new(
            "sink_committer",
            "commit",
            table_name.clone(),
            target_branch.clone(),
        )
        .with_sink_id(sink_id)
        .with_epoch(epoch)
        .with_snapshot_id(snapshot_id);

        let table = commit_retry::run_with_retry(
            catalog.clone(),
            table_ident.clone(),
            expect_schema_id,
            expect_partition_spec_id,
            self.commit_retry_num as usize,
            retry_log_context,
            |table| {
                let table_name = table_name.clone();
                let target_branch = target_branch.clone();
                let data_files = data_files.clone();
                let catalog = catalog.clone();
                async move {
                    let txn = Transaction::new(&table);
                    let append_action = txn
                        .fast_append()
                        .set_snapshot_id(snapshot_id)
                        .set_target_branch(target_branch.clone())
                        .set_snapshot_properties(HashMap::from([(
                            RISINGWAVE_ICEBERG_COMMIT_EPOCH.to_owned(),
                            epoch.to_string(),
                        )]))
                        .add_data_files(data_files);

                    let tx = append_action.apply(txn).map_err(|err| {
                        let err: IcebergError = err.into();
                        tracing::error!(
                            iceberg_component = "sink_committer",
                            iceberg_operation = "commit",
                            sink_id = %sink_id,
                            table = %table_name,
                            epoch,
                            snapshot_id,
                            branch = %target_branch,
                            data_file_count,
                            error = %err.as_report(),
                            "iceberg_sink_commit_fast_append_apply_failed",
                        );
                        CommitError::Commit(anyhow!(err).context("apply iceberg fast_append"))
                    })?;

                    let table = tx.commit(catalog.as_ref()).await.map_err(|err| {
                        let err: IcebergError = err.into();
                        tracing::error!(
                            iceberg_component = "sink_committer",
                            iceberg_operation = "commit",
                            sink_id = %sink_id,
                            table = %table_name,
                            epoch,
                            snapshot_id,
                            branch = %target_branch,
                            data_file_count,
                            error = %err.as_report(),
                            "iceberg_sink_commit_transaction_failed",
                        );
                        CommitError::Commit(anyhow!(err).context("commit iceberg transaction"))
                    })?;
                    Ok(table)
                }
            },
        )
        .await
        .map_err(SinkError::Iceberg)?;
        self.table = table;

        let snapshot_num = self.table.metadata().snapshots().count();
        let catalog_name = self.config.common.catalog_name();
        let table_name = self.table.identifier().to_string();
        let metrics_labels = [&self.param.sink_name, &catalog_name, &table_name];
        GLOBAL_SINK_METRICS
            .iceberg_snapshot_num
            .with_guarded_label_values(&metrics_labels)
            .set(snapshot_num as i64);

        tracing::debug!(
            iceberg_component = "sink_committer",
            iceberg_operation = "commit",
            sink_id = %self.sink_id,
            sink_name = %self.param.sink_name,
            table = %self.table.identifier(),
            branch = %target_branch,
            epoch,
            snapshot_id,
            snapshot_num,
            data_file_count,
            "iceberg_sink_commit_succeeded",
        );

        self.notify_iceberg_compaction_scheduler(false);

        Ok(())
    }

    /// During pre-commit metadata, we record the `snapshot_id` corresponding to each batch of files.
    /// Therefore, the logic for checking whether all files in this batch are present in Iceberg
    /// has been changed to verifying if their corresponding `snapshot_id` exists in Iceberg.
    async fn is_snapshot_id_in_iceberg(&self, snapshot_id: i64) -> Result<bool> {
        let table = self
            .catalog
            .load_table(self.table.identifier())
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err).context("reload iceberg table")))?;
        if table.metadata().snapshot_by_id(snapshot_id).is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if the specified columns already exist in the iceberg table's current schema.
    /// This is used to determine if schema change has already been applied.
    fn check_schema_change_applied(&self, schema_change: &PbSinkSchemaChange) -> Result<bool> {
        let current_schema = self.table.metadata().current_schema();
        let current_arrow_schema = schema_to_arrow_schema(current_schema.as_ref())
            .context("Failed to convert schema")
            .map_err(SinkError::Iceberg)?;

        let iceberg_arrow_convert = IcebergArrowConvert;

        let schema_matches = |expected: &[ArrowField]| {
            schema_contains_same_fields(current_arrow_schema.fields(), expected)
        };

        let original_arrow_fields: Vec<ArrowField> = schema_change
            .original_schema
            .iter()
            .map(|pb_field| {
                let field = Field::from(pb_field);
                iceberg_arrow_convert
                    .to_arrow_field(&field.name, &field.data_type)
                    .context("Failed to convert field to arrow")
                    .map_err(SinkError::Iceberg)
            })
            .collect::<Result<_>>()?;

        // If current schema equals original_schema, then schema change is NOT applied.
        if schema_matches(&original_arrow_fields) {
            tracing::debug!(
                "Current iceberg schema matches original_schema ({} columns); schema change not applied",
                original_arrow_fields.len()
            );
            return Ok(false);
        }

        let expected_after_change = match schema_change.op.as_ref() {
            Some(risingwave_pb::stream_plan::sink_schema_change::Op::AddColumns(
                add_columns_op,
            )) => {
                let add_arrow_fields: Vec<ArrowField> = add_columns_op
                    .fields
                    .iter()
                    .map(|pb_field| {
                        let field = Field::from(pb_field);
                        iceberg_arrow_convert
                            .to_arrow_field(&field.name, &field.data_type)
                            .context("Failed to convert field to arrow")
                            .map_err(SinkError::Iceberg)
                    })
                    .collect::<Result<_>>()?;

                let mut expected_after_change = original_arrow_fields;
                expected_after_change.extend(add_arrow_fields);
                expected_after_change
            }
            Some(risingwave_pb::stream_plan::sink_schema_change::Op::DropColumns(
                drop_columns_op,
            )) => original_arrow_fields
                .into_iter()
                .filter(|field| {
                    !drop_columns_op
                        .column_names
                        .iter()
                        .any(|name| name == field.name())
                })
                .collect_vec(),
            _ => {
                return Err(SinkError::Iceberg(anyhow!(
                    "Unsupported sink schema change op in iceberg sink: {:?}",
                    schema_change.op
                )));
            }
        };

        // If current schema equals the changed schema, then schema change is applied.
        if schema_matches(&expected_after_change) {
            tracing::debug!(
                "Current iceberg schema matches changed schema ({} columns); schema change already applied",
                expected_after_change.len()
            );
            return Ok(true);
        }

        Err(SinkError::Iceberg(anyhow!(
            "Current iceberg schema does not match either original_schema ({} cols) or changed schema; cannot determine whether schema change is applied",
            schema_change.original_schema.len()
        )))
    }

    /// Commit schema changes (e.g., add columns) to the iceberg table.
    /// This function uses Transaction API to atomically update the table schema
    /// with optimistic locking to prevent concurrent conflicts.
    async fn commit_schema_change_impl(&mut self, schema_change: PbSinkSchemaChange) -> Result<()> {
        // Step 1: Build new fields to add
        let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
        let mut new_fields = Vec::new();

        let mut drop_column_names = Vec::new();
        match schema_change.op.as_ref() {
            Some(risingwave_pb::stream_plan::sink_schema_change::Op::AddColumns(
                add_columns_op,
            )) => {
                let add_columns = add_columns_op.fields.iter().map(Field::from).collect_vec();
                for field in &add_columns {
                    // Convert RisingWave Field to Arrow Field using IcebergCreateTableArrowConvert
                    let arrow_field = iceberg_create_table_arrow_convert
                        .to_arrow_field(&field.name, &field.data_type)
                        .with_context(|| {
                            format!("Failed to convert field '{}' to arrow", field.name)
                        })
                        .map_err(SinkError::Iceberg)?;

                    // Convert Arrow DataType to Iceberg Type
                    let iceberg_type = iceberg::arrow::arrow_type_to_type(arrow_field.data_type())
                        .map_err(|err| {
                            SinkError::Iceberg(
                                anyhow!(err)
                                    .context("Failed to convert Arrow type to Iceberg type"),
                            )
                        })?;

                    new_fields.push(AddColumn::optional(&field.name, iceberg_type));
                    tracing::info!("Prepared field '{}' for schema change", field.name);
                }
            }
            Some(risingwave_pb::stream_plan::sink_schema_change::Op::DropColumns(
                drop_columns_op,
            )) => {
                drop_column_names = drop_columns_op.column_names.clone();
            }
            _ => {
                return Err(SinkError::Iceberg(anyhow!(
                    "Unsupported sink schema change op in iceberg sink: {:?}",
                    schema_change.op
                )));
            }
        }

        // Step 2: Create Transaction with UpdateSchemaAction
        tracing::info!(
            "Committing schema change to catalog for table {}",
            self.table.identifier()
        );

        let txn = Transaction::new(&self.table);
        let action_fields_added = new_fields.len();
        let mut action = txn.update_schema();
        for field in new_fields {
            action = action.add_column(field);
        }
        for column_name in &drop_column_names {
            action = action.delete_column(column_name);
        }

        let updated_table = action
            .apply(txn)
            .context("Failed to apply schema update action")
            .map_err(SinkError::Iceberg)?
            .commit(self.catalog.as_ref())
            .await
            .context("Failed to commit table schema change")
            .map_err(SinkError::Iceberg)?;

        self.table = updated_table;

        tracing::info!(
            "Successfully committed schema change, added {} columns and dropped {} columns from iceberg table",
            action_fields_added,
            drop_column_names.len()
        );

        Ok(())
    }

    /// Check the number of snapshots on the given branch lineage since the last rewrite operation.
    /// Returns the number of snapshots since the last rewrite.
    fn count_snapshots_since_rewrite_in_metadata(metadata: &TableMetadata, branch: &str) -> usize {
        // Start from the latest snapshot of the commit branch.
        let mut snapshot_id = metadata
            .snapshot_for_ref(branch)
            .map(|snapshot| snapshot.snapshot_id());
        let mut count = 0;

        // Iterate through snapshots by parent lineage to find the last rewrite.
        while let Some(current_snapshot_id) = snapshot_id {
            let Some(snapshot) = metadata.snapshot_by_id(current_snapshot_id) else {
                break;
            };

            // Check if this snapshot represents a rewrite operation.
            if snapshot.summary().operation == Operation::Replace {
                // Found a rewrite operation, stop counting.
                break;
            }

            // Increment count for each snapshot that is not a rewrite.
            count += 1;
            snapshot_id = snapshot.parent_snapshot_id();
        }

        count
    }

    /// Returns the number of snapshots in the current commit branch since the last rewrite.
    fn count_snapshots_since_rewrite(&self) -> usize {
        let branch = commit_branch(self.config.r#type.as_str(), self.config.write_mode);
        Self::count_snapshots_since_rewrite_in_metadata(self.table.metadata(), branch.as_str())
    }

    /// Wait until snapshot count since last rewrite is below the limit
    async fn wait_for_snapshot_limit(&mut self) -> Result<()> {
        if let Some(max_snapshots) = self.config.max_snapshots_num_before_compaction {
            loop {
                let current_count = self.count_snapshots_since_rewrite();

                if current_count < max_snapshots {
                    tracing::info!(
                        "Snapshot count check passed: {} < {}",
                        current_count,
                        max_snapshots
                    );
                    break;
                }

                tracing::info!(
                    "Snapshot count {} exceeds limit {}, waiting...",
                    current_count,
                    max_snapshots
                );

                self.notify_iceberg_compaction_scheduler(true);

                // Wait for 30 seconds before checking again
                tokio::time::sleep(Duration::from_secs(30)).await;

                // Refresh table after the wait so the next check sees latest snapshots.
                let table_ident = self.table.identifier().clone();
                self.table = self.catalog.load_table(&table_ident).await.map_err(|err| {
                    SinkError::Iceberg(anyhow!(err).context("reload iceberg table"))
                })?;
            }
        }
        Ok(())
    }
}

fn serialize_metadata(metadata: Vec<Vec<u8>>) -> Vec<u8> {
    serde_json::to_vec(&metadata).unwrap()
}

fn deserialize_metadata(bytes: Vec<u8>) -> Vec<Vec<u8>> {
    serde_json::from_slice(&bytes).unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, MAIN_BRANCH, NestedField, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use risingwave_common::array::arrow::arrow_schema_iceberg::{
        DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
        Fields as ArrowFields, Schema as ArrowSchema,
    };

    use super::*;

    #[test]
    fn test_schema_contains_same_fields_allows_binary_large_binary() {
        let current_schema = ArrowSchema::new(vec![
            ArrowField::new("k", ArrowDataType::Int32, true),
            ArrowField::new("v", ArrowDataType::LargeBinary, true),
        ]);
        let expected_fields = vec![
            ArrowField::new("k", ArrowDataType::Int32, true),
            ArrowField::new("v", ArrowDataType::Binary, true),
        ];

        assert!(schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_allows_nested_binary_large_binary() {
        let current_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "s",
                ArrowDataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "payload",
                    ArrowDataType::LargeBinary,
                    true,
                )])),
                true,
            ),
            ArrowField::new(
                "l",
                ArrowDataType::List(ArrowFieldRef::new(ArrowField::new_list_field(
                    ArrowDataType::LargeBinary,
                    true,
                ))),
                true,
            ),
            ArrowField::new_map(
                "m",
                "entries",
                ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                ArrowFieldRef::new(ArrowField::new("value", ArrowDataType::LargeBinary, true)),
                false,
                true,
            ),
        ]);
        let expected_fields = vec![
            ArrowField::new(
                "s",
                ArrowDataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "payload",
                    ArrowDataType::Binary,
                    true,
                )])),
                true,
            ),
            ArrowField::new(
                "l",
                ArrowDataType::List(ArrowFieldRef::new(ArrowField::new_list_field(
                    ArrowDataType::Binary,
                    true,
                ))),
                true,
            ),
            ArrowField::new_map(
                "m",
                "entries",
                ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                ArrowFieldRef::new(ArrowField::new("value", ArrowDataType::Binary, true)),
                false,
                true,
            ),
        ];

        assert!(schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_allows_decimal_precision_delta() {
        let current_schema = ArrowSchema::new(vec![ArrowField::new(
            "d",
            ArrowDataType::Decimal128(28, 10),
            true,
        )]);
        let expected_fields = vec![ArrowField::new(
            "d",
            ArrowDataType::Decimal128(38, 10),
            true,
        )];

        assert!(schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_allows_decimal_precision_and_scale_delta() {
        let current_schema = ArrowSchema::new(vec![ArrowField::new(
            "d",
            ArrowDataType::Decimal128(38, 2),
            true,
        )]);
        let expected_fields = vec![ArrowField::new(
            "d",
            ArrowDataType::Decimal128(38, 10),
            true,
        )];

        assert!(schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_rejects_length_mismatch() {
        let current_schema =
            ArrowSchema::new(vec![ArrowField::new("k", ArrowDataType::Int32, true)]);
        let expected_fields = vec![
            ArrowField::new("k", ArrowDataType::Int32, true),
            ArrowField::new("v", ArrowDataType::Utf8, true),
        ];

        assert!(!schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_rejects_type_mismatch() {
        let current_schema =
            ArrowSchema::new(vec![ArrowField::new("v", ArrowDataType::Utf8, true)]);
        let expected_fields = vec![ArrowField::new("v", ArrowDataType::Int32, true)];

        assert!(!schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_rejects_reused_duplicate_match() {
        let current_schema = ArrowSchema::new(vec![
            ArrowField::new("v", ArrowDataType::Int32, true),
            ArrowField::new("v", ArrowDataType::Utf8, true),
        ]);
        let expected_fields = vec![
            ArrowField::new("v", ArrowDataType::Int32, true),
            ArrowField::new("v", ArrowDataType::Int32, true),
        ];

        assert!(!schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_schema_contains_same_fields_rejects_nested_type_mismatch() {
        let current_schema = ArrowSchema::new(vec![ArrowField::new(
            "s",
            ArrowDataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "payload",
                ArrowDataType::Utf8,
                true,
            )])),
            true,
        )]);
        let expected_fields = vec![ArrowField::new(
            "s",
            ArrowDataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "payload",
                ArrowDataType::Int32,
                true,
            )])),
            true,
        )];

        assert!(!schema_contains_same_fields(
            current_schema.fields(),
            &expected_fields
        ));
    }

    #[test]
    fn test_count_snapshots_since_rewrite_in_metadata_ignores_other_branches() {
        let mut builder = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::new(1, "id", Type::Primitive(PrimitiveType::Long), false).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/table".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();

        for (snapshot_id, parent_snapshot_id, operation) in [
            (1, None, Operation::Append),
            (2, Some(1), Operation::Append),
            (3, Some(2), Operation::Replace),
            (4, Some(3), Operation::Append),
            // Simulate the COW publish snapshot on main. It should not affect
            // the ingestion branch backlog count.
            (5, None, Operation::Overwrite),
        ] {
            builder = builder
                .add_snapshot(snapshot(snapshot_id, parent_snapshot_id, operation))
                .unwrap();
        }

        let metadata = builder
            .set_ref(super::super::ICEBERG_COW_BRANCH, snapshot_ref(4))
            .unwrap()
            .set_ref(MAIN_BRANCH, snapshot_ref(5))
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        let count = IcebergSinkCommitter::count_snapshots_since_rewrite_in_metadata(
            &metadata,
            super::super::ICEBERG_COW_BRANCH,
        );

        assert_eq!(count, 1);
    }

    fn snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(snapshot_id)
            .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
    }

    fn snapshot_ref(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None))
    }
}
