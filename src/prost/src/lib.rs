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

#![allow(unfulfilled_lint_expectations)]
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::doc_lazy_continuation)]
// for derived code of `Message`
#![expect(clippy::doc_markdown)]
#![expect(clippy::upper_case_acronyms)]
#![expect(clippy::needless_lifetimes)]
// For tonic::transport::Endpoint::connect
#![expect(clippy::disallowed_methods)]
#![expect(clippy::enum_variant_names)]
#![expect(clippy::module_inception)]
// FIXME: This should be fixed!!! https://github.com/risingwavelabs/risingwave/issues/19906
#![expect(clippy::large_enum_variant)]
#![feature(step_trait)]

pub mod id;

use std::str::FromStr;

use event_recovery::RecoveryEvent;
use plan_common::AdditionalColumn;
pub use prost::Message;
use risingwave_error::tonic::ToTonicStatus;
use thiserror::Error;

use crate::common::WorkerType;
use crate::ddl_service::streaming_job_resource_type;
use crate::id::{FragmentId, SourceId, WorkerId};
use crate::meta::event_log::event_recovery;
use crate::stream_plan::PbStreamScanType;

#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/catalog.rs")]
pub mod catalog;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/common.rs")]
pub mod common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/compute.rs")]
pub mod compute;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/cloud_service.rs")]
pub mod cloud_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/data.rs")]
pub mod data;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/ddl_service.rs")]
pub mod ddl_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/expr.rs")]
pub mod expr;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/meta.rs")]
pub mod meta;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/plan_common.rs")]
pub mod plan_common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/batch_plan.rs")]
pub mod batch_plan;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/task_service.rs")]
pub mod task_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/connector_service.rs")]
pub mod connector_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/stream_plan.rs")]
pub mod stream_plan;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/stream_service.rs")]
pub mod stream_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/hummock.rs")]
pub mod hummock;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/compactor.rs")]
pub mod compactor;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/user.rs")]
pub mod user;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/source.rs")]
pub mod source;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/monitor_service.rs")]
pub mod monitor_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/backup_service.rs")]
pub mod backup_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/serverless_backfill_controller.rs")]
pub mod serverless_backfill_controller;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/frontend_service.rs")]
pub mod frontend_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/java_binding.rs")]
pub mod java_binding;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/health.rs")]
pub mod health;
#[rustfmt::skip]
#[path = "sim/telemetry.rs"]
pub mod telemetry;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/iceberg_compaction.rs")]
pub mod iceberg_compaction;
#[rustfmt::skip]
pub mod window_function;

#[rustfmt::skip]
#[path = "sim/secret.rs"]
pub mod secret;
#[rustfmt::skip]
#[path = "connector_service.serde.rs"]
pub mod connector_service_serde;
#[rustfmt::skip]
#[path = "catalog.serde.rs"]
pub mod catalog_serde;
#[rustfmt::skip]
#[path = "common.serde.rs"]
pub mod common_serde;
#[rustfmt::skip]
#[path = "compute.serde.rs"]
pub mod compute_serde;
#[rustfmt::skip]
#[path = "cloud_service.serde.rs"]
pub mod cloud_service_serde;
#[rustfmt::skip]
#[path = "data.serde.rs"]
pub mod data_serde;
#[rustfmt::skip]
#[path = "ddl_service.serde.rs"]
pub mod ddl_service_serde;
#[rustfmt::skip]
#[path = "expr.serde.rs"]
pub mod expr_serde;
#[rustfmt::skip]
#[path = "meta.serde.rs"]
pub mod meta_serde;
#[rustfmt::skip]
#[path = "plan_common.serde.rs"]
pub mod plan_common_serde;
#[rustfmt::skip]
#[path = "batch_plan.serde.rs"]
pub mod batch_plan_serde;
#[rustfmt::skip]
#[path = "task_service.serde.rs"]
pub mod task_service_serde;
#[rustfmt::skip]
#[path = "stream_plan.serde.rs"]
pub mod stream_plan_serde;
#[rustfmt::skip]
#[path = "stream_service.serde.rs"]
pub mod stream_service_serde;
#[rustfmt::skip]
#[path = "hummock.serde.rs"]
pub mod hummock_serde;
#[rustfmt::skip]
#[path = "compactor.serde.rs"]
pub mod compactor_serde;
#[rustfmt::skip]
#[path = "user.serde.rs"]
pub mod user_serde;
#[rustfmt::skip]
#[path = "source.serde.rs"]
pub mod source_serde;
#[rustfmt::skip]
#[path = "monitor_service.serde.rs"]
pub mod monitor_service_serde;
#[rustfmt::skip]
#[path = "backup_service.serde.rs"]
pub mod backup_service_serde;
#[rustfmt::skip]
#[path = "java_binding.serde.rs"]
pub mod java_binding_serde;
#[rustfmt::skip]
#[path = "telemetry.serde.rs"]
pub mod telemetry_serde;

#[rustfmt::skip]
#[path = "secret.serde.rs"]
pub mod secret_serde;
#[rustfmt::skip]
#[path = "serverless_backfill_controller.serde.rs"]
pub mod serverless_backfill_controller_serde;

pub const MONITOR_SERVICE_MESSAGE_SIZE_LIMIT: usize = 64 * 1024 * 1024;

#[cfg(not(madsim))]
pub fn configured_monitor_service_client<T>(
    client: monitor_service::monitor_service_client::MonitorServiceClient<T>,
) -> monitor_service::monitor_service_client::MonitorServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    client
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .max_decoding_message_size(MONITOR_SERVICE_MESSAGE_SIZE_LIMIT)
}

#[cfg(madsim)]
pub fn configured_monitor_service_client<F>(
    client: monitor_service::monitor_service_client::MonitorServiceClient<
        tonic::transport::Channel,
        F,
    >,
) -> monitor_service::monitor_service_client::MonitorServiceClient<tonic::transport::Channel, F>
where
    F: tonic::service::Interceptor,
{
    client
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .max_decoding_message_size(MONITOR_SERVICE_MESSAGE_SIZE_LIMIT)
}

#[cfg(not(madsim))]
pub fn configured_monitor_service_server<T>(
    server: monitor_service::monitor_service_server::MonitorServiceServer<T>,
) -> monitor_service::monitor_service_server::MonitorServiceServer<T> {
    server.send_compressed(tonic::codec::CompressionEncoding::Zstd)
}

#[cfg(madsim)]
pub fn configured_monitor_service_server<T, F>(
    server: monitor_service::monitor_service_server::MonitorServiceServer<T, F>,
) -> monitor_service::monitor_service_server::MonitorServiceServer<T, F>
where
    T: monitor_service::monitor_service_server::MonitorService,
    F: tonic::service::Interceptor,
{
    server.send_compressed(tonic::codec::CompressionEncoding::Zstd)
}

#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[error("field `{0}` not found")]
pub struct PbFieldNotFound(pub &'static str);

impl From<PbFieldNotFound> for tonic::Status {
    fn from(e: PbFieldNotFound) -> Self {
        e.to_status_unnamed(tonic::Code::Internal)
    }
}

impl FromStr for crate::expr::table_function::PbType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_name(&s.to_uppercase()).ok_or(())
    }
}

impl FromStr for crate::expr::agg_call::PbKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_name(&s.to_uppercase()).ok_or(())
    }
}

impl stream_plan::MaterializeNode {
    pub fn dist_key_indices(&self) -> Vec<u32> {
        self.get_table()
            .unwrap()
            .distribution_key
            .iter()
            .map(|i| *i as u32)
            .collect()
    }

    pub fn column_descs(&self) -> Vec<plan_common::PbColumnDesc> {
        self.get_table()
            .unwrap()
            .columns
            .iter()
            .map(|c| c.get_column_desc().unwrap().clone())
            .collect()
    }
}

impl stream_plan::StreamScanNode {
    /// See [`Self::upstream_column_ids`].
    pub fn upstream_columns(&self) -> Vec<plan_common::PbColumnDesc> {
        self.upstream_column_ids
            .iter()
            .map(|id| {
                (self.table_desc.as_ref().unwrap().columns.iter())
                    .find(|c| c.column_id == *id)
                    .unwrap()
                    .clone()
            })
            .collect()
    }
}

impl stream_plan::SourceBackfillNode {
    pub fn column_descs(&self) -> Vec<plan_common::PbColumnDesc> {
        self.columns
            .iter()
            .map(|c| c.column_desc.as_ref().unwrap().clone())
            .collect()
    }
}

// Encapsulating the use of parallelism.
impl common::WorkerNode {
    pub fn compute_node_parallelism(&self) -> usize {
        assert_eq!(self.r#type(), WorkerType::ComputeNode);
        self.property
            .as_ref()
            .expect("property should be exist")
            .parallelism as usize
    }

    fn compactor_node_parallelism(&self) -> usize {
        assert_eq!(self.r#type(), WorkerType::Compactor);
        self.property
            .as_ref()
            .expect("property should be exist")
            .parallelism as usize
    }

    pub fn parallelism(&self) -> Option<usize> {
        match self.r#type() {
            WorkerType::ComputeNode => Some(self.compute_node_parallelism()),
            WorkerType::Compactor => Some(self.compactor_node_parallelism()),
            _ => None,
        }
    }

    pub fn resource_group(&self) -> Option<String> {
        self.property
            .as_ref()
            .and_then(|p| p.resource_group.clone())
    }
}

impl stream_plan::SourceNode {
    pub fn column_descs(&self) -> Option<Vec<plan_common::PbColumnDesc>> {
        Some(
            self.source_inner
                .as_ref()?
                .columns
                .iter()
                .map(|c| c.get_column_desc().unwrap().clone())
                .collect(),
        )
    }
}

impl meta::table_fragments::ActorStatus {
    pub fn worker_id(&self) -> WorkerId {
        self.location
            .as_ref()
            .expect("actor location should be exist")
            .worker_node_id
    }
}

impl common::ActorLocation {
    pub fn from_worker(worker_node_id: WorkerId) -> Option<Self> {
        Some(Self { worker_node_id })
    }
}

impl meta::event_log::EventRecovery {
    pub fn event_type(&self) -> &str {
        match self.recovery_event.as_ref() {
            Some(RecoveryEvent::DatabaseStart(_)) => "DATABASE_RECOVERY_START",
            Some(RecoveryEvent::DatabaseSuccess(_)) => "DATABASE_RECOVERY_SUCCESS",
            Some(RecoveryEvent::DatabaseFailure(_)) => "DATABASE_RECOVERY_FAILURE",
            Some(RecoveryEvent::GlobalStart(_)) => "GLOBAL_RECOVERY_START",
            Some(RecoveryEvent::GlobalSuccess(_)) => "GLOBAL_RECOVERY_SUCCESS",
            Some(RecoveryEvent::GlobalFailure(_)) => "GLOBAL_RECOVERY_FAILURE",
            None => "UNKNOWN_RECOVERY_EVENT",
        }
    }

    pub fn database_recovery_start(database_id: u32) -> Self {
        Self {
            recovery_event: Some(RecoveryEvent::DatabaseStart(
                event_recovery::DatabaseRecoveryStart { database_id },
            )),
        }
    }

    pub fn database_recovery_failure(database_id: u32) -> Self {
        Self {
            recovery_event: Some(RecoveryEvent::DatabaseFailure(
                event_recovery::DatabaseRecoveryFailure { database_id },
            )),
        }
    }

    pub fn database_recovery_success(database_id: u32) -> Self {
        Self {
            recovery_event: Some(RecoveryEvent::DatabaseSuccess(
                event_recovery::DatabaseRecoverySuccess { database_id },
            )),
        }
    }

    pub fn global_recovery_start(reason: String) -> Self {
        Self {
            recovery_event: Some(RecoveryEvent::GlobalStart(
                event_recovery::GlobalRecoveryStart { reason },
            )),
        }
    }

    pub fn global_recovery_success(
        reason: String,
        duration_secs: f32,
        running_database_ids: Vec<u32>,
        recovering_database_ids: Vec<u32>,
    ) -> Self {
        Self {
            recovery_event: Some(RecoveryEvent::GlobalSuccess(
                event_recovery::GlobalRecoverySuccess {
                    reason,
                    duration_secs,
                    running_database_ids,
                    recovering_database_ids,
                },
            )),
        }
    }

    pub fn global_recovery_failure(reason: String, error: String) -> Self {
        Self {
            recovery_event: Some(RecoveryEvent::GlobalFailure(
                event_recovery::GlobalRecoveryFailure { reason, error },
            )),
        }
    }
}

impl stream_plan::StreamNode {
    /// Find the external stream source info inside the stream node, if any.
    ///
    /// Returns `source_id`.
    pub fn find_stream_source(&self) -> Option<SourceId> {
        if let Some(crate::stream_plan::stream_node::NodeBody::Source(source)) =
            self.node_body.as_ref()
            && let Some(inner) = &source.source_inner
        {
            return Some(inner.source_id);
        }

        for child in &self.input {
            if let Some(source) = child.find_stream_source() {
                return Some(source);
            }
        }

        None
    }

    /// Find the external stream source info inside the stream node, if any.
    ///
    /// Returns (`source_id`, `upstream_source_fragment_id`).
    ///
    /// Note: we must get upstream fragment id from the merge node, not from the fragment's
    /// `upstream_fragment_ids`. e.g., DynamicFilter may have 2 upstream fragments, but only
    /// one is the upstream source fragment.
    pub fn find_source_backfill(&self) -> Option<(SourceId, FragmentId)> {
        if let Some(crate::stream_plan::stream_node::NodeBody::SourceBackfill(source)) =
            self.node_body.as_ref()
        {
            if let crate::stream_plan::stream_node::NodeBody::Merge(merge) =
                self.input[0].node_body.as_ref().unwrap()
            {
                // Note: avoid using `merge.upstream_actor_id` to prevent misuse.
                // See comments there for details.
                return Some((source.upstream_source_id, merge.upstream_fragment_id));
            } else {
                unreachable!(
                    "source backfill must have a merge node as its input: {:?}",
                    self
                );
            }
        }

        for child in &self.input {
            if let Some(source) = child.find_source_backfill() {
                return Some(source);
            }
        }

        None
    }
}
impl stream_plan::Dispatcher {
    pub fn as_strategy(&self) -> stream_plan::DispatchStrategy {
        stream_plan::DispatchStrategy {
            r#type: self.r#type,
            dist_key_indices: self.dist_key_indices.clone(),
            output_mapping: self.output_mapping.clone(),
        }
    }
}

impl stream_plan::DispatchOutputMapping {
    /// Create a mapping that forwards all columns.
    pub fn identical(len: usize) -> Self {
        Self {
            indices: (0..len as u32).collect(),
            types: Vec::new(),
        }
    }

    /// Create a mapping that forwards columns with given indices, without type conversion.
    pub fn simple(indices: Vec<u32>) -> Self {
        Self {
            indices,
            types: Vec::new(),
        }
    }

    /// Assert that this mapping does not involve type conversion and return the indices.
    pub fn into_simple_indices(self) -> Vec<u32> {
        assert!(
            self.types.is_empty(),
            "types must be empty for simple mapping"
        );
        self.indices
    }
}

impl catalog::StreamSourceInfo {
    /// Refer to [`Self::cdc_source_job`] for details.
    pub fn is_shared(&self) -> bool {
        self.cdc_source_job
    }
}

impl stream_plan::PbStreamScanType {
    pub fn is_reschedulable(&self, is_online: bool) -> bool {
        match self {
            PbStreamScanType::Unspecified => {
                unreachable!()
            }
            // `UpstreamOnly` has no snapshot or backfill state to restore.
            PbStreamScanType::UpstreamOnly => true,
            PbStreamScanType::ArrangementBackfill => true,
            PbStreamScanType::CrossDbSnapshotBackfill => true,
            PbStreamScanType::SnapshotBackfill => !is_online,
            #[expect(deprecated)]
            PbStreamScanType::Chain | PbStreamScanType::Rearrange | PbStreamScanType::Backfill => {
                false
            }
        }
    }
}

impl catalog::Sink {
    // TODO: remove this placeholder
    // creating table sink does not have an id, so we need a placeholder
    pub const UNIQUE_IDENTITY_FOR_CREATING_TABLE_SINK: &'static str = "PLACE_HOLDER";

    pub fn unique_identity(&self) -> String {
        // TODO: use a more unique name
        format!("{}", self.id)
    }

    /// Get `ignore_delete` with backward compatibility.
    ///
    /// Historically we use `sink_type == ForceAppendOnly` to represent this behavior.
    #[allow(deprecated)]
    pub fn ignore_delete(&self) -> bool {
        self.raw_ignore_delete || self.sink_type() == catalog::SinkType::ForceAppendOnly
    }
}

impl stream_plan::SinkDesc {
    /// Get `ignore_delete` with backward compatibility.
    ///
    /// Historically we use `sink_type == ForceAppendOnly` to represent this behavior.
    #[allow(deprecated)]
    pub fn ignore_delete(&self) -> bool {
        self.raw_ignore_delete || self.sink_type() == catalog::SinkType::ForceAppendOnly
    }
}

impl connector_service::SinkParam {
    /// Get `ignore_delete` with backward compatibility.
    ///
    /// Historically we use `sink_type == ForceAppendOnly` to represent this behavior.
    #[allow(deprecated)]
    pub fn ignore_delete(&self) -> bool {
        self.raw_ignore_delete || self.sink_type() == catalog::SinkType::ForceAppendOnly
    }
}

impl catalog::Table {
    /// Get clean watermark column indices with backward compatibility.
    ///
    /// Returns the new `clean_watermark_indices` if set, otherwise derives it from the old
    /// `clean_watermark_index_in_pk` by converting PK index to column index.
    ///
    /// Note: a non-empty slice does not imply that the executor **SHOULD** clean this table
    /// by watermark, but that the storage **CAN** clean this table by watermark. It's actually
    /// the executor's responsibility to decide whether state cleaning is correct on semantics.
    /// Besides, due to historical reasons, this method may return `[pk[0]]` even if the table
    /// has nothing to do with watermark.
    #[expect(deprecated)]
    pub fn get_clean_watermark_column_indices(&self) -> Vec<u32> {
        if !self.clean_watermark_indices.is_empty() {
            // New format: directly return clean_watermark_indices
            self.clean_watermark_indices.clone()
        } else if let Some(pk_idx) = self
            .clean_watermark_index_in_pk
            // At the very beginning, the watermark index was hard-coded to the first column of the pk.
            .or_else(|| (!self.pk.is_empty()).then_some(0))
        {
            // Old format: convert PK index to column index
            // The pk_idx is the position in the PK, we need to find the corresponding column index
            if let Some(col_order) = self.pk.get(pk_idx as usize) {
                vec![col_order.column_index]
            } else {
                if cfg!(debug_assertions) {
                    panic!("clean_watermark_index_in_pk is out of range: {self:?}");
                }
                vec![]
            }
        } else {
            vec![]
        }
    }

    /// Convert clean watermark column indices to PK indices and return the minimum.
    /// Returns None if no clean watermark is configured.
    ///
    /// This is a backward-compatible method to replace the deprecated `clean_watermark_index_in_pk` field.
    ///
    /// Note: a `Some` return value does not imply that the executor **SHOULD** clean this table
    /// by watermark, but that the storage **CAN** clean this table by watermark. It's actually
    /// the executor's responsibility to decide whether state cleaning is correct on semantics.
    /// Besides, due to historical reasons, this method may return `Some(pk[0])` even if the table
    /// has nothing to do with watermark.
    ///
    /// TODO: remove this method after totally deprecating `clean_watermark_index_in_pk`.
    pub fn get_clean_watermark_index_in_pk_compat(&self) -> Option<usize> {
        let clean_watermark_column_indices = self.get_clean_watermark_column_indices();

        // Convert column indices to PK indices
        let clean_watermark_indices_in_pk: Vec<usize> = clean_watermark_column_indices
            .iter()
            .filter_map(|&col_idx| {
                self.pk
                    .iter()
                    .position(|col_order| col_order.column_index == col_idx)
            })
            .collect();

        // Return the minimum PK index
        clean_watermark_indices_in_pk.iter().min().copied()
    }
}

impl std::fmt::Debug for meta::SystemParams {
    /// Directly formatting `SystemParams` can be inaccurate or leak sensitive information.
    ///
    /// Use `SystemParamsReader` instead.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemParams").finish_non_exhaustive()
    }
}

// More compact formats for debugging

impl std::fmt::Debug for data::DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data::DataType {
            precision,
            scale,
            interval_type,
            field_type,
            field_names,
            field_ids,
            type_name,
            // currently all data types are nullable
            is_nullable: _,
        } = self;

        let type_name = data::data_type::TypeName::try_from(*type_name)
            .map(|t| t.as_str_name())
            .unwrap_or("Unknown");

        let mut s = f.debug_struct(type_name);
        if self.precision != 0 {
            s.field("precision", precision);
        }
        if self.scale != 0 {
            s.field("scale", scale);
        }
        if self.interval_type != 0 {
            s.field("interval_type", interval_type);
        }
        if !self.field_type.is_empty() {
            s.field("field_type", field_type);
        }
        if !self.field_names.is_empty() {
            s.field("field_names", field_names);
        }
        if !self.field_ids.is_empty() {
            s.field("field_ids", field_ids);
        }
        s.finish()
    }
}

impl std::fmt::Debug for plan_common::column_desc::GeneratedOrDefaultColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GeneratedColumn(arg0) => f.debug_tuple("GeneratedColumn").field(arg0).finish(),
            Self::DefaultColumn(arg0) => f.debug_tuple("DefaultColumn").field(arg0).finish(),
        }
    }
}

impl std::fmt::Debug for plan_common::ColumnDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // destruct here to avoid missing new fields in the future.
        let plan_common::ColumnDesc {
            column_type,
            column_id,
            name,
            description,
            additional_column_type,
            additional_column,
            generated_or_default_column,
            version,
            nullable,
        } = self;

        let mut s = f.debug_struct("ColumnDesc");
        if let Some(column_type) = column_type {
            s.field("column_type", column_type);
        } else {
            s.field("column_type", &"Unknown");
        }
        s.field("column_id", column_id).field("name", name);
        if let Some(description) = description {
            s.field("description", description);
        }
        if self.additional_column_type != 0 {
            s.field("additional_column_type", additional_column_type);
        }
        s.field("version", version);
        if let Some(AdditionalColumn {
            column_type: Some(column_type),
        }) = additional_column
        {
            // AdditionalColumn { None } means a normal column
            s.field("additional_column", &column_type);
        }
        if let Some(generated_or_default_column) = generated_or_default_column {
            s.field("generated_or_default_column", &generated_or_default_column);
        }
        s.field("nullable", nullable);
        s.finish()
    }
}

impl expr::UserDefinedFunction {
    pub fn name_in_runtime(&self) -> Option<&str> {
        if self.version() < expr::UdfExprVersion::NameInRuntime {
            if self.language == "rust" || self.language == "wasm" {
                // The `identifier` value of Rust and WASM UDF before `NameInRuntime`
                // is not used any more. The real bound function name should be the same
                // as `name`.
                Some(&self.name)
            } else {
                // `identifier`s of other UDFs already mean `name_in_runtime` before `NameInRuntime`.
                self.identifier.as_deref()
            }
        } else {
            // after `PbUdfExprVersion::NameInRuntime`, `identifier` means `name_in_runtime`
            self.identifier.as_deref()
        }
    }
}

impl expr::UserDefinedFunctionMetadata {
    pub fn name_in_runtime(&self) -> Option<&str> {
        if self.version() < expr::UdfExprVersion::NameInRuntime {
            if self.language == "rust" || self.language == "wasm" {
                // The `identifier` value of Rust and WASM UDF before `NameInRuntime`
                // is not used any more. And unfortunately, we don't have the original name
                // in `PbUserDefinedFunctionMetadata`, so we need to extract the name from
                // the old `identifier` value (e.g. `foo()->int32`).
                let old_identifier = self
                    .identifier
                    .as_ref()
                    .expect("Rust/WASM UDF must have identifier");
                Some(
                    old_identifier
                        .split_once("(")
                        .expect("the old identifier must contain `(`")
                        .0,
                )
            } else {
                // `identifier`s of other UDFs already mean `name_in_runtime` before `NameInRuntime`.
                self.identifier.as_deref()
            }
        } else {
            // after `PbUdfExprVersion::NameInRuntime`, `identifier` means `name_in_runtime`
            self.identifier.as_deref()
        }
    }
}

impl streaming_job_resource_type::ResourceType {
    pub fn resource_group(&self) -> Option<String> {
        match self {
            streaming_job_resource_type::ResourceType::Regular(_) => None,
            streaming_job_resource_type::ResourceType::SpecificResourceGroup(group) => {
                Some(group.clone())
            }
            streaming_job_resource_type::ResourceType::ServerlessBackfill(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{DataType, data_type};
    use crate::plan_common::Field;
    use crate::stream_plan::stream_node::NodeBody;

    #[test]
    fn test_getter() {
        let data_type: DataType = DataType {
            is_nullable: true,
            ..Default::default()
        };
        let field = Field {
            data_type: Some(data_type),
            name: "".to_owned(),
        };
        assert!(field.get_data_type().unwrap().is_nullable);
    }

    #[test]
    fn test_enum_getter() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::Double as i32;
        assert_eq!(
            data_type::TypeName::Double,
            data_type.get_type_name().unwrap()
        );
    }

    #[test]
    fn test_enum_unspecified() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::TypeUnspecified as i32;
        assert!(data_type.get_type_name().is_err());
    }

    #[test]
    fn test_primitive_getter() {
        let data_type: DataType = DataType::default();
        let new_data_type = DataType {
            is_nullable: data_type.get_is_nullable(),
            ..Default::default()
        };
        assert!(!new_data_type.is_nullable);
    }

    #[test]
    fn test_size() {
        use static_assertions::const_assert_eq;
        // box all fields in NodeBody to avoid large_enum_variant
        // see https://github.com/risingwavelabs/risingwave/issues/19910
        const_assert_eq!(std::mem::size_of::<NodeBody>(), 16);
    }

    #[test]
    #[expect(deprecated)]
    fn test_get_clean_watermark_index_in_pk_compat() {
        use crate::catalog::Table;
        use crate::common::{ColumnOrder, OrderType};

        fn create_column_order(column_index: u32) -> ColumnOrder {
            ColumnOrder {
                column_index,
                order_type: Some(OrderType::default()),
            }
        }

        // Test case 1: both fields are set
        let table = Table {
            clean_watermark_indices: vec![3, 2],
            clean_watermark_index_in_pk: Some(0),
            pk: vec![
                create_column_order(1),
                create_column_order(2),
                create_column_order(3),
                create_column_order(4),
            ],
            ..Default::default()
        };
        assert_eq!(table.get_clean_watermark_index_in_pk_compat(), Some(1));

        // Test case 2: only old field is set
        let table = Table {
            clean_watermark_indices: vec![],
            clean_watermark_index_in_pk: Some(1),
            pk: vec![create_column_order(0), create_column_order(2)],
            ..Default::default()
        };
        assert_eq!(table.get_clean_watermark_index_in_pk_compat(), Some(1));

        // Test case 3: no clean watermark configured
        let table = Table {
            clean_watermark_indices: vec![],
            clean_watermark_index_in_pk: None,
            ..Default::default()
        };
        assert_eq!(table.get_clean_watermark_index_in_pk_compat(), None);
    }
}
