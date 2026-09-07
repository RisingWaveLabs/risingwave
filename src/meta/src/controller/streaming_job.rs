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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, FragmentTypeFlag, FragmentTypeMask, ICEBERG_SINK_PREFIX, ICEBERG_SOURCE_PREFIX,
    RISINGWAVE_ICEBERG_ROW_ID, ROW_ID_COLUMN_NAME, max_column_id,
};
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::id::JobId;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::system_param::adaptive_parallelism_strategy::parse_strategy;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::stream_graph_visitor::{
    visit_stream_node_body, visit_stream_node_mut, visit_stream_node_stream_scan,
};
use risingwave_common::{bail, current_cluster_version};
use risingwave_connector::allow_alter_on_fly_fields::check_sink_allow_alter_on_fly_fields;
use risingwave_connector::connector_common::validate_connection;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::file_sink::fs::FsSink;
use risingwave_connector::sink::{CONNECTOR_TYPE_KEY, SinkError};
use risingwave_connector::source::{
    ConnectorProperties, UPSTREAM_SOURCE_KEY, pb_connection_type_to_connection_type,
};
use risingwave_connector::{WithOptionsSecResolved, WithPropertiesExt, match_sink_name_str};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{StreamingJob as StreamingJobModel, *};
use risingwave_meta_model::refresh_job::RefreshState;
use risingwave_meta_model::streaming_job::BackfillOrders;
use risingwave_meta_model::user_privilege::Action;
use risingwave_meta_model::*;
use risingwave_pb::catalog::table::PbEngine;
use risingwave_pb::catalog::{PbConnection, PbCreateType, PbTable};
use risingwave_pb::common::ThrottleType;
use risingwave_pb::ddl_service::streaming_job_resource_type;
use risingwave_pb::meta::alter_connector_props_request::{AlterIcebergTableIds, PbExtraOptions};
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::{ObjectDependency as PbObjectDependency, PbObject, PbObjectGroup};
use risingwave_pb::plan_common::PbColumnCatalog;
use risingwave_pb::plan_common::source_refresh_mode::{
    RefreshMode, SourceRefreshModeFullReload, SourceRefreshModeStreaming,
};
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{PbSinkLogStoreType, PbStreamNode, StreamScanType};
use risingwave_pb::user::PbUserInfo;
use risingwave_sqlparser::ast::{Engine, Statement};
use risingwave_sqlparser::parser::Parser;
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{Expr, Query, SimpleExpr};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseTransaction, EntityTrait, IntoActiveModel,
    JoinType, NotSet, PaginatorTrait, QueryFilter, QuerySelect, RelationTrait, TransactionTrait,
};
use thiserror_ext::AsReport;

use super::rename::IndexItemRewriter;
use crate::barrier::Command;
use crate::controller::ObjectModel;
use crate::controller::catalog::{
    CatalogController, DropTableConnectorContext, load_object_models,
};
use crate::controller::fragment::FragmentTypeMaskExt;
use crate::controller::utils::{
    PartialObject, build_object_group_for_delete, check_if_belongs_to_iceberg_table,
    check_relation_name_duplicate, check_sink_into_table_cycle, ensure_job_not_canceled,
    ensure_object_id, ensure_user_id, fetch_target_fragments, format_with_option_secret_resolved,
    get_belong_objects, get_belong_objects_by_ids, get_referring_objects, get_table_columns,
    grant_default_privileges_automatically, insert_fragment_relations,
    list_object_dependencies_by_object_id, list_user_info_by_ids, upsert_user_privileges,
};
use crate::error::MetaErrorInner;
use crate::manager::{NotificationVersion, StreamingJob, StreamingJobType};
use crate::model::{
    FragmentDownstreamRelation, FragmentReplaceUpstream, StreamContext, StreamJobFragments,
    StreamJobFragmentsToCreate,
};
use crate::stream::SplitAssignment;
use crate::{MetaError, MetaResult};

/// Result of [`CatalogController::try_abort_creating_streaming_job`].
pub struct AbortCreatingJobResult {
    /// The job was aborted by this call or already gone. `false` means the job was left untouched.
    pub aborted: bool,
    /// The database of the job, if the job was found.
    pub database_id: Option<DatabaseId>,
    /// The aborted sinks; the caller should clear their iceberg maintenance state via
    /// `IcebergCompactionManager::clear_maintenance_for_aborted_job`.
    pub aborted_sink_ids: Vec<SinkId>,
    /// Runtime and state cleanup information captured before deleting the catalog rows.
    /// This is returned only when handling an explicit cancellation.
    pub cancel_info: Option<CancelStreamingJobInfo>,
}

/// Runtime and state cleanup information for canceling a creating streaming job.
pub struct CancelStreamingJobInfo {
    /// Barrier command that stops every affected streaming job.
    pub command: Command,
    /// Streaming jobs included in `command`.
    pub streaming_job_ids: Vec<JobId>,
    /// State tables to unregister after the barrier is collected.
    pub state_table_ids: Vec<TableId>,
}

#[derive(Debug)]
pub struct IndependentJobChangeLogInfo {
    pub job_id: JobId,
    pub state_table_ids: HashSet<TableId>,
    pub upstream_table_snapshot_epochs: HashMap<TableId, Option<u64>>,
}

#[derive(Debug)]
pub struct TableChangeLogTruncateInfo {
    pub subscription_retention_seconds: HashMap<TableId, u64>,
    pub independent_jobs: Vec<IndependentJobChangeLogInfo>,
}

fn serverless_backfill_resource_group_placeholder(job_id: JobId) -> String {
    format!("SERVERLESS_BACKFILL_RESOURCE_GROUP_TBD_FOR_{job_id}")
}

fn job_initial_catalog_resource_group(
    resource_type: &streaming_job_resource_type::ResourceType,
    job_id: JobId,
) -> Option<String> {
    match resource_type {
        streaming_job_resource_type::ResourceType::Regular(_)
        | streaming_job_resource_type::ResourceType::ServerlessBackfill(false) => None,
        streaming_job_resource_type::ResourceType::SpecificResourceGroup(group) => {
            Some(group.clone())
        }
        streaming_job_resource_type::ResourceType::ServerlessBackfill(true) => {
            Some(serverless_backfill_resource_group_placeholder(job_id))
        }
    }
}

/// A planned fragment update for dependent sources when a connection's properties change.
///
/// We keep it as a named struct (instead of a tuple) for readability and to avoid clippy
/// `type_complexity` warnings.
#[derive(Debug)]
struct DependentSourceFragmentUpdate {
    job_ids: Vec<JobId>,
    with_properties: BTreeMap<String, String>,
    secret_refs: BTreeMap<String, PbSecretRef>,
    is_shared_source: bool,
}

#[derive(Debug)]
struct DependentSinkFragmentUpdate {
    job_id: JobId,
    with_properties: BTreeMap<String, String>,
    secret_refs: BTreeMap<String, PbSecretRef>,
}

#[derive(Debug)]
struct ReplaceOriginalJobInfo {
    max_parallelism: i32,
    timezone: Option<String>,
    config_override: Option<String>,
    adaptive_parallelism_strategy: Option<String>,
    parallelism: StreamingParallelism,
    specific_resource_group: Option<String>,
}

impl ReplaceOriginalJobInfo {
    fn resolved_parallelism(
        &self,
        specified_parallelism: Option<&NonZeroUsize>,
    ) -> StreamingParallelism {
        specified_parallelism
            .map(|n| StreamingParallelism::Fixed(n.get() as _))
            .unwrap_or_else(|| self.parallelism.clone())
    }

    fn stream_context(&self, ctx_override: Option<&StreamContext>) -> StreamContext {
        StreamContext {
            timezone: ctx_override
                .and_then(|ctx| ctx.timezone.clone())
                .or_else(|| self.timezone.clone()),
            // We don't expect replacing a job with a different config override.
            // Thus always use the original config override.
            config_override: self.config_override.clone().unwrap_or_default().into(),
        }
    }

    fn resource_type(&self) -> streaming_job_resource_type::ResourceType {
        match &self.specific_resource_group {
            Some(group) => {
                streaming_job_resource_type::ResourceType::SpecificResourceGroup(group.clone())
            }
            None => streaming_job_resource_type::ResourceType::Regular(true),
        }
    }
}

impl From<streaming_job::Model> for ReplaceOriginalJobInfo {
    fn from(model: streaming_job::Model) -> Self {
        Self {
            max_parallelism: model.max_parallelism,
            timezone: model.timezone,
            config_override: model.config_override,
            adaptive_parallelism_strategy: model.adaptive_parallelism_strategy,
            parallelism: model.parallelism,
            specific_resource_group: model.specific_resource_group,
        }
    }
}

fn update_sink_node_rate_limit(node: &mut PbNodeBody, rate_limit: Option<u32>) -> MetaResult<bool> {
    let PbNodeBody::Sink(node) = node else {
        return Ok(false);
    };
    if node.log_store_type != PbSinkLogStoreType::KvLogStore as i32 {
        return Err(MetaError::invalid_parameter(
            "sink rate limit is only supported for kv log store, please SET sink_decouple = TRUE before CREATE SINK",
        ));
    }
    node.rate_limit = rate_limit;
    Ok(true)
}

impl CatalogController {
    pub async fn get_table_change_log_truncate_info(
        &self,
    ) -> MetaResult<TableChangeLogTruncateInfo> {
        let inner = self.inner.read().await;

        let subscriptions: Vec<(TableId, i64)> = Subscription::find()
            .select_only()
            .columns([
                subscription::Column::DependentTableId,
                subscription::Column::RetentionSeconds,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        let mut subscription_retention_seconds = HashMap::new();
        for (table_id, retention_seconds) in subscriptions {
            let retention_seconds = u64::try_from(retention_seconds).map_err(|_| {
                anyhow!(
                    "subscription on table {} has invalid retention seconds {}",
                    table_id,
                    retention_seconds
                )
            })?;
            subscription_retention_seconds
                .entry(table_id)
                .and_modify(|retention: &mut u64| *retention = (*retention).max(retention_seconds))
                .or_insert(retention_seconds);
        }

        let jobs: Vec<JobId> = StreamingJobModel::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(
                Condition::any()
                    .add(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
                    .add(streaming_job::Column::RefreshIntervalSec.is_not_null()),
            )
            .into_tuple::<JobId>()
            .all(&inner.db)
            .await?;
        let mut job_info: HashMap<_, _> = jobs
            .into_iter()
            .map(|job_id| {
                (
                    job_id,
                    IndependentJobChangeLogInfo {
                        job_id,
                        state_table_ids: HashSet::new(),
                        upstream_table_snapshot_epochs: HashMap::new(),
                    },
                )
            })
            .collect();
        if !job_info.is_empty() {
            let fragments = Fragment::find()
                .filter(fragment::Column::JobId.is_in(job_info.keys().copied()))
                .all(&inner.db)
                .await?;
            for fragment in fragments {
                let info = job_info
                    .get_mut(&fragment.job_id)
                    .expect("job should exist");
                info.state_table_ids
                    .extend(fragment.state_table_ids.inner_ref().iter().copied());
                let mut collection_error = None;
                visit_stream_node_stream_scan(&fragment.stream_node.to_protobuf(), |stream_scan| {
                    let scan_type = match StreamScanType::try_from(stream_scan.stream_scan_type) {
                        Ok(scan_type) => scan_type,
                        Err(err) => {
                            collection_error = Some(anyhow::Error::new(err).context(format!(
                                "invalid persisted stream scan type {} in job {} fragment {}",
                                stream_scan.stream_scan_type, fragment.job_id, fragment.fragment_id
                            )));
                            return;
                        }
                    };
                    if scan_type != StreamScanType::SnapshotBackfill {
                        return;
                    }
                    match info
                        .upstream_table_snapshot_epochs
                        .entry(stream_scan.table_id)
                    {
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            if entry.get() != &stream_scan.snapshot_backfill_epoch {
                                collection_error = Some(anyhow!(
                                    "job {} has inconsistent snapshot epochs for upstream table {}",
                                    fragment.job_id,
                                    stream_scan.table_id
                                ));
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(stream_scan.snapshot_backfill_epoch);
                        }
                    }
                });
                if let Some(err) = collection_error {
                    return Err(err.into());
                }
            }
        }
        let independent_jobs = job_info
            .into_values()
            .filter(|info| !info.upstream_table_snapshot_epochs.is_empty())
            .collect();
        Ok(TableChangeLogTruncateInfo {
            subscription_retention_seconds,
            independent_jobs,
        })
    }

    pub async fn get_pinned_snapshot_epochs(&self) -> MetaResult<HashMap<TableId, HashSet<u64>>> {
        // Hold the catalog read lock across both queries so a job cannot transition out of
        // `Creating` while its fragments are being inspected.
        let inner = self.inner.read().await;
        let creating_job_ids = StreamingJobModel::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple::<JobId>()
            .all(&inner.db)
            .await?;
        if creating_job_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let fragments = Fragment::find()
            .filter(fragment::Column::JobId.is_in(creating_job_ids))
            .all(&inner.db)
            .await?;
        let mut pinned_snapshot_epochs: HashMap<TableId, HashSet<u64>> = HashMap::new();
        for fragment in fragments {
            visit_stream_node_stream_scan(&fragment.stream_node.to_protobuf(), |stream_scan| {
                let scan_type = match StreamScanType::try_from(stream_scan.stream_scan_type) {
                    Ok(scan_type) => scan_type,
                    Err(err) => {
                        tracing::warn!(
                            job_id = %fragment.job_id,
                            fragment_id = %fragment.fragment_id,
                            stream_scan_type = stream_scan.stream_scan_type,
                            error = %err.as_report(),
                            "invalid persisted stream scan type, skip collecting snapshot pin"
                        );
                        return;
                    }
                };
                if !matches!(
                    scan_type,
                    StreamScanType::SnapshotBackfill | StreamScanType::CrossDbSnapshotBackfill
                ) {
                    return;
                }
                let Some(epoch) = stream_scan.snapshot_backfill_epoch else {
                    tracing::warn!(
                        job_id = %fragment.job_id,
                        fragment_id = %fragment.fragment_id,
                        table_id = %stream_scan.table_id,
                        "persisted snapshot backfill epoch is not set, skip collecting snapshot pin"
                    );
                    return;
                };
                pinned_snapshot_epochs
                    .entry(stream_scan.table_id)
                    .or_default()
                    .insert(epoch);
            });
        }
        Ok(pinned_snapshot_epochs)
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn create_streaming_job_obj(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        belong_to_oid: Option<ObjectId>,
        create_type: PbCreateType,
        ctx: StreamContext,
        adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        streaming_parallelism: StreamingParallelism,
        max_parallelism: usize,
        resource_type: streaming_job_resource_type::ResourceType,
        backfill_parallelism: Option<StreamingParallelism>,
        backfill_adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        refresh_interval_sec: Option<u64>,
    ) -> MetaResult<streaming_job::Model> {
        let obj = Self::create_object(txn, obj_type, owner_id, belong_to_oid).await?;
        let job_id = obj.oid.as_job_id();
        let is_serverless_backfill = matches!(
            &resource_type,
            streaming_job_resource_type::ResourceType::ServerlessBackfill(true)
        );
        let model = streaming_job::Model {
            job_id,
            job_status: JobStatus::Initial,
            create_type: create_type.into(),
            timezone: ctx.timezone,
            config_override: Some(ctx.config_override.to_string()),
            adaptive_parallelism_strategy: adaptive_parallelism_strategy
                .as_ref()
                .map(ToString::to_string),
            parallelism: streaming_parallelism,
            backfill_parallelism,
            backfill_adaptive_parallelism_strategy: backfill_adaptive_parallelism_strategy
                .as_ref()
                .map(ToString::to_string),
            backfill_orders: None,
            max_parallelism: max_parallelism as _,
            specific_resource_group: job_initial_catalog_resource_group(&resource_type, job_id),
            is_serverless_backfill,
            refresh_interval_sec: refresh_interval_sec.map(|s| s as i64),
        };
        let job = model.clone().into_active_model();
        StreamingJobModel::insert(job).exec(txn).await?;

        Ok(model)
    }

    /// Create the initial catalogs for the streaming job.
    ///
    /// Some of the fields in the given streaming job are placeholders, which will
    /// be updated later in `prepare_streaming_job`. The catalogs become visible to frontend after
    /// the first barrier is collected in `post_collect_job_fragments`.
    #[expect(clippy::too_many_arguments)]
    #[await_tree::instrument]
    pub async fn create_job_catalog(
        &self,
        streaming_job: &mut StreamingJob,
        ctx: &StreamContext,
        parallelism: &Option<Parallelism>,
        max_parallelism: usize,
        mut dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        backfill_parallelism: &Option<Parallelism>,
        adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        backfill_adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        replace_sink: Option<&SinkId>,
        refresh_interval_sec: Option<u64>,
    ) -> MetaResult<streaming_job::Model> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let create_type = streaming_job.create_type();

        let streaming_parallelism = match (parallelism, self.env.opts.default_parallelism) {
            (None, DefaultParallelism::Full) => StreamingParallelism::Adaptive,
            (None, DefaultParallelism::Default(n)) => StreamingParallelism::Fixed(n.get()),
            (Some(n), _) => StreamingParallelism::Fixed(n.parallelism as _),
        };
        let backfill_parallelism = backfill_parallelism
            .as_ref()
            .map(|p| StreamingParallelism::Fixed(p.parallelism as _))
            .or_else(|| {
                backfill_adaptive_parallelism_strategy
                    .as_ref()
                    .map(|_| StreamingParallelism::Adaptive)
            });

        ensure_user_id(streaming_job.owner() as _, &txn).await?;
        ensure_object_id(ObjectType::Database, streaming_job.database_id(), &txn).await?;
        ensure_object_id(ObjectType::Schema, streaming_job.schema_id(), &txn).await?;
        if let Some(old_sink_id) = replace_sink {
            let StreamingJob::Sink(sink, _) = streaming_job else {
                bail!("replacement sink catalog requires a sink job")
            };
            let (old_sink, old_object) = Sink::find_by_id(*old_sink_id)
                .find_also_related(Object)
                .one(&txn)
                .await?
                .and_then(|(sink, object)| object.map(|object| (sink, object)))
                .ok_or_else(|| MetaError::catalog_id_not_found("sink", *old_sink_id))?;
            let old_streaming_job = StreamingJobModel::find_by_id(old_sink_id.as_job_id())
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("sink", *old_sink_id))?;
            if old_object.obj_type != ObjectType::Sink
                || old_object.database_id != Some(sink.database_id)
                || old_object.schema_id != Some(sink.schema_id)
                || old_sink.name != sink.name
            {
                bail!(
                    "old sink {} does not match replacement sink {}",
                    old_sink_id,
                    sink.name
                );
            }
            if old_sink.target_table.is_some() || sink.target_table.is_some() {
                bail!("replace sink into table is not supported");
            }
            if old_streaming_job.job_status != JobStatus::Created {
                bail!("sink {} is not ready to be replaced", old_sink_id);
            }
        } else {
            check_relation_name_duplicate(
                &streaming_job.name(),
                streaming_job.database_id(),
                streaming_job.schema_id(),
                &txn,
            )
            .await?;
        }

        // check if any dependency is in altering status.
        if !dependencies.is_empty() {
            let altering_cnt = ObjectDependency::find()
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(
                    object_dependency::Column::Oid
                        .is_in(dependencies.clone())
                        .and(object::Column::ObjType.eq(ObjectType::Table))
                        .and(streaming_job::Column::JobStatus.ne(JobStatus::Created))
                        .and(
                            // It means the referring table is just dummy for altering.
                            object::Column::Oid.not_in_subquery(
                                Query::select()
                                    .column(table::Column::TableId)
                                    .from(Table)
                                    .to_owned(),
                            ),
                        ),
                )
                .count(&txn)
                .await?;
            if altering_cnt != 0 {
                return Err(MetaError::permission_denied(
                    "some dependent relations are being altered",
                ));
            }

            // Check if any dependency is a batch refresh job.
            // Streaming on batch refresh materialized views is not supported.
            let batch_refresh_cnt = StreamingJobModel::find()
                .filter(
                    streaming_job::Column::JobId
                        .is_in(dependencies.iter().map(|id| JobId::new(id.as_raw_id())))
                        .and(streaming_job::Column::RefreshIntervalSec.is_not_null()),
                )
                .count(&txn)
                .await?;
            if batch_refresh_cnt != 0 {
                return Err(MetaError::permission_denied(
                    "creating streaming jobs on batch refresh materialized views is not supported",
                ));
            }
        }

        let streaming_job_model = match streaming_job {
            StreamingJob::MaterializedView(table) => {
                let streaming_job_model = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.schema_id.as_object_id()),
                    create_type,
                    ctx.clone(),
                    adaptive_parallelism_strategy,
                    streaming_parallelism,
                    max_parallelism,
                    resource_type.clone(),
                    backfill_parallelism.clone(),
                    backfill_adaptive_parallelism_strategy,
                    refresh_interval_sec,
                )
                .await?;
                table.id = streaming_job_model.job_id.as_mv_table_id();
                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
                streaming_job_model
            }
            StreamingJob::Sink(sink, belong_to_table_id) => {
                if let Some(target_table_id) = sink.target_table
                    && check_sink_into_table_cycle(
                        target_table_id.into(),
                        dependencies.iter().cloned().collect(),
                        &txn,
                    )
                    .await?
                {
                    bail!("Creating such a sink will result in circular dependency.");
                }

                let streaming_job_model = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    sink.owner as _,
                    Some(
                        belong_to_table_id
                            .map(|table_id| table_id.as_object_id())
                            .unwrap_or(sink.schema_id.as_object_id()),
                    ),
                    create_type,
                    ctx.clone(),
                    adaptive_parallelism_strategy,
                    streaming_parallelism,
                    max_parallelism,
                    resource_type.clone(),
                    backfill_parallelism.clone(),
                    backfill_adaptive_parallelism_strategy,
                    None, // refresh_interval_sec: only for MV
                )
                .await?;
                sink.id = streaming_job_model.job_id.as_sink_id();
                if let Some(old_sink_id) = replace_sink {
                    let final_sink_name = sink.name.clone();
                    // The replacement sink cannot reuse the old catalog name until the
                    // cutover transaction deletes the old sink. Use a deterministic temporary
                    // name and rename it back in `post_collect_job_fragments`.
                    sink.name = format!("__rw_replacing_sink_{}_{}", old_sink_id, sink.id);
                    tracing::debug!(
                        old_sink_id = %old_sink_id,
                        new_sink_id = %sink.id,
                        final_name = %final_sink_name,
                        temp_name = %sink.name,
                        "created replacement sink catalog with temporary name"
                    );
                }
                let sink_model: sink::ActiveModel = sink.clone().into();
                Sink::insert(sink_model).exec(&txn).await?;
                streaming_job_model
            }
            StreamingJob::Table(src, table, _) => {
                let streaming_job_model = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.schema_id.as_object_id()),
                    create_type,
                    ctx.clone(),
                    adaptive_parallelism_strategy,
                    streaming_parallelism,
                    max_parallelism,
                    resource_type.clone(),
                    backfill_parallelism.clone(),
                    backfill_adaptive_parallelism_strategy,
                    None, // refresh_interval_sec: only for MV
                )
                .await?;
                let job_id = streaming_job_model.job_id;
                table.id = job_id.as_mv_table_id();
                if let Some(src) = src {
                    let src_obj = Self::create_object(
                        &txn,
                        ObjectType::Source,
                        src.owner as _,
                        Some(job_id.as_object_id()),
                    )
                    .await?;
                    src.id = src_obj.oid.as_source_id();
                    src.optional_associated_table_id = Some(job_id.as_mv_table_id().into());
                    table.optional_associated_source_id = Some(src_obj.oid.as_source_id().into());
                    let source: source::ActiveModel = src.clone().into();
                    Source::insert(source).exec(&txn).await?;
                }
                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;

                if table.refreshable {
                    let trigger_interval_secs = src
                        .as_ref()
                        .and_then(|source_catalog| source_catalog.refresh_mode)
                        .and_then(
                            |source_refresh_mode| match source_refresh_mode.refresh_mode {
                                Some(RefreshMode::FullReload(SourceRefreshModeFullReload {
                                    refresh_interval_sec,
                                })) => refresh_interval_sec,
                                Some(RefreshMode::Streaming(SourceRefreshModeStreaming {})) => None,
                                None => None,
                            },
                        );

                    RefreshJob::insert(refresh_job::ActiveModel {
                        table_id: Set(table.id),
                        last_trigger_time: Set(None),
                        trigger_interval_secs: Set(trigger_interval_secs),
                        current_status: Set(RefreshState::Idle),
                        last_success_time: Set(None),
                    })
                    .exec(&txn)
                    .await?;
                }
                streaming_job_model
            }
            StreamingJob::Index(index, table) => {
                ensure_object_id(ObjectType::Table, index.primary_table_id, &txn).await?;
                let streaming_job_model = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Index,
                    index.owner as _,
                    Some(index.schema_id.as_object_id()),
                    create_type,
                    ctx.clone(),
                    adaptive_parallelism_strategy,
                    streaming_parallelism,
                    max_parallelism,
                    resource_type.clone(),
                    backfill_parallelism.clone(),
                    backfill_adaptive_parallelism_strategy,
                    None, // refresh_interval_sec: only for MV
                )
                .await?;
                // to be compatible with old implementation.
                let job_id = streaming_job_model.job_id;
                index.id = job_id.as_index_id();
                index.index_table_id = job_id.as_mv_table_id();
                table.id = job_id.as_mv_table_id();

                ObjectDependency::insert(object_dependency::ActiveModel {
                    oid: Set(index.primary_table_id.into()),
                    used_by: Set(table.id.into()),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;

                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
                let index_model: index::ActiveModel = index.clone().into();
                Index::insert(index_model).exec(&txn).await?;
                streaming_job_model
            }
            StreamingJob::Source(src) => {
                let streaming_job_model = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Source,
                    src.owner as _,
                    Some(src.schema_id.as_object_id()),
                    create_type,
                    ctx.clone(),
                    adaptive_parallelism_strategy,
                    streaming_parallelism,
                    max_parallelism,
                    resource_type.clone(),
                    backfill_parallelism.clone(),
                    backfill_adaptive_parallelism_strategy,
                    None, // refresh_interval_sec: only for MV
                )
                .await?;
                src.id = streaming_job_model.job_id.as_shared_source_id();
                let source_model: source::ActiveModel = src.clone().into();
                Source::insert(source_model).exec(&txn).await?;
                streaming_job_model
            }
        };

        // collect dependent secrets.
        dependencies.extend(
            streaming_job
                .dependent_secret_ids()?
                .into_iter()
                .map(|id| id.as_object_id()),
        );
        // collect dependent connection
        dependencies.extend(
            streaming_job
                .dependent_connection_ids()?
                .into_iter()
                .map(|id| id.as_object_id()),
        );

        // record object dependency.
        if !dependencies.is_empty() {
            ObjectDependency::insert_many(dependencies.into_iter().map(|oid| {
                object_dependency::ActiveModel {
                    oid: Set(oid),
                    used_by: Set(streaming_job.id().as_object_id()),
                    ..Default::default()
                }
            }))
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        Ok(streaming_job_model)
    }

    /// Create the initial catalogs for internal tables.
    ///
    /// Some of the fields in the given "incomplete" internal tables are placeholders, which will
    /// be updated later in `prepare_streaming_job`. The catalogs become visible to frontend after
    /// the first barrier is collected in `post_collect_job_fragments`.
    ///
    /// Returns a mapping from the temporary table id to the actual global table id.
    pub async fn create_internal_table_catalog(
        &self,
        job: &StreamingJob,
        mut incomplete_internal_tables: Vec<PbTable>,
    ) -> MetaResult<HashMap<TableId, TableId>> {
        let job_id = job.id();
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // Ensure the job exists.
        ensure_job_not_canceled(job_id, &txn).await?;

        let mut table_id_map = HashMap::new();
        for table in &mut incomplete_internal_tables {
            let table_id = Self::create_object(
                &txn,
                ObjectType::Table,
                table.owner as _,
                Some(job_id.as_object_id()),
            )
            .await?
            .oid
            .as_table_id();
            table_id_map.insert(table.id, table_id);
            table.id = table_id;
            table.job_id = Some(job_id);

            let table_model = table::ActiveModel {
                table_id: Set(table_id),
                belongs_to_job_id: Set(Some(job_id)),
                fragment_id: NotSet,
                ..table.clone().into()
            };
            Table::insert(table_model).exec(&txn).await?;
        }
        txn.commit().await?;

        Ok(table_id_map)
    }

    pub async fn update_streaming_job_resource_group(
        &self,
        job_id: JobId,
        resource_group: String,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        ensure_job_not_canceled(job_id, &txn).await?;

        let job = streaming_job::ActiveModel {
            job_id: Set(job_id),
            specific_resource_group: Set(Some(resource_group)),
            ..Default::default()
        };
        StreamingJobModel::update(job).exec(&txn).await?;

        txn.commit().await?;

        Ok(())
    }

    pub async fn prepare_stream_job_fragments(
        &self,
        stream_job_fragments: &StreamJobFragmentsToCreate,
        streaming_job: &StreamingJob,
        for_replace: bool,
        backfill_orders: Option<BackfillOrders>,
    ) -> MetaResult<()> {
        self.prepare_streaming_job(
            stream_job_fragments.stream_job_id(),
            || stream_job_fragments.fragments.values(),
            &stream_job_fragments.downstreams,
            for_replace,
            Some(streaming_job),
            backfill_orders,
        )
        .await
    }

    // TODO: In this function, we also update the `Table` model in the meta store.
    // Given that we've ensured the tables inside `TableFragments` are complete, shall we consider
    // making them the source of truth and performing a full replacement for those in the meta store?
    /// Insert fragments and actors into the meta store. Used both for creating new jobs and
    /// replacing jobs. This does not make a new job visible to frontend.
    #[await_tree::instrument("prepare_streaming_job_for_{}", if for_replace { "replace" } else { "create" }
    )]
    pub async fn prepare_streaming_job<'a, I: Iterator<Item = &'a crate::model::Fragment> + 'a>(
        &self,
        job_id: JobId,
        get_fragments: impl Fn() -> I + 'a,
        downstreams: &FragmentDownstreamRelation,
        for_replace: bool,
        creating_streaming_job: Option<&'a StreamingJob>,
        backfill_orders: Option<BackfillOrders>,
    ) -> MetaResult<()> {
        let fragments = Self::prepare_fragment_models_from_fragments(job_id, get_fragments())?;

        let inner = self.inner.write().await;

        let txn = inner.db.begin().await?;

        // Ensure the job exists.
        ensure_job_not_canceled(job_id, &txn).await?;

        if let Some(backfill_orders) = backfill_orders {
            let job = streaming_job::ActiveModel {
                job_id: Set(job_id),
                backfill_orders: Set(Some(backfill_orders)),
                ..Default::default()
            };
            StreamingJobModel::update(job).exec(&txn).await?;
        }

        let state_table_ids: Vec<_> = fragments
            .iter()
            .flat_map(|fragment| fragment.state_table_ids.inner_ref().clone())
            .collect_vec();

        // Collect fragment IDs before consuming `fragments` for serving mapping notification.
        let inserted_fragment_ids: Vec<crate::model::FragmentId> = fragments
            .iter()
            .map(|f| f.fragment_id as crate::model::FragmentId)
            .collect();

        if !fragments.is_empty() {
            let fragment_models = fragments
                .into_iter()
                .map(|fragment| fragment.into_active_model())
                .collect_vec();
            Fragment::insert_many(fragment_models).exec(&txn).await?;
        }

        // Fields including `fragment_id` and `vnode_count` were placeholder values before.
        // After table fragments are created, update them for all tables.
        if !for_replace {
            let all_tables = StreamJobFragments::collect_tables(get_fragments());
            for state_table_id in state_table_ids {
                // Table's vnode count is not always the fragment's vnode count, so we have to
                // look up the table from `TableFragments`.
                // See `ActorGraphBuilder::new`.
                let table = all_tables
                    .get(&state_table_id)
                    .unwrap_or_else(|| panic!("table {} not found", state_table_id));
                assert_eq!(table.id, state_table_id);
                let vnode_count = table.vnode_count();

                Table::update(table::ActiveModel {
                    table_id: Set(state_table_id as _),
                    fragment_id: Set(Some(table.fragment_id)),
                    vnode_count: Set(vnode_count as _),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
        }

        insert_fragment_relations(&txn, downstreams).await?;

        if !for_replace {
            // Update dml fragment id.
            if let Some(StreamingJob::Table(_, table, _)) = creating_streaming_job {
                Table::update(table::ActiveModel {
                    table_id: Set(table.id),
                    dml_fragment_id: Set(table.dml_fragment_id),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
        }

        txn.commit().await?;

        // Notify serving module about newly inserted fragments so it can establish
        // serving vnode mappings. This is driven by the fragment model insertion,
        // decoupled from the barrier-driven streaming mapping notifications.
        self.env
            .notification_manager()
            .notify_serving_fragment_mapping_update(inserted_fragment_ids);

        self.notify_hummock_table_cache_refill_policy_if_explicit(&inner, job_id)
            .await?;

        Ok(())
    }

    /// Tries to abort a creating streaming job.
    ///
    /// An explicit cancellation always aborts the job. An ordinary creation failure only aborts a
    /// job in the initial status; a job that has reached the creating status is left to recovery.
    #[await_tree::instrument]
    pub async fn try_abort_creating_streaming_job(
        &self,
        job_id: JobId,
        is_cancelled: bool,
    ) -> MetaResult<AbortCreatingJobResult> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(job_id).one(&txn).await?;
        let Some(obj) = obj else {
            tracing::warn!(
                id = %job_id,
                "streaming job not found when aborting creating, might be cancelled already or cleaned by recovery"
            );
            return Ok(AbortCreatingJobResult {
                aborted: true,
                database_id: None,
                aborted_sink_ids: vec![],
                cancel_info: None,
            });
        };
        let database_id = obj
            .database_id
            .ok_or_else(|| anyhow!("obj has no database id: {:?}", obj))?;
        let streaming_job = streaming_job::Entity::find_by_id(job_id).one(&txn).await?;

        if let Some(streaming_job) = &streaming_job {
            if streaming_job.job_status == JobStatus::Created {
                tracing::warn!(%job_id, "streaming job is already created, ignore abort request");
                return Ok(AbortCreatingJobResult {
                    aborted: false,
                    database_id: Some(database_id),
                    aborted_sink_ids: vec![],
                    cancel_info: None,
                });
            }

            if !is_cancelled && streaming_job.job_status == JobStatus::Creating {
                if (obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Sink)
                    && check_if_belongs_to_iceberg_table(&txn, job_id).await?
                {
                    // If the job belongs to an Iceberg table, we still need to clean it.
                } else {
                    // If the job is created in background and still in creating status, we should
                    // not abort it and let recovery handle it.
                    tracing::warn!(
                        id = %job_id,
                        "streaming job is still in creating status"
                    );
                    return Ok(AbortCreatingJobResult {
                        aborted: false,
                        database_id: Some(database_id),
                        aborted_sink_ids: vec![],
                        cancel_info: None,
                    });
                }
            }
        }

        let mut objects_to_abort = vec![obj];
        objects_to_abort.extend(get_belong_objects(&txn, job_id.as_object_id()).await?);
        let object_ids_to_abort = objects_to_abort
            .iter()
            .map(|object| object.oid)
            .collect_vec();
        let streaming_job_ids_to_abort: Vec<JobId> = StreamingJobModel::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobId.is_in(object_ids_to_abort.iter().copied()))
            .into_tuple()
            .all(&txn)
            .await?;
        let object_models = load_object_models(&txn, &objects_to_abort).await?;

        let fragments: Vec<(FragmentId, i32, TableIdArray)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StateTableIds,
            ])
            .filter(fragment::Column::JobId.is_in(streaming_job_ids_to_abort.iter().copied()))
            .into_tuple()
            .all(&txn)
            .await?;
        let cancel_info = if is_cancelled {
            let state_table_ids: HashSet<_> = fragments
                .iter()
                .flat_map(|(_, _, state_table_ids)| state_table_ids.inner_ref().iter().copied())
                .collect();
            let sink_fragment_ids = fragments
                .iter()
                .filter_map(|(fragment_id, fragment_type_mask, _)| {
                    FragmentTypeMask::from(*fragment_type_mask)
                        .contains(FragmentTypeFlag::Sink)
                        .then_some(*fragment_id)
                })
                .collect_vec();
            let sink_target_fragments = fetch_target_fragments(&txn, sink_fragment_ids).await?;
            let mut dropped_sink_fragment_by_targets = HashMap::new();
            for (sink_fragment, target_fragments) in sink_target_fragments {
                if let Some(target_fragment) = target_fragments.first() {
                    dropped_sink_fragment_by_targets
                        .entry(*target_fragment)
                        .or_insert_with(Vec::new)
                        .push(sink_fragment);
                }
            }

            Some(CancelStreamingJobInfo {
                command: Command::DropStreamingJobs {
                    streaming_job_ids: streaming_job_ids_to_abort.iter().copied().collect(),
                    unregistered_state_table_ids: state_table_ids.clone(),
                    dropped_sink_fragment_by_targets,
                },
                streaming_job_ids: streaming_job_ids_to_abort.clone(),
                state_table_ids: state_table_ids.into_iter().collect(),
            })
        } else {
            None
        };

        // A job becomes visible to frontend only after it enters the creating status.
        let need_notify = streaming_job
            .as_ref()
            .is_some_and(|job| job.job_status != JobStatus::Initial);

        if is_cancelled {
            let dropped_tables = object_models.iter().filter_map(|object_info| {
                if let PbObjectInfo::Table(table) = object_info {
                    Some((table.id, table.clone()))
                } else {
                    None
                }
            });
            inner.dropped_tables.extend(dropped_tables);
        }

        let objs = if need_notify {
            objects_to_abort
                .iter()
                .map(|object| PartialObject {
                    oid: object.oid,
                    obj_type: object.obj_type,
                    schema_id: object.schema_id,
                    database_id: object.database_id,
                })
                .collect_vec()
        } else {
            vec![]
        };

        // Query fragment IDs before cascade-deleting them, for serving mapping cleanup.
        let abort_fragment_ids = fragments
            .iter()
            .map(|(fragment_id, _, _)| *fragment_id)
            .collect_vec();

        // Do not walk from an implicit Iceberg sink to its parent table here. The Iceberg table
        // creation error path drops the table explicitly, which then cascades to all objects that
        // belong to it.
        Object::delete_by_id(job_id).exec(&txn).await?;

        let err = if is_cancelled {
            MetaError::cancelled(format!("streaming job {job_id} is cancelled"))
        } else {
            MetaError::catalog_id_not_found("stream job", format!("streaming job {job_id} failed"))
        };
        let abort_reason = format!("streaming job aborted {}", err.as_report());
        for aborted_job_id in streaming_job_ids_to_abort {
            for tx in inner
                .creating_table_finish_notifier
                .get_mut(&database_id)
                .map(|creating_tables| creating_tables.remove(&aborted_job_id).into_iter())
                .into_iter()
                .flatten()
                .flatten()
            {
                let _ = tx.send(Err(abort_reason.clone()));
            }
        }
        txn.commit().await?;

        // Notify serving module about deleted fragments from the aborted job.
        self.env
            .notification_manager()
            .notify_serving_fragment_mapping_delete(
                abort_fragment_ids.iter().map(|id| *id as _).collect(),
            );

        if !objs.is_empty() {
            // We also have notified the frontend about these objects,
            // so we need to notify the frontend to delete them here.
            self.notify_frontend(Operation::Delete, build_object_group_for_delete(objs))
                .await;
        }
        let aborted_sink_ids = objects_to_abort
            .iter()
            .filter(|object| object.obj_type == ObjectType::Sink)
            .map(|object| object.oid.as_sink_id())
            .collect();
        Ok(AbortCreatingJobResult {
            aborted: true,
            database_id: Some(database_id),
            aborted_sink_ids,
            cancel_info,
        })
    }

    async fn build_creating_streaming_job_objects(
        txn: &DatabaseTransaction,
        job_id: JobId,
    ) -> MetaResult<Vec<PbObject>> {
        let job_type = Object::find_by_id(job_id)
            .select_only()
            .column(object::Column::ObjType)
            .into_tuple()
            .one(txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;
        let streaming_job = StreamingJobModel::find_by_id(job_id)
            .one(txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::BelongsToJobId
                    .eq(job_id)
                    .or(table::Column::TableId.eq(job_id.as_mv_table_id())),
            )
            .all(txn)
            .await?;
        let associated_source_id = table_objs
            .iter()
            .find(|(table, _)| table.table_id == job_id.as_mv_table_id())
            .and_then(|(table, _)| table.optional_associated_source_id);
        let mut objects = table_objs
            .into_iter()
            .map(|(table, obj)| PbObject {
                object_info: Some(PbObjectInfo::Table(
                    ObjectModel(table, obj.unwrap(), Some(streaming_job.clone())).into(),
                )),
            })
            .collect_vec();

        match job_type {
            ObjectType::Table => {
                if let Some(source_id) = associated_source_id {
                    let (source, obj) = Source::find_by_id(source_id)
                        .find_also_related(Object)
                        .one(txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Source(
                            ObjectModel(source, obj.unwrap(), None).into(),
                        )),
                    });
                }
            }
            ObjectType::Sink => {
                let (sink, obj) = Sink::find_by_id(job_id.as_sink_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Sink(
                        ObjectModel(sink, obj.unwrap(), Some(streaming_job)).into(),
                    )),
                });
            }
            ObjectType::Index => {
                let (index, obj) = Index::find_by_id(job_id.as_index_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("index", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(
                        ObjectModel(index, obj.unwrap(), Some(streaming_job)).into(),
                    )),
                });
            }
            ObjectType::Source => {
                let (source, obj) = Source::find_by_id(job_id.as_shared_source_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, obj.unwrap(), None).into(),
                    )),
                });
            }
            _ => unreachable!("invalid streaming job type: {job_type:?}"),
        }

        Ok(objects)
    }

    /// Mark a job as creating after its first barrier is collected and notify frontend to add its
    /// creating catalogs.
    #[await_tree::instrument]
    pub async fn post_collect_job_fragments(
        &self,
        job_id: JobId,
        upstream_fragment_new_downstreams: &FragmentDownstreamRelation,
        new_sink_downstream: Option<FragmentDownstreamRelation>,
        split_assignment: Option<&SplitAssignment>,
        replace_sink: Option<&SinkId>,
        notify_creating: bool,
    ) -> MetaResult<Option<Vec<TableId>>> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let mut replace_sink_post_collect = None;

        insert_fragment_relations(&txn, upstream_fragment_new_downstreams).await?;

        if let Some(new_downstream) = new_sink_downstream {
            insert_fragment_relations(&txn, &new_downstream).await?;
        }

        // Mark job as CREATING.
        let mut streaming_job_model = streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Creating),
            ..Default::default()
        };
        if replace_sink.is_some() {
            // The old sink is dropped in this transaction before the replacement job may be
            // marked Created by a later barrier. Preserve it across recovery even when the legacy
            // option to clean all foreground creating jobs is enabled.
            streaming_job_model.create_type = Set(CreateType::Background);
        }
        StreamingJobModel::update(streaming_job_model)
            .exec(&txn)
            .await?;

        if let Some(split_assignment) = split_assignment {
            let fragment_splits = split_assignment
                .iter()
                .map(|(fragment_id, splits)| {
                    (
                        *fragment_id as _,
                        splits.values().flatten().cloned().collect_vec(),
                    )
                })
                .collect();

            self.update_fragment_splits(&txn, &fragment_splits).await?;
        }

        if let Some(old_sink_id) = replace_sink {
            let old_sink_id = *old_sink_id;

            let (old_sink, old_sink_object) = Sink::find_by_id(old_sink_id)
                .find_also_related(Object)
                .one(&txn)
                .await?
                .and_then(|(sink, object)| object.map(|object| (sink, object)))
                .ok_or_else(|| MetaError::catalog_id_not_found("sink", old_sink_id))?;
            if old_sink_object.obj_type != ObjectType::Sink {
                bail!("object {} is not a sink", old_sink_id);
            }
            let final_sink_name = old_sink.name.clone();

            let mut old_objects_to_delete = vec![old_sink_object];
            old_objects_to_delete
                .extend(get_belong_objects(&txn, old_sink_id.as_object_id()).await?);
            let old_object_models = load_object_models(&txn, &old_objects_to_delete).await?;
            let old_state_table_ids = old_object_models
                .iter()
                .filter_map(|object_info| match object_info {
                    PbObjectInfo::Table(table) => Some(table.id),
                    _ => None,
                })
                .collect_vec();
            let old_fragment_ids: Vec<FragmentId> = Fragment::find()
                .select_only()
                .column(fragment::Column::FragmentId)
                .filter(
                    fragment::Column::JobId.is_in(
                        old_objects_to_delete
                            .iter()
                            .map(|object| object.oid.as_job_id()),
                    ),
                )
                .into_tuple()
                .all(&txn)
                .await?;
            let dropped_tables = old_object_models
                .iter()
                .filter_map(|object_info| match object_info {
                    PbObjectInfo::Table(table) => Some(table.clone()),
                    _ => None,
                })
                .collect_vec();

            Sink::update(sink::ActiveModel {
                sink_id: Set(job_id.as_sink_id()),
                name: Set(final_sink_name),
                ..Default::default()
            })
            .exec(&txn)
            .await?;

            let old_objects = old_objects_to_delete
                .into_iter()
                .map(PartialObject::from)
                .collect_vec();
            let old_object_ids = old_objects.iter().map(|obj| obj.oid).collect_vec();
            let updated_user_ids: Vec<UserId> = UserPrivilege::find()
                .select_only()
                .distinct()
                .column(user_privilege::Column::UserId)
                .filter(user_privilege::Column::Oid.is_in(old_object_ids.clone()))
                .into_tuple()
                .all(&txn)
                .await?;

            // Internal objects and their catalog rows follow the old sink's belong-to cascade.
            let res = Object::delete_by_id(old_sink_id).exec(&txn).await?;
            if res.rows_affected == 0 {
                return Err(MetaError::catalog_id_not_found("sink", old_sink_id));
            }

            let new_root_object = Object::find_by_id(job_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
            let mut new_objects_to_notify = vec![new_root_object];
            new_objects_to_notify.extend(get_belong_objects(&txn, job_id.as_object_id()).await?);
            let new_objects = load_object_models(&txn, &new_objects_to_notify)
                .await?
                .into_iter()
                .map(|object_info| PbObject {
                    object_info: Some(object_info),
                })
                .collect_vec();
            let dependencies =
                list_object_dependencies_by_object_id(&txn, job_id.as_object_id()).await?;
            let updated_user_info = list_user_info_by_ids(updated_user_ids, &txn).await?;

            replace_sink_post_collect = Some((
                old_state_table_ids,
                old_fragment_ids,
                old_objects,
                dropped_tables,
                new_objects,
                dependencies,
                updated_user_info,
            ));
        }

        let creating_objects = if notify_creating {
            Some(Self::build_creating_streaming_job_objects(&txn, job_id).await?)
        } else {
            None
        };

        txn.commit().await?;

        if let Some(objects) = creating_objects {
            self.notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects,
                    dependencies: vec![],
                }),
            )
            .await;
        }

        let Some((
            old_state_table_ids,
            old_fragment_ids,
            old_objects,
            dropped_tables,
            new_objects,
            dependencies,
            updated_user_info,
        )) = replace_sink_post_collect
        else {
            return Ok(None);
        };

        inner
            .dropped_tables
            .extend(dropped_tables.into_iter().map(|table| (table.id, table)));
        drop(inner);

        self.env
            .notification_manager()
            .notify_serving_fragment_mapping_delete(
                old_fragment_ids.iter().map(|id| *id as _).collect(),
            );

        let _ = self
            .notify_frontend(
                NotificationOperation::Delete,
                build_object_group_for_delete(old_objects),
            )
            .await;

        let _ = self
            .notify_frontend(
                // The replacement sink is not pre-notified to frontend while creating, so the
                // cutover should add the new finalized sink after deleting the old one.
                NotificationOperation::Add,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: new_objects,
                    dependencies,
                }),
            )
            .await;

        if !updated_user_info.is_empty() {
            let _ = self.notify_users_update(updated_user_info).await;
        }

        Ok(Some(old_state_table_ids))
    }

    pub async fn create_job_catalog_for_replace(
        &self,
        streaming_job: &StreamingJob,
        ctx: Option<&StreamContext>,
        specified_parallelism: Option<&NonZeroUsize>,
        expected_original_max_parallelism: Option<usize>,
    ) -> MetaResult<streaming_job::Model> {
        let id = streaming_job.id();
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // 1. check version.
        streaming_job.verify_version_for_replace(&txn).await?;
        // 2. check concurrent replace.
        let referring_objects =
            get_referring_objects(id.as_object_id(), streaming_job.object_type(), &txn).await?;
        let referring_job_ids = referring_objects
            .iter()
            .map(|object| object.oid.as_job_id())
            .collect_vec();
        let non_created_referring_job_count = StreamingJobModel::find()
            .filter(
                streaming_job::Column::JobId
                    .is_in(referring_job_ids)
                    .and(streaming_job::Column::JobStatus.ne(JobStatus::Created)),
            )
            .count(&txn)
            .await?;
        if non_created_referring_job_count != 0 {
            return Err(MetaError::permission_denied(
                "job is being altered or referenced by some creating jobs",
            ));
        }

        // Check if any dependent job is a batch refresh job.
        // Replace table is not supported when a batch refresh MV depends on it.
        let batch_refresh_dep_cnt = ObjectDependency::find()
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(
                object_dependency::Column::Oid
                    .eq(id)
                    .and(streaming_job::Column::RefreshIntervalSec.is_not_null()),
            )
            .count(&txn)
            .await?;
        if batch_refresh_dep_cnt != 0 {
            return Err(MetaError::permission_denied(
                "replacing a table with dependent batch refresh materialized views is not supported",
            ));
        }

        // 3. check parallelism.
        let original_job = StreamingJobModel::find_by_id(id)
            .one(&txn)
            .await?
            .map(ReplaceOriginalJobInfo::from)
            .ok_or_else(|| MetaError::catalog_id_not_found(streaming_job.job_type_str(), id))?;

        if let Some(max_parallelism) = expected_original_max_parallelism
            && original_job.max_parallelism != max_parallelism as i32
        {
            // We already override the max parallelism in `StreamFragmentGraph` before entering this function.
            // This should not happen in normal cases.
            bail!(
                "cannot use a different max parallelism \
                 when replacing streaming job, \
                 original: {}, new: {}",
                original_job.max_parallelism,
                max_parallelism
            );
        }

        let parallelism = original_job.resolved_parallelism(specified_parallelism);
        let ctx = original_job.stream_context(ctx);
        let adaptive_parallelism_strategy = original_job
            .adaptive_parallelism_strategy
            .as_deref()
            .map(|s| parse_strategy(s).expect("strategy should be validated before persisting"));
        let resource_type = original_job.resource_type();
        let belong_to_oid = Object::find_by_id(id)
            .select_only()
            .column(object::Column::BelongToOid)
            .into_tuple::<Option<ObjectId>>()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(streaming_job.job_type_str(), id))?;

        // 4. create streaming object for new replace table.
        let tmp_model = Self::create_streaming_job_obj(
            &txn,
            streaming_job.object_type(),
            streaming_job.owner() as _,
            belong_to_oid,
            streaming_job.create_type(),
            ctx,
            adaptive_parallelism_strategy,
            parallelism,
            original_job.max_parallelism as _,
            resource_type,
            // `backfill_parallelism` is intentionally NOT inherited from the original job.
            // Replace has no "backfill finish -> restore parallelism" phase, so inheriting it
            // would render actors at the backfill parallelism and never recover to steady-state.
            None,
            None,
            None, // refresh_interval_sec: not applicable for replace jobs
        )
        .await?;

        // 5. record dependency for new replace table.
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(id.as_object_id()),
            used_by: Set(tmp_model.job_id.as_object_id()),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(tmp_model)
    }

    /// `finish_streaming_job` marks job related objects as `Created` and notify frontend.
    pub async fn finish_streaming_job(&self, job_id: JobId) -> MetaResult<()> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // Check if the job belongs to iceberg table.
        if check_if_belongs_to_iceberg_table(&txn, job_id).await? {
            tracing::info!(
                "streaming job {} is for iceberg table, wait for manual finish operation",
                job_id
            );
            return Ok(());
        }

        let (notification_op, objects, updated_user_info, dependencies) =
            self.finish_streaming_job_inner(&txn, job_id).await?;

        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                notification_op,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects,
                    dependencies,
                }),
            )
            .await;

        // notify users about the default privileges
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        inner
            .creating_table_finish_notifier
            .values_mut()
            .for_each(|creating_tables| {
                if let Some(txs) = creating_tables.remove(&job_id) {
                    for tx in txs {
                        let _ = tx.send(Ok(version));
                    }
                }
            });

        Ok(())
    }

    /// `finish_streaming_job` marks job related objects as `Created` and notify frontend.
    pub async fn finish_streaming_job_inner(
        &self,
        txn: &DatabaseTransaction,
        job_id: JobId,
    ) -> MetaResult<(
        Operation,
        Vec<risingwave_pb::meta::Object>,
        Vec<PbUserInfo>,
        Vec<PbObjectDependency>,
    )> {
        let job_type = Object::find_by_id(job_id)
            .select_only()
            .column(object::Column::ObjType)
            .into_tuple()
            .one(txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

        // update `created_at` as now() and `created_at_cluster_version` as current cluster version.
        let res = Object::update_many()
            .col_expr(object::Column::CreatedAt, Expr::current_timestamp().into())
            .col_expr(
                object::Column::CreatedAtClusterVersion,
                current_cluster_version().into(),
            )
            .filter(object::Column::Oid.eq(job_id))
            .exec(txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("streaming job", job_id));
        }

        // mark the target stream job as `Created`.
        let job = streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Created),
            ..Default::default()
        };
        let streaming_job = Some(job.update(txn).await?);

        // notify frontend: job, internal tables.
        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::BelongsToJobId.eq(job_id))
            .all(txn)
            .await?;
        let mut objects = internal_table_objs
            .iter()
            .map(|(table, obj)| PbObject {
                object_info: Some(PbObjectInfo::Table(
                    ObjectModel(table.clone(), obj.clone().unwrap(), streaming_job.clone()).into(),
                )),
            })
            .collect_vec();
        let notification_op = NotificationOperation::Update;
        let mut updated_user_info = vec![];
        let mut need_grant_default_privileges = true;

        match job_type {
            ObjectType::Table => {
                let (table, obj) = Table::find_by_id(job_id.as_mv_table_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", job_id))?;
                if let Some(source_id) = table.optional_associated_source_id {
                    let (src, obj) = Source::find_by_id(source_id)
                        .find_also_related(Object)
                        .one(txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Source(
                            ObjectModel(src, obj.unwrap(), None).into(),
                        )),
                    });
                }
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(
                        ObjectModel(table, obj.unwrap(), streaming_job).into(),
                    )),
                });
            }
            ObjectType::Sink => {
                let (sink, obj) = Sink::find_by_id(job_id.as_sink_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
                if sink.name.starts_with(ICEBERG_SINK_PREFIX) {
                    need_grant_default_privileges = false;
                }
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Sink(
                        ObjectModel(sink, obj.unwrap(), streaming_job).into(),
                    )),
                });
            }
            ObjectType::Index => {
                need_grant_default_privileges = false;
                let (index, obj) = Index::find_by_id(job_id.as_index_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("index", job_id))?;
                {
                    let (table, obj) = Table::find_by_id(index.index_table_id)
                        .find_also_related(Object)
                        .one(txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("table", index.index_table_id)
                        })?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Table(
                            ObjectModel(table, obj.unwrap(), streaming_job.clone()).into(),
                        )),
                    });
                }

                // If the index is created on a table with privileges, we should also
                // grant the privileges for the index and its state tables.
                let primary_table_privileges = UserPrivilege::find()
                    .filter(
                        user_privilege::Column::Oid
                            .eq(index.primary_table_id)
                            .and(user_privilege::Column::Action.eq(Action::Select)),
                    )
                    .all(txn)
                    .await?;
                if !primary_table_privileges.is_empty() {
                    let index_state_table_ids: Vec<TableId> = Table::find()
                        .select_only()
                        .column(table::Column::TableId)
                        .filter(
                            table::Column::BelongsToJobId
                                .eq(job_id)
                                .or(table::Column::TableId.eq(index.index_table_id)),
                        )
                        .into_tuple()
                        .all(txn)
                        .await?;
                    let mut new_privileges = vec![];
                    for privilege in &primary_table_privileges {
                        for state_table_id in &index_state_table_ids {
                            new_privileges.push(user_privilege::ActiveModel {
                                id: Default::default(),
                                oid: Set(state_table_id.as_object_id()),
                                user_id: Set(privilege.user_id),
                                action: Set(Action::Select),
                                dependent_id: Set(privilege.dependent_id),
                                granted_by: Set(privilege.granted_by),
                                with_grant_option: Set(privilege.with_grant_option),
                            });
                        }
                    }
                    upsert_user_privileges(txn, new_privileges).await?;

                    updated_user_info = list_user_info_by_ids(
                        primary_table_privileges.into_iter().map(|p| p.user_id),
                        txn,
                    )
                    .await?;
                }

                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(
                        ObjectModel(index, obj.unwrap(), streaming_job).into(),
                    )),
                });
            }
            ObjectType::Source => {
                let (source, obj) = Source::find_by_id(job_id.as_shared_source_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, obj.unwrap(), None).into(),
                    )),
                });
            }
            _ => unreachable!("invalid job type: {:?}", job_type),
        }

        if need_grant_default_privileges {
            updated_user_info = grant_default_privileges_automatically(txn, job_id).await?;
        }

        let dependencies =
            list_object_dependencies_by_object_id(txn, job_id.as_object_id()).await?;

        Ok((notification_op, objects, updated_user_info, dependencies))
    }

    pub async fn finish_replace_streaming_job(
        &self,
        tmp_id: JobId,
        streaming_job: StreamingJob,
        replace_upstream: FragmentReplaceUpstream,
        sink_into_table_context: SinkIntoTableContext,
        drop_table_connector_ctx: Option<&DropTableConnectorContext>,
        auto_refresh_schema_sinks: Option<Vec<FinishAutoRefreshSchemaSinkContext>>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let (objects, delete_notification_objs, old_fragment_ids, new_fragment_ids) =
            Self::finish_replace_streaming_job_inner(
                tmp_id,
                replace_upstream,
                sink_into_table_context,
                &txn,
                streaming_job,
                drop_table_connector_ctx,
                auto_refresh_schema_sinks,
            )
            .await?;

        txn.commit().await?;

        // Notify serving module: delete old fragment mappings, upsert new ones.
        let notification_manager = self.env.notification_manager();
        notification_manager.notify_serving_fragment_mapping_delete(
            old_fragment_ids.iter().map(|id| *id as _).collect(),
        );
        notification_manager.notify_serving_fragment_mapping_update(
            new_fragment_ids.iter().map(|id| *id as _).collect(),
        );

        let mut version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects,
                    dependencies: vec![],
                }),
            )
            .await;

        if let Some((user_infos, to_drop_objects)) = delete_notification_objs {
            self.notify_users_update(user_infos).await;
            version = self
                .notify_frontend(
                    NotificationOperation::Delete,
                    build_object_group_for_delete(to_drop_objects),
                )
                .await;
        }

        Ok(version)
    }

    fn update_iceberg_source_columns(
        original_source_columns: &[ColumnCatalog],
        original_row_id_index: Option<usize>,
        new_table_columns: &[ColumnCatalog],
    ) -> (Vec<ColumnCatalog>, Option<usize>) {
        let row_id_column_name = original_row_id_index
            .and_then(|idx| original_source_columns.get(idx))
            .map(|col| col.name().to_owned());
        let mut next_column_id = max_column_id(original_source_columns).next();
        let existing_columns: HashMap<String, ColumnCatalog> = original_source_columns
            .iter()
            .cloned()
            .map(|col| (col.name().to_owned(), col))
            .collect();

        let mut new_columns = Vec::new();
        for table_col in new_table_columns
            .iter()
            .filter(|col| !col.is_rw_sys_column())
        {
            let mut source_col_name = table_col.name().to_owned();
            if source_col_name == ROW_ID_COLUMN_NAME {
                source_col_name = RISINGWAVE_ICEBERG_ROW_ID.to_owned();
            }

            if let Some(existing) = existing_columns.get(&source_col_name) {
                new_columns.push(existing.clone());
            } else {
                let mut new_col = table_col.clone();
                new_col.column_desc.name = source_col_name;
                new_col.column_desc.column_id = next_column_id;
                next_column_id = next_column_id.next();
                new_columns.push(new_col);
            }
        }

        let mut seen_names: HashSet<String> = new_columns
            .iter()
            .map(|col| col.name().to_owned())
            .collect();
        for col in original_source_columns
            .iter()
            .filter(|col| col.is_iceberg_hidden_column())
        {
            if seen_names.insert(col.name().to_owned()) {
                new_columns.push(col.clone());
            }
        }

        let new_row_id_index = row_id_column_name
            .as_ref()
            .and_then(|name| new_columns.iter().position(|col| col.name() == name));

        (new_columns, new_row_id_index)
    }

    pub async fn finish_replace_streaming_job_inner(
        tmp_id: JobId,
        replace_upstream: FragmentReplaceUpstream,
        SinkIntoTableContext {
            updated_sink_catalogs,
        }: SinkIntoTableContext,
        txn: &DatabaseTransaction,
        streaming_job: StreamingJob,
        drop_table_connector_ctx: Option<&DropTableConnectorContext>,
        auto_refresh_schema_sinks: Option<Vec<FinishAutoRefreshSchemaSinkContext>>,
    ) -> MetaResult<(
        Vec<PbObject>,
        Option<(Vec<PbUserInfo>, Vec<PartialObject>)>,
        Vec<FragmentId>,
        Vec<FragmentId>,
    )> {
        let original_job_id = streaming_job.id();
        let job_type = streaming_job.job_type();

        // Query old fragment IDs (will be deleted) and new fragment IDs (will be reassigned).
        let old_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::JobId.eq(original_job_id))
            .into_tuple()
            .all(txn)
            .await?;
        let new_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::JobId.eq(tmp_id))
            .into_tuple()
            .all(txn)
            .await?;

        let mut index_item_rewriter = None;
        let mut updated_iceberg_source_id: Option<SourceId> = None;

        // Update catalog
        match streaming_job {
            StreamingJob::Table(_, table, _table_job_type) => {
                let original_column_catalogs =
                    get_table_columns(txn, original_job_id.as_mv_table_id()).await?;
                let schema_changed = original_column_catalogs.to_protobuf() != table.columns;
                let is_iceberg = table
                    .engine
                    .and_then(|engine| PbEngine::try_from(engine).ok())
                    == Some(PbEngine::Iceberg);
                if is_iceberg && schema_changed {
                    let iceberg_source_name = format!("{}{}", ICEBERG_SOURCE_PREFIX, table.name);
                    let source = Source::find()
                        .inner_join(Object)
                        .filter(
                            object::Column::DatabaseId
                                .eq(table.database_id)
                                .and(object::Column::SchemaId.eq(table.schema_id))
                                .and(source::Column::Name.eq(&iceberg_source_name)),
                        )
                        .one(txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("source", iceberg_source_name)
                        })?;

                    let source_id = source.source_id;
                    let source_version = source.version;
                    let source_columns: Vec<ColumnCatalog> = source
                        .columns
                        .to_protobuf()
                        .into_iter()
                        .map(ColumnCatalog::from)
                        .collect();
                    let source_row_id_index = source
                        .row_id_index
                        .and_then(|idx| usize::try_from(idx).ok());
                    let table_columns: Vec<ColumnCatalog> = table
                        .columns
                        .iter()
                        .cloned()
                        .map(ColumnCatalog::from)
                        .collect();
                    let (updated_columns, updated_row_id_index) =
                        Self::update_iceberg_source_columns(
                            &source_columns,
                            source_row_id_index,
                            &table_columns,
                        );
                    let updated_columns: Vec<PbColumnCatalog> = updated_columns
                        .into_iter()
                        .map(|col| col.to_protobuf())
                        .collect();

                    let mut source_active = source.into_active_model();
                    source_active.columns = Set(ColumnCatalogArray::from(updated_columns));
                    source_active.row_id_index = Set(updated_row_id_index.map(|idx| idx as i32));
                    source_active.version = Set(source_version + 1);
                    source_active.update(txn).await?;
                    updated_iceberg_source_id = Some(source_id);
                }

                index_item_rewriter = Some({
                    let original_columns = original_column_catalogs
                        .to_protobuf()
                        .into_iter()
                        .map(|c| c.column_desc.unwrap())
                        .collect_vec();
                    let new_columns = table
                        .columns
                        .iter()
                        .map(|c| c.column_desc.clone().unwrap())
                        .collect_vec();

                    IndexItemRewriter {
                        original_columns,
                        new_columns,
                    }
                });

                // For sinks created in earlier versions, we need to set the original_target_columns.
                for sink_id in updated_sink_catalogs {
                    Sink::update(sink::ActiveModel {
                        sink_id: Set(sink_id as _),
                        original_target_columns: Set(Some(original_column_catalogs.clone())),
                        ..Default::default()
                    })
                    .exec(txn)
                    .await?;
                }
                // Update the table catalog with the new one. (column catalog is also updated here)
                let mut table = table::ActiveModel::from(table);
                if let Some(drop_table_connector_ctx) = drop_table_connector_ctx
                    && drop_table_connector_ctx.to_change_streaming_job_id == original_job_id
                {
                    // drop table connector, the rest logic is in `drop_table_associated_source`
                    table.optional_associated_source_id = Set(None);
                }

                Table::update(table).exec(txn).await?;
            }
            StreamingJob::Source(source) => {
                // Update the source catalog with the new one.
                let source = source::ActiveModel::from(source);
                Source::update(source).exec(txn).await?;
            }
            StreamingJob::MaterializedView(table) => {
                // Update the table catalog with the new one.
                let table = table::ActiveModel::from(table);
                Table::update(table).exec(txn).await?;
            }
            _ => unreachable!(
                "invalid streaming job type: {:?}",
                streaming_job.job_type_str()
            ),
        }

        async fn finish_fragments(
            txn: &DatabaseTransaction,
            tmp_id: JobId,
            original_job_id: JobId,
            replace_upstream: FragmentReplaceUpstream,
        ) -> MetaResult<()> {
            // 0. update internal tables
            // Fields including `fragment_id` were placeholder values before.
            // After table fragments are created, update them for all internal tables.
            let fragment_info: Vec<(FragmentId, I32Array)> = Fragment::find()
                .select_only()
                .columns([
                    fragment::Column::FragmentId,
                    fragment::Column::StateTableIds,
                ])
                .filter(fragment::Column::JobId.eq(tmp_id))
                .into_tuple()
                .all(txn)
                .await?;
            for (fragment_id, state_table_ids) in fragment_info {
                for state_table_id in state_table_ids.into_inner() {
                    let state_table_id = TableId::new(state_table_id as _);
                    Table::update(table::ActiveModel {
                        table_id: Set(state_table_id),
                        fragment_id: Set(Some(fragment_id)),
                        // No need to update `vnode_count` because it must remain the same.
                        ..Default::default()
                    })
                    .exec(txn)
                    .await?;
                }
            }

            // 1. replace old fragments/actors with new ones.
            Fragment::delete_many()
                .filter(fragment::Column::JobId.eq(original_job_id))
                .exec(txn)
                .await?;
            Fragment::update_many()
                .col_expr(fragment::Column::JobId, SimpleExpr::from(original_job_id))
                .filter(fragment::Column::JobId.eq(tmp_id))
                .exec(txn)
                .await?;

            // 2. update merges.
            // update downstream fragment's Merge node, and upstream_fragment_id
            for (fragment_id, fragment_replace_map) in replace_upstream {
                let (fragment_id, mut stream_node) =
                    Fragment::find_by_id(fragment_id as FragmentId)
                        .select_only()
                        .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
                        .into_tuple::<(FragmentId, StreamNode)>()
                        .one(txn)
                        .await?
                        .map(|(id, node)| (id, node.to_protobuf()))
                        .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;

                visit_stream_node_mut(&mut stream_node, |body| {
                    if let PbNodeBody::Merge(m) = body
                        && let Some(new_fragment_id) =
                            fragment_replace_map.get(&m.upstream_fragment_id)
                    {
                        m.upstream_fragment_id = *new_fragment_id;
                    }
                });
                Fragment::update(fragment::ActiveModel {
                    fragment_id: Set(fragment_id),
                    stream_node: Set(StreamNode::from(&stream_node)),
                    ..Default::default()
                })
                .exec(txn)
                .await?;
            }

            // 3. remove dummy object.
            Object::delete_by_id(tmp_id).exec(txn).await?;

            Ok(())
        }

        finish_fragments(txn, tmp_id, original_job_id, replace_upstream).await?;

        // 4. update catalogs and notify.
        let mut objects = vec![];
        match job_type {
            StreamingJobType::Table(_) | StreamingJobType::MaterializedView => {
                let (table, table_obj) = Table::find_by_id(original_job_id.as_mv_table_id())
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", original_job_id))?;
                let streaming_job = streaming_job::Entity::find_by_id(table.job_id())
                    .one(txn)
                    .await?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(
                        ObjectModel(table, table_obj.unwrap(), streaming_job).into(),
                    )),
                })
            }
            StreamingJobType::Source => {
                let (source, source_obj) =
                    Source::find_by_id(original_job_id.as_shared_source_id())
                        .find_also_related(Object)
                        .one(txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("object", original_job_id)
                        })?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, source_obj.unwrap(), None).into(),
                    )),
                })
            }
            _ => unreachable!("invalid streaming job type for replace: {:?}", job_type),
        }

        if let Some(source_id) = updated_iceberg_source_id {
            let (source, source_obj) = Source::find_by_id(source_id)
                .find_also_related(Object)
                .one(txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
            objects.push(PbObject {
                object_info: Some(PbObjectInfo::Source(
                    ObjectModel(source, source_obj.unwrap(), None).into(),
                )),
            });
        }

        if let Some(expr_rewriter) = index_item_rewriter {
            let index_items: Vec<(IndexId, ExprNodeArray)> = Index::find()
                .select_only()
                .columns([index::Column::IndexId, index::Column::IndexItems])
                .filter(index::Column::PrimaryTableId.eq(original_job_id))
                .into_tuple()
                .all(txn)
                .await?;
            for (index_id, nodes) in index_items {
                let mut pb_nodes = nodes.to_protobuf();
                pb_nodes
                    .iter_mut()
                    .for_each(|x| expr_rewriter.rewrite_expr(x));
                let index = index::ActiveModel {
                    index_id: Set(index_id),
                    index_items: Set(pb_nodes.into()),
                    ..Default::default()
                }
                .update(txn)
                .await?;
                let (index_obj, streaming_job) = Object::find_by_id(index.index_id)
                    .find_also_related(streaming_job::Entity)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", index.index_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(
                        ObjectModel(index, index_obj, streaming_job).into(),
                    )),
                });
            }
        }

        if let Some(sinks) = auto_refresh_schema_sinks {
            for finish_sink_context in sinks {
                finish_fragments(
                    txn,
                    finish_sink_context.tmp_sink_id.as_job_id(),
                    finish_sink_context.original_sink_id.as_job_id(),
                    Default::default(),
                )
                .await?;
                let (mut sink, sink_obj) = Sink::find_by_id(finish_sink_context.original_sink_id)
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", original_job_id))?;
                let sink_streaming_job =
                    streaming_job::Entity::find_by_id(sink.sink_id.as_job_id())
                        .one(txn)
                        .await?;
                let columns = ColumnCatalogArray::from(finish_sink_context.columns);
                Sink::update(sink::ActiveModel {
                    sink_id: Set(finish_sink_context.original_sink_id),
                    columns: Set(columns.clone()),
                    ..Default::default()
                })
                .exec(txn)
                .await?;
                sink.columns = columns;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Sink(
                        ObjectModel(sink, sink_obj.unwrap(), sink_streaming_job.clone()).into(),
                    )),
                });
                if let Some(new_log_store_table) = finish_sink_context.new_log_store_table {
                    let log_store_table_id = new_log_store_table.id;
                    let new_log_store_table_columns: ColumnCatalogArray =
                        new_log_store_table.columns.clone().into();
                    let new_log_store_table_value_indices =
                        new_log_store_table.value_indices.clone();
                    let (mut table, table_obj) = Table::find_by_id(log_store_table_id)
                        .find_also_related(Object)
                        .one(txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("table", original_job_id))?;
                    Table::update(table::ActiveModel {
                        table_id: Set(log_store_table_id),
                        columns: Set(new_log_store_table_columns.clone()),
                        value_indices: Set(new_log_store_table_value_indices.clone().into()),
                        ..Default::default()
                    })
                    .exec(txn)
                    .await?;
                    table.columns = new_log_store_table_columns;
                    table.value_indices = new_log_store_table_value_indices.into();
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Table(
                            ObjectModel(table, table_obj.unwrap(), sink_streaming_job.clone())
                                .into(),
                        )),
                    });
                }
            }
        }

        let mut notification_objs: Option<(Vec<PbUserInfo>, Vec<PartialObject>)> = None;
        if let Some(drop_table_connector_ctx) = drop_table_connector_ctx {
            notification_objs =
                Some(Self::drop_table_associated_source(txn, drop_table_connector_ctx).await?);
        }

        Ok((
            objects,
            notification_objs,
            old_fragment_ids,
            new_fragment_ids,
        ))
    }

    /// Abort the replacing streaming job by deleting the temporary job object.
    pub async fn try_abort_replacing_streaming_job(
        &self,
        tmp_job_id: JobId,
        tmp_sink_ids: Option<Vec<ObjectId>>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let mut root_ids: Vec<ObjectId> = vec![tmp_job_id.into()];
        if let Some(sink_ids) = &tmp_sink_ids {
            root_ids.extend(sink_ids.iter().copied());
        }
        let mut temporary_objects = Object::find()
            .filter(object::Column::Oid.is_in(root_ids.iter().copied()))
            .all(&txn)
            .await?;
        temporary_objects.extend(get_belong_objects_by_ids(&txn, root_ids.iter().copied()).await?);

        // Query fragment IDs before cascade-deleting the temporary jobs and objects that belong to
        // them.
        let abort_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(
                fragment::Column::JobId.is_in(
                    temporary_objects
                        .iter()
                        .map(|object| object.oid.as_job_id()),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        Object::delete_many()
            .filter(object::Column::Oid.is_in(root_ids))
            .exec(&txn)
            .await?;
        txn.commit().await?;

        // Notify serving module about deleted fragments from the aborted replace job.
        self.env
            .notification_manager()
            .notify_serving_fragment_mapping_delete(
                abort_fragment_ids.iter().map(|id| *id as _).collect(),
            );

        Ok(())
    }

    // edit the `rate_limit` of the `Source` node in given `source_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_source_rate_limit_by_source_id(
        &self,
        source_id: SourceId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        {
            let active_source = source::ActiveModel {
                source_id: Set(source_id),
                rate_limit: Set(rate_limit.map(|v| v as i32)),
                ..Default::default()
            };
            Source::update(active_source).exec(&txn).await?;
        }

        let (source, obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;

        let is_fs_source = source.with_properties.inner_ref().is_new_fs_connector();
        let streaming_job_ids: Vec<JobId> =
            if let Some(table_id) = source.optional_associated_table_id {
                vec![table_id.as_job_id()]
            } else if let Some(source_info) = &source.source_info
                && source_info.to_protobuf().is_shared()
            {
                vec![source_id.as_share_source_job_id()]
            } else {
                ObjectDependency::find()
                    .select_only()
                    .column(object_dependency::Column::UsedBy)
                    .filter(object_dependency::Column::Oid.eq(source_id))
                    .into_tuple()
                    .all(&txn)
                    .await?
            };

        if streaming_job_ids.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "source id {source_id} not used by any streaming job"
            )));
        }

        let fragments: Vec<(FragmentId, JobId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.is_in(streaming_job_ids))
            .into_tuple()
            .all(&txn)
            .await?;
        let mut fragments = fragments
            .into_iter()
            .map(|(id, job_id, mask, stream_node)| {
                (
                    id,
                    job_id,
                    FragmentTypeMask::from(mask as u32),
                    stream_node.to_protobuf(),
                )
            })
            .collect_vec();

        fragments.retain_mut(|(_, _, fragment_type_mask, stream_node)| {
            let mut found = false;
            if fragment_type_mask.contains(FragmentTypeFlag::Source) {
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::Source(node) = node
                        && let Some(node_inner) = &mut node.source_inner
                        && node_inner.source_id == source_id
                    {
                        node_inner.rate_limit = rate_limit;
                        found = true;
                    }
                });
            }
            if is_fs_source {
                // in older versions, there's no fragment type flag for `FsFetch` node,
                // so we just scan all fragments for StreamFsFetch node if using fs connector
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::StreamFsFetch(node) = node {
                        fragment_type_mask.add(FragmentTypeFlag::FsFetch);
                        if let Some(node_inner) = &mut node.node_inner
                            && node_inner.source_id == source_id
                        {
                            node_inner.rate_limit = rate_limit;
                            found = true;
                        }
                    }
                });
            }
            found
        });

        assert!(
            !fragments.is_empty(),
            "source id should be used by at least one fragment"
        );

        let fragment_nodes = fragments
            .iter()
            .map(|(fragment_id, _, _, stream_node)| (*fragment_id, stream_node.clone()))
            .collect();

        for (fragment_id, _, fragment_type_mask, stream_node) in fragments {
            Fragment::update(fragment::ActiveModel {
                fragment_id: Set(fragment_id),
                fragment_type_mask: Set(fragment_type_mask.into()),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        let relation_info = PbObjectInfo::Source(ObjectModel(source, obj.unwrap(), None).into());
        let _version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(relation_info),
                    }],
                    dependencies: vec![],
                }),
            )
            .await;

        Ok(fragment_nodes)
    }

    // edit the content of fragments in given `table_id`
    // return the updated stream nodes to be applied
    pub async fn mutate_fragments_by_job_id(
        &self,
        job_id: JobId,
        // returns true if the mutation is applied
        mut fragments_mutation_fn: impl FnMut(FragmentTypeMask, &mut PbStreamNode) -> MetaResult<bool>,
        // error message when no relevant fragments is found
        err_msg: &'static str,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.eq(job_id))
            .into_tuple()
            .all(&txn)
            .await?;
        let mut fragments = fragments
            .into_iter()
            .map(|(id, mask, stream_node)| {
                (id, FragmentTypeMask::from(mask), stream_node.to_protobuf())
            })
            .collect_vec();

        let fragments = fragments
            .iter_mut()
            .map(|(_, fragment_type_mask, stream_node)| {
                fragments_mutation_fn(*fragment_type_mask, stream_node)
            })
            .collect::<MetaResult<Vec<bool>>>()?
            .into_iter()
            .zip_eq_debug(std::mem::take(&mut fragments))
            .filter_map(|(keep, fragment)| if keep { Some(fragment) } else { None })
            .collect::<Vec<_>>();

        if fragments.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "job id {job_id}: {}",
                err_msg
            )));
        }

        let fragment_nodes = fragments
            .iter()
            .map(|(id, _, stream_node)| (*id, stream_node.clone()))
            .collect();
        for (id, _, stream_node) in fragments {
            Fragment::update(fragment::ActiveModel {
                fragment_id: Set(id),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        Ok(fragment_nodes)
    }

    async fn mutate_fragment_by_fragment_id(
        &self,
        fragment_id: FragmentId,
        mut fragment_mutation_fn: impl FnMut(FragmentTypeMask, &mut PbStreamNode) -> MetaResult<bool>,
        err_msg: &'static str,
    ) -> MetaResult<PbStreamNode> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (fragment_type_mask, stream_node): (i32, StreamNode) =
            Fragment::find_by_id(fragment_id)
                .select_only()
                .columns([
                    fragment::Column::FragmentTypeMask,
                    fragment::Column::StreamNode,
                ])
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
        let mut pb_stream_node = stream_node.to_protobuf();
        let fragment_type_mask = FragmentTypeMask::from(fragment_type_mask);

        if !fragment_mutation_fn(fragment_type_mask, &mut pb_stream_node)? {
            return Err(MetaError::invalid_parameter(format!(
                "fragment id {fragment_id}: {}",
                err_msg
            )));
        }

        Fragment::update(fragment::ActiveModel {
            fragment_id: Set(fragment_id),
            stream_node: Set(StreamNode::from(&pb_stream_node)),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(pb_stream_node)
    }

    pub async fn update_backfill_orders_by_job_id(
        &self,
        job_id: JobId,
        backfill_orders: Option<BackfillOrders>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        ensure_job_not_canceled(job_id, &txn).await?;

        streaming_job::ActiveModel {
            job_id: Set(job_id),
            backfill_orders: Set(backfill_orders),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }

    // edit the `rate_limit` of the `Chain` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_backfill_rate_limit_by_job_id(
        &self,
        job_id: JobId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        let update_backfill_rate_limit =
            |fragment_type_mask: FragmentTypeMask, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if fragment_type_mask
                    .contains_any(FragmentTypeFlag::backfill_rate_limit_fragments())
                {
                    visit_stream_node_mut(stream_node, |node| match node {
                        PbNodeBody::StreamCdcScan(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::StreamScan(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::SourceBackfill(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        _ => {}
                    });
                }
                Ok(found)
            };

        self.mutate_fragments_by_job_id(
            job_id,
            update_backfill_rate_limit,
            "stream scan node or source node not found",
        )
        .await
    }

    // edit the `rate_limit` of the `Sink` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_sink_rate_limit_by_job_id(
        &self,
        sink_id: SinkId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        let update_sink_rate_limit =
            |fragment_type_mask: FragmentTypeMask, stream_node: &mut PbStreamNode| {
                let mut found = Ok(false);
                if fragment_type_mask.contains_any(FragmentTypeFlag::sink_rate_limit_fragments()) {
                    visit_stream_node_mut(stream_node, |node| {
                        if found.is_err() {
                            return;
                        }
                        match update_sink_node_rate_limit(node, rate_limit) {
                            Ok(true) => found = Ok(true),
                            Ok(false) => {}
                            Err(err) => found = Err(err),
                        }
                    });
                }
                found
            };

        self.mutate_fragments_by_job_id(
            sink_id.as_job_id(),
            update_sink_rate_limit,
            "sink node not found",
        )
        .await
    }

    pub async fn update_dml_rate_limit_by_job_id(
        &self,
        job_id: JobId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        let update_dml_rate_limit =
            |fragment_type_mask: FragmentTypeMask, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if fragment_type_mask.contains_any(FragmentTypeFlag::dml_rate_limit_fragments()) {
                    visit_stream_node_mut(stream_node, |node| {
                        if let PbNodeBody::Dml(node) = node {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                    });
                }
                Ok(found)
            };

        self.mutate_fragments_by_job_id(job_id, update_dml_rate_limit, "dml node not found")
            .await
    }

    pub async fn update_source_props_by_source_id(
        &self,
        source_id: SourceId,
        alter_props: BTreeMap<String, String>,
        alter_secret_refs: BTreeMap<String, PbSecretRef>,
        skip_alter_on_fly_check: bool,
    ) -> MetaResult<WithOptionsSecResolved> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (source, _obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;
        let connector = source.with_properties.0.get_connector().unwrap();
        let is_shared_source = source.is_shared();

        let mut dep_source_job_ids: Vec<JobId> = Vec::new();
        if !is_shared_source {
            // mv using non-shared source holds a copy of source in their fragments
            dep_source_job_ids = ObjectDependency::find()
                .select_only()
                .column(object_dependency::Column::UsedBy)
                .filter(object_dependency::Column::Oid.eq(source_id))
                .into_tuple()
                .all(&txn)
                .await?;
        }

        // Validate that connector type is not being changed
        if let Some(new_connector) = alter_props.get(UPSTREAM_SOURCE_KEY)
            && new_connector != &connector
        {
            return Err(MetaError::invalid_parameter(format!(
                "Cannot change connector type from '{}' to '{}'. Drop and recreate the source instead.",
                connector, new_connector
            )));
        }

        // Only check alter-on-fly restrictions for SQL ALTER SOURCE, not for admin risectl operations
        if !skip_alter_on_fly_check {
            let prop_keys: Vec<String> =
                build_altered_field_names(&alter_props, &alter_secret_refs);
            risingwave_connector::allow_alter_on_fly_fields::check_source_allow_alter_on_fly_fields(
                &connector, &prop_keys,
            )?;
        }

        let mut options_with_secret = WithOptionsSecResolved::new(
            source.with_properties.0.clone(),
            source
                .secret_ref
                .map(|secret_ref| secret_ref.to_protobuf())
                .unwrap_or_default(),
        );
        let (to_add_secret_dep, to_remove_secret_dep) =
            options_with_secret.handle_update(alter_props, alter_secret_refs)?;

        tracing::info!(
            "applying new properties to source: source_id={}, options_with_secret={:?}",
            source_id,
            options_with_secret
        );
        // check if the alter-ed props are valid for each Connector
        let _ = ConnectorProperties::extract(options_with_secret.clone(), true)?;
        // todo: validate via source manager

        let mut associate_table_id = None;

        // can be source_id or table_id
        // if updating an associated source, the preferred_id is the table_id
        // otherwise, it is the source_id
        let mut preferred_id = source_id.as_object_id();
        let rewrite_sql = {
            let definition = source.definition.clone();

            let [mut stmt]: [_; 1] = Parser::parse_sql(&definition)
                .map_err(|e| {
                    MetaError::from(MetaErrorInner::Connector(ConnectorError::from(
                        anyhow!(e).context("Failed to parse source definition SQL"),
                    )))
                })?
                .try_into()
                .unwrap();

            match &mut stmt {
                Statement::CreateSource { stmt } => {
                    stmt.with_properties.0 =
                        format_with_option_secret_resolved(&txn, &options_with_secret).await?;
                }
                Statement::CreateTable { with_options, .. } => {
                    *with_options =
                        format_with_option_secret_resolved(&txn, &options_with_secret).await?;
                    associate_table_id = source.optional_associated_table_id;
                    preferred_id = associate_table_id.unwrap().as_object_id();
                }
                _ => unreachable!(),
            }

            stmt.to_string()
        };

        update_secret_dependencies(&txn, to_add_secret_dep, to_remove_secret_dep, preferred_id)
            .await?;

        let active_source_model = source::ActiveModel {
            source_id: Set(source_id),
            definition: Set(rewrite_sql.clone()),
            with_properties: Set(options_with_secret.as_plaintext().clone().into()),
            secret_ref: Set((!options_with_secret.as_secret().is_empty())
                .then(|| SecretRef::from(options_with_secret.as_secret().clone()))),
            ..Default::default()
        };
        Source::update(active_source_model).exec(&txn).await?;

        if let Some(associate_table_id) = associate_table_id {
            // update the associated table statement accordly
            let active_table_model = table::ActiveModel {
                table_id: Set(associate_table_id),
                definition: Set(rewrite_sql),
                ..Default::default()
            };
            Table::update(active_table_model).exec(&txn).await?;
        }

        let to_check_job_ids = vec![if let Some(associate_table_id) = associate_table_id {
            // if updating table with connector, the fragment_id is table id
            associate_table_id.as_job_id()
        } else {
            source_id.as_share_source_job_id()
        }]
        .into_iter()
        .chain(dep_source_job_ids)
        .collect_vec();

        // update fragments
        update_connector_props_fragments(
            &txn,
            to_check_job_ids,
            FragmentTypeFlag::Source,
            |node, found| {
                if let PbNodeBody::Source(node) = node
                    && let Some(source_inner) = &mut node.source_inner
                {
                    source_inner.with_properties = options_with_secret.as_plaintext().clone();
                    source_inner.secret_refs = options_with_secret.as_secret().clone();
                    *found = true;
                }
            },
            is_shared_source,
        )
        .await?;

        let mut to_update_objs = Vec::with_capacity(2);
        let (source, obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;
        to_update_objs.push(PbObject {
            object_info: Some(PbObjectInfo::Source(
                ObjectModel(source, obj.unwrap(), None).into(),
            )),
        });

        if let Some(associate_table_id) = associate_table_id {
            let (table, obj) = Table::find_by_id(associate_table_id)
                .find_also_related(Object)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", associate_table_id))?;
            let streaming_job = streaming_job::Entity::find_by_id(table.job_id())
                .one(&txn)
                .await?;
            to_update_objs.push(PbObject {
                object_info: Some(PbObjectInfo::Table(
                    ObjectModel(table, obj.unwrap(), streaming_job).into(),
                )),
            });
        }

        txn.commit().await?;

        self.notify_frontend(
            NotificationOperation::Update,
            NotificationInfo::ObjectGroup(PbObjectGroup {
                objects: to_update_objs,
                dependencies: vec![],
            }),
        )
        .await;

        Ok(options_with_secret)
    }

    pub async fn update_sink_props_by_sink_id(
        &self,
        sink_id: SinkId,
        alter_props: BTreeMap<String, String>,
        alter_secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> MetaResult<WithOptionsSecResolved> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (sink, _obj) = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), sink_id))?;

        let connector_type = sink
            .properties
            .inner_ref()
            .get(CONNECTOR_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(anyhow!("connector not specified when alter sink")))?
            .to_lowercase();

        let prop_keys = build_altered_field_names(&alter_props, &alter_secret_refs);

        check_sink_allow_alter_on_fly_fields(&connector_type, &prop_keys)
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        let mut options_with_secret = WithOptionsSecResolved::new(
            sink.properties.0.clone(),
            sink.secret_ref
                .map(|secret_ref| secret_ref.to_protobuf())
                .unwrap_or_default(),
        );

        let (to_add_secret_dep, to_remove_secret_dep) =
            options_with_secret.handle_update(alter_props.clone(), alter_secret_refs)?;

        // check if the alter-ed props are valid for all connector
        validate_sink_props(&connector_type, &options_with_secret, &prop_keys)?;

        tracing::info!(
            "applying new properties to sink: sink_id={}, options_with_secret={:?}",
            sink_id,
            options_with_secret
        );

        // rewrite the sql for definition field
        let rewrite_sql = {
            let definition = sink.definition.clone();
            let [mut stmt]: [_; 1] = Parser::parse_sql(&definition)
                .map_err(|e| {
                    MetaError::from(MetaErrorInner::Connector(ConnectorError::from(
                        anyhow!(e).context("Failed to parse sink definition SQL"),
                    )))
                })?
                .try_into()
                .unwrap();
            match &mut stmt {
                Statement::CreateSink { stmt } => {
                    stmt.with_properties.0 =
                        format_with_option_secret_resolved(&txn, &options_with_secret).await?;
                }
                _ => {
                    return Err(MetaError::from(MetaErrorInner::Connector(
                        ConnectorError::from(anyhow!(
                            "Sink definition is not a CREATE SINK statement"
                        )),
                    )));
                }
            }

            stmt.to_string()
        };

        update_secret_dependencies(
            &txn,
            to_add_secret_dep,
            to_remove_secret_dep,
            sink_id.as_object_id(),
        )
        .await?;

        let active_sink = sink::ActiveModel {
            sink_id: Set(sink_id),
            properties: Set(risingwave_meta_model::Property(
                options_with_secret.as_plaintext().clone(),
            )),
            secret_ref: Set((!options_with_secret.as_secret().is_empty())
                .then(|| SecretRef::from(options_with_secret.as_secret().clone()))),
            definition: Set(rewrite_sql),
            ..Default::default()
        };
        Sink::update(active_sink).exec(&txn).await?;

        update_sink_fragment_props(&txn, sink_id, &options_with_secret).await?;
        let (sink, obj) = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), sink_id))?;
        let streaming_job = streaming_job::Entity::find_by_id(sink.sink_id.as_job_id())
            .one(&txn)
            .await?;
        txn.commit().await?;
        let relation_infos = vec![PbObject {
            object_info: Some(PbObjectInfo::Sink(
                ObjectModel(sink, obj.unwrap(), streaming_job).into(),
            )),
        }];

        let _version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: relation_infos,
                    dependencies: vec![],
                }),
            )
            .await;

        Ok(options_with_secret)
    }

    pub async fn update_iceberg_table_props_by_table_id(
        &self,
        table_id: TableId,
        alter_props: BTreeMap<String, String>,
        alter_secret_refs: BTreeMap<String, PbSecretRef>,
        alter_iceberg_table_props: Option<PbExtraOptions>,
    ) -> MetaResult<(WithOptionsSecResolved, SinkId)> {
        let PbExtraOptions::AlterIcebergTableIds(AlterIcebergTableIds { sink_id, source_id }) =
            alter_iceberg_table_props.ok_or_else(|| {
                MetaError::invalid_parameter("alter_iceberg_table_props is required")
            })?;
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (sink, _obj) = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), sink_id))?;

        let connector_type = sink
            .properties
            .inner_ref()
            .get(CONNECTOR_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(anyhow!("connector not specified when alter sink")))?
            .to_lowercase();

        let prop_keys = build_altered_field_names(&alter_props, &alter_secret_refs);

        check_sink_allow_alter_on_fly_fields(&connector_type, &prop_keys)
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        let mut options_with_secret = WithOptionsSecResolved::new(
            sink.properties.0.clone(),
            sink.secret_ref
                .map(|secret_ref| secret_ref.to_protobuf())
                .unwrap_or_default(),
        );

        let (to_add_secret_dep, to_remove_secret_dep) =
            options_with_secret.handle_update(alter_props.clone(), alter_secret_refs)?;

        // check if the alter-ed props are valid for all connector
        validate_sink_props(&connector_type, &options_with_secret, &prop_keys)?;

        // rewrite the sql for definition field
        let rewrite_sql = {
            let definition = sink.definition.clone();
            let [mut stmt]: [_; 1] = Parser::parse_sql(&definition)
                .map_err(|e| {
                    MetaError::from(MetaErrorInner::Connector(ConnectorError::from(
                        anyhow!(e).context("Failed to parse iceberg sink definition SQL"),
                    )))
                })?
                .try_into()
                .unwrap();

            match &mut stmt {
                Statement::CreateTable {
                    with_options,
                    engine,
                    ..
                } => {
                    if !matches!(engine, Engine::Iceberg) {
                        return Err(SinkError::Config(anyhow!(
                            "only iceberg table can be altered as sink"
                        ))
                        .into());
                    }
                    *with_options =
                        format_with_option_secret_resolved(&txn, &options_with_secret).await?;
                }
                _ => {
                    return Err(MetaError::from(MetaErrorInner::Connector(
                        ConnectorError::from(anyhow!(
                            "definition is not a create iceberg table statement"
                        )),
                    )));
                }
            }

            stmt.to_string()
        };

        update_secret_dependencies(
            &txn,
            to_add_secret_dep.clone(),
            to_remove_secret_dep.clone(),
            source_id.as_object_id(),
        )
        .await?;

        update_secret_dependencies(
            &txn,
            to_add_secret_dep,
            to_remove_secret_dep,
            sink_id.as_object_id(),
        )
        .await?;

        let active_sink = sink::ActiveModel {
            sink_id: Set(sink_id),
            properties: Set(risingwave_meta_model::Property(
                options_with_secret.as_plaintext().clone(),
            )),
            secret_ref: Set((!options_with_secret.as_secret().is_empty())
                .then(|| SecretRef::from(options_with_secret.as_secret().clone()))),
            definition: Set(rewrite_sql.clone()),
            ..Default::default()
        };
        let active_source = source::ActiveModel {
            source_id: Set(source_id),
            with_properties: Set(risingwave_meta_model::Property(
                options_with_secret.as_plaintext().clone(),
            )),
            secret_ref: Set((!options_with_secret.as_secret().is_empty())
                .then(|| SecretRef::from(options_with_secret.as_secret().clone()))),
            definition: Set(rewrite_sql.clone()),
            ..Default::default()
        };
        let active_table = table::ActiveModel {
            table_id: Set(table_id),
            definition: Set(rewrite_sql),
            ..Default::default()
        };
        Sink::update(active_sink).exec(&txn).await?;
        Source::update(active_source).exec(&txn).await?;
        Table::update(active_table).exec(&txn).await?;

        update_sink_fragment_props(&txn, sink_id, &options_with_secret).await?;

        let (sink, sink_obj) = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), sink_id))?;
        let sink_streaming_job = streaming_job::Entity::find_by_id(sink.sink_id.as_job_id())
            .one(&txn)
            .await?;
        let (source, source_obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;
        let (table, table_obj) = Table::find_by_id(table_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Table.as_str(), table_id))?;
        let table_streaming_job = streaming_job::Entity::find_by_id(table.job_id())
            .one(&txn)
            .await?;
        let mut dependencies =
            list_object_dependencies_by_object_id(&txn, source_id.as_object_id()).await?;
        dependencies
            .extend(list_object_dependencies_by_object_id(&txn, sink_id.as_object_id()).await?);
        txn.commit().await?;
        let relation_infos = vec![
            PbObject {
                object_info: Some(PbObjectInfo::Sink(
                    ObjectModel(sink, sink_obj.unwrap(), sink_streaming_job).into(),
                )),
            },
            PbObject {
                object_info: Some(PbObjectInfo::Source(
                    ObjectModel(source, source_obj.unwrap(), None).into(),
                )),
            },
            PbObject {
                object_info: Some(PbObjectInfo::Table(
                    ObjectModel(table, table_obj.unwrap(), table_streaming_job).into(),
                )),
            },
        ];
        let _version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: relation_infos,
                    dependencies,
                }),
            )
            .await;

        Ok((options_with_secret, sink_id))
    }

    /// Update connection properties and all dependent sources/sinks in a single transaction
    pub async fn update_connection_and_dependent_objects_props(
        &self,
        connection_id: ConnectionId,
        alter_props: BTreeMap<String, String>,
        alter_secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> MetaResult<(
        WithOptionsSecResolved,                   // Connection's new properties
        Vec<(SourceId, HashMap<String, String>)>, // Source ID and their complete properties
        Vec<(SinkId, HashMap<String, String>)>,   // Sink ID and their complete properties
    )> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        // Find all dependent sources and sinks first
        let dependent_sources: Vec<SourceId> = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .filter(source::Column::ConnectionId.eq(connection_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let dependent_sinks: Vec<SinkId> = Sink::find()
            .select_only()
            .column(sink::Column::SinkId)
            .filter(sink::Column::ConnectionId.eq(connection_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let (connection_catalog, _obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Connection.as_str(), connection_id)
            })?;

        // Validate that props can be altered
        let prop_keys: Vec<String> = alter_props
            .keys()
            .chain(alter_secret_refs.keys())
            .cloned()
            .collect();

        // Map the connection type enum to the string name expected by the validation function
        let connection_type_str = pb_connection_type_to_connection_type(
            &connection_catalog.params.to_protobuf().connection_type(),
        )
        .ok_or_else(|| MetaError::invalid_parameter("Unspecified connection type"))?;

        risingwave_connector::allow_alter_on_fly_fields::check_connection_allow_alter_on_fly_fields(
            connection_type_str, &prop_keys,
        )?;

        let connection_pb = connection_catalog.params.to_protobuf();
        let mut connection_options_with_secret = WithOptionsSecResolved::new(
            connection_pb.properties.into_iter().collect(),
            connection_pb.secret_refs.into_iter().collect(),
        );

        let (to_add_secret_dep, to_remove_secret_dep) = connection_options_with_secret
            .handle_update(alter_props.clone(), alter_secret_refs.clone())?;

        tracing::debug!(
            "applying new properties to connection and dependents: connection_id={}, sources={:?}, sinks={:?}",
            connection_id,
            dependent_sources,
            dependent_sinks
        );

        // Validate connection
        {
            let conn_params_pb = risingwave_pb::catalog::ConnectionParams {
                connection_type: connection_pb.connection_type,
                properties: connection_options_with_secret
                    .as_plaintext()
                    .clone()
                    .into_iter()
                    .collect(),
                secret_refs: connection_options_with_secret
                    .as_secret()
                    .clone()
                    .into_iter()
                    .collect(),
            };
            let connection = PbConnection {
                id: connection_id as _,
                info: Some(risingwave_pb::catalog::connection::Info::ConnectionParams(
                    conn_params_pb,
                )),
                ..Default::default()
            };
            validate_connection(&connection).await?;
        }

        // Update connection secret dependencies
        update_secret_dependencies(
            &txn,
            to_add_secret_dep,
            to_remove_secret_dep,
            connection_id.as_object_id(),
        )
        .await?;

        // Update the connection with new properties
        let updated_connection_params = risingwave_pb::catalog::ConnectionParams {
            connection_type: connection_pb.connection_type,
            properties: connection_options_with_secret
                .as_plaintext()
                .clone()
                .into_iter()
                .collect(),
            secret_refs: connection_options_with_secret
                .as_secret()
                .clone()
                .into_iter()
                .collect(),
        };
        let active_connection_model = connection::ActiveModel {
            connection_id: Set(connection_id),
            params: Set(ConnectionParams::from(&updated_connection_params)),
            ..Default::default()
        };
        Connection::update(active_connection_model)
            .exec(&txn)
            .await?;

        // Batch update dependent sources and collect their complete properties
        let mut updated_sources_with_props: Vec<(SourceId, HashMap<String, String>)> = Vec::new();

        if !dependent_sources.is_empty() {
            // Batch fetch all dependent sources
            let sources_with_objs = Source::find()
                .find_also_related(Object)
                .filter(source::Column::SourceId.is_in(dependent_sources.iter().cloned()))
                .all(&txn)
                .await?;

            // Prepare batch updates
            let mut source_updates = Vec::new();
            let mut fragment_updates: Vec<DependentSourceFragmentUpdate> = Vec::new();

            for (source, _obj) in sources_with_objs {
                let source_id = source.source_id;

                let mut source_options_with_secret = WithOptionsSecResolved::new(
                    source.with_properties.0.clone(),
                    source
                        .secret_ref
                        .clone()
                        .map(|secret_ref| secret_ref.to_protobuf())
                        .unwrap_or_default(),
                );
                let (source_to_add_secret_dep, source_to_remove_secret_dep) =
                    source_options_with_secret
                        .handle_update(alter_props.clone(), alter_secret_refs.clone())?;

                // Validate the updated source properties
                let _ = ConnectorProperties::extract(source_options_with_secret.clone(), true)?;

                // Keep source-level secret dependencies in sync with the source properties that
                // are rewritten from the altered connection.
                let source_used_by_id = source
                    .optional_associated_table_id
                    .map(|table_id| table_id.as_object_id())
                    .unwrap_or_else(|| source_id.as_object_id());

                update_secret_dependencies(
                    &txn,
                    source_to_add_secret_dep,
                    source_to_remove_secret_dep,
                    source_used_by_id,
                )
                .await?;

                // Prepare source update
                let active_source = source::ActiveModel {
                    source_id: Set(source_id),
                    with_properties: Set(Property(
                        source_options_with_secret.as_plaintext().clone(),
                    )),
                    secret_ref: Set((!source_options_with_secret.as_secret().is_empty()).then(
                        || {
                            risingwave_meta_model::SecretRef::from(
                                source_options_with_secret.as_secret().clone(),
                            )
                        },
                    )),
                    ..Default::default()
                };
                source_updates.push(active_source);

                // Prepare fragment update:
                // - If the source is a table-associated source, update fragments for the table job.
                // - Otherwise update the shared source job.
                // - For non-shared sources, also update any dependent streaming jobs that embed a copy.
                let is_shared_source = source.is_shared();
                let mut dep_source_job_ids: Vec<JobId> = Vec::new();
                if !is_shared_source {
                    dep_source_job_ids = ObjectDependency::find()
                        .select_only()
                        .column(object_dependency::Column::UsedBy)
                        .filter(object_dependency::Column::Oid.eq(source_id))
                        .into_tuple()
                        .all(&txn)
                        .await?;
                }

                let base_job_id =
                    if let Some(associate_table_id) = source.optional_associated_table_id {
                        associate_table_id.as_job_id()
                    } else {
                        source_id.as_share_source_job_id()
                    };
                let job_ids = vec![base_job_id]
                    .into_iter()
                    .chain(dep_source_job_ids)
                    .collect_vec();

                fragment_updates.push(DependentSourceFragmentUpdate {
                    job_ids,
                    with_properties: source_options_with_secret.as_plaintext().clone(),
                    secret_refs: source_options_with_secret.as_secret().clone(),
                    is_shared_source,
                });

                // Collect the complete properties for runtime broadcast
                let complete_source_props = LocalSecretManager::global()
                    .fill_secrets(
                        source_options_with_secret.as_plaintext().clone(),
                        source_options_with_secret.as_secret().clone(),
                    )
                    .map_err(MetaError::from)?
                    .into_iter()
                    .collect::<HashMap<String, String>>();
                updated_sources_with_props.push((source_id, complete_source_props));
            }

            for source_update in source_updates {
                Source::update(source_update).exec(&txn).await?;
            }

            // Batch execute fragment updates
            for DependentSourceFragmentUpdate {
                job_ids,
                with_properties,
                secret_refs,
                is_shared_source,
            } in fragment_updates
            {
                update_connector_props_fragments(
                    &txn,
                    job_ids,
                    FragmentTypeFlag::Source,
                    |node, found| {
                        if let PbNodeBody::Source(node) = node
                            && let Some(source_inner) = &mut node.source_inner
                        {
                            source_inner.with_properties = with_properties.clone();
                            source_inner.secret_refs = secret_refs.clone();
                            *found = true;
                        }
                    },
                    is_shared_source,
                )
                .await?;
            }
        }

        // Batch update dependent sinks and collect their complete properties
        let mut updated_sinks_with_props: Vec<(SinkId, HashMap<String, String>)> = Vec::new();

        if !dependent_sinks.is_empty() {
            // Batch fetch all dependent sinks
            let sinks_with_objs = Sink::find()
                .find_also_related(Object)
                .filter(sink::Column::SinkId.is_in(dependent_sinks.iter().cloned()))
                .all(&txn)
                .await?;

            // Prepare batch updates
            let mut sink_updates = Vec::new();
            let mut sink_fragment_updates = Vec::new();

            for (sink, _obj) in sinks_with_objs {
                let sink_id = sink.sink_id;

                let connector_type = sink
                    .properties
                    .inner_ref()
                    .get(CONNECTOR_TYPE_KEY)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!("connector not specified when alter sink"))
                    })?
                    .to_lowercase();

                let mut sink_options_with_secret = WithOptionsSecResolved::new(
                    sink.properties.0.clone(),
                    sink.secret_ref
                        .clone()
                        .map(|secret_ref| secret_ref.to_protobuf())
                        .unwrap_or_default(),
                );

                let (sink_to_add_secret_dep, sink_to_remove_secret_dep) = sink_options_with_secret
                    .handle_update(alter_props.clone(), alter_secret_refs.clone())?;

                // check if the alter-ed props are valid for all connector
                validate_sink_props(&connector_type, &sink_options_with_secret, &prop_keys)?;

                update_secret_dependencies(
                    &txn,
                    sink_to_add_secret_dep,
                    sink_to_remove_secret_dep,
                    sink_id.as_object_id(),
                )
                .await?;

                // Prepare sink update
                let active_sink = sink::ActiveModel {
                    sink_id: Set(sink_id),
                    properties: Set(risingwave_meta_model::Property(
                        sink_options_with_secret.as_plaintext().clone(),
                    )),
                    secret_ref: Set((!sink_options_with_secret.as_secret().is_empty())
                        .then(|| SecretRef::from(sink_options_with_secret.as_secret().clone()))),
                    ..Default::default()
                };
                sink_updates.push(active_sink);

                // Prepare fragment updates for this sink
                sink_fragment_updates.push(DependentSinkFragmentUpdate {
                    job_id: sink_id.as_job_id(),
                    with_properties: sink_options_with_secret.as_plaintext().clone(),
                    secret_refs: sink_options_with_secret.as_secret().clone(),
                });

                // Collect the complete properties for runtime broadcast
                let complete_sink_props = LocalSecretManager::global()
                    .fill_secrets(
                        sink_options_with_secret.as_plaintext().clone(),
                        sink_options_with_secret.as_secret().clone(),
                    )
                    .map_err(MetaError::from)?
                    .into_iter()
                    .collect::<HashMap<String, String>>();

                updated_sinks_with_props.push((sink_id, complete_sink_props));
            }

            // Batch execute sink updates
            for sink_update in sink_updates {
                Sink::update(sink_update).exec(&txn).await?;
            }

            // Batch execute sink fragment updates using the reusable function
            for DependentSinkFragmentUpdate {
                job_id,
                with_properties,
                secret_refs,
            } in sink_fragment_updates
            {
                update_connector_props_fragments(
                    &txn,
                    vec![job_id],
                    FragmentTypeFlag::Sink,
                    |node, found| {
                        if let PbNodeBody::Sink(node) = node
                            && let Some(sink_desc) = &mut node.sink_desc
                            && sink_desc.id == job_id.as_raw_id()
                        {
                            sink_desc.properties = with_properties.clone();
                            sink_desc.secret_refs = secret_refs.clone();
                            *found = true;
                        }
                    },
                    true,
                )
                .await?;
            }
        }

        // Collect all updated objects for frontend notification
        let mut updated_objects = Vec::new();

        // Add connection
        let (connection, obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Connection.as_str(), connection_id)
            })?;
        updated_objects.push(PbObject {
            object_info: Some(PbObjectInfo::Connection(
                ObjectModel(connection, obj.unwrap(), None).into(),
            )),
        });

        // Add sources
        for source_id in &dependent_sources {
            let (source, obj) = Source::find_by_id(*source_id)
                .find_also_related(Object)
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(ObjectType::Source.as_str(), *source_id)
                })?;
            updated_objects.push(PbObject {
                object_info: Some(PbObjectInfo::Source(
                    ObjectModel(source, obj.unwrap(), None).into(),
                )),
            });
        }

        // Add sinks
        for sink_id in &dependent_sinks {
            let (sink, obj) = Sink::find_by_id(*sink_id)
                .find_also_related(Object)
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), *sink_id)
                })?;
            let streaming_job = streaming_job::Entity::find_by_id(sink.sink_id.as_job_id())
                .one(&txn)
                .await?;
            updated_objects.push(PbObject {
                object_info: Some(PbObjectInfo::Sink(
                    ObjectModel(sink, obj.unwrap(), streaming_job).into(),
                )),
            });
        }

        // Commit the transaction
        txn.commit().await?;

        // Notify frontend about all updated objects
        if !updated_objects.is_empty() {
            self.notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: updated_objects,
                    dependencies: vec![],
                }),
            )
            .await;
        }

        Ok((
            connection_options_with_secret,
            updated_sources_with_props,
            updated_sinks_with_props,
        ))
    }

    pub async fn update_fragment_rate_limit_by_fragment_id(
        &self,
        fragment_id: FragmentId,
        throttle_type: ThrottleType,
        rate_limit: Option<u32>,
    ) -> MetaResult<PbStreamNode> {
        let update_rate_limit = |fragment_type_mask: FragmentTypeMask,
                                 stream_node: &mut PbStreamNode| {
            let mut found = Ok(false);
            match throttle_type {
                ThrottleType::Source => {
                    visit_stream_node_mut(stream_node, |node| match node {
                        PbNodeBody::Source(node) => {
                            if let Some(node_inner) = &mut node.source_inner {
                                node_inner.rate_limit = rate_limit;
                                found = Ok(true);
                            }
                        }
                        PbNodeBody::StreamFsFetch(node) => {
                            if let Some(node_inner) = &mut node.node_inner {
                                node_inner.rate_limit = rate_limit;
                                found = Ok(true);
                            }
                        }
                        _ => {}
                    });
                }
                ThrottleType::Backfill => {
                    if fragment_type_mask
                        .contains_any(FragmentTypeFlag::backfill_rate_limit_fragments())
                    {
                        visit_stream_node_mut(stream_node, |node| match node {
                            PbNodeBody::StreamCdcScan(node) => {
                                node.rate_limit = rate_limit;
                                found = Ok(true);
                            }
                            PbNodeBody::StreamScan(node) => {
                                node.rate_limit = rate_limit;
                                found = Ok(true);
                            }
                            PbNodeBody::SourceBackfill(node) => {
                                node.rate_limit = rate_limit;
                                found = Ok(true);
                            }
                            _ => {}
                        });
                    }
                }
                ThrottleType::Sink => {
                    if fragment_type_mask
                        .contains_any(FragmentTypeFlag::sink_rate_limit_fragments())
                    {
                        visit_stream_node_mut(stream_node, |node| {
                            if found.is_err() {
                                return;
                            }
                            match update_sink_node_rate_limit(node, rate_limit) {
                                Ok(true) => found = Ok(true),
                                Ok(false) => {}
                                Err(err) => found = Err(err),
                            }
                        });
                    }
                }
                ThrottleType::Dml => {
                    if fragment_type_mask.contains_any(FragmentTypeFlag::dml_rate_limit_fragments())
                    {
                        visit_stream_node_mut(stream_node, |node| {
                            if let PbNodeBody::Dml(node) = node {
                                node.rate_limit = rate_limit;
                                found = Ok(true);
                            }
                        });
                    }
                }
                ThrottleType::Unspecified => {}
            }
            found
        };
        self.mutate_fragment_by_fragment_id(
            fragment_id,
            update_rate_limit,
            "rate limit node not found",
        )
        .await
    }

    /// Note: `FsFetch` created in old versions are not included.
    /// Since this is only used for debugging, it should be fine.
    pub async fn list_rate_limits(&self) -> MetaResult<Vec<RateLimitInfo>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(FragmentId, JobId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(FragmentTypeMask::intersects_any(
                FragmentTypeFlag::rate_limit_fragments(),
            ))
            .into_tuple()
            .all(&txn)
            .await?;

        let mut rate_limits = Vec::new();
        for (fragment_id, job_id, fragment_type_mask, stream_node) in fragments {
            let stream_node = stream_node.to_protobuf();
            visit_stream_node_body(&stream_node, |node| {
                let mut rate_limit = None;
                let mut node_name = None;

                match node {
                    // source rate limit
                    PbNodeBody::Source(node) => {
                        if let Some(node_inner) = &node.source_inner {
                            rate_limit = node_inner.rate_limit;
                            node_name = Some("SOURCE");
                        }
                    }
                    PbNodeBody::StreamFsFetch(node) => {
                        if let Some(node_inner) = &node.node_inner {
                            rate_limit = node_inner.rate_limit;
                            node_name = Some("FS_FETCH");
                        }
                    }
                    // backfill rate limit
                    PbNodeBody::SourceBackfill(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("SOURCE_BACKFILL");
                    }
                    PbNodeBody::StreamScan(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("STREAM_SCAN");
                    }
                    PbNodeBody::StreamCdcScan(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("STREAM_CDC_SCAN");
                    }
                    PbNodeBody::Sink(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("SINK");
                    }
                    PbNodeBody::Dml(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("DML");
                    }
                    _ => {}
                }

                if let Some(rate_limit) = rate_limit {
                    rate_limits.push(RateLimitInfo {
                        fragment_id,
                        job_id,
                        fragment_type_mask: fragment_type_mask as u32,
                        rate_limit,
                        node_name: node_name.unwrap().to_owned(),
                    });
                }
            });
        }

        Ok(rate_limits)
    }
}

fn validate_sink_props(
    connector_type: &str,
    options_with_secret: &WithOptionsSecResolved,
    altered_field_names: &[String],
) -> MetaResult<()> {
    check_sink_allow_alter_on_fly_fields(connector_type, altered_field_names)
        .map_err(|e| SinkError::Config(anyhow!(e)))?;

    let resolved_props = LocalSecretManager::global()
        .fill_secrets(
            options_with_secret.as_plaintext().clone(),
            options_with_secret.as_secret().clone(),
        )
        .map_err(MetaError::from)?;
    let resolved_alter_props = altered_field_names
        .iter()
        .filter_map(|field_name| {
            resolved_props
                .get(field_name)
                .map(|value| (field_name.clone(), value.clone()))
        })
        .collect();

    match_sink_name_str!(
        connector_type,
        SinkType,
        SinkType::validate_alter_config_change(&resolved_props, &resolved_alter_props),
        |sink: &str| Err(SinkError::Config(anyhow!("unsupported sink type {}", sink)))
    )?;

    Ok(())
}

fn build_altered_field_names(
    alter_props: &BTreeMap<String, String>,
    alter_secret_refs: &BTreeMap<String, PbSecretRef>,
) -> Vec<String> {
    alter_props
        .keys()
        .chain(alter_secret_refs.keys())
        .cloned()
        .collect()
}

async fn update_sink_fragment_props(
    txn: &DatabaseTransaction,
    sink_id: SinkId,
    options_with_secret: &WithOptionsSecResolved,
) -> MetaResult<()> {
    let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
        .select_only()
        .columns([
            fragment::Column::FragmentId,
            fragment::Column::FragmentTypeMask,
            fragment::Column::StreamNode,
        ])
        .filter(fragment::Column::JobId.eq(sink_id))
        .into_tuple()
        .all(txn)
        .await?;
    let props = options_with_secret.as_plaintext();
    let secret_refs = options_with_secret.as_secret().clone();
    let fragments = fragments
        .into_iter()
        .filter(|(_, fragment_type_mask, _)| {
            *fragment_type_mask & FragmentTypeFlag::Sink as i32 != 0
        })
        .filter_map(|(id, _, stream_node)| {
            let mut stream_node = stream_node.to_protobuf();
            let mut found = false;
            visit_stream_node_mut(&mut stream_node, |node| {
                if let PbNodeBody::Sink(node) = node
                    && let Some(sink_desc) = &mut node.sink_desc
                    && sink_desc.id == sink_id
                {
                    sink_desc.properties = props.clone();
                    sink_desc.secret_refs = secret_refs.clone();
                    found = true;
                }
            });
            if found { Some((id, stream_node)) } else { None }
        })
        .collect_vec();
    assert!(
        !fragments.is_empty(),
        "sink id should be used by at least one fragment"
    );
    for (id, stream_node) in fragments {
        Fragment::update(fragment::ActiveModel {
            fragment_id: Set(id),
            stream_node: Set(StreamNode::from(&stream_node)),
            ..Default::default()
        })
        .exec(txn)
        .await?;
    }
    Ok(())
}

pub struct SinkIntoTableContext {
    /// For alter table (e.g., add column), this is the list of existing sink ids
    /// otherwise empty.
    pub updated_sink_catalogs: Vec<SinkId>,
}

pub struct FinishAutoRefreshSchemaSinkContext {
    pub tmp_sink_id: SinkId,
    pub original_sink_id: SinkId,
    pub columns: Vec<PbColumnCatalog>,
    pub new_log_store_table: Option<Box<PbTable>>,
}

async fn update_connector_props_fragments<F>(
    txn: &DatabaseTransaction,
    job_ids: Vec<JobId>,
    expect_flag: FragmentTypeFlag,
    mut alter_stream_node_fn: F,
    is_shared_source: bool,
) -> MetaResult<()>
where
    F: FnMut(&mut PbNodeBody, &mut bool),
{
    let fragments: Vec<(FragmentId, StreamNode)> = Fragment::find()
        .select_only()
        .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
        .filter(
            fragment::Column::JobId
                .is_in(job_ids.clone())
                .and(FragmentTypeMask::intersects(expect_flag)),
        )
        .into_tuple()
        .all(txn)
        .await?;
    let fragments = fragments
        .into_iter()
        .filter_map(|(id, stream_node)| {
            let mut stream_node = stream_node.to_protobuf();
            let mut found = false;
            visit_stream_node_mut(&mut stream_node, |node| {
                alter_stream_node_fn(node, &mut found);
            });
            if found { Some((id, stream_node)) } else { None }
        })
        .collect_vec();
    if is_shared_source || job_ids.len() > 1 {
        // the first element is the source_id or associated table_id
        // if the source is non-shared, there is no updated fragments
        // job_ids.len() > 1 means the source is used by other streaming jobs, so there should be at least one fragment updated
        assert!(
            !fragments.is_empty(),
            "job ids {:?} (type: {:?}) should be used by at least one fragment",
            job_ids,
            expect_flag
        );
    }

    for (id, stream_node) in fragments {
        Fragment::update(fragment::ActiveModel {
            fragment_id: Set(id),
            stream_node: Set(StreamNode::from(&stream_node)),
            ..Default::default()
        })
        .exec(txn)
        .await?;
    }

    Ok(())
}

async fn update_secret_dependencies(
    txn: &DatabaseTransaction,
    to_add_secrets: Vec<SecretId>,
    to_remove_secrets: Vec<SecretId>,
    object_id: ObjectId,
) -> MetaResult<()> {
    // Update secret dependencies atomically within the transaction.
    // Add new dependencies for secrets that are newly referenced.
    if !to_add_secrets.is_empty() {
        ObjectDependency::insert_many(to_add_secrets.into_iter().map(|secret_id| {
            object_dependency::ActiveModel {
                oid: Set(secret_id.into()),
                used_by: Set(object_id),
                ..Default::default()
            }
        }))
        .exec(txn)
        .await?;
    }
    // Remove dependencies for secrets that are no longer referenced.
    // This allows the secrets to be deleted after this source/sink no longer uses them.
    if !to_remove_secrets.is_empty() {
        let _ = ObjectDependency::delete_many()
            .filter(
                object_dependency::Column::Oid
                    .is_in(to_remove_secrets)
                    .and(object_dependency::Column::UsedBy.eq(object_id)),
            )
            .exec(txn)
            .await?;
    }
    Ok(())
}
