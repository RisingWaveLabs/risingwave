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

mod alter_op;
mod comment;
mod create_op;
mod drop_op;
mod get_op;
mod list_op;
mod test;
mod util;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::mem::take;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use itertools::Itertools;
use risingwave_common::catalog::{
    DEFAULT_SCHEMA_NAME, FragmentTypeFlag, FragmentTypeMask, SYSTEM_SCHEMAS, TableOption,
};
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::config::{StreamingConfig, merge_streaming_config_section};
use risingwave_common::current_cluster_version;
use risingwave_common::id::JobId;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont_mut;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_connector::source::cdc::build_cdc_table_id;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::*;
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::{
    ConnectionId, CreateType, DatabaseId, FragmentId, I32Array, IndexId, JobStatus, ObjectId,
    Property, SchemaId, SecretId, SinkFormatDesc, SinkId, SourceId, StreamNode, StreamSourceInfo,
    StreamingParallelism, SubscriptionId, TableId, TableIdArray, UserId, ViewId, connection,
    database, fragment, function, index, object, object_dependency, pending_sink_state, schema,
    secret, sink, source, streaming_job, subscription, table, user_privilege, view,
};
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::subscription::SubscriptionState;
use risingwave_pb::catalog::table::{OptionalAssociatedSourceId, PbTableType};
use risingwave_pb::catalog::{
    PbComment, PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSecret, PbSink, PbSource,
    PbSubscription, PbTable, PbView,
};
use risingwave_pb::common::ObjectType as PbObjectType;
use risingwave_pb::meta::cancel_creating_jobs_request::PbCreatingJobInfo;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy;
use risingwave_pb::meta::{
    PbObject, PbObjectGroup, PbTableCacheRefillPolicies, PbTableRefillRuntimeConfig,
};
use risingwave_pb::telemetry::PbTelemetryEventStage;
use risingwave_pb::user::PbUserInfo;
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{Expr, Query, SimpleExpr};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QuerySelect, RelationTrait,
    SelectColumns, TransactionTrait, Value,
};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::info;

pub(crate) use self::util::load_object_models;
use super::utils::{
    check_subscription_name_duplicate, get_internal_tables_by_id, load_streaming_jobs_by_ids,
    rename_relation, rename_relation_refer,
};
use crate::controller::ObjectModel;
use crate::controller::catalog::util::{
    prepare_object_models_for_schema_change, update_internal_tables,
};
use crate::controller::fragment::FragmentTypeMaskExt;
use crate::controller::utils::*;
use crate::manager::{
    IGNORED_NOTIFICATION_VERSION, MetaSrvEnv, NotificationVersion,
    get_referred_connection_ids_from_source, get_referred_secret_ids_from_source,
};
use crate::rpc::ddl_controller::DropMode;
use crate::telemetry::{MetaTelemetryJobDesc, report_event};
use crate::{MetaError, MetaResult};

pub type Catalog = (
    Vec<PbDatabase>,
    Vec<PbSchema>,
    Vec<PbTable>,
    Vec<PbSource>,
    Vec<PbSink>,
    Vec<PbSubscription>,
    Vec<PbIndex>,
    Vec<PbView>,
    Vec<PbFunction>,
    Vec<PbConnection>,
    Vec<PbSecret>,
);

pub type CatalogControllerRef = Arc<CatalogController>;

const STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH: &str = "streaming.developer.cache_refill_policy";

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    pub(crate) env: MetaSrvEnv,
    pub(crate) inner: RwLock<CatalogControllerInner>,
}

#[derive(Clone, Default, Debug)]
pub struct DropTableConnectorContext {
    // we only apply one drop connector action for one table each time, so no need to vector here
    pub(crate) to_change_streaming_job_id: JobId,
    pub(crate) to_remove_state_table_id: TableId,
    pub(crate) to_remove_source_id: SourceId,
}

#[derive(Clone, Default, Debug)]
pub struct ReleaseContext {
    pub(crate) database_id: DatabaseId,
    pub(crate) removed_streaming_job_ids: Vec<JobId>,
    /// Dropped state table list, need to unregister from hummock.
    pub(crate) removed_state_table_ids: Vec<TableId>,

    /// Dropped secrets, need to remove from secret manager.
    pub(crate) removed_secret_ids: Vec<SecretId>,
    /// Dropped sources (when `DROP SOURCE`), need to unregister from source manager.
    pub(crate) removed_source_ids: Vec<SourceId>,
    /// Dropped Source fragments (when `DROP MATERIALIZED VIEW` referencing sources),
    /// need to unregister from source manager.
    pub(crate) removed_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,

    pub(crate) removed_fragments: HashSet<FragmentId>,

    /// Removed sink fragment by target fragment.
    pub(crate) removed_sink_fragment_by_targets: HashMap<FragmentId, Vec<FragmentId>>,

    /// Dropped iceberg table sinks
    pub(crate) removed_iceberg_table_sinks: Vec<PbSink>,

    /// Dropped iceberg sink ids. Used to clear iceberg maintenance (compaction
    /// schedule and snapshot expiration) in `IcebergCompactionManager`.
    pub(crate) removed_iceberg_sink_ids: Vec<SinkId>,

    /// Dropped Iceberg pk-index sink ids. Used to unregister per-sink commit workers
    /// owned by `IcebergPkIndexSinkManager`. Filtered via `is_iceberg_pk_index_sink` on
    /// the sink properties so user-created pk-index sinks (any name) are included.
    pub(crate) removed_iceberg_pk_index_sink_ids: Vec<SinkId>,
}

#[derive(Default)]
pub(crate) struct CleanedDirtyStreamingJobs {
    pub(crate) streaming_job_ids: Vec<JobId>,
    /// Only populated for per-database recovery.
    pub(crate) dropped_table_ids: Vec<TableId>,
    pub(crate) source_ids: Vec<SourceId>,
    /// Cleaned dirty sink jobs, whose iceberg maintenance state must be cleared.
    pub(crate) sink_ids: Vec<SinkId>,
}

fn explicit_cache_refill_policy(config_override: &str) -> MetaResult<Option<CacheRefillPolicy>> {
    if config_override.trim().is_empty() {
        return Ok(None);
    }

    let table: toml::Table =
        toml::from_str(config_override).context("invalid streaming job config override")?;
    let has_explicit_policy = table
        .get("streaming")
        .and_then(toml::Value::as_table)
        .and_then(|table| table.get("developer"))
        .and_then(toml::Value::as_table)
        .is_some_and(|table| table.contains_key("cache_refill_policy"));
    if !has_explicit_policy {
        return Ok(None);
    }

    let merged = merge_streaming_config_section(&StreamingConfig::default(), config_override)
        .context("invalid streaming job config override")?
        .context("empty streaming job config override")?;
    Ok(Some(merged.developer.cache_refill_policy))
}

impl CatalogControllerInner {
    pub(crate) async fn table_cache_refill_policies_snapshot_if_job_has_explicit_policy(
        &self,
        job_id: JobId,
    ) -> MetaResult<Option<PbTableCacheRefillPolicies>> {
        let config_override: Option<String> = StreamingJob::find_by_id(job_id)
            .select_only()
            .column(streaming_job::Column::ConfigOverride)
            .into_tuple()
            .one(&self.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

        if explicit_cache_refill_policy(&config_override.unwrap_or_default())?.is_none() {
            return Ok(None);
        }

        Ok(Some(self.table_cache_refill_policies_snapshot().await?))
    }

    pub async fn table_cache_refill_policies_snapshot(
        &self,
    ) -> MetaResult<PbTableCacheRefillPolicies> {
        let job_configs = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .column(streaming_job::Column::ConfigOverride)
            .filter(
                streaming_job::Column::JobId.in_subquery(
                    Query::select()
                        .distinct()
                        .column(fragment::Column::JobId)
                        .from(Fragment)
                        .to_owned(),
                ),
            )
            .into_tuple::<(JobId, Option<String>)>()
            .all(&self.db)
            .await?;
        let mut policies_by_job = HashMap::new();
        for (job_id, config_override) in job_configs {
            if let Some(policy) =
                explicit_cache_refill_policy(&config_override.unwrap_or_default())?
            {
                policies_by_job.insert(job_id, policy);
            }
        }
        if policies_by_job.is_empty() {
            return Ok(PbTableCacheRefillPolicies::default());
        }

        let result_table_ids = policies_by_job
            .keys()
            .map(|job_id| job_id.as_mv_table_id())
            .collect_vec();
        let tables: Vec<(TableId, TableType, Option<JobId>)> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .column(table::Column::TableType)
            .column(table::Column::BelongsToJobId)
            .filter(
                table::Column::TableType
                    .eq(TableType::Internal)
                    .and(table::Column::BelongsToJobId.is_in(policies_by_job.keys().copied()))
                    .or(table::Column::TableId.is_in(result_table_ids)),
            )
            .into_tuple()
            .all(&self.db)
            .await?;

        let mut table_policies = Vec::new();
        let mut internal_table_policies = Vec::new();
        for (table_id, table_type, belongs_to_job_id) in tables {
            if table_type == TableType::Internal {
                let Some(policy) =
                    belongs_to_job_id.and_then(|job_id| policies_by_job.get(&job_id))
                else {
                    continue;
                };
                internal_table_policies.push(PbTableCacheRefillPolicy {
                    table_id: table_id.as_raw_id(),
                    policy: policy.to_protobuf() as i32,
                });
                continue;
            }

            let Some(policy) = policies_by_job.get(&table_id.as_job_id()) else {
                continue;
            };
            table_policies.push(PbTableCacheRefillPolicy {
                table_id: table_id.as_raw_id(),
                policy: policy.to_protobuf() as i32,
            });
        }
        table_policies.sort_unstable_by_key(|policy| policy.table_id);
        internal_table_policies.sort_unstable_by_key(|policy| policy.table_id);

        Ok(PbTableCacheRefillPolicies {
            table_policies,
            internal_table_policies,
        })
    }
}

impl CatalogController {
    pub(crate) async fn notify_hummock_table_cache_refill_policy_if_explicit(
        &self,
        inner: &CatalogControllerInner,
        job_id: JobId,
    ) -> MetaResult<()> {
        if let Some(policies) = inner
            .table_cache_refill_policies_snapshot_if_job_has_explicit_policy(job_id)
            .await?
        {
            self.env
                .notification_manager()
                .notify_hummock(
                    NotificationOperation::Update,
                    NotificationInfo::TableRefillRuntimeConfig(PbTableRefillRuntimeConfig {
                        table_cache_refill_policies: Some(policies),
                        ..Default::default()
                    }),
                )
                .await;
        }
        Ok(())
    }

    pub async fn table_cache_refill_policies_snapshot(
        &self,
    ) -> MetaResult<PbTableCacheRefillPolicies> {
        let inner = self.inner.read().await;
        inner.table_cache_refill_policies_snapshot().await
    }

    pub async fn new(env: MetaSrvEnv) -> MetaResult<Self> {
        let meta_store = env.meta_store();
        let catalog_controller = Self {
            env,
            inner: RwLock::new(CatalogControllerInner {
                db: meta_store.conn,
                creating_table_finish_notifier: HashMap::new(),
                dropped_tables: HashMap::new(),
            }),
        };

        catalog_controller.init().await?;
        Ok(catalog_controller)
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_inner_read_guard(&self) -> RwLockReadGuard<'_, CatalogControllerInner> {
        self.inner.read().await
    }

    pub async fn get_inner_write_guard(&self) -> RwLockWriteGuard<'_, CatalogControllerInner> {
        self.inner.write().await
    }
}

pub struct CatalogControllerInner {
    pub(crate) db: DatabaseConnection,
    /// Registered finish notifiers for creating tables.
    ///
    /// `DdlController` will update this map, and pass the `tx` side to `CatalogController`.
    /// On notifying, we can remove the entry from this map.
    #[expect(clippy::type_complexity)]
    pub creating_table_finish_notifier:
        HashMap<DatabaseId, HashMap<JobId, Vec<Sender<Result<NotificationVersion, String>>>>>,
    /// Tables have been dropped from the meta store, but the corresponding barrier remains unfinished.
    pub dropped_tables: HashMap<TableId, PbTable>,
}

impl CatalogController {
    pub(crate) async fn notify_frontend(
        &self,
        operation: NotificationOperation,
        info: NotificationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }

    pub(crate) async fn notify_frontend_relation_info(
        &self,
        operation: NotificationOperation,
        relation_info: PbObjectInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend_object_info(operation, relation_info)
            .await
    }

    /// Trivially advance the notification version and notify to frontend,
    /// return the notification version for frontend to wait for.
    ///
    /// Cannot simply return the current version, because the current version may not be sent
    /// to frontend, and the frontend may endlessly wait for this version, until a frontend
    /// related notification is sent.
    pub(crate) async fn notify_frontend_trivial(&self) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![],
                    dependencies: vec![],
                }),
            )
            .await
    }
}

impl CatalogController {
    pub async fn finish_create_subscription_catalog(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // update `created_at` as now() and `created_at_cluster_version` as current cluster version.
        let res = Object::update_many()
            .col_expr(object::Column::CreatedAt, Expr::current_timestamp().into())
            .col_expr(
                object::Column::CreatedAtClusterVersion,
                current_cluster_version().into(),
            )
            .filter(object::Column::Oid.eq(subscription_id))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                "subscription",
                subscription_id,
            ));
        }

        // mark the target subscription as `Create`.
        let job = subscription::ActiveModel {
            subscription_id: Set(subscription_id),
            subscription_state: Set(SubscriptionState::Created.into()),
            ..Default::default()
        };
        Subscription::update(job).exec(&txn).await?;

        let _ = grant_default_privileges_automatically(&txn, subscription_id).await?;

        txn.commit().await?;

        Ok(())
    }

    pub async fn notify_create_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        let (subscription, obj) = Subscription::find_by_id(subscription_id)
            .find_also_related(Object)
            .filter(subscription::Column::SubscriptionState.eq(SubscriptionState::Created as i32))
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("subscription", subscription_id))?;

        let dependencies =
            list_object_dependencies_by_object_id(&txn, subscription_id.into()).await?;
        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: PbObjectInfo::Subscription(
                            ObjectModel(subscription, obj.unwrap(), None).into(),
                        )
                        .into(),
                    }],
                    dependencies,
                }),
            )
            .await;

        // notify default privileges about the new subscription
        let updated_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(subscription_id.as_object_id()))
            .into_tuple()
            .all(&inner.db)
            .await?;

        if !updated_user_ids.is_empty() {
            let updated_user_infos = list_user_info_by_ids(updated_user_ids, &inner.db).await?;
            version = self.notify_users_update(updated_user_infos).await;
        }

        Ok(version)
    }

    // for telemetry
    pub async fn get_connector_usage(&self) -> MetaResult<jsonbb::Value> {
        // get connector usage by source/sink
        // the expect format is like:
        // {
        //     "source": [{
        //         "$source_id": {
        //             "connector": "kafka",
        //             "format": "plain",
        //             "encode": "json"
        //         },
        //     }],
        //     "sink": [{
        //         "$sink_id": {
        //             "connector": "pulsar",
        //             "format": "upsert",
        //             "encode": "avro"
        //         },
        //     }],
        // }

        let inner = self.inner.read().await;
        let source_props_and_info: Vec<(i32, Property, Option<StreamSourceInfo>)> = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .column(source::Column::WithProperties)
            .column(source::Column::SourceInfo)
            .into_tuple()
            .all(&inner.db)
            .await?;
        let sink_props_and_info: Vec<(i32, Property, Option<SinkFormatDesc>)> = Sink::find()
            .select_only()
            .column(sink::Column::SinkId)
            .column(sink::Column::Properties)
            .column(sink::Column::SinkFormatDesc)
            .into_tuple()
            .all(&inner.db)
            .await?;
        drop(inner);

        let get_connector_from_property = |property: &Property| -> String {
            property
                .0
                .get(UPSTREAM_SOURCE_KEY)
                .cloned()
                .unwrap_or_default()
        };

        let source_report: Vec<jsonbb::Value> = source_props_and_info
            .iter()
            .map(|(oid, property, info)| {
                let connector_name = get_connector_from_property(property);
                let mut format = None;
                let mut encode = None;
                if let Some(info) = info {
                    let pb_info = info.to_protobuf();
                    format = Some(pb_info.format().as_str_name());
                    encode = Some(pb_info.row_encode().as_str_name());
                }
                jsonbb::json!({
                    oid.to_string(): {
                        "connector": connector_name,
                        "format": format,
                        "encode": encode,
                    },
                })
            })
            .collect_vec();

        let sink_report: Vec<jsonbb::Value> = sink_props_and_info
            .iter()
            .map(|(oid, property, info)| {
                let connector_name = get_connector_from_property(property);
                let mut format = None;
                let mut encode = None;
                if let Some(info) = info {
                    let pb_info = info.to_protobuf();
                    format = Some(pb_info.format().as_str_name());
                    encode = Some(pb_info.encode().as_str_name());
                }
                jsonbb::json!({
                    oid.to_string(): {
                        "connector": connector_name,
                        "format": format,
                        "encode": encode,
                    },
                })
            })
            .collect_vec();

        Ok(jsonbb::json!({
                "source": source_report,
                "sink": sink_report,
        }))
    }

    pub async fn clean_dirty_subscription(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let filter_condition = object::Column::ObjType.eq(ObjectType::Subscription).and(
            object::Column::Oid.not_in_subquery(
                Query::select()
                    .column(subscription::Column::SubscriptionId)
                    .from(Subscription)
                    .and_where(
                        subscription::Column::SubscriptionState
                            .eq(SubscriptionState::Created as i32),
                    )
                    .take(),
            ),
        );

        let filter_condition = if let Some(database_id) = database_id {
            filter_condition.and(object::Column::DatabaseId.eq(database_id))
        } else {
            filter_condition
        };
        Object::delete_many()
            .filter(filter_condition)
            .exec(&txn)
            .await?;
        txn.commit().await?;
        // We don't need to notify the frontend, because the Init subscription is not send to frontend.
        Ok(())
    }

    /// Cleans up jobs in `Initial` status. Creating jobs are cleaned up when their creation
    /// progress cannot be recovered, or when `clean_all_foreground_jobs_on_recovery` is enabled
    /// and they use foreground creation.
    pub(crate) async fn clean_dirty_creating_jobs(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<CleanedDirtyStreamingJobs> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let clean_all_foreground_jobs = self.env.opts.clean_all_foreground_jobs_on_recovery;

        let candidate_job_filter =
            streaming_job::Column::JobStatus.is_in([JobStatus::Initial, JobStatus::Creating]);
        let candidate_job_filter = if let Some(database_id) = database_id {
            candidate_job_filter.and(object::Column::DatabaseId.eq(database_id))
        } else {
            candidate_job_filter
        };
        type CandidateJob = (
            JobId,
            JobStatus,
            CreateType,
            ObjectId,
            ObjectType,
            Option<SchemaId>,
            Option<DatabaseId>,
        );
        let candidate_jobs: Vec<CandidateJob> = streaming_job::Entity::find()
            .select_only()
            .columns([
                streaming_job::Column::JobId,
                streaming_job::Column::JobStatus,
                streaming_job::Column::CreateType,
            ])
            .columns([
                object::Column::Oid,
                object::Column::ObjType,
                object::Column::SchemaId,
                object::Column::DatabaseId,
            ])
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .filter(candidate_job_filter)
            .into_tuple()
            .all(&txn)
            .await?;
        let creating_job_ids = candidate_jobs
            .iter()
            .filter(|(_, job_status, create_type, ..)| {
                *job_status == JobStatus::Creating
                    && (!clean_all_foreground_jobs || *create_type == CreateType::Foreground)
            })
            .map(|(job_id, ..)| *job_id)
            .collect_vec();

        let dirty_creating_job_ids: HashSet<JobId> =
            if clean_all_foreground_jobs || creating_job_ids.is_empty() {
                creating_job_ids.into_iter().collect()
            } else {
                Fragment::find()
                    .select_only()
                    .column(fragment::Column::JobId)
                    .filter(fragment::Column::JobId.is_in(creating_job_ids))
                    .filter(FragmentTypeMask::contains_non_recoverable_fragment())
                    .into_tuple()
                    .all(&txn)
                    .await?
                    .into_iter()
                    .collect()
            };

        let mut dirty_job_objs = candidate_jobs
            .into_iter()
            .filter(|(job_id, job_status, ..)| {
                *job_status == JobStatus::Initial || dirty_creating_job_ids.contains(job_id)
            })
            .map(
                |(_, _, _, oid, obj_type, schema_id, database_id)| PartialObject {
                    oid,
                    obj_type,
                    schema_id,
                    database_id,
                },
            )
            .collect_vec();

        // Check if there are any pending iceberg table jobs.
        let dirty_iceberg_jobs = find_dirty_iceberg_table_jobs(&txn, database_id).await?;
        if !dirty_iceberg_jobs.is_empty() {
            dirty_job_objs.extend(dirty_iceberg_jobs);
        }

        Self::clean_dirty_sink_downstreams(&txn).await?;

        if dirty_job_objs.is_empty() {
            return Ok(CleanedDirtyStreamingJobs::default());
        }

        self.log_cleaned_dirty_jobs(&dirty_job_objs, &txn).await?;

        let mut dirty_objects = Object::find()
            .filter(object::Column::Oid.is_in(dirty_job_objs.iter().map(|obj| obj.oid)))
            .all(&txn)
            .await?;
        dirty_objects.extend(
            get_belong_objects_by_ids(&txn, dirty_job_objs.iter().map(|obj| obj.oid)).await?,
        );
        let dirty_object_ids = dirty_objects
            .iter()
            .map(|object| object.oid)
            .collect::<HashSet<_>>();
        let dirty_catalog_models = load_object_models(&txn, &dirty_objects).await?;
        let dirty_job_ids = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobId.is_in(dirty_object_ids.iter().copied()))
            .into_tuple()
            .all(&txn)
            .await?;
        let dirty_sink_ids = dirty_objects
            .iter()
            .filter(|object| object.obj_type == ObjectType::Sink)
            .map(|object| object.oid.as_sink_id())
            .collect_vec();
        // Object deletion cascades to the fragments of the dirty streaming jobs. Keep their IDs
        // before the transaction deletes them so the serving mapping can be notified after commit.
        let dirty_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::JobId.is_in(dirty_job_ids.iter().copied()))
            .into_tuple()
            .all(&txn)
            .await?;
        // Frontend deletion is idempotent, so notify every dirty job even if frontend did not
        // observe its creating catalog or it is a temporary replacement object.
        let to_notify_objs = dirty_job_objs.clone();
        let mut objects_to_notify = to_notify_objs.clone();
        objects_to_notify.extend(
            get_belong_objects_by_ids(&txn, to_notify_objs.iter().map(|obj| obj.oid))
                .await?
                .into_iter()
                .map(PartialObject::from),
        );

        // The source ids for dirty tables with connector.
        // FIXME: we should also clean dirty sources.
        let dirty_associated_source_ids: Vec<SourceId> = dirty_catalog_models
            .iter()
            .filter_map(|object_info| match object_info {
                PbObjectInfo::Table(table) => {
                    table
                        .optional_associated_source_id
                        .map(|source_id| match source_id {
                            OptionalAssociatedSourceId::AssociatedSourceId(source_id) => source_id,
                        })
                }
                _ => None,
            })
            .collect_vec();
        // Per-database recovery does not run the global Hummock purge. Keep table catalogs so the
        // caller can reuse the normal dropped-table cleanup path after the catalog rows are deleted.
        let dropped_tables = if database_id.is_some() {
            dirty_catalog_models
                .iter()
                .filter_map(|object_info| match object_info {
                    PbObjectInfo::Table(table) => Some(table.clone()),
                    _ => None,
                })
                .collect_vec()
        } else {
            vec![]
        };
        let dropped_table_ids = dropped_tables.iter().map(|table| table.id).collect_vec();

        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(dirty_job_objs.iter().map(|obj| obj.oid)))
            .exec(&txn)
            .await?;
        assert!(res.rows_affected > 0);

        txn.commit().await?;
        inner
            .dropped_tables
            .extend(dropped_tables.into_iter().map(|t| (t.id, t)));

        self.env
            .notification_manager()
            .notify_serving_fragment_mapping_delete(dirty_fragment_ids);

        let object_group = build_object_group_for_delete(objects_to_notify);

        let _version = self
            .notify_frontend(NotificationOperation::Delete, object_group)
            .await;

        Ok(CleanedDirtyStreamingJobs {
            streaming_job_ids: dirty_job_ids,
            dropped_table_ids,
            source_ids: dirty_associated_source_ids,
            sink_ids: dirty_sink_ids,
        })
    }

    pub async fn comment_on(&self, comment: PbComment) -> MetaResult<NotificationVersion> {
        let object_type = PbObjectType::try_from(comment.object_type)
            .map_err(|_| MetaError::invalid_parameter("comment object type"))?;
        if object_type == PbObjectType::Unspecified {
            return Err(MetaError::invalid_parameter("comment object type"));
        }

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let object_id = ObjectId::new(comment.object_id);
        let object = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
        if object.obj_type != ObjectType::from(object_type) {
            return Err(MetaError::invalid_parameter(format!(
                "{} is not a {}",
                object_id,
                object_type.as_str_name().to_ascii_lowercase()
            )));
        }

        let object_info = match object_type {
            PbObjectType::Table | PbObjectType::Mview => {
                Self::comment_on_table(&txn, object, object_type, comment).await?
            }
            unsupported => {
                return Err(MetaError::invalid_parameter(format!(
                    "comment on {} is not supported yet",
                    unsupported.as_str_name().to_ascii_lowercase()
                )));
            }
        };

        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(NotificationOperation::Update, object_info)
            .await;

        Ok(version)
    }

    async fn notify_hummock_dropped_tables(&self, tables: Vec<PbTable>) {
        if tables.is_empty() {
            return;
        }
        let objects = tables
            .into_iter()
            .map(|t| PbObject {
                object_info: Some(PbObjectInfo::Table(t)),
            })
            .collect();
        let group = NotificationInfo::ObjectGroup(PbObjectGroup {
            objects,
            dependencies: vec![],
        });
        self.env
            .notification_manager()
            .notify_hummock(NotificationOperation::Delete, group.clone())
            .await;
        self.env
            .notification_manager()
            .notify_compactor(NotificationOperation::Delete, group)
            .await;
    }

    pub async fn complete_dropped_tables(&self, table_ids: impl IntoIterator<Item = TableId>) {
        let mut inner = self.inner.write().await;
        let tables = inner.complete_dropped_tables(table_ids);
        self.notify_hummock_dropped_tables(tables).await;
    }

    pub async fn cleanup_dropped_tables(&self) {
        let mut inner = self.inner.write().await;
        let tables = inner.dropped_tables.drain().map(|(_, t)| t).collect();
        self.notify_hummock_dropped_tables(tables).await;
    }

    pub async fn stats(&self) -> MetaResult<CatalogStats> {
        let inner = self.inner.read().await;

        let mut table_num_map: HashMap<_, _> = Table::find()
            .select_only()
            .column(table::Column::TableType)
            .column_as(table::Column::TableId.count(), "num")
            .group_by(table::Column::TableType)
            .having(table::Column::TableType.ne(TableType::Internal))
            .into_tuple::<(TableType, i64)>()
            .all(&inner.db)
            .await?
            .into_iter()
            .map(|(table_type, num)| (table_type, num as u64))
            .collect();

        let source_num = Source::find().count(&inner.db).await?;
        let sink_num = Sink::find().count(&inner.db).await?;
        let function_num = Function::find().count(&inner.db).await?;
        let streaming_job_num = StreamingJob::find().count(&inner.db).await?;

        let actor_num = {
            let guard = self.env.shared_actor_info.read_guard();
            guard
                .iter_over_fragments()
                .map(|(_, fragment)| fragment.actors.len() as u64)
                .sum::<u64>()
        };
        let database_num = Database::find().count(&inner.db).await?;

        Ok(CatalogStats {
            table_num: table_num_map.remove(&TableType::Table).unwrap_or(0),
            mview_num: table_num_map
                .remove(&TableType::MaterializedView)
                .unwrap_or(0),
            index_num: table_num_map.remove(&TableType::Index).unwrap_or(0),
            source_num,
            sink_num,
            function_num,
            streaming_job_num,
            actor_num,
            database_num,
        })
    }

    pub async fn fetch_sink_with_state_table_ids(
        &self,
        sink_ids: HashSet<SinkId>,
    ) -> MetaResult<HashMap<SinkId, Vec<TableId>>> {
        let inner = self.inner.read().await;

        let query = Fragment::find()
            .select_only()
            .columns([fragment::Column::JobId, fragment::Column::StateTableIds])
            .filter(
                fragment::Column::JobId
                    .is_in(sink_ids)
                    .and(FragmentTypeMask::intersects(FragmentTypeFlag::Sink)),
            );

        let rows: Vec<(JobId, TableIdArray)> = query.into_tuple().all(&inner.db).await?;

        debug_assert!(rows.iter().map(|(job_id, _)| job_id).all_unique());

        let result = rows
            .into_iter()
            .map(|(job_id, table_id_array)| (job_id.as_sink_id(), table_id_array.0))
            .collect::<HashMap<_, _>>();

        Ok(result)
    }

    pub async fn list_all_pending_sinks(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashSet<SinkId>> {
        let inner = self.inner.read().await;

        let mut query = pending_sink_state::Entity::find()
            .select_only()
            .columns([pending_sink_state::Column::SinkId])
            .filter(
                pending_sink_state::Column::SinkState.eq(pending_sink_state::SinkState::Pending),
            )
            .distinct();

        if let Some(db_id) = database_id {
            query = query
                .join(
                    JoinType::InnerJoin,
                    pending_sink_state::Relation::Object.def(),
                )
                .filter(object::Column::DatabaseId.eq(db_id));
        }

        let result: Vec<SinkId> = query.into_tuple().all(&inner.db).await?;

        Ok(result.into_iter().collect())
    }

    pub async fn abort_pending_sink_epochs(
        &self,
        sink_committed_epoch: HashMap<SinkId, u64>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        for (sink_id, committed_epoch) in sink_committed_epoch {
            pending_sink_state::Entity::update_many()
                .col_expr(
                    pending_sink_state::Column::SinkState,
                    Expr::value(pending_sink_state::SinkState::Aborted),
                )
                .filter(
                    pending_sink_state::Column::SinkId
                        .eq(sink_id)
                        .and(pending_sink_state::Column::Epoch.gt(committed_epoch as i64)),
                )
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok(())
    }
}

/// `CatalogStats` is a struct to store the statistics of all catalogs.
pub struct CatalogStats {
    pub table_num: u64,
    pub mview_num: u64,
    pub index_num: u64,
    pub source_num: u64,
    pub sink_num: u64,
    pub function_num: u64,
    pub streaming_job_num: u64,
    pub actor_num: u64,
    pub database_num: u64,
}

impl CatalogControllerInner {
    pub async fn snapshot(&self) -> MetaResult<(Catalog, Vec<PbUserInfo>)> {
        let databases = self.list_databases().await?;
        let schemas = self.list_schemas().await?;
        let tables = self.list_tables().await?;
        let sources = self.list_sources().await?;
        let sinks = self.list_sinks().await?;
        let subscriptions = self.list_subscriptions().await?;
        let indexes = self.list_indexes().await?;
        let views = self.list_views().await?;
        let functions = self.list_functions().await?;
        let connections = self.list_connections().await?;
        let secrets = self.list_secrets().await?;

        let users = self.list_users().await?;

        Ok((
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
        ))
    }

    async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let db_objs = Database::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(db_objs
            .into_iter()
            .map(|(db, obj)| ObjectModel(db, obj.unwrap(), None).into())
            .collect())
    }

    async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let schema_objs = Schema::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(schema_objs
            .into_iter()
            .map(|(schema, obj)| ObjectModel(schema, obj.unwrap(), None).into())
            .collect())
    }

    async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let mut user_infos: Vec<PbUserInfo> = User::find()
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        for user_info in &mut user_infos {
            user_info.grant_privileges = get_user_privilege(user_info.id as _, &self.db).await?;
        }
        Ok(user_infos)
    }

    /// `list_all_tables` return all tables and internal tables.
    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            &self.db,
            table_objs.iter().map(|(table, _)| table.job_id()),
        )
        .await?;

        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| {
                let job_id = table.job_id();
                let streaming_job = streaming_jobs.get(&job_id).cloned();
                ObjectModel(table, obj.unwrap(), streaming_job).into()
            })
            .collect())
    }

    /// `list_tables` returns all streaming-job tables and their internal tables.
    async fn list_tables(&self) -> MetaResult<Vec<PbTable>> {
        let mut table_objs = Table::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        let all_streaming_jobs: HashMap<JobId, streaming_job::Model> = StreamingJob::find()
            .all(&self.db)
            .await?
            .into_iter()
            .map(|job| (job.job_id, job))
            .collect();
        table_objs.retain(|(table, _)| {
            all_streaming_jobs
                .get(&table.job_id())
                .is_some_and(|job| job.job_status != JobStatus::Initial)
        });

        let mut tables = Vec::with_capacity(table_objs.len());
        for (table, obj) in table_objs {
            let job_id = table.job_id();
            let streaming_job = all_streaming_jobs.get(&job_id).cloned();
            let pb_table: PbTable = ObjectModel(table, obj.unwrap(), streaming_job).into();
            tables.push(pb_table);
        }
        Ok(tables)
    }

    /// `list_sources` returns all sources, including shared and associated sources whose streaming
    /// jobs are still being created.
    async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let mut source_objs = Source::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .is_null()
                    .or(streaming_job::Column::JobStatus.ne(JobStatus::Initial)),
            )
            .all(&self.db)
            .await?;

        let visible_table_ids: HashSet<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(
                table::Column::OptionalAssociatedSourceId
                    .is_not_null()
                    .and(streaming_job::Column::JobStatus.ne(JobStatus::Initial)),
            )
            .into_tuple()
            .all(&self.db)
            .await?
            .into_iter()
            .collect();
        source_objs.retain(|(source, _)| {
            source
                .optional_associated_table_id
                .is_none_or(|table_id| visible_table_ids.contains(&table_id))
        });

        Ok(source_objs
            .into_iter()
            .map(|(source, obj)| ObjectModel(source, obj.unwrap(), None).into())
            .collect())
    }

    /// `list_sinks` return all sinks.
    async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .is_null()
                    .or(streaming_job::Column::JobStatus.ne(JobStatus::Initial)),
            )
            .all(&self.db)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            &self.db,
            sink_objs.iter().map(|(sink, _)| sink.sink_id.as_job_id()),
        )
        .await?;

        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| {
                let streaming_job = streaming_jobs.get(&sink.sink_id.as_job_id()).cloned();
                ObjectModel(sink, obj.unwrap(), streaming_job).into()
            })
            .collect())
    }

    /// `list_subscriptions` return all `CREATED` subscriptions.
    async fn list_subscriptions(&self) -> MetaResult<Vec<PbSubscription>> {
        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .filter(subscription::Column::SubscriptionState.eq(SubscriptionState::Created as i32))
            .all(&self.db)
            .await?;

        Ok(subscription_objs
            .into_iter()
            .map(|(subscription, obj)| ObjectModel(subscription, obj.unwrap(), None).into())
            .collect())
    }

    async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let view_objs = View::find().find_also_related(Object).all(&self.db).await?;

        Ok(view_objs
            .into_iter()
            .map(|(view, obj)| ObjectModel(view, obj.unwrap(), None).into())
            .collect())
    }

    /// `list_indexes` returns all streaming indexes.
    async fn list_indexes(&self) -> MetaResult<Vec<PbIndex>> {
        let index_objs = Index::find()
            .find_also_related(Object)
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.ne(JobStatus::Initial))
            .all(&self.db)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            &self.db,
            index_objs
                .iter()
                .map(|(index, _)| index.index_id.as_job_id()),
        )
        .await?;
        Ok(index_objs
            .into_iter()
            .map(|(index, obj)| {
                let streaming_job = streaming_jobs.get(&index.index_id.as_job_id()).cloned();
                ObjectModel(index, obj.unwrap(), streaming_job).into()
            })
            .collect())
    }

    async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap(), None).into())
            .collect())
    }

    pub async fn list_secrets(&self) -> MetaResult<Vec<PbSecret>> {
        let secret_objs = Secret::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(secret_objs
            .into_iter()
            .map(|(secret, obj)| ObjectModel(secret, obj.unwrap(), None).into())
            .collect())
    }

    async fn list_functions(&self) -> MetaResult<Vec<PbFunction>> {
        let func_objs = Function::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(func_objs
            .into_iter()
            .map(|(func, obj)| ObjectModel(func, obj.unwrap(), None).into())
            .collect())
    }

    pub(crate) fn register_finish_notifier(
        &mut self,
        database_id: DatabaseId,
        id: JobId,
        sender: Sender<Result<NotificationVersion, String>>,
    ) {
        self.creating_table_finish_notifier
            .entry(database_id)
            .or_default()
            .entry(id)
            .or_default()
            .push(sender);
    }

    pub(crate) async fn streaming_job_is_finished(&mut self, id: JobId) -> MetaResult<bool> {
        let status = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobStatus)
            .filter(streaming_job::Column::JobId.eq(id))
            .into_tuple::<JobStatus>()
            .one(&self.db)
            .await?;

        status
            .map(|status| status == JobStatus::Created)
            .ok_or_else(|| {
                MetaError::catalog_id_not_found("streaming job", "may have been cancelled/dropped")
            })
    }

    pub(crate) fn notify_finish_failed(&mut self, database_id: Option<DatabaseId>, err: String) {
        if let Some(database_id) = database_id {
            if let Some(creating_tables) = self.creating_table_finish_notifier.remove(&database_id)
            {
                for tx in creating_tables.into_values().flatten() {
                    let _ = tx.send(Err(err.clone()));
                }
            }
        } else {
            for tx in take(&mut self.creating_table_finish_notifier)
                .into_values()
                .flatten()
                .flat_map(|(_, txs)| txs.into_iter())
            {
                let _ = tx.send(Err(err.clone()));
            }
        }
    }

    pub async fn list_time_travel_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .filter(table::Column::TableType.is_in(vec![
                TableType::Table,
                TableType::MaterializedView,
                TableType::Index,
            ]))
            .column(table::Column::TableId)
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(table_ids)
    }

    /// Since the tables have been dropped from both meta store and streaming jobs, this method removes those table copies.
    /// Returns the removed table copies.
    pub(crate) fn complete_dropped_tables(
        &mut self,
        table_ids: impl IntoIterator<Item = TableId>,
    ) -> Vec<PbTable> {
        table_ids
            .into_iter()
            .filter_map(|table_id| {
                self.dropped_tables.remove(&table_id).map_or_else(
                    || {
                        tracing::warn!(%table_id, "table not found");
                        None
                    },
                    Some,
                )
            })
            .collect()
    }
}
