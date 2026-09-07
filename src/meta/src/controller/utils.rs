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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    FragmentTypeFlag, FragmentTypeMask, ICEBERG_SINK_PREFIX, ICEBERG_SOURCE_PREFIX,
};
use risingwave_common::hash::{ActorMapping, VnodeBitmapExt, WorkerSlotId, WorkerSlotMapping};
use risingwave_common::id::{JobId, SubscriptionId};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_common::{bail, hash};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::*;
use risingwave_meta_model::streaming_job::BackfillOrders;
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::user_privilege::Action;
use risingwave_meta_model::{
    ActorId, ColumnCatalogArray, CreateType, DataTypeArray, DatabaseId, DispatcherType, FragmentId,
    JobStatus, ObjectId, PrivilegeId, SchemaId, SinkId, SourceId, StreamNode, StreamSourceInfo,
    TableId, TableIdArray, UserId, WorkerId, connection, database, fragment, fragment_relation,
    function, index, object, object_dependency, schema, secret, sink, source, streaming_job,
    subscription, table, user, user_default_privilege, user_privilege, view,
};
use risingwave_meta_model_migration::WithQuery;
use risingwave_pb::catalog::{
    PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSecret, PbSink, PbSource,
    PbSubscription, PbTable, PbView,
};
use risingwave_pb::common::{PbObjectType, WorkerNode};
use risingwave_pb::expr::{PbExprNode, expr_node};
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::Info as NotificationInfo;
use risingwave_pb::meta::{
    ObjectDependency as PbObjectDependency, PbFragmentWorkerSlotMapping, PbObject, PbObjectGroup,
};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{ColumnCatalog, DefaultColumnDesc};
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_pb::stream_plan::{PbDispatchOutputMapping, PbDispatcher, PbDispatcherType};
use risingwave_pb::user::grant_privilege::{PbActionWithGrantOption, PbObject as PbGrantObject};
use risingwave_pb::user::{PbAction, PbGrantPrivilege, PbUserInfo};
use risingwave_sqlparser::ast::{
    Ident, ObjectName, SecretRefAsType, SecretRefValue, SqlOption, Statement as SqlStatement,
};
use risingwave_sqlparser::parser::Parser;
use sea_orm::sea_query::{
    Alias, CommonTableExpression, Expr, OnConflict, Query, QueryStatementBuilder, SelectStatement,
    UnionType, WithClause,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, DerivePartialModel, EntityTrait,
    FromQueryResult, IntoActiveModel, JoinType, Order, PaginatorTrait, QueryFilter, QuerySelect,
    RelationTrait, Set, Statement,
};
use thiserror_ext::AsReport;
use tracing::warn;

use crate::barrier::SharedFragmentInfo;
use crate::controller::ObjectModel;
use crate::controller::fragment::FragmentTypeMaskExt;
use crate::controller::scale::resolve_streaming_job_definition;
use crate::model::{FragmentDownstreamRelation, StreamContext};
use crate::{MetaError, MetaResult};

/// This function will construct a query using recursive cte to find all objects[(id, `obj_type`)] that are used by the given object.
///
/// # Examples
///
/// ```
/// use risingwave_meta::controller::utils::construct_obj_dependency_query;
/// use sea_orm::sea_query::*;
/// use sea_orm::*;
///
/// let query = construct_obj_dependency_query(1.into());
///
/// assert_eq!(
///     query.to_string(MysqlQueryBuilder),
///     r#"WITH RECURSIVE `used_by_object_ids` (`used_by`) AS (SELECT `used_by` FROM `object_dependency` WHERE `object_dependency`.`oid` = 1 UNION ALL (SELECT `oid` FROM `object` WHERE `object`.`database_id` = 1 OR `object`.`schema_id` = 1) UNION ALL (SELECT `object_dependency`.`used_by` FROM `object_dependency` INNER JOIN `used_by_object_ids` ON `used_by_object_ids`.`used_by` = `oid`)) SELECT DISTINCT `oid`, `obj_type`, `schema_id`, `database_id` FROM `used_by_object_ids` INNER JOIN `object` ON `used_by_object_ids`.`used_by` = `oid` ORDER BY `oid` DESC"#
/// );
/// assert_eq!(
///     query.to_string(PostgresQueryBuilder),
///     r#"WITH RECURSIVE "used_by_object_ids" ("used_by") AS (SELECT "used_by" FROM "object_dependency" WHERE "object_dependency"."oid" = 1 UNION ALL (SELECT "oid" FROM "object" WHERE "object"."database_id" = 1 OR "object"."schema_id" = 1) UNION ALL (SELECT "object_dependency"."used_by" FROM "object_dependency" INNER JOIN "used_by_object_ids" ON "used_by_object_ids"."used_by" = "oid")) SELECT DISTINCT "oid", "obj_type", "schema_id", "database_id" FROM "used_by_object_ids" INNER JOIN "object" ON "used_by_object_ids"."used_by" = "oid" ORDER BY "oid" DESC"#
/// );
/// assert_eq!(
///     query.to_string(SqliteQueryBuilder),
///     r#"WITH RECURSIVE "used_by_object_ids" ("used_by") AS (SELECT "used_by" FROM "object_dependency" WHERE "object_dependency"."oid" = 1 UNION ALL SELECT "oid" FROM "object" WHERE "object"."database_id" = 1 OR "object"."schema_id" = 1 UNION ALL SELECT "object_dependency"."used_by" FROM "object_dependency" INNER JOIN "used_by_object_ids" ON "used_by_object_ids"."used_by" = "oid") SELECT DISTINCT "oid", "obj_type", "schema_id", "database_id" FROM "used_by_object_ids" INNER JOIN "object" ON "used_by_object_ids"."used_by" = "oid" ORDER BY "oid" DESC"#
/// );
/// ```
pub fn construct_obj_dependency_query(obj_id: ObjectId) -> WithQuery {
    let cte_alias = Alias::new("used_by_object_ids");
    let cte_return_alias = Alias::new("used_by");

    let mut base_query = SelectStatement::new()
        .column(object_dependency::Column::UsedBy)
        .from(ObjectDependency)
        .and_where(object_dependency::Column::Oid.eq(obj_id))
        .to_owned();

    let belonged_obj_query = SelectStatement::new()
        .column(object::Column::Oid)
        .from(Object)
        .and_where(
            object::Column::DatabaseId
                .eq(obj_id)
                .or(object::Column::SchemaId.eq(obj_id)),
        )
        .to_owned();

    let cte_referencing = Query::select()
        .column((ObjectDependency, object_dependency::Column::UsedBy))
        .from(ObjectDependency)
        .inner_join(
            cte_alias.clone(),
            Expr::col((cte_alias.clone(), cte_return_alias.clone()))
                .equals(object_dependency::Column::Oid),
        )
        .to_owned();

    let mut common_table_expr = CommonTableExpression::new();
    common_table_expr
        .query(
            base_query
                .union(UnionType::All, belonged_obj_query)
                .union(UnionType::All, cte_referencing)
                .to_owned(),
        )
        .column(cte_return_alias.clone())
        .table_name(cte_alias.clone());

    SelectStatement::new()
        .distinct()
        .columns([
            object::Column::Oid,
            object::Column::ObjType,
            object::Column::SchemaId,
            object::Column::DatabaseId,
        ])
        .from(cte_alias.clone())
        .inner_join(
            Object,
            Expr::col((cte_alias, cte_return_alias)).equals(object::Column::Oid),
        )
        .order_by(object::Column::Oid, Order::Desc)
        .to_owned()
        .with(
            WithClause::new()
                .recursive(true)
                .cte(common_table_expr)
                .to_owned(),
        )
}

fn to_pb_object_type(obj_type: ObjectType) -> PbObjectType {
    match obj_type {
        ObjectType::Database => PbObjectType::Database,
        ObjectType::Schema => PbObjectType::Schema,
        ObjectType::Table => PbObjectType::Table,
        ObjectType::Source => PbObjectType::Source,
        ObjectType::Sink => PbObjectType::Sink,
        ObjectType::View => PbObjectType::View,
        ObjectType::Index => PbObjectType::Index,
        ObjectType::Function => PbObjectType::Function,
        ObjectType::Connection => PbObjectType::Connection,
        ObjectType::Subscription => PbObjectType::Subscription,
        ObjectType::Secret => PbObjectType::Secret,
    }
}

/// Collect object dependencies with optional object id filtering, and optionally exclude non-created streaming jobs.
async fn list_object_dependencies_impl(
    txn: &DatabaseTransaction,
    object_id: Option<ObjectId>,
    include_creating: bool,
) -> MetaResult<Vec<PbObjectDependency>> {
    let referenced_alias = Alias::new("referenced_obj");
    let mut query = ObjectDependency::find()
        .select_only()
        .columns([
            object_dependency::Column::Oid,
            object_dependency::Column::UsedBy,
        ])
        .column_as(
            Expr::col((referenced_alias.clone(), object::Column::ObjType)),
            "referenced_obj_type",
        )
        .join(
            JoinType::InnerJoin,
            object_dependency::Relation::Object1.def(),
        )
        .join_as(
            JoinType::InnerJoin,
            object_dependency::Relation::Object2.def(),
            referenced_alias.clone(),
        );
    if let Some(object_id) = object_id {
        query = query.filter(object_dependency::Column::UsedBy.eq(object_id));
    }
    let mut obj_dependencies: Vec<PbObjectDependency> = query
        .into_tuple()
        .all(txn)
        .await?
        .into_iter()
        .map(|(oid, used_by, referenced_type)| PbObjectDependency {
            object_id: used_by,
            referenced_object_id: oid,
            referenced_object_type: to_pb_object_type(referenced_type) as i32,
        })
        .collect();

    let mut sink_query = Sink::find()
        .select_only()
        .columns([sink::Column::SinkId, sink::Column::TargetTable])
        .filter(sink::Column::TargetTable.is_not_null());
    if let Some(object_id) = object_id {
        sink_query = sink_query.filter(
            sink::Column::SinkId
                .eq(object_id)
                .or(sink::Column::TargetTable.eq(object_id)),
        );
    }
    let sink_dependencies: Vec<(SinkId, TableId)> = sink_query.into_tuple().all(txn).await?;
    obj_dependencies.extend(sink_dependencies.into_iter().map(|(sink_id, table_id)| {
        PbObjectDependency {
            object_id: table_id.into(),
            referenced_object_id: sink_id.into(),
            referenced_object_type: PbObjectType::Sink as i32,
        }
    }));

    if !include_creating {
        let mut streaming_job_ids = obj_dependencies
            .iter()
            .map(|dependency| dependency.object_id)
            .collect_vec();
        streaming_job_ids.sort_unstable();
        streaming_job_ids.dedup();

        if !streaming_job_ids.is_empty() {
            let non_created_jobs: HashSet<JobId> = StreamingJob::find()
                .select_only()
                .columns([streaming_job::Column::JobId])
                .filter(
                    streaming_job::Column::JobId
                        .is_in(streaming_job_ids)
                        .and(streaming_job::Column::JobStatus.ne(JobStatus::Created)),
                )
                .into_tuple()
                .all(txn)
                .await?
                .into_iter()
                .collect();

            if !non_created_jobs.is_empty() {
                obj_dependencies.retain(|dependency| {
                    !non_created_jobs.contains(&dependency.object_id.as_job_id())
                });
            }
        }
    }

    Ok(obj_dependencies)
}

/// List object dependencies with optional filtering of non-created streaming jobs.
pub async fn list_object_dependencies(
    txn: &DatabaseTransaction,
    include_creating: bool,
) -> MetaResult<Vec<PbObjectDependency>> {
    list_object_dependencies_impl(txn, None, include_creating).await
}

/// List object dependencies for the specified object id without filtering by job status.
pub async fn list_object_dependencies_by_object_id(
    txn: &DatabaseTransaction,
    object_id: ObjectId,
) -> MetaResult<Vec<PbObjectDependency>> {
    list_object_dependencies_impl(txn, Some(object_id), true).await
}

/// This function will construct a query using recursive cte to find if dependent objects are already relying on the target table.
///
/// # Examples
///
/// ```
/// use risingwave_meta::controller::utils::construct_sink_cycle_check_query;
/// use sea_orm::sea_query::*;
/// use sea_orm::*;
///
/// let query = construct_sink_cycle_check_query(1.into(), vec![2.into(), 3.into()]);
///
/// assert_eq!(
///     query.to_string(MysqlQueryBuilder),
///     r#"WITH RECURSIVE `used_by_object_ids_with_sink` (`oid`, `used_by`) AS (SELECT `oid`, `used_by` FROM `object_dependency` WHERE `object_dependency`.`oid` = 1 UNION ALL (SELECT `obj_dependency_with_sink`.`oid`, `obj_dependency_with_sink`.`used_by` FROM (SELECT `oid`, `used_by` FROM `object_dependency` UNION ALL (SELECT `sink_id`, `target_table` FROM `sink` WHERE `sink`.`target_table` IS NOT NULL)) AS `obj_dependency_with_sink` INNER JOIN `used_by_object_ids_with_sink` ON `used_by_object_ids_with_sink`.`used_by` = `obj_dependency_with_sink`.`oid` WHERE `used_by_object_ids_with_sink`.`used_by` <> `used_by_object_ids_with_sink`.`oid`)) SELECT COUNT(`used_by_object_ids_with_sink`.`used_by`) FROM `used_by_object_ids_with_sink` WHERE `used_by_object_ids_with_sink`.`used_by` IN (2, 3)"#
/// );
/// assert_eq!(
///     query.to_string(PostgresQueryBuilder),
///     r#"WITH RECURSIVE "used_by_object_ids_with_sink" ("oid", "used_by") AS (SELECT "oid", "used_by" FROM "object_dependency" WHERE "object_dependency"."oid" = 1 UNION ALL (SELECT "obj_dependency_with_sink"."oid", "obj_dependency_with_sink"."used_by" FROM (SELECT "oid", "used_by" FROM "object_dependency" UNION ALL (SELECT "sink_id", "target_table" FROM "sink" WHERE "sink"."target_table" IS NOT NULL)) AS "obj_dependency_with_sink" INNER JOIN "used_by_object_ids_with_sink" ON "used_by_object_ids_with_sink"."used_by" = "obj_dependency_with_sink"."oid" WHERE "used_by_object_ids_with_sink"."used_by" <> "used_by_object_ids_with_sink"."oid")) SELECT COUNT("used_by_object_ids_with_sink"."used_by") FROM "used_by_object_ids_with_sink" WHERE "used_by_object_ids_with_sink"."used_by" IN (2, 3)"#
/// );
/// assert_eq!(
///     query.to_string(SqliteQueryBuilder),
///     r#"WITH RECURSIVE "used_by_object_ids_with_sink" ("oid", "used_by") AS (SELECT "oid", "used_by" FROM "object_dependency" WHERE "object_dependency"."oid" = 1 UNION ALL SELECT "obj_dependency_with_sink"."oid", "obj_dependency_with_sink"."used_by" FROM (SELECT "oid", "used_by" FROM "object_dependency" UNION ALL SELECT "sink_id", "target_table" FROM "sink" WHERE "sink"."target_table" IS NOT NULL) AS "obj_dependency_with_sink" INNER JOIN "used_by_object_ids_with_sink" ON "used_by_object_ids_with_sink"."used_by" = "obj_dependency_with_sink"."oid" WHERE "used_by_object_ids_with_sink"."used_by" <> "used_by_object_ids_with_sink"."oid") SELECT COUNT("used_by_object_ids_with_sink"."used_by") FROM "used_by_object_ids_with_sink" WHERE "used_by_object_ids_with_sink"."used_by" IN (2, 3)"#
/// );
/// ```
pub fn construct_sink_cycle_check_query(
    target_table: ObjectId,
    dependent_objects: Vec<ObjectId>,
) -> WithQuery {
    let cte_alias = Alias::new("used_by_object_ids_with_sink");
    let depend_alias = Alias::new("obj_dependency_with_sink");

    let mut base_query = SelectStatement::new()
        .columns([
            object_dependency::Column::Oid,
            object_dependency::Column::UsedBy,
        ])
        .from(ObjectDependency)
        .and_where(object_dependency::Column::Oid.eq(target_table))
        .to_owned();

    let query_sink_deps = SelectStatement::new()
        .columns([sink::Column::SinkId, sink::Column::TargetTable])
        .from(Sink)
        .and_where(sink::Column::TargetTable.is_not_null())
        .to_owned();

    let cte_referencing = Query::select()
        .column((depend_alias.clone(), object_dependency::Column::Oid))
        .column((depend_alias.clone(), object_dependency::Column::UsedBy))
        .from_subquery(
            SelectStatement::new()
                .columns([
                    object_dependency::Column::Oid,
                    object_dependency::Column::UsedBy,
                ])
                .from(ObjectDependency)
                .union(UnionType::All, query_sink_deps)
                .to_owned(),
            depend_alias.clone(),
        )
        .inner_join(
            cte_alias.clone(),
            Expr::col((cte_alias.clone(), object_dependency::Column::UsedBy))
                .eq(Expr::col((depend_alias, object_dependency::Column::Oid))),
        )
        .and_where(
            Expr::col((cte_alias.clone(), object_dependency::Column::UsedBy)).ne(Expr::col((
                cte_alias.clone(),
                object_dependency::Column::Oid,
            ))),
        )
        .to_owned();

    let mut common_table_expr = CommonTableExpression::new();
    common_table_expr
        .query(base_query.union(UnionType::All, cte_referencing).to_owned())
        .columns([
            object_dependency::Column::Oid,
            object_dependency::Column::UsedBy,
        ])
        .table_name(cte_alias.clone());

    SelectStatement::new()
        .expr(Expr::col((cte_alias.clone(), object_dependency::Column::UsedBy)).count())
        .from(cte_alias.clone())
        .and_where(
            Expr::col((cte_alias, object_dependency::Column::UsedBy)).is_in(dependent_objects),
        )
        .to_owned()
        .with(
            WithClause::new()
                .recursive(true)
                .cte(common_table_expr)
                .to_owned(),
        )
}

#[derive(Clone, DerivePartialModel, FromQueryResult, Debug)]
#[sea_orm(entity = "Object")]
pub struct PartialObject {
    pub oid: ObjectId,
    pub obj_type: ObjectType,
    pub schema_id: Option<SchemaId>,
    pub database_id: Option<DatabaseId>,
}

impl From<object::Model> for PartialObject {
    fn from(object: object::Model) -> Self {
        Self {
            oid: object.oid,
            obj_type: object.obj_type,
            schema_id: object.schema_id,
            database_id: object.database_id,
        }
    }
}

pub async fn get_belong_objects<C>(db: &C, object_id: ObjectId) -> MetaResult<Vec<object::Model>>
where
    C: ConnectionTrait,
{
    get_belong_objects_by_ids(db, [object_id]).await
}

/// Returns all objects that transitively belong to `object_ids`, excluding `object_ids`.
pub async fn get_belong_objects_by_ids<C>(
    db: &C,
    object_ids: impl IntoIterator<Item = ObjectId>,
) -> MetaResult<Vec<object::Model>>
where
    C: ConnectionTrait,
{
    let mut parents = object_ids.into_iter().collect_vec();
    let mut visited = parents.iter().copied().collect::<HashSet<_>>();
    let mut objects = vec![];
    loop {
        let children = Object::find()
            .filter(object::Column::BelongToOid.is_in(parents))
            .all(db)
            .await?
            .into_iter()
            .filter(|object| visited.insert(object.oid))
            .collect_vec();
        if children.is_empty() {
            break;
        }

        parents = children.iter().map(|object| object.oid).collect();
        objects.extend(children);
    }

    Ok(objects)
}

#[derive(Clone, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "Fragment")]
pub struct PartialFragmentStateTables {
    pub fragment_id: FragmentId,
    pub job_id: ObjectId,
    pub state_table_ids: TableIdArray,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct PartialActorLocation {
    pub actor_id: ActorId,
    pub fragment_id: FragmentId,
    pub worker_id: WorkerId,
}

#[derive(FromQueryResult, Debug, Eq, PartialEq, Clone)]
pub struct FragmentDesc {
    pub fragment_id: FragmentId,
    pub job_id: JobId,
    pub fragment_type_mask: i32,
    pub distribution_type: DistributionType,
    pub state_table_ids: TableIdArray,
    pub parallelism: i64,
    pub vnode_count: i32,
    pub stream_node: StreamNode,
    pub parallelism_policy: String,
}

/// List all objects that are using the given one in a cascade way. It runs a recursive CTE to find all the dependencies.
pub async fn get_referring_objects_cascade<C>(
    obj_id: ObjectId,
    object_type: ObjectType,
    db: &C,
) -> MetaResult<Vec<PartialObject>>
where
    C: ConnectionTrait,
{
    let query = construct_obj_dependency_query(obj_id);
    let (sql, values) = query.build_any(&*db.get_database_backend().get_query_builder());
    let mut objects = PartialObject::find_by_statement(Statement::from_sql_and_values(
        db.get_database_backend(),
        sql,
        values,
    ))
    .all(db)
    .await?;

    let target_objects = std::iter::once((obj_id, object_type))
        .chain(objects.iter().map(|object| (object.oid, object.obj_type)))
        .collect_vec();
    let mut existing_object_ids: HashSet<ObjectId> =
        objects.iter().map(|object| object.oid).collect();
    for (target_id, target_type) in target_objects {
        let incoming_sink_objects =
            get_incoming_sink_objects_for_target(target_id, target_type, db).await?;
        for incoming_sink_object in incoming_sink_objects {
            if existing_object_ids.insert(incoming_sink_object.oid) {
                objects.push(incoming_sink_object);
            }
        }
    }
    Ok(objects)
}

/// Check if create a sink with given dependent objects into the target table will cause a cycle, return true if it will.
pub async fn check_sink_into_table_cycle<C>(
    target_table: ObjectId,
    dependent_objs: Vec<ObjectId>,
    db: &C,
) -> MetaResult<bool>
where
    C: ConnectionTrait,
{
    if dependent_objs.is_empty() {
        return Ok(false);
    }

    // special check for self referencing
    if dependent_objs.contains(&target_table) {
        return Ok(true);
    }

    let query = construct_sink_cycle_check_query(target_table, dependent_objs);
    let (sql, values) = query.build_any(&*db.get_database_backend().get_query_builder());

    let res = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            values,
        ))
        .await?
        .unwrap();

    let cnt: i64 = res.try_get_by(0)?;

    Ok(cnt != 0)
}

/// Formats SQL options with secret values properly resolved
///
/// This function processes configuration options that may contain sensitive data:
/// - Plaintext options are directly converted to `SqlOption`
/// - Secret options are retrieved from the database and formatted as "SECRET {name}"
///   without exposing the actual secret value
///
/// # Arguments
/// * `txn` - Database transaction for retrieving secrets
/// * `options_with_secret` - Container of options with both plaintext and secret values
///
/// # Returns
/// * `MetaResult<Vec<SqlOption>>` - List of formatted SQL options or error
pub async fn format_with_option_secret_resolved(
    txn: &DatabaseTransaction,
    options_with_secret: &WithOptionsSecResolved,
) -> MetaResult<Vec<SqlOption>> {
    let mut options = Vec::new();
    for (k, v) in options_with_secret.as_plaintext() {
        let sql_option = SqlOption::try_from((k, &v.to_owned()))
            .map_err(|e| MetaError::invalid_parameter(e.to_report_string()))?;
        options.push(sql_option);
    }
    for (name, secret_ref) in options_with_secret.as_secret() {
        let secret_ref_value = resolve_secret_ref_value(txn, secret_ref).await?;
        options.push(SqlOption::from_secret_ref(name, secret_ref_value));
    }
    Ok(options)
}

async fn resolve_secret_ref_value(
    txn: &DatabaseTransaction,
    secret_ref: &PbSecretRef,
) -> MetaResult<SecretRefValue> {
    let (secret, object) = Secret::find_by_id(secret_ref.secret_id)
        .find_also_related(Object)
        .one(txn)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_ref.secret_id))?;
    let object =
        object.ok_or_else(|| MetaError::catalog_id_not_found("object", secret_ref.secret_id))?;
    let schema_id = object.schema_id.ok_or_else(|| {
        MetaError::invalid_parameter(format!(
            "secret {} does not belong to a schema",
            secret_ref.secret_id
        ))
    })?;
    let schema = Schema::find_by_id(schema_id)
        .one(txn)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;

    let ref_as = match PbRefAsType::try_from(secret_ref.ref_as) {
        Ok(PbRefAsType::File) => SecretRefAsType::File,
        Ok(PbRefAsType::Text | PbRefAsType::Unspecified) => SecretRefAsType::Text,
        Err(_) => {
            return Err(MetaError::invalid_parameter(format!(
                "invalid reference type {} for secret {}",
                secret_ref.ref_as, secret_ref.secret_id
            )));
        }
    };

    Ok(SecretRefValue {
        secret_name: ObjectName(vec![
            Ident::from_real_value(&schema.name),
            Ident::from_real_value(&secret.name),
        ]),
        ref_as,
    })
}

/// `ensure_object_id` ensures the existence of target object in the cluster.
pub async fn ensure_object_id<C>(
    object_type: ObjectType,
    obj_id: impl Into<ObjectId>,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let obj_id = obj_id.into();
    let count = Object::find_by_id(obj_id).count(db).await?;
    if count == 0 {
        return Err(MetaError::catalog_id_not_found(
            object_type.as_str(),
            obj_id,
        ));
    }
    Ok(())
}

pub async fn ensure_job_not_canceled<C>(job_id: JobId, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Object::find_by_id(job_id).count(db).await?;
    if count == 0 {
        return Err(MetaError::cancelled(format!(
            "job {} might be cancelled manually or by recovery",
            job_id
        )));
    }
    Ok(())
}

/// `ensure_user_id` ensures the existence of target user in the cluster.
pub async fn ensure_user_id<C>(user_id: UserId, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = User::find_by_id(user_id).count(db).await?;
    if count == 0 {
        return Err(anyhow!("user {} was concurrently dropped", user_id).into());
    }
    Ok(())
}

/// `check_database_name_duplicate` checks whether the database name is already used in the cluster.
pub async fn check_database_name_duplicate<C>(name: &str, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Database::find()
        .filter(database::Column::Name.eq(name))
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated("database", name));
    }
    Ok(())
}

/// `check_function_signature_duplicate` checks whether the function name and its signature is already used in the target namespace.
pub async fn check_function_signature_duplicate<C>(
    pb_function: &PbFunction,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Function::find()
        .inner_join(Object)
        .filter(
            object::Column::DatabaseId
                .eq(pb_function.database_id)
                .and(object::Column::SchemaId.eq(pb_function.schema_id))
                .and(function::Column::Name.eq(&pb_function.name))
                .and(
                    function::Column::ArgTypes
                        .eq(DataTypeArray::from(pb_function.arg_types.clone())),
                ),
        )
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated("function", &pb_function.name));
    }
    Ok(())
}

/// `check_connection_name_duplicate` checks whether the connection name is already used in the target namespace.
pub async fn check_connection_name_duplicate<C>(
    pb_connection: &PbConnection,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Connection::find()
        .inner_join(Object)
        .filter(
            object::Column::DatabaseId
                .eq(pb_connection.database_id)
                .and(object::Column::SchemaId.eq(pb_connection.schema_id))
                .and(connection::Column::Name.eq(&pb_connection.name)),
        )
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated(
            "connection",
            &pb_connection.name,
        ));
    }
    Ok(())
}

pub async fn check_secret_name_duplicate<C>(pb_secret: &PbSecret, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Secret::find()
        .inner_join(Object)
        .filter(
            object::Column::DatabaseId
                .eq(pb_secret.database_id)
                .and(object::Column::SchemaId.eq(pb_secret.schema_id))
                .and(secret::Column::Name.eq(&pb_secret.name)),
        )
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated("secret", &pb_secret.name));
    }
    Ok(())
}

pub async fn check_subscription_name_duplicate<C>(
    pb_subscription: &PbSubscription,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Subscription::find()
        .inner_join(Object)
        .filter(
            object::Column::DatabaseId
                .eq(pb_subscription.database_id)
                .and(object::Column::SchemaId.eq(pb_subscription.schema_id))
                .and(subscription::Column::Name.eq(&pb_subscription.name)),
        )
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated(
            "subscription",
            &pb_subscription.name,
        ));
    }
    Ok(())
}

/// `check_user_name_duplicate` checks whether the user is already existed in the cluster.
pub async fn check_user_name_duplicate<C>(name: &str, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = User::find()
        .filter(user::Column::Name.eq(name))
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated("user", name));
    }
    Ok(())
}

/// `check_relation_name_duplicate` checks whether the relation name is already used in the target namespace.
pub async fn check_relation_name_duplicate<C>(
    name: &str,
    database_id: DatabaseId,
    schema_id: SchemaId,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    macro_rules! check_duplicated {
        ($obj_type:expr, $entity:ident, $table:ident) => {
            let object_id = Object::find()
                .select_only()
                .column(object::Column::Oid)
                .inner_join($entity)
                .filter(
                    object::Column::DatabaseId
                        .eq(Some(database_id))
                        .and(object::Column::SchemaId.eq(Some(schema_id)))
                        .and($table::Column::Name.eq(name)),
                )
                .into_tuple::<ObjectId>()
                .one(db)
                .await?;
            if let Some(oid) = object_id {
                let check_creation = if $obj_type == ObjectType::View {
                    false
                } else if $obj_type == ObjectType::Source {
                    let source_info = Source::find_by_id(oid.as_source_id())
                        .select_only()
                        .column(source::Column::SourceInfo)
                        .into_tuple::<Option<StreamSourceInfo>>()
                        .one(db)
                        .await?
                        .unwrap();
                    source_info.map_or(false, |info| info.to_protobuf().is_shared())
                } else {
                    true
                };
                let job_id = oid.as_job_id();
                return if check_creation
                    && !matches!(
                        StreamingJob::find_by_id(job_id)
                            .select_only()
                            .column(streaming_job::Column::JobStatus)
                            .into_tuple::<JobStatus>()
                            .one(db)
                            .await?,
                        Some(JobStatus::Created)
                    ) {
                    Err(MetaError::catalog_under_creation(
                        $obj_type.as_str(),
                        name,
                        job_id,
                    ))
                } else {
                    Err(MetaError::catalog_duplicated($obj_type.as_str(), name))
                };
            }
        };
    }
    check_duplicated!(ObjectType::Table, Table, table);
    check_duplicated!(ObjectType::Source, Source, source);
    check_duplicated!(ObjectType::Sink, Sink, sink);
    check_duplicated!(ObjectType::Index, Index, index);
    check_duplicated!(ObjectType::View, View, view);

    Ok(())
}

/// `check_schema_name_duplicate` checks whether the schema name is already used in the target database.
pub async fn check_schema_name_duplicate<C>(
    name: &str,
    database_id: DatabaseId,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Object::find()
        .inner_join(Schema)
        .filter(
            object::Column::ObjType
                .eq(ObjectType::Schema)
                .and(object::Column::DatabaseId.eq(Some(database_id)))
                .and(schema::Column::Name.eq(name)),
        )
        .count(db)
        .await?;
    if count != 0 {
        return Err(MetaError::catalog_duplicated("schema", name));
    }

    Ok(())
}

/// Validates a RESTRICT drop and returns referring objects that should be dropped together.
///
/// Objects related through `belong_to_oid` are catalog-owned by the target. Indexes are also
/// logically owned by their primary table for dropping, although they remain schema-owned catalog
/// objects. These objects are dropped together with the target; all other referrers prevent the
/// drop.
pub async fn validate_restrict_drop_and_collect_owned_objects<C>(
    object_type: ObjectType,
    object_id: ObjectId,
    db: &C,
) -> MetaResult<Vec<PartialObject>>
where
    C: ConnectionTrait,
{
    let referring_objects = get_referring_objects(object_id, object_type, db).await?;
    let owned_object_ids = get_belong_objects(db, object_id)
        .await?
        .into_iter()
        .map(|object| object.oid)
        .collect::<HashSet<_>>();
    let mut non_owned_objects = referring_objects.clone();
    non_owned_objects.retain(|object| !owned_object_ids.contains(&object.oid));
    if object_type == ObjectType::Table {
        non_owned_objects.retain(|object| object.obj_type != ObjectType::Index);
    }

    if !non_owned_objects.is_empty() {
        let referring_objs_map = non_owned_objects
            .into_iter()
            .into_group_map_by(|o| o.obj_type);
        let mut details = vec![];
        for (obj_type, objs) in referring_objs_map {
            match obj_type {
                ObjectType::Table => {
                    let tables: Vec<(String, String)> = Object::find()
                        .join(JoinType::InnerJoin, object::Relation::Table.def())
                        .join(JoinType::InnerJoin, object::Relation::Database2.def())
                        .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                        .select_only()
                        .column(schema::Column::Name)
                        .column(table::Column::Name)
                        .filter(object::Column::Oid.is_in(objs.iter().map(|o| o.oid)))
                        .into_tuple()
                        .all(db)
                        .await?;
                    details.extend(tables.into_iter().map(|(schema_name, table_name)| {
                        format!(
                            "materialized view {}.{} depends on it",
                            schema_name, table_name
                        )
                    }));
                }
                ObjectType::Sink => {
                    let sinks: Vec<(String, String)> = Object::find()
                        .join(JoinType::InnerJoin, object::Relation::Sink.def())
                        .join(JoinType::InnerJoin, object::Relation::Database2.def())
                        .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                        .select_only()
                        .column(schema::Column::Name)
                        .column(sink::Column::Name)
                        .filter(object::Column::Oid.is_in(objs.iter().map(|o| o.oid)))
                        .into_tuple()
                        .all(db)
                        .await?;
                    details.extend(sinks.into_iter().map(|(schema_name, sink_name)| {
                        format!("sink {}.{} depends on it", schema_name, sink_name)
                    }));
                }
                ObjectType::View => {
                    let views: Vec<(String, String)> = Object::find()
                        .join(JoinType::InnerJoin, object::Relation::View.def())
                        .join(JoinType::InnerJoin, object::Relation::Database2.def())
                        .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                        .select_only()
                        .column(schema::Column::Name)
                        .column(view::Column::Name)
                        .filter(object::Column::Oid.is_in(objs.iter().map(|o| o.oid)))
                        .into_tuple()
                        .all(db)
                        .await?;
                    details.extend(views.into_iter().map(|(schema_name, view_name)| {
                        format!("view {}.{} depends on it", schema_name, view_name)
                    }));
                }
                ObjectType::Subscription => {
                    let subscriptions: Vec<(String, String)> = Object::find()
                        .join(JoinType::InnerJoin, object::Relation::Subscription.def())
                        .join(JoinType::InnerJoin, object::Relation::Database2.def())
                        .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                        .select_only()
                        .column(schema::Column::Name)
                        .column(subscription::Column::Name)
                        .filter(object::Column::Oid.is_in(objs.iter().map(|o| o.oid)))
                        .into_tuple()
                        .all(db)
                        .await?;
                    details.extend(subscriptions.into_iter().map(
                        |(schema_name, subscription_name)| {
                            format!(
                                "subscription {}.{} depends on it",
                                schema_name, subscription_name
                            )
                        },
                    ));
                }
                ObjectType::Source => {
                    let sources: Vec<(String, String)> = Object::find()
                        .join(JoinType::InnerJoin, object::Relation::Source.def())
                        .join(JoinType::InnerJoin, object::Relation::Database2.def())
                        .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                        .select_only()
                        .column(schema::Column::Name)
                        .column(source::Column::Name)
                        .filter(object::Column::Oid.is_in(objs.iter().map(|o| o.oid)))
                        .into_tuple()
                        .all(db)
                        .await?;
                    details.extend(sources.into_iter().map(|(schema_name, view_name)| {
                        format!("source {}.{} depends on it", schema_name, view_name)
                    }));
                }
                ObjectType::Connection => {
                    let connections: Vec<(String, String)> = Object::find()
                        .join(JoinType::InnerJoin, object::Relation::Connection.def())
                        .join(JoinType::InnerJoin, object::Relation::Database2.def())
                        .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                        .select_only()
                        .column(schema::Column::Name)
                        .column(connection::Column::Name)
                        .filter(object::Column::Oid.is_in(objs.iter().map(|o| o.oid)))
                        .into_tuple()
                        .all(db)
                        .await?;
                    details.extend(connections.into_iter().map(|(schema_name, view_name)| {
                        format!("connection {}.{} depends on it", schema_name, view_name)
                    }));
                }
                // only the table, source, sink, subscription, view, connection and index will depend on other objects.
                _ => bail!("unexpected referring object type: {}", obj_type.as_str()),
            }
        }
        if details.is_empty() {
            return Ok(referring_objects);
        }

        return Err(MetaError::permission_denied(format!(
            "{} used by {} other objects. \nDETAIL: {}\n\
            {}",
            object_type.as_str(),
            details.len(),
            details.join("\n"),
            match object_type {
                ObjectType::Function | ObjectType::Connection | ObjectType::Secret =>
                    "HINT: DROP the dependent objects first.",
                ObjectType::Database | ObjectType::Schema => unreachable!(),
                _ => "HINT:  Use DROP ... CASCADE to drop the dependent objects too.",
            }
        )));
    }
    Ok(referring_objects)
}

async fn get_incoming_sink_objects_for_target<C>(
    target_id: ObjectId,
    target_type: ObjectType,
    db: &C,
) -> MetaResult<Vec<PartialObject>>
where
    C: ConnectionTrait,
{
    match target_type {
        ObjectType::Table => {
            let incoming_sink_ids = Sink::find()
                .select_only()
                .column(sink::Column::SinkId)
                .filter(sink::Column::TargetTable.eq(target_id.as_table_id()))
                .into_tuple::<SinkId>()
                .all(db)
                .await?;
            Object::find()
                .filter(object::Column::Oid.is_in(incoming_sink_ids))
                .into_partial_model()
                .all(db)
                .await
                .map_err(Into::into)
        }
        _ => Ok(vec![]),
    }
}

/// List all objects that are using the given one.
pub async fn get_referring_objects<C>(
    object_id: ObjectId,
    object_type: ObjectType,
    db: &C,
) -> MetaResult<Vec<PartialObject>>
where
    C: ConnectionTrait,
{
    let mut objects: Vec<PartialObject> = ObjectDependency::find()
        .filter(object_dependency::Column::Oid.eq(object_id))
        .join(
            JoinType::InnerJoin,
            object_dependency::Relation::Object1.def(),
        )
        .into_partial_model()
        .all(db)
        .await?;

    let incoming_sink_objects =
        get_incoming_sink_objects_for_target(object_id, object_type, db).await?;
    let mut existing_object_ids: HashSet<ObjectId> =
        objects.iter().map(|object| object.oid).collect();
    objects.extend(
        incoming_sink_objects
            .into_iter()
            .filter(|object| existing_object_ids.insert(object.oid)),
    );

    Ok(objects)
}

/// `ensure_schema_empty` ensures that the schema is empty, used by `DROP SCHEMA`.
pub async fn ensure_schema_empty<C>(schema_id: SchemaId, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Object::find()
        .filter(object::Column::SchemaId.eq(Some(schema_id)))
        .count(db)
        .await?;
    if count != 0 {
        return Err(MetaError::permission_denied("schema is not empty"));
    }

    Ok(())
}

/// `list_user_info_by_ids` lists all users' info by their ids.
pub async fn list_user_info_by_ids<C>(
    user_ids: impl IntoIterator<Item = UserId>,
    db: &C,
) -> MetaResult<Vec<PbUserInfo>>
where
    C: ConnectionTrait,
{
    let mut user_infos = vec![];
    for user_id in user_ids {
        let user = User::find_by_id(user_id)
            .one(db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", user_id))?;
        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = get_user_privilege(user_id, db).await?;
        user_infos.push(user_info);
    }
    Ok(user_infos)
}

/// `get_object_owner` returns the owner of the given object.
pub async fn get_object_owner<C>(object_id: ObjectId, db: &C) -> MetaResult<UserId>
where
    C: ConnectionTrait,
{
    let obj_owner: UserId = Object::find_by_id(object_id)
        .select_only()
        .column(object::Column::OwnerId)
        .into_tuple()
        .one(db)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
    Ok(obj_owner)
}

/// `construct_privilege_dependency_query` constructs a query to find all privileges that are dependent on the given one.
///
/// # Examples
///
/// ```
/// use risingwave_meta::controller::utils::construct_privilege_dependency_query;
/// use sea_orm::sea_query::*;
/// use sea_orm::*;
///
/// let query = construct_privilege_dependency_query(vec![1, 2, 3]);
///
/// assert_eq!(
///    query.to_string(MysqlQueryBuilder),
///   r#"WITH RECURSIVE `granted_privilege_ids` (`id`, `user_id`) AS (SELECT `id`, `user_id` FROM `user_privilege` WHERE `user_privilege`.`id` IN (1, 2, 3) UNION ALL (SELECT `user_privilege`.`id`, `user_privilege`.`user_id` FROM `user_privilege` INNER JOIN `granted_privilege_ids` ON `granted_privilege_ids`.`id` = `dependent_id`)) SELECT `id`, `user_id` FROM `granted_privilege_ids`"#
/// );
/// assert_eq!(
///   query.to_string(PostgresQueryBuilder),
///  r#"WITH RECURSIVE "granted_privilege_ids" ("id", "user_id") AS (SELECT "id", "user_id" FROM "user_privilege" WHERE "user_privilege"."id" IN (1, 2, 3) UNION ALL (SELECT "user_privilege"."id", "user_privilege"."user_id" FROM "user_privilege" INNER JOIN "granted_privilege_ids" ON "granted_privilege_ids"."id" = "dependent_id")) SELECT "id", "user_id" FROM "granted_privilege_ids""#
/// );
/// assert_eq!(
///  query.to_string(SqliteQueryBuilder),
///  r#"WITH RECURSIVE "granted_privilege_ids" ("id", "user_id") AS (SELECT "id", "user_id" FROM "user_privilege" WHERE "user_privilege"."id" IN (1, 2, 3) UNION ALL SELECT "user_privilege"."id", "user_privilege"."user_id" FROM "user_privilege" INNER JOIN "granted_privilege_ids" ON "granted_privilege_ids"."id" = "dependent_id") SELECT "id", "user_id" FROM "granted_privilege_ids""#
/// );
/// ```
pub fn construct_privilege_dependency_query(ids: Vec<PrivilegeId>) -> WithQuery {
    let cte_alias = Alias::new("granted_privilege_ids");
    let cte_return_privilege_alias = Alias::new("id");
    let cte_return_user_alias = Alias::new("user_id");

    let mut base_query = SelectStatement::new()
        .columns([user_privilege::Column::Id, user_privilege::Column::UserId])
        .from(UserPrivilege)
        .and_where(user_privilege::Column::Id.is_in(ids))
        .to_owned();

    let cte_referencing = Query::select()
        .columns([
            (UserPrivilege, user_privilege::Column::Id),
            (UserPrivilege, user_privilege::Column::UserId),
        ])
        .from(UserPrivilege)
        .inner_join(
            cte_alias.clone(),
            Expr::col((cte_alias.clone(), cte_return_privilege_alias.clone()))
                .equals(user_privilege::Column::DependentId),
        )
        .to_owned();

    let mut common_table_expr = CommonTableExpression::new();
    common_table_expr
        .query(base_query.union(UnionType::All, cte_referencing).to_owned())
        .columns([
            cte_return_privilege_alias.clone(),
            cte_return_user_alias.clone(),
        ])
        .table_name(cte_alias.clone());

    SelectStatement::new()
        .columns([cte_return_privilege_alias, cte_return_user_alias])
        .from(cte_alias)
        .to_owned()
        .with(
            WithClause::new()
                .recursive(true)
                .cte(common_table_expr)
                .to_owned(),
        )
}

pub async fn get_internal_tables_by_id<C>(job_id: JobId, db: &C) -> MetaResult<Vec<TableId>>
where
    C: ConnectionTrait,
{
    let table_ids: Vec<TableId> = Table::find()
        .select_only()
        .column(table::Column::TableId)
        .filter(
            table::Column::TableType
                .eq(TableType::Internal)
                .and(table::Column::BelongsToJobId.eq(job_id)),
        )
        .into_tuple()
        .all(db)
        .await?;
    Ok(table_ids)
}

pub async fn get_index_state_tables_by_table_id<C>(
    table_id: TableId,
    db: &C,
) -> MetaResult<Vec<TableId>>
where
    C: ConnectionTrait,
{
    let mut index_table_ids: Vec<TableId> = Index::find()
        .select_only()
        .column(index::Column::IndexTableId)
        .filter(index::Column::PrimaryTableId.eq(table_id))
        .into_tuple()
        .all(db)
        .await?;

    if !index_table_ids.is_empty() {
        let internal_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(
                table::Column::TableType
                    .eq(TableType::Internal)
                    .and(table::Column::BelongsToJobId.is_in(index_table_ids.clone())),
            )
            .into_tuple()
            .all(db)
            .await?;

        index_table_ids.extend(internal_table_ids);
    }

    Ok(index_table_ids)
}

/// `get_iceberg_related_object_ids` returns the related object ids of the iceberg source, sink and internal tables.
pub async fn get_iceberg_related_object_ids<C>(
    object_id: ObjectId,
    db: &C,
) -> MetaResult<Vec<ObjectId>>
where
    C: ConnectionTrait,
{
    let object = Object::find_by_id(object_id)
        .one(db)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
    if object.obj_type != ObjectType::Table {
        return Ok(vec![]);
    }

    let table = Table::find_by_id(object_id.as_table_id())
        .one(db)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;
    if !matches!(table.engine, Some(table::Engine::Iceberg)) {
        return Ok(vec![]);
    }

    let database_id = object.database_id.unwrap();
    let schema_id = object.schema_id.unwrap();

    let mut related_objects = vec![];

    let iceberg_sink_name = format!("{}{}", ICEBERG_SINK_PREFIX, table.name);
    let iceberg_sink_id = Sink::find()
        .inner_join(Object)
        .select_only()
        .column(sink::Column::SinkId)
        .filter(
            object::Column::DatabaseId
                .eq(database_id)
                .and(object::Column::SchemaId.eq(schema_id))
                .and(sink::Column::Name.eq(&iceberg_sink_name)),
        )
        .into_tuple::<SinkId>()
        .one(db)
        .await?;
    if let Some(sink_id) = iceberg_sink_id {
        related_objects.push(sink_id.as_object_id());
        let sink_internal_tables = get_internal_tables_by_id(sink_id.as_job_id(), db).await?;
        related_objects.extend(
            sink_internal_tables
                .into_iter()
                .map(|tid| tid.as_object_id()),
        );
    } else {
        warn!(
            "iceberg table {} missing sink {}",
            table.name, iceberg_sink_name
        );
    }

    let iceberg_source_name = format!("{}{}", ICEBERG_SOURCE_PREFIX, table.name);
    let iceberg_source_id = Source::find()
        .inner_join(Object)
        .select_only()
        .column(source::Column::SourceId)
        .filter(
            object::Column::DatabaseId
                .eq(database_id)
                .and(object::Column::SchemaId.eq(schema_id))
                .and(source::Column::Name.eq(&iceberg_source_name)),
        )
        .into_tuple::<SourceId>()
        .one(db)
        .await?;
    if let Some(source_id) = iceberg_source_id {
        related_objects.push(source_id.as_object_id());
    } else {
        warn!(
            "iceberg table {} missing source {}",
            table.name, iceberg_source_name
        );
    }

    Ok(related_objects)
}

/// Load streaming jobs by job ids.
pub(crate) async fn load_streaming_jobs_by_ids<C>(
    txn: &C,
    job_ids: impl IntoIterator<Item = JobId>,
) -> MetaResult<HashMap<JobId, streaming_job::Model>>
where
    C: ConnectionTrait,
{
    let job_ids: HashSet<JobId> = job_ids.into_iter().collect();
    if job_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let jobs = streaming_job::Entity::find()
        .filter(streaming_job::Column::JobId.is_in(job_ids.clone()))
        .all(txn)
        .await?;
    Ok(jobs.into_iter().map(|job| (job.job_id, job)).collect())
}

#[derive(Clone, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "UserPrivilege")]
pub struct PartialUserPrivilege {
    pub id: PrivilegeId,
    pub user_id: UserId,
}

pub async fn get_referring_privileges_cascade<C>(
    ids: Vec<PrivilegeId>,
    db: &C,
) -> MetaResult<Vec<PartialUserPrivilege>>
where
    C: ConnectionTrait,
{
    let query = construct_privilege_dependency_query(ids);
    let (sql, values) = query.build_any(&*db.get_database_backend().get_query_builder());
    let privileges = PartialUserPrivilege::find_by_statement(Statement::from_sql_and_values(
        db.get_database_backend(),
        sql,
        values,
    ))
    .all(db)
    .await?;

    Ok(privileges)
}

/// `ensure_privileges_not_referred` ensures that the privileges are not granted to any other users.
pub async fn ensure_privileges_not_referred<C>(ids: Vec<PrivilegeId>, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = UserPrivilege::find()
        .filter(user_privilege::Column::DependentId.is_in(ids))
        .count(db)
        .await?;
    if count != 0 {
        return Err(MetaError::permission_denied(format!(
            "privileges granted to {} other ones.",
            count
        )));
    }
    Ok(())
}

/// `get_user_privilege` returns the privileges of the given user.
pub async fn get_user_privilege<C>(user_id: UserId, db: &C) -> MetaResult<Vec<PbGrantPrivilege>>
where
    C: ConnectionTrait,
{
    let user_privileges = UserPrivilege::find()
        .find_also_related(Object)
        .filter(user_privilege::Column::UserId.eq(user_id))
        .all(db)
        .await?;
    Ok(user_privileges
        .into_iter()
        .map(|(privilege, object)| {
            let object = object.unwrap();
            let obj = match object.obj_type {
                ObjectType::Database => PbGrantObject::DatabaseId(object.oid.as_database_id()),
                ObjectType::Schema => PbGrantObject::SchemaId(object.oid.as_schema_id()),
                ObjectType::Table | ObjectType::Index => {
                    PbGrantObject::TableId(object.oid.as_table_id())
                }
                ObjectType::Source => PbGrantObject::SourceId(object.oid.as_source_id()),
                ObjectType::Sink => PbGrantObject::SinkId(object.oid.as_sink_id()),
                ObjectType::View => PbGrantObject::ViewId(object.oid.as_view_id()),
                ObjectType::Function => PbGrantObject::FunctionId(object.oid.as_function_id()),
                ObjectType::Connection => {
                    PbGrantObject::ConnectionId(object.oid.as_connection_id())
                }
                ObjectType::Subscription => {
                    PbGrantObject::SubscriptionId(object.oid.as_subscription_id())
                }
                ObjectType::Secret => PbGrantObject::SecretId(object.oid.as_secret_id()),
            };
            PbGrantPrivilege {
                action_with_opts: vec![PbActionWithGrantOption {
                    action: PbAction::from(privilege.action) as _,
                    with_grant_option: privilege.with_grant_option,
                    granted_by: privilege.granted_by as _,
                }],
                object: Some(obj),
            }
        })
        .collect())
}

/// Insert user privileges idempotently using the same key as `idx_user_privilege_item`.
/// An existing privilege is preserved, except that its grant option is upgraded when requested.
pub(crate) async fn upsert_user_privileges<C>(
    db: &C,
    privileges: impl IntoIterator<Item = user_privilege::ActiveModel>,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    for privilege in privileges {
        let mut on_conflict = OnConflict::columns([
            user_privilege::Column::UserId,
            user_privilege::Column::Oid,
            user_privilege::Column::Action,
            user_privilege::Column::GrantedBy,
        ]);
        if *privilege.with_grant_option.as_ref() {
            on_conflict.update_column(user_privilege::Column::WithGrantOption);
        } else {
            // Workaround to support MYSQL for `DO NOTHING`.
            on_conflict.update_column(user_privilege::Column::UserId);
        }

        UserPrivilege::insert(privilege)
            .on_conflict(on_conflict)
            .do_nothing()
            .exec(db)
            .await?;
    }
    Ok(())
}

pub async fn get_table_columns(
    txn: &impl ConnectionTrait,
    id: TableId,
) -> MetaResult<ColumnCatalogArray> {
    let columns = Table::find_by_id(id)
        .select_only()
        .columns([table::Column::Columns])
        .into_tuple::<ColumnCatalogArray>()
        .one(txn)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("table", id))?;
    Ok(columns)
}

/// `grant_default_privileges_automatically` grants default privileges automatically
/// for the given new object. It returns the list of user infos whose privileges are updated.
pub async fn grant_default_privileges_automatically<C>(
    db: &C,
    object_id: impl Into<ObjectId>,
) -> MetaResult<Vec<PbUserInfo>>
where
    C: ConnectionTrait,
{
    let object_id = object_id.into();
    let object = Object::find_by_id(object_id)
        .one(db)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
    assert_ne!(object.obj_type, ObjectType::Database);

    let for_mview_filter = if object.obj_type == ObjectType::Table {
        let table_type = Table::find_by_id(object_id.as_table_id())
            .select_only()
            .column(table::Column::TableType)
            .into_tuple::<TableType>()
            .one(db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;
        user_default_privilege::Column::ForMaterializedView
            .eq(table_type == TableType::MaterializedView)
    } else {
        user_default_privilege::Column::ForMaterializedView.eq(false)
    };
    let schema_filter = if let Some(schema_id) = &object.schema_id {
        user_default_privilege::Column::SchemaId.eq(*schema_id)
    } else {
        user_default_privilege::Column::SchemaId.is_null()
    };

    let default_privileges: Vec<(UserId, UserId, Action, bool)> = UserDefaultPrivilege::find()
        .select_only()
        .columns([
            user_default_privilege::Column::Grantee,
            user_default_privilege::Column::GrantedBy,
            user_default_privilege::Column::Action,
            user_default_privilege::Column::WithGrantOption,
        ])
        .filter(
            user_default_privilege::Column::DatabaseId
                .eq(object.database_id.unwrap())
                .and(schema_filter)
                .and(user_default_privilege::Column::UserId.eq(object.owner_id))
                .and(user_default_privilege::Column::ObjectType.eq(object.obj_type))
                .and(for_mview_filter),
        )
        .into_tuple()
        .all(db)
        .await?;
    if default_privileges.is_empty() {
        return Ok(vec![]);
    }

    let updated_user_ids = default_privileges
        .iter()
        .map(|(grantee, _, _, _)| *grantee)
        .collect::<HashSet<_>>();

    let mut new_privileges = vec![];
    for (grantee, granted_by, action, with_grant_option) in default_privileges {
        new_privileges.push(user_privilege::ActiveModel {
            user_id: Set(grantee),
            oid: Set(object_id),
            granted_by: Set(granted_by),
            action: Set(action),
            with_grant_option: Set(with_grant_option),
            ..Default::default()
        });
        if action == Action::Select {
            // Grant SELECT privilege for internal tables if the action is SELECT.
            let internal_table_ids = get_internal_tables_by_id(object_id.as_job_id(), db).await?;
            new_privileges.extend(internal_table_ids.into_iter().map(|internal_table_id| {
                user_privilege::ActiveModel {
                    user_id: Set(grantee),
                    oid: Set(internal_table_id.as_object_id()),
                    granted_by: Set(granted_by),
                    action: Set(Action::Select),
                    with_grant_option: Set(with_grant_option),
                    ..Default::default()
                }
            }));

            // Additionally, grant SELECT privilege for iceberg related objects if the action is SELECT.
            let iceberg_privilege_object_ids =
                get_iceberg_related_object_ids(object_id, db).await?;
            new_privileges.extend(iceberg_privilege_object_ids.into_iter().map(
                |iceberg_object_id| user_privilege::ActiveModel {
                    user_id: Set(grantee),
                    oid: Set(iceberg_object_id),
                    granted_by: Set(granted_by),
                    action: Set(action),
                    with_grant_option: Set(with_grant_option),
                    ..Default::default()
                },
            ));
        }
    }
    upsert_user_privileges(db, new_privileges).await?;

    let updated_user_infos = list_user_info_by_ids(updated_user_ids, db).await?;
    Ok(updated_user_infos)
}

// todo: remove it after migrated to sql backend.
pub fn extract_grant_obj_id(object: &PbGrantObject) -> ObjectId {
    match object {
        PbGrantObject::DatabaseId(id) => (*id).into(),
        PbGrantObject::SchemaId(id) => (*id).into(),
        PbGrantObject::TableId(id) => (*id).into(),
        PbGrantObject::SourceId(id) => (*id).into(),
        PbGrantObject::SinkId(id) => (*id).into(),
        PbGrantObject::ViewId(id) => (*id).into(),
        PbGrantObject::FunctionId(id) => (*id).into(),
        PbGrantObject::SubscriptionId(id) => (*id).into(),
        PbGrantObject::ConnectionId(id) => (*id).into(),
        PbGrantObject::SecretId(id) => (*id).into(),
    }
}

pub async fn insert_fragment_relations(
    db: &impl ConnectionTrait,
    downstream_fragment_relations: &FragmentDownstreamRelation,
) -> MetaResult<()> {
    let mut relations = vec![];
    for (upstream_fragment_id, downstreams) in downstream_fragment_relations {
        for downstream in downstreams {
            relations.push(
                fragment_relation::Model {
                    source_fragment_id: *upstream_fragment_id as _,
                    target_fragment_id: downstream.downstream_fragment_id as _,
                    dispatcher_type: downstream.dispatcher_type,
                    dist_key_indices: downstream
                        .dist_key_indices
                        .iter()
                        .map(|idx| *idx as i32)
                        .collect_vec()
                        .into(),
                    output_indices: downstream
                        .output_mapping
                        .indices
                        .iter()
                        .map(|idx| *idx as i32)
                        .collect_vec()
                        .into(),
                    output_type_mapping: Some(downstream.output_mapping.types.clone().into()),
                }
                .into_active_model(),
            );
        }
    }
    if !relations.is_empty() {
        FragmentRelation::insert_many(relations).exec(db).await?;
    }
    Ok(())
}

pub fn compose_dispatchers(
    source_fragment_distribution: DistributionType,
    source_fragment_actors: &HashMap<crate::model::ActorId, Option<Bitmap>>,
    target_fragment_id: crate::model::FragmentId,
    target_fragment_distribution: DistributionType,
    target_fragment_actors: &HashMap<crate::model::ActorId, Option<Bitmap>>,
    dispatcher_type: DispatcherType,
    dist_key_indices: Vec<u32>,
    output_mapping: PbDispatchOutputMapping,
) -> (
    HashMap<crate::model::ActorId, PbDispatcher>,
    Option<HashMap<crate::model::ActorId, crate::model::ActorId>>,
) {
    match dispatcher_type {
        DispatcherType::Hash => {
            let dispatcher = PbDispatcher {
                r#type: PbDispatcherType::from(dispatcher_type) as _,
                dist_key_indices,
                output_mapping: output_mapping.into(),
                hash_mapping: Some(
                    ActorMapping::from_bitmaps(
                        &target_fragment_actors
                            .iter()
                            .map(|(actor_id, bitmap)| {
                                (
                                    *actor_id as _,
                                    bitmap
                                        .clone()
                                        .expect("downstream hash dispatch must have distribution"),
                                )
                            })
                            .collect(),
                    )
                    .to_protobuf(),
                ),
                dispatcher_id: target_fragment_id,
                downstream_actor_id: target_fragment_actors.keys().copied().collect(),
            };
            (
                source_fragment_actors
                    .keys()
                    .map(|source_actor_id| (*source_actor_id, dispatcher.clone()))
                    .collect(),
                None,
            )
        }
        DispatcherType::Broadcast | DispatcherType::Simple => {
            let dispatcher = PbDispatcher {
                r#type: PbDispatcherType::from(dispatcher_type) as _,
                dist_key_indices,
                output_mapping: output_mapping.into(),
                hash_mapping: None,
                dispatcher_id: target_fragment_id,
                downstream_actor_id: target_fragment_actors.keys().copied().collect(),
            };
            (
                source_fragment_actors
                    .keys()
                    .map(|source_actor_id| (*source_actor_id, dispatcher.clone()))
                    .collect(),
                None,
            )
        }
        DispatcherType::NoShuffle => {
            let no_shuffle_map = resolve_no_shuffle_actor_mapping(
                source_fragment_distribution,
                source_fragment_actors
                    .iter()
                    .map(|(&id, bitmap)| (id, bitmap)),
                target_fragment_distribution,
                target_fragment_actors
                    .iter()
                    .map(|(&id, bitmap)| (id, bitmap)),
            );
            let dispatchers = no_shuffle_map
                .iter()
                .map(|(&upstream_actor_id, &downstream_actor_id)| {
                    (
                        upstream_actor_id,
                        PbDispatcher {
                            r#type: PbDispatcherType::NoShuffle as _,
                            dist_key_indices: dist_key_indices.clone(),
                            output_mapping: output_mapping.clone().into(),
                            hash_mapping: None,
                            dispatcher_id: target_fragment_id,
                            downstream_actor_id: vec![downstream_actor_id],
                        },
                    )
                })
                .collect();
            (dispatchers, Some(no_shuffle_map))
        }
    }
}

/// Resolve 1:1 actor mapping between two no-shuffle-connected fragments.
///
/// Returns `Vec<(upstream_actor_id, downstream_actor_id)>` pairs. The actor id
/// type is generic so the same logic can be reused both for real `ActorId` mapping
/// and for alignment checks with synthetic ids (e.g. `(WorkerId, actor_idx)`).
///
/// For `Single` distribution the single actors are paired directly.
/// For `Hash` distribution actors are matched by their bitmap's first vnode and
/// full bitmap equality is asserted.
pub fn resolve_no_shuffle_actor_mapping<
    'a,
    ActorId: Copy + Eq + std::hash::Hash + std::fmt::Debug,
>(
    source_fragment_distribution: DistributionType,
    source_fragment_actors: impl IntoIterator<Item = (ActorId, &'a Option<Bitmap>)>,
    target_fragment_distribution: DistributionType,
    target_fragment_actors: impl IntoIterator<Item = (ActorId, &'a Option<Bitmap>)>,
) -> HashMap<ActorId, ActorId> {
    assert_eq!(source_fragment_distribution, target_fragment_distribution);

    match source_fragment_distribution {
        DistributionType::Single => {
            let assert_singleton = |bitmap: &Option<Bitmap>| {
                assert!(
                    bitmap.as_ref().map(|bitmap| bitmap.all()).unwrap_or(true),
                    "not singleton: {:?}",
                    bitmap
                );
            };
            let (source_actor_id, bitmap) =
                Itertools::exactly_one(source_fragment_actors.into_iter())
                    .ok()
                    .expect("Single distribution should have exactly one source actor");
            assert_singleton(bitmap);
            let (target_actor_id, bitmap) =
                Itertools::exactly_one(target_fragment_actors.into_iter())
                    .ok()
                    .expect("Single distribution should have exactly one target actor");
            assert_singleton(bitmap);
            HashMap::from([(source_actor_id, target_actor_id)])
        }
        DistributionType::Hash => {
            // Build target index keyed by first vnode (one pass over target).
            let mut target_by_vnode: HashMap<_, _> = target_fragment_actors
                .into_iter()
                .map(|(actor_id, bitmap)| {
                    let bitmap = bitmap
                        .as_ref()
                        .expect("hash distribution should have bitmap");
                    let first_vnode = bitmap.iter_vnodes().next().expect("non-empty bitmap");
                    (first_vnode, (actor_id, bitmap))
                })
                .collect();

            let target_count = target_by_vnode.len();

            // One pass over source, matching against the target index.
            let mapping: HashMap<_, _> = source_fragment_actors
                .into_iter()
                .map(|(source_actor_id, source_bitmap)| {
                    let source_bitmap = source_bitmap
                        .as_ref()
                        .expect("hash distribution should have bitmap");
                    let first_vnode = source_bitmap
                        .iter_vnodes()
                        .next()
                        .expect("non-empty bitmap");
                    let (target_actor_id, target_bitmap) =
                        target_by_vnode.remove(&first_vnode).unwrap_or_else(|| {
                            panic!(
                                "cannot find matched target actor: {:?} first_vnode {:?}",
                                source_actor_id, first_vnode,
                            )
                        });
                    assert_eq!(
                        source_bitmap, target_bitmap,
                        "bitmap mismatch for source {:?} target {:?} at first_vnode {:?}",
                        source_actor_id, target_actor_id, first_vnode,
                    );
                    (source_actor_id, target_actor_id)
                })
                .collect();

            assert_eq!(
                mapping.len(),
                target_count,
                "no-shuffle should have equal upstream downstream actor count: {} vs {}",
                mapping.len(),
                target_count,
            );

            mapping
        }
    }
}

pub fn rebuild_fragment_mapping(fragment: &SharedFragmentInfo) -> PbFragmentWorkerSlotMapping {
    let fragment_worker_slot_mapping = match fragment.distribution_type {
        DistributionType::Single => {
            let actor = Itertools::exactly_one(fragment.actors.values()).unwrap();
            WorkerSlotMapping::new_single(WorkerSlotId::new(actor.worker_id as _, 0))
        }
        DistributionType::Hash => {
            let actor_bitmaps: HashMap<_, _> = fragment
                .actors
                .iter()
                .map(|(actor_id, actor_info)| {
                    let vnode_bitmap = actor_info
                        .vnode_bitmap
                        .as_ref()
                        .cloned()
                        .expect("actor bitmap shouldn't be none in hash fragment");

                    (*actor_id as hash::ActorId, vnode_bitmap)
                })
                .collect();

            let actor_mapping = ActorMapping::from_bitmaps(&actor_bitmaps);

            let actor_locations = fragment
                .actors
                .iter()
                .map(|(actor_id, actor_info)| (*actor_id as hash::ActorId, actor_info.worker_id))
                .collect();

            actor_mapping.to_worker_slot(&actor_locations)
        }
    };

    PbFragmentWorkerSlotMapping {
        fragment_id: fragment.fragment_id,
        mapping: Some(fragment_worker_slot_mapping.to_protobuf()),
    }
}

/// For the given streaming jobs, returns
/// - All source fragments
/// - All sink fragments
/// - All fragments
pub async fn get_fragments_for_jobs<C>(
    db: &C,
    streaming_jobs: Vec<JobId>,
) -> MetaResult<(
    HashMap<SourceId, BTreeSet<FragmentId>>,
    HashSet<FragmentId>,
    HashSet<FragmentId>,
)>
where
    C: ConnectionTrait,
{
    if streaming_jobs.is_empty() {
        return Ok((HashMap::default(), HashSet::default(), HashSet::default()));
    }

    let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
        .select_only()
        .columns([
            fragment::Column::FragmentId,
            fragment::Column::FragmentTypeMask,
            fragment::Column::StreamNode,
        ])
        .filter(fragment::Column::JobId.is_in(streaming_jobs))
        .into_tuple()
        .all(db)
        .await?;

    let fragment_ids: HashSet<_> = fragments
        .iter()
        .map(|(fragment_id, _, _)| *fragment_id)
        .collect();

    let mut source_fragment_ids: HashMap<SourceId, BTreeSet<FragmentId>> = HashMap::new();
    let mut sink_fragment_ids: HashSet<FragmentId> = HashSet::new();
    for (fragment_id, mask, stream_node) in fragments {
        if FragmentTypeMask::from(mask).contains(FragmentTypeFlag::Source)
            && let Some(source_id) = stream_node.to_protobuf().find_stream_source()
        {
            source_fragment_ids
                .entry(source_id)
                .or_default()
                .insert(fragment_id);
        }
        if FragmentTypeMask::from(mask).contains(FragmentTypeFlag::Sink) {
            sink_fragment_ids.insert(fragment_id);
        }
    }

    Ok((source_fragment_ids, sink_fragment_ids, fragment_ids))
}

/// Build a object group for notifying the deletion of the given objects.
///
/// Note that only id fields are filled in the object info, as the arguments are partial objects.
/// As a result, the returned notification info should only be used for deletion.
pub(crate) fn build_object_group_for_delete(
    partial_objects: Vec<PartialObject>,
) -> NotificationInfo {
    let mut objects = vec![];
    for obj in partial_objects {
        match obj.obj_type {
            ObjectType::Database => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Database(PbDatabase {
                    id: obj.oid.as_database_id(),
                    ..Default::default()
                })),
            }),
            ObjectType::Schema => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Schema(PbSchema {
                    id: obj.oid.as_schema_id(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Table => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Table(PbTable {
                    id: obj.oid.as_table_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Source => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Source(PbSource {
                    id: obj.oid.as_source_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Sink => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Sink(PbSink {
                    id: obj.oid.as_sink_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Subscription => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Subscription(PbSubscription {
                    id: obj.oid.as_subscription_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::View => objects.push(PbObject {
                object_info: Some(PbObjectInfo::View(PbView {
                    id: obj.oid.as_view_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Index => {
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(PbIndex {
                        id: obj.oid.as_index_id(),
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                });
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(PbTable {
                        id: obj.oid.as_table_id(),
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                });
            }
            ObjectType::Function => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Function(PbFunction {
                    id: obj.oid.as_function_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Connection => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Connection(PbConnection {
                    id: obj.oid.as_connection_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
            ObjectType::Secret => objects.push(PbObject {
                object_info: Some(PbObjectInfo::Secret(PbSecret {
                    id: obj.oid.as_secret_id(),
                    schema_id: obj.schema_id.unwrap(),
                    database_id: obj.database_id.unwrap(),
                    ..Default::default()
                })),
            }),
        }
    }
    NotificationInfo::ObjectGroup(PbObjectGroup {
        objects,
        dependencies: vec![],
    })
}

pub fn extract_external_table_name_from_definition(table_definition: &str) -> Option<String> {
    let [mut definition]: [_; 1] = Parser::parse_sql(table_definition)
        .context("unable to parse table definition")
        .inspect_err(|e| {
            tracing::error!(
                target: "auto_schema_change",
                error = %e.as_report(),
                "failed to parse table definition")
        })
        .unwrap()
        .try_into()
        .unwrap();
    if let SqlStatement::CreateTable { cdc_table_info, .. } = &mut definition {
        cdc_table_info
            .clone()
            .map(|cdc_table_info| cdc_table_info.external_table_name)
    } else {
        None
    }
}

/// `rename_relation` renames the target relation and its definition,
/// it commits the changes to the transaction and returns the updated relations and the old name.
pub async fn rename_relation(
    txn: &DatabaseTransaction,
    object_type: ObjectType,
    object_id: ObjectId,
    object_name: &str,
) -> MetaResult<(Vec<PbObject>, String)> {
    use sea_orm::ActiveModelTrait;

    use crate::controller::rename::alter_relation_rename;

    let mut to_update_relations = vec![];
    // rename relation.
    macro_rules! rename_relation {
        ($entity:ident, $table:ident, $identity:ident, $object_id:expr) => {{
            let (mut relation, obj) = $entity::find_by_id($object_id)
                .find_also_related(Object)
                .one(txn)
                .await?
                .unwrap();
            let obj = obj.unwrap();
            let old_name = relation.name.clone();
            relation.name = object_name.into();
            if obj.obj_type != ObjectType::View {
                relation.definition = alter_relation_rename(&relation.definition, object_name);
            }
            let active_model = $table::ActiveModel {
                $identity: Set(relation.$identity),
                name: Set(object_name.into()),
                definition: Set(relation.definition.clone()),
                ..Default::default()
            };
            active_model.update(txn).await?;
            let streaming_job = streaming_job::Entity::find_by_id($object_id.as_raw_id())
                .one(txn)
                .await?;
            to_update_relations.push(PbObject {
                object_info: Some(PbObjectInfo::$entity(
                    ObjectModel(relation, obj, streaming_job).into(),
                )),
            });
            old_name
        }};
    }
    // TODO: check is there any thing to change for shared source?
    let old_name = match object_type {
        ObjectType::Table => {
            let associated_source_id: Option<SourceId> = Source::find()
                .select_only()
                .column(source::Column::SourceId)
                .filter(source::Column::OptionalAssociatedTableId.eq(object_id))
                .into_tuple()
                .one(txn)
                .await?;
            if let Some(source_id) = associated_source_id {
                rename_relation!(Source, source, source_id, source_id);
            }
            rename_relation!(Table, table, table_id, object_id.as_table_id())
        }
        ObjectType::Source => {
            rename_relation!(Source, source, source_id, object_id.as_source_id())
        }
        ObjectType::Sink => rename_relation!(Sink, sink, sink_id, object_id.as_sink_id()),
        ObjectType::Subscription => {
            rename_relation!(
                Subscription,
                subscription,
                subscription_id,
                object_id.as_subscription_id()
            )
        }
        ObjectType::View => rename_relation!(View, view, view_id, object_id.as_view_id()),
        ObjectType::Index => {
            let (mut index, obj) = Index::find_by_id(object_id.as_index_id())
                .find_also_related(Object)
                .one(txn)
                .await?
                .unwrap();
            let streaming_job = streaming_job::Entity::find_by_id(index.index_id.as_job_id())
                .one(txn)
                .await?;
            index.name = object_name.into();
            let index_table_id = index.index_table_id;
            let old_name = rename_relation!(Table, table, table_id, index_table_id);

            // the name of index and its associated table is the same.
            let active_model = index::ActiveModel {
                index_id: sea_orm::ActiveValue::Set(index.index_id),
                name: sea_orm::ActiveValue::Set(object_name.into()),
                ..Default::default()
            };
            active_model.update(txn).await?;
            to_update_relations.push(PbObject {
                object_info: Some(PbObjectInfo::Index(
                    ObjectModel(index, obj.unwrap(), streaming_job).into(),
                )),
            });
            old_name
        }
        _ => unreachable!("only relation name can be altered."),
    };

    Ok((to_update_relations, old_name))
}

pub async fn get_database_resource_group<C>(txn: &C, database_id: DatabaseId) -> MetaResult<String>
where
    C: ConnectionTrait,
{
    let database_resource_group: Option<String> = Database::find_by_id(database_id)
        .select_only()
        .column(database::Column::ResourceGroup)
        .into_tuple()
        .one(txn)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;

    Ok(database_resource_group.unwrap_or_else(|| DEFAULT_RESOURCE_GROUP.to_owned()))
}

pub async fn get_existing_job_resource_group<C>(
    txn: &C,
    streaming_job_id: JobId,
) -> MetaResult<String>
where
    C: ConnectionTrait,
{
    let (job_specific_resource_group, database_resource_group): (Option<String>, Option<String>) =
        StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .column(streaming_job::Column::SpecificResourceGroup)
            .column(database::Column::ResourceGroup)
            .into_tuple()
            .one(txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

    Ok(job_specific_resource_group.unwrap_or_else(|| {
        database_resource_group.unwrap_or_else(|| DEFAULT_RESOURCE_GROUP.to_owned())
    }))
}

pub fn filter_workers_by_resource_group(
    workers: &HashMap<WorkerId, WorkerNode>,
    resource_group: &str,
) -> BTreeSet<WorkerId> {
    workers
        .iter()
        .filter(|&(_, worker)| {
            worker
                .resource_group()
                .map(|node_label| node_label.as_str() == resource_group)
                .unwrap_or(false)
        })
        .map(|(id, _)| *id)
        .collect()
}

/// `rename_relation_refer` updates the definition of relations that refer to the target one,
/// it commits the changes to the transaction and returns all the updated relations.
pub async fn rename_relation_refer(
    txn: &DatabaseTransaction,
    object_type: ObjectType,
    object_id: ObjectId,
    object_name: &str,
    old_name: &str,
) -> MetaResult<Vec<PbObject>> {
    use sea_orm::ActiveModelTrait;

    use crate::controller::rename::alter_relation_rename_refs;

    let mut to_update_relations = vec![];
    macro_rules! rename_relation_ref {
        ($entity:ident, $table:ident, $identity:ident, $object_id:expr) => {{
            let (mut relation, obj) = $entity::find_by_id($object_id)
                .find_also_related(Object)
                .one(txn)
                .await?
                .unwrap();
            relation.definition =
                alter_relation_rename_refs(&relation.definition, old_name, object_name);
            let active_model = $table::ActiveModel {
                $identity: Set(relation.$identity),
                definition: Set(relation.definition.clone()),
                ..Default::default()
            };
            active_model.update(txn).await?;
            let streaming_job = streaming_job::Entity::find_by_id($object_id.as_raw_id())
                .one(txn)
                .await?;
            to_update_relations.push(PbObject {
                object_info: Some(PbObjectInfo::$entity(
                    ObjectModel(relation, obj.unwrap(), streaming_job).into(),
                )),
            });
        }};
    }
    let objs = get_referring_objects(object_id, object_type, txn).await?;

    for obj in objs {
        match obj.obj_type {
            ObjectType::Table => {
                rename_relation_ref!(Table, table, table_id, obj.oid.as_table_id())
            }
            ObjectType::Sink => {
                rename_relation_ref!(Sink, sink, sink_id, obj.oid.as_sink_id())
            }
            ObjectType::Subscription => {
                rename_relation_ref!(
                    Subscription,
                    subscription,
                    subscription_id,
                    obj.oid.as_subscription_id()
                )
            }
            ObjectType::View => rename_relation_ref!(View, view, view_id, obj.oid.as_view_id()),
            ObjectType::Index => {
                let index_table_id: Option<TableId> = Index::find_by_id(obj.oid.as_index_id())
                    .select_only()
                    .column(index::Column::IndexTableId)
                    .into_tuple()
                    .one(txn)
                    .await?;
                rename_relation_ref!(Table, table, table_id, index_table_id.unwrap());
            }
            _ => {
                bail!(
                    "only the table, sink, subscription, view and index will depend on other objects."
                )
            }
        }
    }

    Ok(to_update_relations)
}

/// Validate that subscription can be safely deleted, meeting any of the following conditions:
/// 1. The upstream table is not referred to by any cross-db mv.
/// 2. After deleting the subscription, the upstream table still has at least one subscription.
pub async fn validate_subscription_deletion<C>(
    txn: &C,
    subscription_id: SubscriptionId,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let upstream_table_id: ObjectId = Subscription::find_by_id(subscription_id)
        .select_only()
        .column(subscription::Column::DependentTableId)
        .into_tuple()
        .one(txn)
        .await?
        .ok_or_else(|| MetaError::catalog_id_not_found("subscription", subscription_id))?;

    let cnt = Subscription::find()
        .filter(subscription::Column::DependentTableId.eq(upstream_table_id))
        .count(txn)
        .await?;
    if cnt > 1 {
        // Ensure that at least one subscription is remained for the upstream table
        // once the subscription is dropped.
        return Ok(());
    }

    // Ensure that the upstream table is not referred by any cross-db mv.
    let obj_alias = Alias::new("o1");
    let used_by_alias = Alias::new("o2");
    let count = ObjectDependency::find()
        .join_as(
            JoinType::InnerJoin,
            object_dependency::Relation::Object2.def(),
            obj_alias.clone(),
        )
        .join_as(
            JoinType::InnerJoin,
            object_dependency::Relation::Object1.def(),
            used_by_alias.clone(),
        )
        .filter(
            object_dependency::Column::Oid
                .eq(upstream_table_id)
                .and(object_dependency::Column::UsedBy.ne(subscription_id))
                .and(
                    Expr::col((obj_alias, object::Column::DatabaseId))
                        .ne(Expr::col((used_by_alias, object::Column::DatabaseId))),
                ),
        )
        .count(txn)
        .await?;

    if count != 0 {
        return Err(MetaError::permission_denied(format!(
            "Referenced by {} cross-db objects.",
            count
        )));
    }

    Ok(())
}

pub async fn fetch_target_fragments<C>(
    txn: &C,
    src_fragment_id: impl IntoIterator<Item = FragmentId>,
) -> MetaResult<HashMap<FragmentId, Vec<FragmentId>>>
where
    C: ConnectionTrait,
{
    let source_target_fragments: Vec<(FragmentId, FragmentId)> = FragmentRelation::find()
        .select_only()
        .columns([
            fragment_relation::Column::SourceFragmentId,
            fragment_relation::Column::TargetFragmentId,
        ])
        .filter(fragment_relation::Column::SourceFragmentId.is_in(src_fragment_id))
        .into_tuple()
        .all(txn)
        .await?;

    let source_target_fragments = source_target_fragments.into_iter().into_group_map();

    Ok(source_target_fragments)
}

pub async fn get_sink_fragment_by_ids<C>(
    txn: &C,
    sink_ids: Vec<SinkId>,
) -> MetaResult<HashMap<SinkId, FragmentId>>
where
    C: ConnectionTrait,
{
    let sink_num = sink_ids.len();
    let sink_fragment_ids: Vec<(SinkId, FragmentId)> = Fragment::find()
        .select_only()
        .columns([fragment::Column::JobId, fragment::Column::FragmentId])
        .filter(
            fragment::Column::JobId
                .is_in(sink_ids)
                .and(FragmentTypeMask::intersects(FragmentTypeFlag::Sink)),
        )
        .into_tuple()
        .all(txn)
        .await?;

    if sink_fragment_ids.len() != sink_num {
        return Err(anyhow::anyhow!(
            "expected exactly one sink fragment for each sink, but got {} fragments for {} sinks",
            sink_fragment_ids.len(),
            sink_num
        )
        .into());
    }

    Ok(sink_fragment_ids.into_iter().collect())
}

pub async fn has_table_been_migrated<C>(txn: &C, table_id: TableId) -> MetaResult<bool>
where
    C: ConnectionTrait,
{
    let mview_fragment: Vec<i32> = Fragment::find()
        .select_only()
        .column(fragment::Column::FragmentTypeMask)
        .filter(
            fragment::Column::JobId
                .eq(table_id)
                .and(FragmentTypeMask::intersects(FragmentTypeFlag::Mview)),
        )
        .into_tuple()
        .all(txn)
        .await?;

    let mview_fragment_len = mview_fragment.len();
    if mview_fragment_len != 1 {
        bail!(
            "expected exactly one mview fragment for table {}, found {}",
            table_id,
            mview_fragment_len
        );
    }

    let mview_fragment = mview_fragment.into_iter().next().unwrap();
    let migrated =
        FragmentTypeMask::from(mview_fragment).contains(FragmentTypeFlag::UpstreamSinkUnion);

    Ok(migrated)
}

pub async fn check_if_belongs_to_iceberg_table<C>(txn: &C, job_id: JobId) -> MetaResult<bool>
where
    C: ConnectionTrait,
{
    if let Some(engine) = Table::find_by_id(job_id.as_mv_table_id())
        .select_only()
        .column(table::Column::Engine)
        .into_tuple::<table::Engine>()
        .one(txn)
        .await?
        && engine == table::Engine::Iceberg
    {
        return Ok(true);
    }
    if let Some(parent_oid) = Object::find_by_id(job_id)
        .select_only()
        .column(object::Column::BelongToOid)
        .into_tuple::<Option<ObjectId>>()
        .one(txn)
        .await?
        .flatten()
        && Table::find_by_id(parent_oid.as_table_id())
            .filter(table::Column::Engine.eq(table::Engine::Iceberg))
            .one(txn)
            .await?
            .is_some()
    {
        return Ok(true);
    }
    Ok(false)
}

pub async fn find_dirty_iceberg_table_jobs<C>(
    txn: &C,
    database_id: Option<DatabaseId>,
) -> MetaResult<Vec<PartialObject>>
where
    C: ConnectionTrait,
{
    let mut filter_condition = streaming_job::Column::JobStatus
        .ne(JobStatus::Created)
        .and(object::Column::ObjType.is_in([ObjectType::Table, ObjectType::Sink]))
        .and(streaming_job::Column::CreateType.eq(CreateType::Background));
    if let Some(database_id) = database_id {
        filter_condition = filter_condition.and(object::Column::DatabaseId.eq(database_id));
    }
    let creating_table_sink_jobs: Vec<PartialObject> = StreamingJob::find()
        .select_only()
        .columns([
            object::Column::Oid,
            object::Column::ObjType,
            object::Column::SchemaId,
            object::Column::DatabaseId,
        ])
        .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
        .filter(filter_condition)
        .into_partial_model()
        .all(txn)
        .await?;

    let mut dirty_iceberg_table_jobs = vec![];
    for job in creating_table_sink_jobs {
        if check_if_belongs_to_iceberg_table(txn, job.oid.as_job_id()).await? {
            tracing::info!("Found dirty iceberg job with id: {}", job.oid);
            dirty_iceberg_table_jobs.push(job);
        }
    }

    Ok(dirty_iceberg_table_jobs)
}

pub fn build_select_node_list(
    from: &[ColumnCatalog],
    to: &[ColumnCatalog],
) -> MetaResult<Vec<PbExprNode>> {
    let mut exprs = Vec::with_capacity(to.len());
    let idx_by_col_id = from
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.column_desc.as_ref().unwrap().column_id, idx))
        .collect::<HashMap<_, _>>();

    for to_col in to {
        let to_col = to_col.column_desc.as_ref().unwrap();
        let to_col_type_ref = to_col.column_type.as_ref().unwrap();
        let to_col_type = DataType::from(to_col_type_ref);
        if let Some(from_idx) = idx_by_col_id.get(&to_col.column_id) {
            let from_col_type = DataType::from(
                from[*from_idx]
                    .column_desc
                    .as_ref()
                    .unwrap()
                    .column_type
                    .as_ref()
                    .unwrap(),
            );
            if !to_col_type.equals_datatype(&from_col_type) {
                return Err(anyhow!(
                    "Column type mismatch: {:?} != {:?}",
                    from_col_type,
                    to_col_type
                )
                .into());
            }
            exprs.push(PbExprNode {
                function_type: expr_node::Type::Unspecified.into(),
                return_type: Some(to_col_type_ref.clone()),
                rex_node: Some(expr_node::RexNode::InputRef(*from_idx as _)),
            });
        } else {
            let to_default_node =
                if let Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                    expr,
                    ..
                })) = &to_col.generated_or_default_column
                {
                    expr.clone().unwrap()
                } else {
                    let null = Datum::None.to_protobuf();
                    PbExprNode {
                        function_type: expr_node::Type::Unspecified.into(),
                        return_type: Some(to_col_type_ref.clone()),
                        rex_node: Some(expr_node::RexNode::Constant(null)),
                    }
                };
            exprs.push(to_default_node);
        }
    }

    Ok(exprs)
}

#[derive(Clone, Debug, Default)]
pub struct StreamingJobExtraInfo {
    pub timezone: Option<String>,
    pub config_override: Arc<str>,
    pub job_definition: String,
    pub backfill_orders: Option<BackfillOrders>,
    pub refresh_interval_sec: Option<u64>,
}

impl StreamingJobExtraInfo {
    pub fn stream_context(&self) -> StreamContext {
        StreamContext {
            timezone: self.timezone.clone(),
            config_override: self.config_override.clone(),
        }
    }
}

/// Tuple of (`job_id`, `timezone`, `config_override`, `backfill_orders`, `refresh_interval_sec`)
type StreamingJobExtraInfoRow = (
    JobId,
    Option<String>,
    Option<String>,
    Option<BackfillOrders>,
    Option<i64>,
);

pub async fn get_streaming_job_extra_info<C>(
    txn: &C,
    job_ids: Vec<JobId>,
) -> MetaResult<HashMap<JobId, StreamingJobExtraInfo>>
where
    C: ConnectionTrait,
{
    let pairs: Vec<StreamingJobExtraInfoRow> = StreamingJob::find()
        .select_only()
        .columns([
            streaming_job::Column::JobId,
            streaming_job::Column::Timezone,
            streaming_job::Column::ConfigOverride,
            streaming_job::Column::BackfillOrders,
            streaming_job::Column::RefreshIntervalSec,
        ])
        .filter(streaming_job::Column::JobId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let job_ids = job_ids.into_iter().collect();

    let mut definitions = resolve_streaming_job_definition(txn, &job_ids).await?;

    let result = pairs
        .into_iter()
        .map(
            |(job_id, timezone, config_override, backfill_orders, refresh_interval_sec)| {
                let job_definition = definitions.remove(&job_id).unwrap_or_default();
                (
                    job_id,
                    StreamingJobExtraInfo {
                        timezone,
                        config_override: config_override.unwrap_or_default().into(),
                        job_definition,
                        backfill_orders,
                        refresh_interval_sec: refresh_interval_sec.map(|s| s as u64),
                    },
                )
            },
        )
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cdc_table_name() {
        let ddl1 = "CREATE TABLE t1 () FROM pg_source TABLE 'public.t1'";
        let ddl2 = "CREATE TABLE t2 (v1 int) FROM pg_source TABLE 'mydb.t2'";
        assert_eq!(
            extract_external_table_name_from_definition(ddl1),
            Some("public.t1".into())
        );
        assert_eq!(
            extract_external_table_name_from_definition(ddl2),
            Some("mydb.t2".into())
        );
    }
}
