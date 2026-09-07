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

use risingwave_pb::common::PbObjectType;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{DatabaseId, ObjectId, SchemaId, UserId};

#[derive(
    Clone, Debug, Hash, PartialEq, Eq, Copy, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum ObjectType {
    #[sea_orm(string_value = "DATABASE")]
    Database,
    #[sea_orm(string_value = "SCHEMA")]
    Schema,
    #[sea_orm(string_value = "TABLE")]
    Table,
    #[sea_orm(string_value = "SOURCE")]
    Source,
    #[sea_orm(string_value = "SINK")]
    Sink,
    #[sea_orm(string_value = "VIEW")]
    View,
    #[sea_orm(string_value = "INDEX")]
    Index,
    #[sea_orm(string_value = "FUNCTION")]
    Function,
    #[sea_orm(string_value = "CONNECTION")]
    Connection,
    #[sea_orm(string_value = "SUBSCRIPTION")]
    Subscription,
    #[sea_orm(string_value = "SECRET")]
    Secret,
}

impl ObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectType::Database => "database",
            ObjectType::Schema => "schema",
            ObjectType::Table => "table",
            ObjectType::Source => "source",
            ObjectType::Sink => "sink",
            ObjectType::View => "view",
            ObjectType::Index => "index",
            ObjectType::Function => "function",
            ObjectType::Connection => "connection",
            ObjectType::Subscription => "subscription",
            ObjectType::Secret => "secret",
        }
    }
}

impl From<PbObjectType> for ObjectType {
    fn from(pb_object_type: PbObjectType) -> Self {
        match pb_object_type {
            PbObjectType::Database => ObjectType::Database,
            PbObjectType::Schema => ObjectType::Schema,
            PbObjectType::Table | PbObjectType::Mview => ObjectType::Table,
            PbObjectType::Source => ObjectType::Source,
            PbObjectType::Sink => ObjectType::Sink,
            PbObjectType::View => ObjectType::View,
            PbObjectType::Index => ObjectType::Index,
            PbObjectType::Function => ObjectType::Function,
            PbObjectType::Connection => ObjectType::Connection,
            PbObjectType::Subscription => ObjectType::Subscription,
            PbObjectType::Secret => ObjectType::Secret,
            PbObjectType::Unspecified => {
                unreachable!("Unspecified object type")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "object")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub oid: ObjectId,
    pub obj_type: ObjectType,
    pub owner_id: UserId,
    pub schema_id: Option<SchemaId>,
    pub database_id: Option<DatabaseId>,
    /// The parent object this object belongs to. The self foreign key uses `ON DELETE CASCADE`.
    ///
    /// This is distinct from `object_dependency`, which records semantic references used for
    /// dependency checks and `DROP ... CASCADE`. The belong-to rules are:
    ///
    /// - database: no parent;
    /// - schema: its database;
    /// - named schema objects, including indexes and subscriptions: their schema;
    /// - associated sources: the relation they belong to;
    /// - internal tables: their streaming job;
    /// - implicit Iceberg sinks and sources: their Iceberg table.
    ///
    /// New objects derive `database_id` and `schema_id` from this parent in
    /// `CatalogController::create_object`.
    ///
    /// Object-type validation and cycle prevention remain application responsibilities.
    pub belong_to_oid: Option<ObjectId>,
    pub initialized_at: DateTime,
    pub created_at: DateTime,
    pub initialized_at_cluster_version: Option<String>,
    pub created_at_cluster_version: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::connection::Entity")]
    Connection,
    #[sea_orm(has_many = "super::database::Entity")]
    Database,
    #[sea_orm(has_many = "super::fragment::Entity")]
    Fragment,
    #[sea_orm(has_many = "super::function::Entity")]
    Function,
    #[sea_orm(has_many = "super::index::Entity")]
    Index,
    #[sea_orm(
        belongs_to = "Entity",
        from = "Column::DatabaseId",
        to = "Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SelfRef2,
    #[sea_orm(
        belongs_to = "Entity",
        from = "Column::SchemaId",
        to = "Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SelfRef1,
    #[sea_orm(
        belongs_to = "Entity",
        from = "Column::BelongToOid",
        to = "Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SelfRef3,
    #[sea_orm(
        belongs_to = "super::database::Entity",
        from = "Column::DatabaseId",
        to = "super::database::Column::DatabaseId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Database2,
    #[sea_orm(has_many = "super::schema::Entity")]
    Schema,
    #[sea_orm(has_many = "super::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "super::subscription::Entity")]
    Subscription,
    #[sea_orm(has_many = "super::source::Entity")]
    Source,
    #[sea_orm(has_many = "super::table::Entity")]
    Table,
    #[sea_orm(has_many = "super::streaming_job::Entity")]
    StreamingJob,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::OwnerId",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    User,
    #[sea_orm(has_many = "super::user_privilege::Entity")]
    UserPrivilege,
    #[sea_orm(has_many = "super::view::Entity")]
    View,
    #[sea_orm(
        belongs_to = "super::schema::Entity",
        from = "Column::SchemaId",
        to = "super::schema::Column::SchemaId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Schema2,
}

impl Related<super::connection::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Connection.def()
    }
}

impl Related<super::database::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Database.def()
    }
}

impl Related<super::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl Related<super::function::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Function.def()
    }
}

impl Related<super::index::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Index.def()
    }
}

impl Related<super::schema::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Schema.def()
    }
}

impl Related<super::sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<super::subscription::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Subscription.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

impl Related<super::streaming_job::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::StreamingJob.def()
    }
}

impl Related<super::user::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::User.def()
    }
}

impl Related<super::user_privilege::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UserPrivilege.def()
    }
}

impl Related<super::view::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::View.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
