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

use risingwave_common::id::TableId;
use risingwave_pb::catalog::PbSubscription;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::SubscriptionId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "subscription")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub subscription_id: SubscriptionId,
    pub name: String,
    pub retention_seconds: i64,
    pub definition: String,
    pub subscription_state: i32,
    pub dependent_table_id: TableId,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::SubscriptionId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::DependentTableId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    DependentObject,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbSubscription> for ActiveModel {
    fn from(pb_subscription: PbSubscription) -> Self {
        Self {
            subscription_id: Set(pb_subscription.id),
            name: Set(pb_subscription.name),
            retention_seconds: Set(pb_subscription.retention_seconds as _),
            definition: Set(pb_subscription.definition),
            subscription_state: Set(pb_subscription.subscription_state),
            dependent_table_id: Set(pb_subscription.dependent_table_id),
        }
    }
}
