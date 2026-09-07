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

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{WorkerId, WorkerResource};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "worker_property")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub worker_id: WorkerId,
    pub parallelism: i32,
    pub is_streaming: bool,
    pub is_serving: bool,
    pub is_unschedulable: bool,
    pub internal_rpc_host_addr: Option<String>,
    pub resource_group: Option<String>,
    pub is_iceberg_compactor: bool,
    pub resource: Option<WorkerResource>,
    pub started_at: Option<i64>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::worker::Entity",
        from = "Column::WorkerId",
        to = "super::worker::Column::WorkerId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Worker,
}

impl Related<super::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
