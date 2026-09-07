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

use std::collections::BTreeMap;

use risingwave_pb::catalog::PbFunction;
use risingwave_pb::catalog::function::Kind;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{DataType, DataTypeArray, FunctionId, Property};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum FunctionKind {
    #[sea_orm(string_value = "Scalar")]
    Scalar,
    #[sea_orm(string_value = "Table")]
    Table,
    #[sea_orm(string_value = "Aggregate")]
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "function")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub function_id: FunctionId,
    pub name: String,
    // encode Vec<String> as comma separated string
    pub arg_names: String,
    pub arg_types: DataTypeArray,
    pub return_type: DataType,
    pub language: String,
    pub runtime: Option<String>,
    pub link: Option<String>,
    pub name_in_runtime: Option<String>,
    pub body: Option<String>,
    pub compressed_binary: Option<Vec<u8>>,
    pub kind: FunctionKind,
    // Keep these as dedicated columns rather than entries in `options`.
    pub always_retry_on_network_error: bool,
    pub unsafe_skip_materializing_exprs: bool,
    pub options: Option<Property>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::FunctionId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<Kind> for FunctionKind {
    fn from(kind: Kind) -> Self {
        match kind {
            Kind::Scalar(_) => Self::Scalar,
            Kind::Table(_) => Self::Table,
            Kind::Aggregate(_) => Self::Aggregate,
        }
    }
}

impl From<FunctionKind> for Kind {
    fn from(value: FunctionKind) -> Self {
        match value {
            FunctionKind::Scalar => Self::Scalar(Default::default()),
            FunctionKind::Table => Self::Table(Default::default()),
            FunctionKind::Aggregate => Self::Aggregate(Default::default()),
        }
    }
}

impl From<PbFunction> for ActiveModel {
    fn from(function: PbFunction) -> Self {
        let mut options = BTreeMap::new();
        if let Some(b) = function.is_batched {
            options.insert("batch".to_string(), b.to_string());
        }
        if let Some(b) = function.is_async {
            options.insert("async".to_string(), b.to_string());
        }
        Self {
            function_id: Set(function.id),
            name: Set(function.name),
            arg_names: Set(function.arg_names.join(",")),
            arg_types: Set(DataTypeArray::from(function.arg_types)),
            return_type: Set(DataType::from(&function.return_type.unwrap())),
            language: Set(function.language),
            runtime: Set(function.runtime),
            link: Set(function.link),
            name_in_runtime: Set(function.name_in_runtime),
            body: Set(function.body),
            compressed_binary: Set(function.compressed_binary),
            kind: Set(function.kind.unwrap().into()),
            always_retry_on_network_error: Set(function.always_retry_on_network_error),
            unsafe_skip_materializing_exprs: Set(function.unsafe_skip_materializing_exprs),
            options: Set(Some(options.into())),
        }
    }
}
