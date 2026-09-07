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

use risingwave_common::id::{FunctionId, SchemaId, UserId};
use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::{SysCatalogReaderImpl, get_acl_items};
use crate::error::Result;

#[derive(Fields)]
struct RwFunction {
    #[primary_key]
    id: FunctionId,
    name: String,
    schema_id: SchemaId,
    owner: UserId,
    r#type: String,
    arg_type_ids: Vec<i32>,
    return_type_id: i32,
    language: String,
    link: Option<String>,
    acl: Vec<String>,
    always_retry_on_network_error: bool,
    unsafe_skip_materializing_exprs: bool,
    created_at: Option<Timestamptz>,
    created_at_cluster_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_functions")]
fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwFunction>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let users = user_reader.get_all_users();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema
                .iter_function_with_acl(current_user)
                .map(|function| RwFunction {
                    id: function.id,
                    name: function.name.clone(),
                    schema_id: schema.id(),
                    owner: function.owner,
                    r#type: function.kind.to_string(),
                    arg_type_ids: function.arg_types.iter().map(|t| t.to_oid()).collect(),
                    return_type_id: function.return_type.to_oid(),
                    language: function.language.clone(),
                    link: function.link.clone(),
                    acl: get_acl_items(function.id, false, &users, username_map),
                    always_retry_on_network_error: function.always_retry_on_network_error,
                    unsafe_skip_materializing_exprs: function.unsafe_skip_materializing_exprs,
                    created_at: function.created_at_epoch.map(|e| e.as_timestamptz()),
                    created_at_cluster_version: function.created_at_cluster_version.clone(),
                })
        })
        .collect())
}
