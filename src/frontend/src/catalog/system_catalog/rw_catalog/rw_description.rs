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

use std::iter;

use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::id::ObjectId;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(objoid, classoid, objsubid)]
struct RwDescription {
    // table_id, view_id, function_id, etc.
    objoid: ObjectId,
    // rw_tables, rw_views, rw_functions, etc.
    classoid: i32,
    // If `objoid` is a relation id, then non-null `objsubid` is column number.
    objsubid: Option<i32>,
    description: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_description")]
fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwDescription>> {
    let build_row = |object_id: ObjectId,
                     catalog_id,
                     index: Option<i32>,
                     description: Option<Box<str>>| RwDescription {
        objoid: object_id,
        classoid: catalog_id,
        objsubid: index,
        description: description.map(|s| s.into()),
    };

    let catalog_reader = reader.catalog_reader.read_guard();
    let rw_catalog =
        catalog_reader.get_schema_by_name(&reader.auth_context.database, RW_CATALOG_SCHEMA_NAME)?;
    let schemas = catalog_reader
        .iter_schemas(&reader.auth_context.database)?
        .filter(|schema| schema.id() != rw_catalog.id());

    let rw_tables_id: i32 = rw_catalog
        .get_system_table_by_name("rw_tables")
        .map(|st| st.id.as_raw_id())
        .unwrap_or_default() as _;
    let rw_materialized_views_id: i32 = rw_catalog
        .get_system_table_by_name("rw_materialized_views")
        .map(|st| st.id.as_raw_id())
        .unwrap_or_default() as _;

    Ok(schemas
        .flat_map(|schema| {
            schema
                .iter_user_table()
                .flat_map(|table| {
                    iter::once(build_row(
                        table.id.as_object_id(),
                        rw_tables_id,
                        None,
                        table.description.as_deref().map(Into::into),
                    ))
                    .chain(table.columns.iter().map(|col| {
                        build_row(
                            table.id.as_object_id(),
                            rw_tables_id,
                            Some(col.column_id().get_id() as _),
                            col.column_desc.description.as_deref().map(Into::into),
                        )
                    }))
                })
                .chain(schema.iter_all_mvs().flat_map(|table| {
                    iter::once(build_row(
                        table.id.as_object_id(),
                        rw_materialized_views_id,
                        None,
                        table.description.as_deref().map(Into::into),
                    ))
                    .chain(table.columns.iter().map(|col| {
                        build_row(
                            table.id.as_object_id(),
                            rw_materialized_views_id,
                            Some(col.column_id().get_id() as _),
                            col.column_desc.description.as_deref().map(Into::into),
                        )
                    }))
                }))
        })
        .collect())
}
