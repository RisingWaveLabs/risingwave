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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_pb::catalog::PbComment;
use risingwave_pb::common::ObjectType;
use risingwave_sqlparser::ast::{CommentObject, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};

/// Builds a comment request using the current object-based wire format.
///
/// Deprecated table-specific fields are deliberately left unset. Mixed-version `COMMENT ON`
/// requests may fail during a rolling upgrade, but they cannot be misinterpreted as another object
/// type or column.
fn build_comment(
    object_id: u32,
    object_type: ObjectType,
    column_index: Option<u32>,
    description: Option<String>,
) -> PbComment {
    PbComment {
        column_index,
        description,
        object_id,
        object_type: object_type as i32,
        ..Default::default()
    }
}

pub async fn handle_comment(
    handler_args: HandlerArgs,
    object_type: CommentObject,
    object_name: ObjectName,
    comment: Option<String>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let comment = comment.filter(|s| !s.is_empty());

    let comment = {
        let mut binder = Binder::new_for_ddl(&session);
        match object_type {
            CommentObject::Column => {
                let [tab @ .., col] = object_name.0.as_slice() else {
                    return Err(ErrorCode::BindError(format!(
                        "Invalid column: {}",
                        object_name.real_value()
                    ))
                    .into());
                };
                if tab.is_empty() {
                    return Err(ErrorCode::BindError(format!(
                        "Invalid column: {}",
                        object_name.real_value()
                    ))
                    .into());
                }

                let (schema, table) = Binder::resolve_schema_qualified_name(
                    &session.database(),
                    &ObjectName(tab.to_vec()),
                )?;

                let table = binder.bind_table(schema.as_deref(), &table)?;
                let comment_object_type = match table.table_catalog.table_type() {
                    TableType::Table => ObjectType::Table,
                    TableType::MaterializedView => ObjectType::Mview,
                    _ => {
                        return Err(ErrorCode::WrongObjectType(format!(
                            "{} is not a table or materialized view",
                            table.table_catalog.name
                        ))
                        .into());
                    }
                };
                if table.table_catalog.owner != session.user_id() && !session.is_super_user() {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "must be owner of relation {}",
                        table.table_catalog.name
                    ))
                    .into());
                }
                let column = binder.bind_column(std::slice::from_ref(col))?;

                build_comment(
                    table.table_id.as_raw_id(),
                    comment_object_type,
                    column.as_input_ref().map(|input_ref| input_ref.index as _),
                    comment,
                )
            }
            CommentObject::Table | CommentObject::MaterializedView => {
                let (schema, table) =
                    Binder::resolve_schema_qualified_name(&session.database(), &object_name)?;
                let table = binder.bind_table(schema.as_deref(), &table)?;

                let (expected_table_type, expected_object_name, comment_object_type) =
                    match object_type {
                        CommentObject::Table => (TableType::Table, "table", ObjectType::Table),
                        CommentObject::MaterializedView => (
                            TableType::MaterializedView,
                            "materialized view",
                            ObjectType::Mview,
                        ),
                        CommentObject::Column => unreachable!(),
                    };

                if table.table_catalog.table_type() != expected_table_type {
                    return Err(ErrorCode::WrongObjectType(format!(
                        "{} is not a {}",
                        table.table_catalog.name, expected_object_name
                    ))
                    .into());
                }
                if table.table_catalog.owner != session.user_id() && !session.is_super_user() {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "must be owner of relation {}",
                        table.table_catalog.name
                    ))
                    .into());
                }

                build_comment(
                    table.table_id.as_raw_id(),
                    comment_object_type,
                    None,
                    comment,
                )
            }
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.comment_on(comment).await?;

    Ok(PgResponse::empty_result(StatementType::COMMENT))
}
