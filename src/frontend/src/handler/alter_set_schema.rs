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

use pgwire::pg_response::StatementType;
use risingwave_sqlparser::ast::{ObjectName, OperateFunctionArg};

use super::alter_utils::validate_table_type_for_alter;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::{Binder, bind_data_type};

// Steps for validation:
// 1. Check permission to alter original object.
// 2. Check duplicate name in the new schema.
// 3. Check permission to create in the new schema.

/// Handle `ALTER [TABLE | [MATERIALIZED] VIEW | SOURCE | SINK | CONNECTION | FUNCTION] <name> SET SCHEMA <schema_name>` statements.
pub async fn handle_alter_set_schema(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    new_schema_name: ObjectName,
    stmt_type: StatementType,
    func_args: Option<Vec<OperateFunctionArg>>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_obj_name) = Binder::resolve_schema_qualified_name(db_name, &obj_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let new_schema_name = Binder::resolve_schema_name(new_schema_name)?;
    let object = {
        let catalog_reader = session.env().catalog_reader().read_guard();

        match stmt_type {
            StatementType::ALTER_TABLE | StatementType::ALTER_MATERIALIZED_VIEW => {
                let (table, old_schema_name) = catalog_reader.get_created_table_by_name(
                    db_name,
                    schema_path,
                    &real_obj_name,
                )?;
                validate_table_type_for_alter(table, stmt_type, "schema")?;
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**table)?;
                catalog_reader.check_relation_name_duplicated(
                    db_name,
                    &new_schema_name,
                    table.name(),
                )?;
                table.id.into()
            }
            StatementType::ALTER_VIEW => {
                let (view, old_schema_name) =
                    catalog_reader.get_view_by_name(db_name, schema_path, &real_obj_name)?;
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**view)?;
                catalog_reader.check_relation_name_duplicated(
                    db_name,
                    &new_schema_name,
                    view.name(),
                )?;
                view.id.into()
            }
            StatementType::ALTER_SOURCE => {
                let (source, old_schema_name) =
                    catalog_reader.get_source_by_name(db_name, schema_path, &real_obj_name)?;
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**source)?;
                catalog_reader.check_relation_name_duplicated(
                    db_name,
                    &new_schema_name,
                    &source.name,
                )?;
                source.id.into()
            }
            StatementType::ALTER_SINK => {
                let (sink, old_schema_name) = catalog_reader.get_created_sink_by_name(
                    db_name,
                    schema_path,
                    &real_obj_name,
                )?;
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**sink)?;
                catalog_reader.check_relation_name_duplicated(
                    db_name,
                    &new_schema_name,
                    &sink.name,
                )?;
                sink.id.into()
            }
            StatementType::ALTER_SUBSCRIPTION => {
                let (subscription, old_schema_name) = catalog_reader.get_subscription_by_name(
                    db_name,
                    schema_path,
                    &real_obj_name,
                )?;
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**subscription)?;
                catalog_reader.check_relation_name_duplicated(
                    db_name,
                    &new_schema_name,
                    &subscription.name,
                )?;
                subscription.id.into()
            }
            StatementType::ALTER_CONNECTION => {
                let (connection, old_schema_name) =
                    catalog_reader.get_connection_by_name(db_name, schema_path, &real_obj_name)?;
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**connection)?;
                catalog_reader.check_connection_name_duplicated(
                    db_name,
                    &new_schema_name,
                    &connection.name,
                )?;
                connection.id.into()
            }
            StatementType::ALTER_FUNCTION => {
                let (function, old_schema_name) = if let Some(args) = func_args {
                    let mut arg_types = Vec::with_capacity(args.len());
                    for arg in args {
                        arg_types.push(bind_data_type(&arg.data_type)?);
                    }
                    catalog_reader.get_function_by_name_args(
                        db_name,
                        schema_path,
                        &real_obj_name,
                        &arg_types,
                    )?
                } else {
                    let (functions, old_schema_name) = catalog_reader.get_functions_by_name(
                        db_name,
                        schema_path,
                        &real_obj_name,
                    )?;
                    if functions.len() > 1 {
                        return Err(ErrorCode::CatalogError(format!("function name {real_obj_name:?} is not unique\nHINT: Specify the argument list to select the function unambiguously.").into()).into());
                    }
                    (
                        functions.into_iter().next().expect("no functions"),
                        old_schema_name,
                    )
                };
                if old_schema_name == new_schema_name {
                    return Ok(RwPgResponse::empty_result(stmt_type));
                }
                session.check_privilege_for_drop_alter(old_schema_name, &**function)?;
                catalog_reader.check_function_name_duplicated(
                    db_name,
                    &new_schema_name,
                    &function.name,
                    &function.arg_types,
                )?;
                function.id.into()
            }
            _ => unreachable!(),
        }
    };

    let (_, new_schema_id) =
        session.get_database_and_schema_id_for_create(Some(new_schema_name))?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_set_schema(object, new_schema_id)
        .await?;

    Ok(RwPgResponse::empty_result(stmt_type))
}
