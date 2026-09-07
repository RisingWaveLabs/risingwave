// Copyright 2022 RisingWave Labs
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
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use super::util::{LongRunningNotificationAction, execute_with_long_running_notification};
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::catalog::{CatalogError, CatalogErrorInner};
use crate::error::Result;
use crate::handler::HandlerArgs;

pub async fn handle_drop_mv(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    if_exists: bool,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, &table_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let table_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            match reader.get_any_table_by_name(&session.database(), schema_path, &table_name) {
                Ok((t, s)) => (t, s),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::builder(StatementType::DROP_MATERIALIZED_VIEW)
                            .notice(format!(
                                "materialized view \"{}\" does not exist, skipping",
                                table_name
                            ))
                            .into())
                    } else if let CatalogErrorInner::NotFound {
                        object_type: "table",
                        name,
                    } = e.inner()
                    {
                        Err(CatalogError::not_found("materialized view", name).into())
                    } else {
                        Err(e.into())
                    };
                }
            };

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        match table.table_type() {
            TableType::MaterializedView => {}
            _ => return Err(table.bad_drop_error()),
        }

        table.id()
    };

    let catalog_writer = session.catalog_writer()?;

    execute_with_long_running_notification(
        catalog_writer.drop_materialized_view(table_id, cascade, if_exists),
        &session,
        "DROP MATERIALIZED VIEW",
        LongRunningNotificationAction::SuggestRecover,
    )
    .await?;

    Ok(PgResponse::empty_result(
        StatementType::DROP_MATERIALIZED_VIEW,
    ))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_mv_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_create_mv = "create materialized view mv as select v1 from t;";
        let sql_drop_mv = "drop materialized view mv;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_create_mv).await.unwrap();
        frontend.run_sql(sql_drop_mv).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let table =
            catalog_reader.get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv");
        assert!(table.is_err());
    }
}
