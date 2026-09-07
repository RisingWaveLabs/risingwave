// Copyright 2025 RisingWave Labs
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
use risingwave_expr::bail;
use risingwave_sqlparser::ast::{Ident, ObjectName, SqlOption};

use super::alter_table_column::fetch_table_catalog_for_alter;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::alter_source_props::handle_alter_table_connector_props;
use crate::utils::resolve_connection_ref_and_secret_ref;
use crate::{Binder, WithOptions};

pub async fn handle_alter_table_props(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    changed_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let (original_table, _) = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;
    match original_table.engine() {
        risingwave_common::catalog::Engine::Hummock => {
            handle_alter_table_connector_props(handler_args, table_name, changed_props).await
        }
        risingwave_common::catalog::Engine::Iceberg => {
            handle_alter_iceberg_table_props(handler_args, table_name, changed_props).await
        }
    }
}

pub async fn handle_alter_iceberg_table_props(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    changed_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let (original_table, _) = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;
    let (sink_id, source_id, table_id) = if let Some(sink_name) = original_table.iceberg_sink_name()
        && let Some(source_name) = original_table.iceberg_source_name()
    {
        let mut source_names = table_name.0.clone();
        let mut sink_names = table_name.0.clone();
        source_names.pop();
        sink_names.pop();
        source_names.push(Ident::new_unchecked(source_name));
        sink_names.push(Ident::new_unchecked(sink_name));
        let reader = session.env().catalog_reader().read_guard();
        let (schema_name, real_table_name) =
            Binder::resolve_schema_qualified_name(db_name, &table_name)?;
        let (_schema_name, real_sink_name) =
            Binder::resolve_schema_qualified_name(db_name, &ObjectName(sink_names))?;
        let (_schema_name, real_source_name) =
            Binder::resolve_schema_qualified_name(db_name, &ObjectName(source_names))?;
        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
        let (sink, _schema_name) =
            reader.get_sink_by_name(db_name, schema_path, &real_sink_name, false)?;
        let (source, _schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_table_name, false)?;
        session.check_privilege_for_drop_alter(schema_name, &**sink)?;
        session.check_privilege_for_drop_alter(schema_name, &**source)?;
        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        (sink.id, source.id, table.id)
    } else {
        return Err(ErrorCode::NotSupported(
            "ALTER TABLE With is only supported for iceberg tables".to_owned(),
            "Try `ALTER TABLE .. ADD/DROP COLUMN ...`".to_owned(),
        )
        .into());
    };

    let meta_client = session.env().meta_client();
    let (resolved_with_options, _, connector_conn_ref) = resolve_connection_ref_and_secret_ref(
        WithOptions::try_from(changed_props.as_ref() as &[SqlOption])?,
        &session,
        None,
    )?;
    let (changed_props, changed_secret_refs) = resolved_with_options.into_parts();
    if connector_conn_ref.is_some() {
        bail!("ALTER ICEBERG TABLE does not support CONNECTION")
    }
    meta_client
        .alter_iceberg_table_props(
            table_id,
            sink_id,
            source_id,
            changed_props,
            changed_secret_refs,
            connector_conn_ref,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}
