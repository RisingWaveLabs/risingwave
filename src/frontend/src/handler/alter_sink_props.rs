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

use pgwire::pg_response::StatementType;
use risingwave_sqlparser::ast::{ObjectName, SqlOption};

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::alter_source_props::ensure_alter_props_not_set_by_connection;
use crate::utils::resolve_connection_ref_and_secret_ref;
use crate::{Binder, WithOptions};

pub async fn handle_alter_sink_props(
    handler_args: HandlerArgs,
    sink_name: ObjectName,
    changed_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let user_name = &session.user_name();
    let sink_id = {
        let db_name = &session.database();
        let search_path = session.config().search_path();

        let reader = session.env().catalog_reader().read_guard();
        let (schema_name, real_table_name) =
            Binder::resolve_schema_qualified_name(db_name, &sink_name)?;
        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
        let (sink, schema_name) =
            reader.get_any_sink_by_name(db_name, schema_path, &real_table_name)?;

        if sink.target_table.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(
                "ALTER SINK CONNECTOR is not for SINK INTO TABLE".to_owned(),
            )
            .into());
        }
        session.check_privilege_for_drop_alter(schema_name, &**sink)?;

        ensure_alter_props_not_set_by_connection(
            &reader,
            db_name,
            sink.connection_id,
            &changed_props,
        )?;

        sink.id
    };

    let meta_client = session.env().meta_client();
    let (resolved_with_options, _, connector_conn_ref) = resolve_connection_ref_and_secret_ref(
        WithOptions::try_from(changed_props.as_ref() as &[SqlOption])?,
        &session,
        None,
    )?;
    let (changed_props, changed_secret_refs) = resolved_with_options.into_parts();
    if connector_conn_ref.is_some() {
        return Err(ErrorCode::InvalidInputSyntax(
            "ALTER SINK does not support CONNECTION".to_owned(),
        )
        .into());
    }

    meta_client
        .alter_sink_props(
            sink_id,
            changed_props,
            changed_secret_refs,
            connector_conn_ref, // always None, keep the interface for future extension
        )
        .await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_SINK))
}
