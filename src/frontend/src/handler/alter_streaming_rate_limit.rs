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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail;
use risingwave_pb::common::ThrottleType as PbThrottleType;
use risingwave_pb::meta::ThrottleTarget as PbThrottleTarget;
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::{Catalog, SchemaPath};
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::handler::util::{LongRunningNotificationAction, execute_with_long_running_notification};
use crate::session::SessionImpl;
use crate::{Binder, TableCatalog};

fn check_table_mismatch<'a>(
    reader: &'a Catalog,
    session: &SessionImpl,
    db_name: &str,
    schema_path: SchemaPath<'_>,
    table_name: &str,
) -> Result<&'a TableCatalog> {
    let (table, schema_name) = reader.get_table_by_name(db_name, schema_path, table_name, true)?;
    if table.table_type != TableType::Table {
        return Err(
            ErrorCode::InvalidInputSyntax(format!("\"{table_name}\" is not a TABLE",)).into(),
        );
    }
    session.check_privilege_for_drop_alter(schema_name, &**table)?;
    Ok(table)
}

pub async fn handle_alter_streaming_rate_limit(
    handler_args: HandlerArgs,
    throttle_target: PbThrottleTarget,
    throttle_type: PbThrottleType,
    table_name: ObjectName,
    rate_limit: i32,
) -> Result<RwPgResponse> {
    let session = handler_args.clone().session;
    let db_name = &session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, &table_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (stmt_type, id) = match (throttle_target, throttle_type) {
        (PbThrottleTarget::Mv, PbThrottleType::Backfill) => {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_any_table_by_name(db_name, schema_path, &real_table_name)?;
            if table.table_type != TableType::MaterializedView {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a materialized view",
                ))
                .into());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (StatementType::ALTER_MATERIALIZED_VIEW, table.id.as_raw_id())
        }
        (PbThrottleTarget::Source, PbThrottleType::Source) => {
            let reader = session.env().catalog_reader().read_guard();
            let (source, schema_name) =
                reader.get_source_by_name(db_name, schema_path, &real_table_name)?;
            session.check_privilege_for_drop_alter(schema_name, &**source)?;
            (StatementType::ALTER_SOURCE, source.id.as_raw_id())
        }
        (PbThrottleTarget::Table, PbThrottleType::Dml) => {
            let reader = session.env().catalog_reader().read_guard();
            let table =
                check_table_mismatch(&reader, &session, db_name, schema_path, &real_table_name)?;
            (StatementType::ALTER_TABLE, table.id.as_raw_id())
        }
        (PbThrottleTarget::Table, PbThrottleType::Source) => {
            let reader = session.env().catalog_reader().read_guard();
            let table =
                check_table_mismatch(&reader, &session, db_name, schema_path, &real_table_name)?;
            let source_id = if let Some(id) = table.associated_source_id {
                id.as_raw_id()
            } else {
                bail!("ALTER SOURCE_RATE_LIMIT is not for table without source")
            };
            (StatementType::ALTER_TABLE, source_id)
        }
        (PbThrottleTarget::Table, PbThrottleType::Backfill) => {
            let reader = session.env().catalog_reader().read_guard();
            let table =
                check_table_mismatch(&reader, &session, db_name, schema_path, &real_table_name)?;
            if table.cdc_table_type.is_none() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a CDC table",
                ))
                .into());
            }
            (StatementType::ALTER_TABLE, table.id.as_raw_id())
        }
        (PbThrottleTarget::Sink, PbThrottleType::Sink) => {
            let reader = session.env().catalog_reader().read_guard();
            let (sink, schema_name) =
                reader.get_any_sink_by_name(db_name, schema_path, &real_table_name)?;
            if sink.target_table.is_some() {
                bail!("ALTER SINK_RATE_LIMIT is not for sink into table")
            }
            session.check_privilege_for_drop_alter(schema_name, &**sink)?;
            (StatementType::ALTER_SINK, sink.id.as_raw_id())
        }
        (PbThrottleTarget::Sink, PbThrottleType::Backfill) => {
            let reader = session.env().catalog_reader().read_guard();
            let (sink, schema_name) =
                reader.get_any_sink_by_name(db_name, schema_path, &real_table_name)?;
            session.check_privilege_for_drop_alter(schema_name, &**sink)?;
            (StatementType::ALTER_SINK, sink.id.as_raw_id())
        }
        _ => bail!(
            "Unsupported throttle target: {:?} and throttle type: {:?}",
            throttle_target,
            throttle_type
        ),
    };
    execute_with_long_running_notification(
        handle_alter_streaming_rate_limit_by_id(
            &session,
            throttle_target,
            throttle_type,
            id,
            rate_limit,
            stmt_type,
        ),
        &session,
        "ALTER STREAMING RATE LIMIT",
        LongRunningNotificationAction::SuggestRecover,
    )
    .await
}

pub async fn handle_alter_streaming_rate_limit_by_id(
    session: &SessionImpl,
    throttle_target: PbThrottleTarget,
    throttle_type: PbThrottleType,
    id: u32,
    rate_limit: i32,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let meta_client = session.env().meta_client();

    let rate_limit = if rate_limit < 0 {
        None
    } else {
        Some(rate_limit as u32)
    };

    meta_client
        .apply_throttle(throttle_target, throttle_type, id, rate_limit)
        .await?;

    Ok(PgResponse::empty_result(stmt_type))
}
