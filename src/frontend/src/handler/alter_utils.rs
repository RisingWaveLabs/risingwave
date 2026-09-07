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
use risingwave_common::bail;
use risingwave_pb::id::JobId;
use risingwave_sqlparser::ast::ObjectName;

use crate::Binder;
use crate::catalog::CatalogError;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::{TableCatalog, TableType};
use crate::error::{Result, bail_invalid_input_syntax};
use crate::session::SessionImpl;

pub(super) fn validate_table_type_for_alter(
    table: &TableCatalog,
    alter_stmt_type: StatementType,
    alter_target: &str,
) -> Result<()> {
    match (table.table_type(), alter_stmt_type) {
        (TableType::Internal, _) => {
            // We treat internal tables as not found because they are not user-addressable objects.
            Err(CatalogError::not_found("table", table.name()).into())
        }
        (TableType::Table, StatementType::ALTER_TABLE)
        | (TableType::MaterializedView, StatementType::ALTER_MATERIALIZED_VIEW)
        | (TableType::Index, StatementType::ALTER_INDEX) => Ok(()),
        _ => {
            bail_invalid_input_syntax!(
                "cannot alter {alter_target} of {} {} by {}",
                table.table_type().to_prost().as_str_name(),
                table.name(),
                alter_stmt_type,
            );
        }
    }
}

/// Resolve the **streaming** job id for alter operations.
///
/// This function will decide which catalog to lookup based on the given statement type, which should
/// be one of `ALTER TABLE`, `ALTER MATERIALIZED VIEW`, `ALTER SOURCE`, `ALTER SINK`, `ALTER INDEX`.
pub(super) fn resolve_streaming_job_id_for_alter(
    session: &SessionImpl,
    obj_name: ObjectName,
    alter_stmt_type: StatementType,
    alter_target: &str,
) -> Result<JobId> {
    let db_name = &session.database();
    let (schema_name, real_table_name) = Binder::resolve_schema_qualified_name(db_name, &obj_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
    let reader = session.env().catalog_reader().read_guard();

    let job_id = match alter_stmt_type {
        StatementType::ALTER_TABLE
        | StatementType::ALTER_MATERIALIZED_VIEW
        | StatementType::ALTER_INDEX => {
            let (table, schema_name) =
                reader.get_table_by_name(db_name, schema_path, &real_table_name, true)?;

            validate_table_type_for_alter(table, alter_stmt_type, alter_target)?;

            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            table.id.as_job_id()
        }
        StatementType::ALTER_SOURCE => {
            let (source, schema_name) =
                reader.get_source_by_name(db_name, schema_path, &real_table_name)?;

            if !source.info.is_shared() {
                bail_invalid_input_syntax!(
                    "cannot alter {alter_target} of non-shared source.\n\
                     Use `ALTER MATERIALIZED VIEW` to alter the materialized view using the source instead."
                );
            }

            session.check_privilege_for_drop_alter(schema_name, &**source)?;
            source.id.as_share_source_job_id()
        }
        StatementType::ALTER_SINK => {
            let (sink, schema_name) =
                reader.get_any_sink_by_name(db_name, schema_path, &real_table_name)?;

            session.check_privilege_for_drop_alter(schema_name, &**sink)?;
            sink.id.as_job_id()
        }
        _ => bail!(
            "invalid statement type for alter {alter_target}: {:?}",
            alter_stmt_type
        ),
    };

    Ok(job_id)
}
