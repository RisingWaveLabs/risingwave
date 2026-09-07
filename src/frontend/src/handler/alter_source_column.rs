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
use risingwave_common::catalog::max_column_id;
use risingwave_connector::source::{SourceEncode, SourceStruct, extract_source_struct};
use risingwave_sqlparser::ast::{AlterSourceOperation, ObjectName};

use super::create_source::{generate_stream_graph_for_source, reject_variant_columns};
use super::create_table::bind_sql_columns;
use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result, RwError};

// Note for future drop column:
// 1. Dependencies of generated columns

/// Handle `ALTER TABLE [ADD] COLUMN` statements.
pub async fn handle_alter_source_column(
    handler_args: HandlerArgs,
    source_name: ObjectName,
    operation: AlterSourceOperation,
) -> Result<RwPgResponse> {
    // Get original definition
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, &source_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let mut catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        (**source).clone()
    };

    if catalog.associated_table_id.is_some() {
        return Err(ErrorCode::NotSupported(
            "alter table with connector with ALTER SOURCE statement".to_owned(),
            "try to use ALTER TABLE instead".to_owned(),
        )
        .into());
    };

    // Currently only allow source without schema registry
    let SourceStruct { encode, .. } = extract_source_struct(&catalog.info)?;
    match encode {
        SourceEncode::Avro | SourceEncode::Protobuf => {
            return Err(ErrorCode::NotSupported(
                "alter source with schema registry".to_owned(),
                "try `ALTER SOURCE .. FORMAT .. ENCODE .. (...)` instead".to_owned(),
            )
            .into());
        }
        SourceEncode::Json if catalog.info.use_schema_registry => {
            return Err(ErrorCode::NotSupported(
                "alter source with schema registry".to_owned(),
                "try `ALTER SOURCE .. FORMAT .. ENCODE .. (...)` instead".to_owned(),
            )
            .into());
        }
        SourceEncode::Invalid | SourceEncode::Native | SourceEncode::None => {
            return Err(RwError::from(ErrorCode::NotSupported(
                format!("alter source with encode {:?}", encode),
                "Only source with encode JSON | BYTES | CSV | PARQUET can be altered".into(),
            )));
        }
        SourceEncode::Json | SourceEncode::Csv | SourceEncode::Bytes | SourceEncode::Parquet => {}
    }

    let columns = &mut catalog.columns;
    match operation {
        AlterSourceOperation::AddColumn { column_def } => {
            let new_column_name = column_def.name.real_value();
            if columns
                .iter()
                .any(|c| c.column_desc.name == new_column_name)
            {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{new_column_name}\" of source \"{source_name}\" already exists"
                )))?
            }

            // add column name is from user, so we still have check for reserved column name
            let mut bound_column = bind_sql_columns(&[column_def], false)?.remove(0);
            // PARQUET reads variant via the extension type; other alterable encodings cannot
            // produce variant values, and this path bypasses the CREATE-time gate.
            if encode != SourceEncode::Parquet {
                reject_variant_columns(
                    std::slice::from_ref(&bound_column),
                    "for this source encoding",
                )?;
            }
            bound_column.column_desc.column_id = max_column_id(columns).next();
            columns.push(bound_column);
            // No need to update the definition here. It will be done by purification later.
        }
        _ => unreachable!(),
    }

    // update version
    catalog.version += 1;
    catalog.fill_purified_create_sql();

    let catalog_writer = session.catalog_writer()?;
    if catalog.info.is_shared() {
        let graph = generate_stream_graph_for_source(handler_args, catalog.clone())?;
        catalog_writer
            .replace_source(catalog.to_prost(), graph)
            .await?
    } else {
        catalog_writer.alter_source(catalog.to_prost()).await?
    };

    Ok(PgResponse::empty_result(StatementType::ALTER_SOURCE))
}

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_alter_source_column_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = r#"create source s_shared (v1 int) with (
            connector = 'kafka',
            topic = 'abc',
            properties.bootstrap.server = 'localhost:29092',
        ) FORMAT PLAIN ENCODE JSON;"#;

        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();

        frontend
            .run_sql_with_session(session.clone(), "SET streaming_use_shared_source TO false;")
            .await
            .unwrap();
        let sql = r#"create source s (v1 int) with (
            connector = 'kafka',
            topic = 'abc',
            properties.bootstrap.server = 'localhost:29092',
          ) FORMAT PLAIN ENCODE JSON;"#;

        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();

        let get_source = |name: &str| {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, name)
                .unwrap()
                .0
                .clone()
        };

        let source = get_source("s");

        let sql = "alter source s_shared add column v2 varchar;";
        frontend.run_sql(sql).await.unwrap();

        let altered_source = get_source("s_shared");
        let altered_columns: BTreeMap<_, _> = altered_source
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Check the new column is added.
        // Check the old columns and IDs are not changed.
        expect_test::expect![[r#"
            {
                "_row_id": (
                    Serial,
                    #0,
                ),
                "_rw_kafka_offset": (
                    Varchar,
                    #4,
                ),
                "_rw_kafka_partition": (
                    Varchar,
                    #3,
                ),
                "_rw_kafka_timestamp": (
                    Timestamptz,
                    #2,
                ),
                "v1": (
                    Int32,
                    #1,
                ),
                "v2": (
                    Varchar,
                    #5,
                ),
            }
        "#]]
        .assert_debug_eq(&altered_columns);

        // Check version
        assert_eq!(source.version + 1, altered_source.version);

        // Check definition
        expect_test::expect!["CREATE SOURCE s_shared (v1 INT, v2 CHARACTER VARYING) WITH (connector = 'kafka', topic = 'abc', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE JSON"].assert_eq(&altered_source.definition);

        let sql = "alter source s add column v2 varchar;";
        frontend.run_sql(sql).await.unwrap();

        let altered_source = get_source("s");
        let altered_columns: BTreeMap<_, _> = altered_source
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Check the new column is added.
        // Check the old columns and IDs are not changed.
        expect_test::expect![[r#"
            {
                "_row_id": (
                    Serial,
                    #0,
                ),
                "_rw_kafka_timestamp": (
                    Timestamptz,
                    #2,
                ),
                "v1": (
                    Int32,
                    #1,
                ),
                "v2": (
                    Varchar,
                    #3,
                ),
            }
        "#]]
        .assert_debug_eq(&altered_columns);

        // Check version
        assert_eq!(source.version + 1, altered_source.version);

        // Check definition
        expect_test::expect!["CREATE SOURCE s (v1 INT, v2 CHARACTER VARYING) WITH (connector = 'kafka', topic = 'abc', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE JSON"].assert_eq(&altered_source.definition);
    }
}
