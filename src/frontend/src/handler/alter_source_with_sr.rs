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

use std::sync::Arc;

use either::Either;
use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{ColumnCatalog, max_column_id};
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::{EncodeType, FormatType};
use risingwave_sqlparser::ast::{
    CompatibleFormatEncode, CreateSourceStatement, Encode, Format, FormatEncodeOptions, ObjectName,
    SqlOption, Statement,
};

use super::create_source::{
    generate_stream_graph_for_source, schema_has_schema_registry, validate_compatibility,
};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::{ErrorCode, Result};
use crate::handler::create_source::{CreateSourceType, bind_columns_from_source};
use crate::session::SessionImpl;
use crate::utils::resolve_secret_ref_in_with_options;
use crate::{Binder, WithOptions};

fn format_type_to_format(from: FormatType) -> Option<Format> {
    Some(match from {
        FormatType::Unspecified => return None,
        FormatType::Native => Format::Native,
        FormatType::Debezium => Format::Debezium,
        FormatType::DebeziumMongo => Format::DebeziumMongo,
        FormatType::Maxwell => Format::Maxwell,
        FormatType::Canal => Format::Canal,
        FormatType::Upsert => Format::Upsert,
        FormatType::Plain => Format::Plain,
        FormatType::None => Format::None,
    })
}

fn encode_type_to_encode(from: EncodeType) -> Option<Encode> {
    Some(match from {
        EncodeType::Unspecified => return None,
        EncodeType::Native => Encode::Native,
        EncodeType::Avro => Encode::Avro,
        EncodeType::Csv => Encode::Csv,
        EncodeType::Protobuf => Encode::Protobuf,
        EncodeType::Json => Encode::Json,
        EncodeType::Bytes => Encode::Bytes,
        EncodeType::Template => Encode::Template,
        EncodeType::Parquet => Encode::Parquet,
        EncodeType::None => Encode::None,
        EncodeType::Text => Encode::Text,
    })
}

/// Returns the columns in `columns_a` but not in `columns_b`.
///
/// Note:
/// - The comparison is done by name and data type, without checking `ColumnId`.
/// - Hidden columns and `INCLUDE ... AS ...` columns are ignored. Because it's only for the special handling of alter sr.
///   For the newly resolved `columns_from_resolve_source` (created by [`bind_columns_from_source`]), it doesn't contain hidden columns (`_row_id`) and `INCLUDE ... AS ...` columns.
///   This is fragile and we should really refactor it later.
/// - Generated columns are ignored when calculating dropped columns, because they are defined in SQL and should be preserved during schema refresh.
/// - Column with the same name but different data type is considered as a different column, i.e., altering the data type of a column
///   will be treated as dropping the old column and adding a new column. Note that we don't reject here like we do in `ALTER TABLE REFRESH SCHEMA`,
///   because there's no data persistence (thus compatibility concern) in the source case.
fn columns_minus(columns_a: &[ColumnCatalog], columns_b: &[ColumnCatalog]) -> Vec<ColumnCatalog> {
    columns_a
        .iter()
        .filter(|col_a| {
            !col_a.is_hidden()
                && !col_a.is_connector_additional_column()
                && !col_a.is_generated()
                && !columns_b.iter().any(|col_b| {
                    col_a.name() == col_b.name() && col_a.data_type() == col_b.data_type()
                })
        })
        .cloned()
        .collect()
}

/// Fetch the source catalog.
pub fn fetch_source_catalog_with_db_schema_id(
    session: &SessionImpl,
    name: &ObjectName,
) -> Result<Arc<SourceCatalog>> {
    let db_name = &session.database();
    let (schema_name, real_source_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let reader = session.env().catalog_reader().read_guard();
    let (source, schema_name) =
        reader.get_source_by_name(db_name, schema_path, &real_source_name)?;

    session.check_privilege_for_drop_alter(schema_name, &**source)?;

    Ok(Arc::clone(source))
}

/// Check if the original source is created with `FORMAT .. ENCODE ..` clause,
/// and if the FORMAT and ENCODE are modified.
pub fn check_format_encode(
    original_source: &SourceCatalog,
    new_format_encode: &FormatEncodeOptions,
) -> Result<()> {
    let StreamSourceInfo {
        format, row_encode, ..
    } = original_source.info;
    let (Some(old_format), Some(old_row_encode)) = (
        format_type_to_format(FormatType::try_from(format).unwrap()),
        encode_type_to_encode(EncodeType::try_from(row_encode).unwrap()),
    ) else {
        return Err(ErrorCode::NotSupported(
            "altering a legacy source which is not created using `FORMAT .. ENCODE ..` Clause"
                .to_owned(),
            "try this feature by creating a fresh source".to_owned(),
        )
        .into());
    };

    if new_format_encode.format != old_format || new_format_encode.row_encode != old_row_encode {
        bail_not_implemented!(
            "the original definition is FORMAT {:?} ENCODE {:?}, and altering them is not supported yet",
            &old_format,
            &old_row_encode,
        );
    }

    Ok(())
}

/// Refresh the source registry and get the added/dropped columns.
pub async fn refresh_sr_and_get_columns_diff(
    original_source: &SourceCatalog,
    format_encode: &FormatEncodeOptions,
    session: &Arc<SessionImpl>,
) -> Result<(StreamSourceInfo, Vec<ColumnCatalog>, Vec<ColumnCatalog>)> {
    let mut with_properties = original_source.with_properties.clone();
    validate_compatibility(format_encode, &mut with_properties)?;

    if with_properties.is_cdc_connector() {
        bail_not_implemented!("altering a cdc source is not supported");
    }

    let (Some(columns_from_resolve_source), source_info) = bind_columns_from_source(
        session,
        format_encode,
        Either::Right(&with_properties),
        CreateSourceType::for_replace(original_source),
    )
    .await?
    else {
        // Source without schema registry is rejected.
        unreachable!("source without schema registry is rejected")
    };

    let mut added_columns = columns_minus(&columns_from_resolve_source, &original_source.columns);
    // The newly resolved columns' column IDs also starts from 1. They cannot be used directly.
    let mut next_col_id = max_column_id(&original_source.columns).next();
    for col in &mut added_columns {
        col.column_desc.column_id = next_col_id;
        next_col_id = next_col_id.next();
    }
    let dropped_columns = columns_minus(&original_source.columns, &columns_from_resolve_source);
    tracing::debug!(
        ?added_columns,
        ?dropped_columns,
        ?columns_from_resolve_source,
        original_source = ?original_source.columns
    );

    Ok((source_info, added_columns, dropped_columns))
}

fn get_format_encode_from_source(source: &SourceCatalog) -> Result<FormatEncodeOptions> {
    let stmt = source.create_sql_ast()?;
    let format_encode = match stmt {
        Statement::CreateSource {
            stmt: CreateSourceStatement { format_encode, .. },
        } => format_encode,
        // Sources that back a `CREATE TABLE … FORMAT … ENCODE …` statement store the
        // original `CREATE TABLE` SQL as their definition.  Extract format_encode from it.
        Statement::CreateTable {
            format_encode: Some(format_encode),
            ..
        } => format_encode,
        stmt => {
            return Err(ErrorCode::InternalError(format!(
                "expected CREATE SOURCE or CREATE TABLE with FORMAT/ENCODE, got: {:?}",
                stmt
            ))
            .into());
        }
    };
    Ok(format_encode.into_v2_with_warning())
}

pub async fn handler_refresh_schema(
    handler_args: HandlerArgs,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let source = fetch_source_catalog_with_db_schema_id(&handler_args.session, &name)?;
    let format_encode = get_format_encode_from_source(&source)?;
    handle_alter_source_with_sr(handler_args, name, format_encode).await
}

pub async fn handle_alter_source_with_sr(
    handler_args: HandlerArgs,
    name: ObjectName,
    format_encode: FormatEncodeOptions,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let source = fetch_source_catalog_with_db_schema_id(&session, &name)?;
    let mut source = source.as_ref().clone();

    if source.associated_table_id.is_some() {
        return Err(ErrorCode::NotSupported(
            "alter table with connector using ALTER SOURCE statement".to_owned(),
            "try to use ALTER TABLE instead".to_owned(),
        )
        .into());
    };

    check_format_encode(&source, &format_encode)?;

    if !schema_has_schema_registry(&format_encode) {
        return Err(ErrorCode::NotSupported(
            "altering a source without schema registry".to_owned(),
            "try `ALTER SOURCE .. ADD COLUMN ...` instead".to_owned(),
        )
        .into());
    }

    let (source_info, added_columns, dropped_columns) =
        refresh_sr_and_get_columns_diff(&source, &format_encode, &session).await?;

    if !dropped_columns.is_empty() {
        bail_not_implemented!(
            "this altering statement will drop columns, which is not supported yet: {}",
            dropped_columns
                .iter()
                .map(|col| format!("({}: {})", col.name(), col.data_type()))
                .join(", ")
        );
    }

    source.info = source_info;
    source.columns.extend(added_columns);
    source.definition = alter_definition_format_encode(
        source.create_sql_ast_purified()?,
        format_encode.row_options.clone(),
    )?;

    let (format_encode_options, format_encode_secret_ref) = resolve_secret_ref_in_with_options(
        WithOptions::try_from(format_encode.row_options())?,
        session.as_ref(),
    )?
    .into_parts();
    source
        .info
        .format_encode_options
        .extend(format_encode_options);

    source
        .info
        .format_encode_secret_refs
        .extend(format_encode_secret_ref);

    // update version
    source.version += 1;

    let pb_source = source.to_prost();
    let catalog_writer = session.catalog_writer()?;
    if source.info.is_shared() {
        let graph = generate_stream_graph_for_source(handler_args, source.clone())?;
        catalog_writer.replace_source(pb_source, graph).await?
    } else {
        catalog_writer.alter_source(pb_source).await?;
    }
    Ok(RwPgResponse::empty_result(StatementType::ALTER_SOURCE))
}

/// Apply the new `format_encode_options` to the source/table definition.
pub fn alter_definition_format_encode(
    mut stmt: Statement,
    format_encode_options: Vec<SqlOption>,
) -> Result<String> {
    match &mut stmt {
        Statement::CreateSource {
            stmt: CreateSourceStatement { format_encode, .. },
        }
        | Statement::CreateTable {
            format_encode: Some(format_encode),
            ..
        } => {
            match format_encode {
                CompatibleFormatEncode::V2(schema) => {
                    schema.row_options = format_encode_options;
                }
                // TODO: Confirm the behavior of legacy source schema.
                // Legacy source schema should be rejected by the handler and never reaches here.
                CompatibleFormatEncode::RowFormat(_schema) => unreachable!(),
            }
        }
        _ => unreachable!(),
    }

    Ok(stmt.to_string())
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    #[tokio::test]
    async fn test_alter_source_with_sr_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE src
            WITH (
                connector = 'kafka',
                topic = 'test-topic',
                properties.bootstrap.server = 'localhost:29092'
            )
            FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        frontend
            .run_sql_with_session(session.clone(), "SET streaming_use_shared_source TO false;")
            .await
            .unwrap();
        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();

        let get_source = || {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "src")
                .unwrap()
                .0
                .clone()
        };

        let source = get_source();
        expect_test::expect!["CREATE SOURCE src (id INT, country STRUCT<address CHARACTER VARYING, city STRUCT<address CHARACTER VARYING, zipcode CHARACTER VARYING>, zipcode CHARACTER VARYING>, zipcode BIGINT, rate REAL) WITH (connector = 'kafka', topic = 'test-topic', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://')"].assert_eq(&source.create_sql_purified().replace(proto_file.path().to_str().unwrap(), ""));

        let sql = format!(
            r#"ALTER SOURCE src FORMAT UPSERT ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        assert!(
            frontend
                .run_sql(sql)
                .await
                .unwrap_err()
                .to_string()
                .contains("the original definition is FORMAT Plain ENCODE Protobuf")
        );

        let sql = format!(
            r#"ALTER SOURCE src FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordAlterType',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        let res_str = frontend.run_sql(sql).await.unwrap_err().to_string();
        assert!(res_str.contains("id: integer"));
        assert!(res_str.contains("zipcode: bigint"));

        let sql = format!(
            r#"ALTER SOURCE src FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordExt',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        frontend.run_sql(sql).await.unwrap();

        let altered_source = get_source();

        let name_column = altered_source
            .columns
            .iter()
            .find(|col| col.column_desc.name == "name")
            .unwrap();
        assert_eq!(name_column.column_desc.data_type, DataType::Varchar);

        expect_test::expect!["CREATE SOURCE src (id INT, country STRUCT<address CHARACTER VARYING, city STRUCT<address CHARACTER VARYING, zipcode CHARACTER VARYING>, zipcode CHARACTER VARYING>, zipcode BIGINT, rate REAL, name CHARACTER VARYING) WITH (connector = 'kafka', topic = 'test-topic', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecordExt', schema.location = 'file://')"].assert_eq(&altered_source.create_sql_purified().replace(proto_file.path().to_str().unwrap(), ""));
    }
}
