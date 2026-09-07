// Copyright 2026 RisingWave Labs
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

use anyhow::{Context, anyhow};
use fancy_regex::Regex;
use itertools::Itertools;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_connector::source::cdc::external::{
    DATABASE_NAME_KEY, ExternalTableConfig, ExternalTableImpl, SCHEMA_NAME_KEY, SchemaTableName,
    TABLE_NAME_KEY,
};
use risingwave_connector::source::cdc::{
    MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR,
};
use risingwave_sqlparser::ast::{ColumnDef, ColumnOption, SourceWatermark, TableConstraint};
use thiserror_ext::AsReport;

use crate::error::{ErrorCode, Result, RwError};
use crate::handler::create_source::reject_variant_columns;
use crate::handler::create_table::{bind_sql_columns, bind_sql_pk_names, bind_table_constraints};

/// Derive connector properties and normalize `external_table_name` for CDC tables.
///
/// Returns (`connector_properties`, `normalized_external_table_name`) where:
/// - For SQL Server: Normalizes 'db.schema.table' (3 parts) to 'schema.table' (2 parts),
///   because users can optionally include database name for verification, but it needs to be
///   stripped to match the format returned by Debezium's `extract_table_name()`.
/// - For MySQL/Postgres: Returns the original `external_table_name` unchanged.
pub(crate) fn derive_with_options_for_cdc_table(
    source_with_properties: &WithOptionsSecResolved,
    external_table_name: String,
) -> Result<(WithOptionsSecResolved, String)> {
    // we should remove the prefix from `full_table_name`
    let source_database_name: &str = source_with_properties
        .get("database.name")
        .ok_or_else(|| anyhow!("The source with properties does not contain 'database.name'"))?
        .as_str();
    let mut with_options = source_with_properties.clone();
    if let Some(connector) = source_with_properties.get(UPSTREAM_SOURCE_KEY) {
        match connector.as_str() {
            MYSQL_CDC_CONNECTOR => {
                // MySQL doesn't allow '.' in database name and table name, so we can split the
                // external table name by '.' to get the table name
                let (db_name, table_name) = external_table_name.split_once('.').ok_or_else(|| {
                    anyhow!("The upstream table name must contain database name prefix, e.g. 'database.table'")
                })?;
                // We allow multiple database names in the source definition
                if !source_database_name
                    .split(',')
                    .map(|s| s.trim())
                    .any(|name| name == db_name)
                {
                    return Err(anyhow!(
                        "The database name `{}` in the FROM clause is not included in the database name `{}` in source definition",
                        db_name,
                        source_database_name
                    ).into());
                }
                with_options.insert(DATABASE_NAME_KEY.into(), db_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());
                // Return original external_table_name unchanged for MySQL
                return Ok((with_options, external_table_name));
            }
            POSTGRES_CDC_CONNECTOR => {
                let (schema_name, table_name) =
                    parse_postgres_cdc_external_table_name(&external_table_name)?;

                // insert 'schema.name' into connect properties
                with_options.insert(SCHEMA_NAME_KEY.into(), schema_name);
                with_options.insert(TABLE_NAME_KEY.into(), table_name);
                // Return original external_table_name unchanged for Postgres
                return Ok((with_options, external_table_name));
            }
            SQL_SERVER_CDC_CONNECTOR => {
                // SQL Server external table name must be in one of two formats:
                // 1. 'schemaName.tableName' (2 parts) - database is already specified in source
                // 2. 'databaseName.schemaName.tableName' (3 parts) - for explicit verification
                //
                // We do NOT allow single table name (e.g., 't') because:
                // - Unlike database name (already in source), schema name is NOT pre-specified
                // - User must explicitly provide schema (even if it's 'dbo')
                let parts: Vec<&str> = external_table_name.split('.').collect();
                let (schema_name, table_name) = match parts.len() {
                    3 => {
                        // Format: database.schema.table
                        // Verify that the database name matches the one in source definition
                        let db_name = parts[0];
                        let schema_name = parts[1];
                        let table_name = parts[2];

                        if db_name != source_database_name {
                            return Err(anyhow!(
                                "The database name '{}' in FROM clause does not match the database name '{}' specified in source definition. \
                                 You can either use 'schema.table' format (recommended) or ensure the database name matches.",
                                db_name,
                                source_database_name
                            ).into());
                        }
                        (schema_name, table_name)
                    }
                    2 => {
                        // Format: schema.table (recommended)
                        // Database name is taken from source definition
                        let schema_name = parts[0];
                        let table_name = parts[1];
                        (schema_name, table_name)
                    }
                    1 => {
                        // Format: table only
                        // Reject with clear error message
                        return Err(anyhow!(
                            "Invalid table name format '{}'. For SQL Server CDC, you must specify the schema name. \
                             Use 'schema.table' format (e.g., 'dbo.{}') or 'database.schema.table' format (e.g., '{}.dbo.{}').",
                            external_table_name,
                            external_table_name,
                            source_database_name,
                            external_table_name
                        ).into());
                    }
                    _ => {
                        // Invalid format (4+ parts or empty)
                        return Err(anyhow!(
                            "Invalid table name format '{}'. Expected 'schema.table' or 'database.schema.table'.",
                            external_table_name
                        ).into());
                    }
                };

                // Insert schema and table names into connector properties
                with_options.insert(SCHEMA_NAME_KEY.into(), schema_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());

                // Normalize external_table_name to 'schema.table' format
                // This ensures consistency with extract_table_name() in message.rs
                let normalized_external_table_name = format!("{}.{}", schema_name, table_name);
                return Ok((with_options, normalized_external_table_name));
            }
            _ => {
                return Err(RwError::from(anyhow!(
                    "connector {} is not supported for cdc table",
                    connector
                )));
            }
        };
    }
    unreachable!("All valid CDC connectors should have returned by now")
}

/// Parse the schema/table name from the CDC `TABLE` clause.
///
/// Column names do not need the same parsing here: wildcard schema derivation reads
/// them from PostgreSQL catalogs after the exact table has been identified.
fn parse_postgres_cdc_external_table_name(external_table_name: &str) -> Result<(String, String)> {
    let mut parts = vec![];
    let mut current = String::new();
    let mut chars = external_table_name.chars().peekable();
    let mut in_quote = false;
    let mut just_closed_quote = false;

    while let Some(ch) = chars.next() {
        if in_quote {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quote = false;
                    just_closed_quote = true;
                }
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                '.' => {
                    if current.is_empty() {
                        return Err(anyhow!(
                            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                            external_table_name
                        )
                        .into());
                    }
                    parts.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                }
                '"' if current.is_empty() => {
                    in_quote = true;
                }
                '"' => {
                    return Err(anyhow!(
                        "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                        external_table_name
                    )
                    .into());
                }
                _ if just_closed_quote => {
                    return Err(anyhow!(
                        "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                        external_table_name
                    )
                    .into());
                }
                _ => current.push(ch),
            }
        }
    }

    if in_quote || current.is_empty() {
        return Err(anyhow!(
            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
            external_table_name
        )
        .into());
    }
    parts.push(current);

    if let [schema_name, table_name] = parts.as_slice() {
        Ok((schema_name.clone(), table_name.clone()))
    } else {
        Err(
            anyhow!("The upstream table name must contain schema name prefix, e.g. 'public.table'")
                .into(),
        )
    }
}

/// Reject a CDC table when a primary-key column is filtered out of Debezium change-event
/// values via `debezium.column.exclude.list` or `debezium.column.include.list`.
///
/// Debezium's column filters only apply to the change-event **value** payload. Message keys are
/// always built from the upstream PRIMARY KEY and are not affected. If a PK column is filtered out
/// of the value, RisingWave reads NULL for that PK column from the payload, causing silent data
/// corruption: UPDATE turns into a fresh INSERT (PK mismatch with the original row) and DELETE
/// silently no-ops.
///
/// Debezium entries are regex patterns matched against the fully qualified column name
/// `<namespace>.<table>.<column>`, where namespace is `schema` for Postgres / SQL Server and
/// `database` for MySQL.
pub(crate) fn reject_pk_filtered_by_debezium_column_filter(
    pk_names: &[String],
    cdc_with_options: &WithOptionsSecResolved,
) -> Result<()> {
    const EXCLUDE_KEY: &str = "debezium.column.exclude.list";
    const INCLUDE_KEY: &str = "debezium.column.include.list";

    let st = SchemaTableName::from_properties(cdc_with_options.as_plaintext());
    reject_pk_filtered_by_debezium_column_filter_inner(
        pk_names,
        &st,
        cdc_with_options.get(EXCLUDE_KEY).map(String::as_str),
        cdc_with_options.get(INCLUDE_KEY).map(String::as_str),
    )
}

fn reject_pk_filtered_by_debezium_column_filter_inner(
    pk_names: &[String],
    st: &SchemaTableName,
    exclude_list: Option<&str>,
    include_list: Option<&str>,
) -> Result<()> {
    const EXCLUDE_KEY: &str = "debezium.column.exclude.list";
    const INCLUDE_KEY: &str = "debezium.column.include.list";

    let pk_full_names = pk_names
        .iter()
        .map(|pk| (pk, format!("{}.{}.{}", st.schema_name, st.table_name, pk)))
        .collect_vec();

    if let Some(exclude_list) = exclude_list {
        let patterns = compile_debezium_column_filter_patterns(EXCLUDE_KEY, exclude_list)?;
        for (pk, pk_full_name) in &pk_full_names {
            for (pattern, regex) in &patterns {
                if regex.is_match(pk_full_name).map_err(|err| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "failed to evaluate Debezium column filter pattern `{pattern}` in `{EXCLUDE_KEY}`: {}",
                        err.as_report()
                    ))
                })? {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "primary key column `{pk}` is excluded by `{EXCLUDE_KEY}` pattern \
                         `{pattern}`. Excluding a PK column causes silent data corruption: \
                         Debezium keeps the PK in the message key but drops it from the payload, \
                         so RisingWave cannot match UPDATE/DELETE events against the original row."
                    ))
                    .into());
                }
            }
        }
    }

    if let Some(include_list) = include_list {
        let patterns = compile_debezium_column_filter_patterns(INCLUDE_KEY, include_list)?;
        for (pk, pk_full_name) in &pk_full_names {
            let mut included = false;
            for (_, regex) in &patterns {
                if regex.is_match(pk_full_name).map_err(|err| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "failed to evaluate Debezium column filter pattern in `{INCLUDE_KEY}`: {}",
                        err.as_report()
                    ))
                })? {
                    included = true;
                    break;
                }
            }
            if !included {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "primary key column `{pk}` is not included by `{INCLUDE_KEY}`. Omitting a PK \
                     column causes silent data corruption: Debezium keeps the PK in the message key \
                     but drops it from the payload, so RisingWave cannot match UPDATE/DELETE events \
                     against the original row."
                ))
                .into());
            }
        }
    }

    Ok(())
}

fn compile_debezium_column_filter_patterns(
    key: &str,
    filter_list: &str,
) -> Result<Vec<(String, Regex)>> {
    filter_list
        .split(',')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(|pattern| {
            let anchored_pattern = format!("(?i:^(?:{pattern})$)");
            let regex = Regex::new(&anchored_pattern).map_err(|err| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid Debezium column filter pattern `{pattern}` in `{key}`: {}",
                    err.as_report()
                ))
            })?;
            Ok((pattern.to_owned(), regex))
        })
        .collect()
}

// For both table from cdc source and table with cdc connector
pub(crate) fn not_null_check_for_cdc_table(
    wildcard_idx: &Option<usize>,
    column_defs: &Vec<ColumnDef>,
) -> Result<()> {
    if !wildcard_idx.is_some()
        && column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table with NOT NULL constraint is not supported".to_owned(),
            "Please remove the NOT NULL constraint for columns".to_owned(),
        )
        .into());
    }
    Ok(())
}

// Only for table from cdc source
pub(crate) fn sanity_check_for_table_on_cdc_source(
    append_only: bool,
    column_defs: &Vec<ColumnDef>,
    wildcard_idx: &Option<usize>,
    constraints: &Vec<TableConstraint>,
    source_watermarks: &Vec<SourceWatermark>,
) -> Result<()> {
    // wildcard cannot be used with column definitions
    if wildcard_idx.is_some() && !column_defs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "wildcard(*) and column definitions cannot be used together".to_owned(),
            "Remove the wildcard or column definitions".to_owned(),
        )
        .into());
    }

    // cdc table must have primary key constraint or primary key column
    if !wildcard_idx.is_some()
        && !constraints.iter().any(|c| {
            matches!(
                c,
                TableConstraint::Unique {
                    is_primary: true,
                    ..
                }
            )
        })
        && !column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::Unique { is_primary: true }))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table without primary key constraint is not supported".to_owned(),
            "Please define a primary key".to_owned(),
        )
        .into());
    }

    if append_only {
        return Err(ErrorCode::NotSupported(
            "append only modifier on the table created from a CDC source".into(),
            "Remove the APPEND ONLY clause".into(),
        )
        .into());
    }

    if !source_watermarks.is_empty()
        && source_watermarks
            .iter()
            .any(|watermark| !watermark.with_ttl)
    {
        return Err(ErrorCode::NotSupported(
            "non-TTL watermark defined on the table created from a CDC source".into(),
            "Use `WATERMARK ... WITH TTL` instead.".into(),
        )
        .into());
    }

    Ok(())
}

/// Derive the schema of a CDC table from its upstream external table.
pub(crate) async fn bind_cdc_table_schema_externally(
    cdc_with_options: WithOptionsSecResolved,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    let (options, secret_refs) = cdc_with_options.into_parts();
    let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
        .context("failed to extract external table config")?;

    let table = ExternalTableImpl::connect(config)
        .await
        .context("failed to auto derive table schema")?;

    Ok((
        table
            .column_descs()
            .iter()
            .cloned()
            .map(|column_desc| ColumnCatalog {
                column_desc,
                is_hidden: false,
            })
            .collect(),
        table.pk_names().clone(),
    ))
}

/// Derive the schema of a CDC table from explicit SQL columns and constraints.
pub(crate) fn bind_cdc_table_schema(
    column_defs: &Vec<ColumnDef>,
    constraints: &Vec<TableConstraint>,
    is_for_replace_plan: bool,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    let columns = bind_sql_columns(column_defs, is_for_replace_plan)?;
    // CDC parsers cannot produce variant values, and this path has no FORMAT/ENCODE gate.
    reject_variant_columns(&columns, "on a table created from a CDC source")?;

    let pk_names = bind_sql_pk_names(column_defs, bind_table_constraints(constraints)?)?;
    Ok((columns, pk_names))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema_table_name() -> SchemaTableName {
        SchemaTableName {
            schema_name: "public".to_owned(),
            table_name: "orders".to_owned(),
        }
    }

    fn pk_names() -> Vec<String> {
        vec!["plan_id".to_owned(), "site_id".to_owned()]
    }

    #[test]
    fn test_debezium_filter_rejects_literal_excluded_pk() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some("public.orders.site_id"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
        assert!(
            err.to_report_string()
                .contains("debezium.column.exclude.list")
        );
    }

    #[test]
    fn test_debezium_filter_rejects_include_list_missing_pk() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("public.orders.plan_id,public.orders.payload"),
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
        assert!(
            err.to_report_string()
                .contains("debezium.column.include.list")
        );
    }

    #[test]
    fn test_debezium_filter_accepts_include_list_covering_all_pks() {
        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("public.orders.plan_id,public.orders.site_id,public.orders.payload"),
        )
        .unwrap();
    }

    #[test]
    fn test_debezium_filter_matches_regex_patterns() {
        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some(r"public[.]orders[.](plan_id|site_id),public[.]orders[.]payload"),
        )
        .unwrap();

        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some(r".*[.]orders[.]site_id"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
    }

    #[test]
    fn test_debezium_filter_matches_patterns_case_insensitively() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some("PUBLIC.ORDERS.SITE_ID"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));

        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("Public.Orders.Plan_ID,Public.Orders.Site_ID"),
        )
        .unwrap();
    }

    #[test]
    fn test_parse_postgres_cdc_external_table_name() {
        for (input, expected) in [
            ("public.Note", ("public", "Note")),
            ("public.\"Note\"", ("public", "Note")),
            (
                "\"Mixed.Schema\".\"Note.Table\"",
                ("Mixed.Schema", "Note.Table"),
            ),
            ("public.\"Note\"\"Archive\"", ("public", "Note\"Archive")),
        ] {
            assert_eq!(
                parse_postgres_cdc_external_table_name(input).unwrap(),
                (expected.0.to_owned(), expected.1.to_owned()),
                "input: {input}"
            );
        }

        for input in [
            "Note",
            "public.",
            ".Note",
            "public.\"Note",
            "public.\"Note\"Archive",
            "public.Note.Archive",
        ] {
            assert!(
                parse_postgres_cdc_external_table_name(input).is_err(),
                "input should be rejected: {input}"
            );
        }
    }
}
