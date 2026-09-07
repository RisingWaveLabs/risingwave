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

use anyhow::{Context, anyhow};
use fancy_regex::Regex;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail_not_implemented;
use risingwave_sqlparser::ast::{FormatEncodeOptions, ObjectName, Statement};
use thiserror_ext::AsReport;

use super::alter_table_column::fetch_table_catalog_for_alter;
use super::create_source::{SqlColumnStrategy, schema_has_schema_registry};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse, get_replace_table_plan};
use crate::TableCatalog;
use crate::error::{ErrorCode, Result};

fn get_format_encode_from_table(table: &TableCatalog) -> Result<Option<FormatEncodeOptions>> {
    let stmt = table.create_sql_ast()?;
    let Statement::CreateTable { format_encode, .. } = stmt else {
        unreachable!()
    };
    Ok(format_encode.map(|schema| schema.into_v2_with_warning()))
}

pub async fn handle_refresh_schema(
    handler_args: HandlerArgs,
    table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let (original_table, has_incoming_sinks) =
        fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    if has_incoming_sinks {
        bail_not_implemented!("alter table with incoming sinks");
    }

    let format_encode = get_format_encode_from_table(&original_table)?;
    if !format_encode
        .as_ref()
        .is_some_and(schema_has_schema_registry)
    {
        return Err(ErrorCode::NotSupported(
            "tables without schema registry cannot be refreshed".to_owned(),
            "try `ALTER TABLE .. ADD/DROP COLUMN ...` instead".to_owned(),
        )
        .into());
    }

    let definition = original_table
        .create_sql_ast_purified()
        .context("unable to parse original table definition")?;

    let (source, table, graph, job_type) = {
        let result = Box::pin(get_replace_table_plan(
            &session,
            table_name,
            definition,
            &original_table,
            SqlColumnStrategy::Ignore,
        ))
        .await;
        match result {
            Ok((source, table, graph, job_type)) => Ok((source, table, graph, job_type)),
            Err(e) => {
                let report = e.to_report_string();
                // NOTE(yuhao): This is a workaround for reporting errors when columns to drop is referenced by generated column.
                // Finding the actual columns to drop requires generating `PbSource` from the sql definition
                // and fetching schema from schema registry, which will cause a lot of unnecessary refactor.
                // Here we match the error message to yield when failing to bind generated column exprs.
                let re = Regex::new(r#"failed to bind the expression in generated column "(.*?)""#)
                    .unwrap();
                let captures = re.captures(&report).map_err(anyhow::Error::from)?;
                if let Some(gen_col_name) = captures.and_then(|captures| captures.get(1)) {
                    Err(anyhow!(e).context(format!("failed to refresh schema because some of the columns to drop are referenced by a generated column \"{}\"",
                gen_col_name.as_str())).into())
                } else {
                    Err(e)
                }
            }
        }
    }?;
    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_table(
            source.map(|x| x.to_prost()),
            table.to_prost(),
            graph,
            job_type,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}
