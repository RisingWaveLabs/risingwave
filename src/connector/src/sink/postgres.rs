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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use phf::phf_set;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{Row, RowExt};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use simd_json::prelude::ArrayTrait;
use thiserror_ext::AsReport;
use tokio_postgres::types::Type as PgType;

use super::{
    LogSinker, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkLogReader,
};
use crate::connector_common::{
    PgConnectionConfig, PostgresExternalTable, SslMode, TcpKeepaliveConfig, create_pg_client,
};
use crate::enforce_secret::EnforceSecret;
use crate::parser::scalar_adapter::{ScalarAdapter, validate_pg_type_to_rw_type};
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{Result, Sink, SinkParam, SinkWriterParam};

pub const POSTGRES_SINK: &str = "postgres";

const CHECK_FOREIGN_KEY_SQL: &str = r#"
    SELECT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1
          AND t.relname = $2
          AND c.contype = 'f'
    )
"#;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "Default::default")]
    pub ssl_mode: SslMode,
    #[serde(rename = "ssl.root.cert")]
    pub ssl_root_cert: Option<String>,
    #[serde(default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"
    #[serde(default, rename = "tcp.keepalive.enable")]
    #[serde_as(as = "DisplayFromStr")]
    pub tcp_keepalive_enable: bool,

    #[serde(flatten)]
    pub tcp_keepalive: Option<TcpKeepaliveConfig>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(PostgresConfig);

impl EnforceSecret for PostgresConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf_set! {
        "password", "ssl.root.cert"
    };
}

fn default_max_batch_rows() -> usize {
    1024
}

fn default_schema() -> String {
    "public".to_owned()
}

fn tcp_keepalive_from_config(config: &PostgresConfig) -> Option<TcpKeepaliveConfig> {
    if config.tcp_keepalive_enable {
        config
            .tcp_keepalive
            .clone()
            .or_else(|| Some(TcpKeepaliveConfig::default()))
    } else {
        None
    }
}

async fn ensure_no_foreign_key(config: &PostgresConfig) -> Result<()> {
    let pg_conn = config.pg_connection_config();
    let client = create_pg_client(&pg_conn, tcp_keepalive_from_config(config)).await?;

    ensure_no_foreign_key_with_client(&client, &config.schema, &config.table).await
}

async fn ensure_no_foreign_key_with_client(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<()> {
    let has_foreign_key = client
        .query_one(CHECK_FOREIGN_KEY_SQL, &[&schema, &table])
        .await
        .context("failed to check foreign key constraints")?
        .get::<_, bool>(0);

    if has_foreign_key {
        return Err(SinkError::Config(anyhow!(
            "Postgres sink does not support target table \"{}\".\"{}\" with foreign key constraints. Please remove foreign key constraints from the target table or choose a different sink table.",
            schema,
            table,
        )));
    }

    Ok(())
}

impl PostgresConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<PostgresConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }

    pub fn pg_connection_config(&self) -> PgConnectionConfig {
        PgConnectionConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            ssl_mode: self.ssl_mode.clone(),
            ssl_root_cert: self.ssl_root_cert.clone(),
        }
    }
}

#[derive(Debug)]
pub struct PostgresSink {
    pub config: PostgresConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl PostgresSink {
    pub fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl EnforceSecret for PostgresSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            PostgresConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for PostgresSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = PostgresConfig::from_btreemap(param.properties)?;
        PostgresSink::new(config, schema, pk_indices, param.sink_type.is_append_only())
    }
}

impl Sink for PostgresSink {
    type LogSinker = PostgresSinkWriter;

    const SINK_NAME: &'static str = POSTGRES_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert Postgres sink (please define in `primary_key` field)"
            )));
        }

        ensure_no_foreign_key(&self.config).await?;

        // Verify our sink schema is compatible with Postgres
        {
            let pg_conn = self.config.pg_connection_config();
            let pg_table = PostgresExternalTable::connect(
                &pg_conn,
                &self.config.schema,
                &self.config.table,
                self.is_append_only,
                None,
            )
            .await
            .context(format!(
                "failed to connect to database: {}, schema: {}, table: {}",
                self.config.database, self.config.schema, self.config.table
            ))?;

            // Check that names and types match, order of columns doesn't matter.
            {
                let pg_columns = pg_table.column_descs();
                let sink_columns = self.schema.fields();
                if pg_columns.len() < sink_columns.len() {
                    return Err(SinkError::Config(anyhow!(
                        "Column count mismatch: Postgres table has {} columns, but sink schema has {} columns, sink should have less or equal columns to the Postgres table",
                        pg_columns.len(),
                        sink_columns.len()
                    )));
                }

                let pg_columns_lookup = pg_columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect::<BTreeMap<_, _>>();
                for sink_column in sink_columns {
                    let pg_column = pg_columns_lookup.get(&sink_column.name);
                    match pg_column {
                        None => {
                            return Err(SinkError::Config(anyhow!(
                                "Column `{}` not found in Postgres table `{}`",
                                sink_column.name,
                                self.config.table
                            )));
                        }
                        Some(pg_column) => {
                            if !validate_pg_type_to_rw_type(pg_column, &sink_column.data_type()) {
                                return Err(SinkError::Config(anyhow!(
                                    "Column `{}` in Postgres table `{}` has type `{}`, but sink schema defines it as type `{}`",
                                    sink_column.name,
                                    self.config.table,
                                    pg_column,
                                    sink_column.data_type()
                                )));
                            }
                        }
                    }
                }
            }

            // check that pk matches
            {
                let pg_pk_names = pg_table.pk_names();
                let sink_pk_names = self
                    .pk_indices
                    .iter()
                    .map(|i| &self.schema.fields()[*i].name)
                    .collect::<HashSet<_>>();
                if pg_pk_names.len() != sink_pk_names.len() {
                    return Err(SinkError::Config(anyhow!(
                        "Primary key mismatch: Postgres table has primary key on columns {:?}, but sink schema defines primary key on columns {:?}",
                        pg_pk_names,
                        sink_pk_names
                    )));
                }
                for name in pg_pk_names {
                    if !sink_pk_names.contains(name) {
                        return Err(SinkError::Config(anyhow!(
                            "Primary key mismatch: Postgres table has primary key on column `{}`, but sink schema does not define it as a primary key",
                            name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        PostgresSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await
    }
}

pub struct PostgresSinkWriter {
    is_append_only: bool,
    client: tokio_postgres::Client,
    pk_indices: Vec<usize>,
    pk_types: Vec<PgType>,
    schema_types: Vec<PgType>,
    raw_insert_sql: Arc<String>,
    raw_upsert_sql: Arc<String>,
    raw_delete_sql: Arc<String>,
    insert_sql: Arc<tokio_postgres::Statement>,
    delete_sql: Arc<tokio_postgres::Statement>,
    upsert_sql: Arc<tokio_postgres::Statement>,
}

impl PostgresSinkWriter {
    async fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let tcp_keepalive = tcp_keepalive_from_config(&config);

        let pg_conn = config.pg_connection_config();
        let client = create_pg_client(&pg_conn, tcp_keepalive).await?;

        ensure_no_foreign_key_with_client(&client, &config.schema, &config.table).await?;

        let pk_indices_lookup = pk_indices.iter().copied().collect::<HashSet<_>>();

        // Rewrite schema types for serialization
        let (pk_types, schema_types) = {
            let name_to_type = PostgresExternalTable::type_mapping(
                &pg_conn,
                &config.schema,
                &config.table,
                is_append_only,
            )
            .await?;
            let mut schema_types = Vec::with_capacity(schema.fields.len());
            let mut pk_types = Vec::with_capacity(pk_indices.len());
            for (i, field) in schema.fields.iter().enumerate() {
                let field_name = &field.name;
                let actual_data_type = name_to_type.get(field_name).map(|t| (*t).clone());
                let actual_data_type = actual_data_type
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Column `{}` not found in sink schema",
                            field_name
                        ))
                    })?
                    .clone();
                if pk_indices_lookup.contains(&i) {
                    pk_types.push(actual_data_type.clone())
                }
                schema_types.push(actual_data_type);
            }
            (pk_types, schema_types)
        };

        let raw_insert_sql = create_insert_sql(&schema, &config.schema, &config.table);
        let raw_upsert_sql = create_upsert_sql(
            &schema,
            &config.schema,
            &config.table,
            &pk_indices,
            &pk_indices_lookup,
        );
        let raw_delete_sql = create_delete_sql(&schema, &config.schema, &config.table, &pk_indices);

        let insert_sql = client
            .prepare(&raw_insert_sql)
            .await
            .with_context(|| format!("failed to prepare insert statement: {}", raw_insert_sql))?;
        let upsert_sql = client
            .prepare(&raw_upsert_sql)
            .await
            .with_context(|| format!("failed to prepare upsert statement: {}", raw_upsert_sql))?;
        let delete_sql = client
            .prepare(&raw_delete_sql)
            .await
            .with_context(|| format!("failed to prepare delete statement: {}", raw_delete_sql))?;

        let writer = Self {
            is_append_only,
            client,
            pk_indices,
            pk_types,
            schema_types,
            raw_insert_sql: Arc::new(raw_insert_sql),
            raw_upsert_sql: Arc::new(raw_upsert_sql),
            raw_delete_sql: Arc::new(raw_delete_sql),
            insert_sql: Arc::new(insert_sql),
            delete_sql: Arc::new(delete_sql),
            upsert_sql: Arc::new(upsert_sql),
        };
        Ok(writer)
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        // https://www.postgresql.org/docs/current/limits.html
        // We have a limit of 65,535 parameters in a single query, as restricted by the PostgreSQL protocol.
        if self.is_append_only {
            self.write_batch_append_only(chunk).await
        } else {
            self.write_batch_non_append_only(chunk).await
        }
    }

    async fn write_batch_append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let transaction = Arc::new(self.client.transaction().await?);
        let mut insert_futures = FuturesUnordered::new();
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let pg_row = convert_row_to_pg_row(row, &self.schema_types);
                    let insert_sql = self.insert_sql.clone();
                    let raw_insert_sql = self.raw_insert_sql.clone();
                    let transaction = transaction.clone();
                    let future = async move {
                        transaction
                            .execute_raw(insert_sql.as_ref(), &pg_row)
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to execute insert statement: {}, parameters: {:?}",
                                    raw_insert_sql, pg_row
                                )
                            })
                    };
                    insert_futures.push(future);
                }
                _ => {
                    tracing::error!(
                        "row ignored: an append-only sink should not receive update-insert, update-delete, or delete operations"
                    );
                }
            }
        }

        while let Some(result) = insert_futures.next().await {
            result?;
        }
        if let Some(transaction) = Arc::into_inner(transaction) {
            transaction.commit().await?;
        } else {
            tracing::error!("transaction handle was lost");
        }

        Ok(())
    }

    async fn write_batch_non_append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let transaction = Arc::new(self.client.transaction().await?);
        let mut delete_futures = FuturesUnordered::new();
        let mut upsert_futures = FuturesUnordered::new();
        for (op, row) in chunk.rows() {
            match op {
                Op::Delete | Op::UpdateDelete => {
                    let pg_row =
                        convert_row_to_pg_row(row.project(&self.pk_indices), &self.pk_types);
                    let delete_sql = self.delete_sql.clone();
                    let raw_delete_sql = self.raw_delete_sql.clone();
                    let transaction = transaction.clone();
                    let future = async move {
                        transaction
                            .execute_raw(delete_sql.as_ref(), &pg_row)
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to execute delete statement: {}, parameters: {:?}",
                                    raw_delete_sql, pg_row
                                )
                            })
                    };
                    delete_futures.push(future);
                }
                Op::Insert | Op::UpdateInsert => {
                    let pg_row = convert_row_to_pg_row(row, &self.schema_types);
                    let upsert_sql = self.upsert_sql.clone();
                    let raw_upsert_sql = self.raw_upsert_sql.clone();
                    let transaction = transaction.clone();
                    let future = async move {
                        transaction
                            .execute_raw(upsert_sql.as_ref(), &pg_row)
                            .await
                            .with_context(|| {
                                format!(
                                    "failed to execute upsert statement: {}, parameters: {:?}",
                                    raw_upsert_sql, pg_row
                                )
                            })
                    };
                    upsert_futures.push(future);
                }
            }
        }
        while let Some(result) = delete_futures.next().await {
            result?;
        }
        while let Some(result) = upsert_futures.next().await {
            result?;
        }
        if let Some(transaction) = Arc::into_inner(transaction) {
            transaction.commit().await?;
        } else {
            tracing::error!("transaction handle was lost");
        }
        Ok(())
    }
}

#[async_trait]
impl LogSinker for PostgresSinkWriter {
    async fn consume_log_and_sink(mut self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        loop {
            let (epoch, item) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    self.write_batch(chunk).await?;
                    log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                }
                LogStoreReadItem::Barrier { .. } => {
                    log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                }
            }
        }
    }
}

fn create_insert_sql(schema: &Schema, schema_name: &str, table_name: &str) -> String {
    let normalized_table_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let number_of_columns = schema.len();
    let columns: String = schema
        .fields()
        .iter()
        .map(|field| quote_identifier(&field.name))
        .join(", ");
    let column_parameters: String = (0..number_of_columns)
        .map(|i| format!("${}", i + 1))
        .join(", ");
    format!("INSERT INTO {normalized_table_name} ({columns}) VALUES ({column_parameters})")
}

fn create_delete_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    pk_indices: &[usize],
) -> String {
    let normalized_table_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let pk_indices = if pk_indices.is_empty() {
        (0..schema.len()).collect_vec()
    } else {
        pk_indices.to_vec()
    };
    let pk = {
        let pk_symbols = pk_indices
            .iter()
            .map(|pk_index| quote_identifier(&schema.fields()[*pk_index].name))
            .join(", ");
        format!("({})", pk_symbols)
    };
    let parameters: String = (0..pk_indices.len())
        .map(|i| format!("${}", i + 1))
        .join(", ");
    format!("DELETE FROM {normalized_table_name} WHERE {pk} in (({parameters}))")
}

fn create_upsert_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    pk_indices: &[usize],
    pk_indices_lookup: &HashSet<usize>,
) -> String {
    let insert_sql = create_insert_sql(schema, schema_name, table_name);
    if pk_indices.is_empty() {
        return insert_sql;
    }
    let pk_columns = pk_indices
        .iter()
        .map(|pk_index| quote_identifier(&schema.fields()[*pk_index].name))
        .collect_vec()
        .join(", ");
    let update_parameters: String = (0..schema.len())
        .filter(|i| !pk_indices_lookup.contains(i))
        .map(|i| {
            let column = quote_identifier(&schema.fields()[i].name);
            format!("{column} = EXCLUDED.{column}")
        })
        .collect_vec()
        .join(", ");
    if update_parameters.is_empty() {
        format!("{insert_sql} on conflict ({pk_columns}) do nothing")
    } else {
        format!("{insert_sql} on conflict ({pk_columns}) do update set {update_parameters}")
    }
}

/// Quote an identifier for PostgreSQL.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

type PgDatum = Option<ScalarAdapter>;
type PgRow = Vec<PgDatum>;

fn convert_row_to_pg_row(row: impl Row, schema_types: &[PgType]) -> PgRow {
    let mut buffer = Vec::with_capacity(row.len());
    for (i, datum_ref) in row.iter().enumerate() {
        let pg_datum = datum_ref.map(|s| {
            match ScalarAdapter::from_scalar(s, &schema_types[i]) {
                Ok(scalar) => Some(scalar),
                Err(e) => {
                    tracing::error!(error=%e.as_report(), scalar=?s, "Failed to convert scalar to pg value");
                    None
                }
            }
        });
        buffer.push(pg_datum.flatten());
    }
    buffer
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use expect_test::{Expect, expect};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;

    fn check(actual: impl Display, expect: Expect) {
        let actual = actual.to_string();
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_create_insert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_insert_sql(&schema, schema_name, table_name);
        check(
            sql,
            expect![[r#"INSERT INTO "test_schema"."test_table" ("a", "b") VALUES ($1, $2)"#]],
        );
    }

    #[test]
    fn test_create_delete_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, schema_name, table_name, &[1]);
        check(
            sql,
            expect![[r#"DELETE FROM "test_schema"."test_table" WHERE ("b") in (($1))"#]],
        );
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, schema_name, table_name, &[0, 1]);
        check(
            sql,
            expect![[r#"DELETE FROM "test_schema"."test_table" WHERE ("a", "b") in (($1, $2))"#]],
        );
    }

    #[test]
    fn test_create_upsert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let pk_indices_lookup = HashSet::from_iter([1]);
        let sql = create_upsert_sql(&schema, schema_name, table_name, &[1], &pk_indices_lookup);
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("a", "b") VALUES ($1, $2) on conflict ("b") do update set "a" = EXCLUDED."a""#
            ]],
        );
    }

    #[test]
    fn test_create_upsert_sql_all_columns_are_primary_keys() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "user_id".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "client_id".to_owned(),
            },
        ]);
        let schema_name = "test_schema";
        let table_name = "test_table";
        let pk_indices_lookup = HashSet::from_iter([0, 1]);
        let sql = create_upsert_sql(
            &schema,
            schema_name,
            table_name,
            &[0, 1],
            &pk_indices_lookup,
        );
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("user_id", "client_id") VALUES ($1, $2) on conflict ("user_id", "client_id") do nothing"#
            ]],
        );
    }
}
