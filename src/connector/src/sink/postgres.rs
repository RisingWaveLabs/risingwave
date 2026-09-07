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

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use itertools::Itertools;
use phf::phf_set;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use simd_json::prelude::ArrayTrait;
use thiserror_ext::AsReport;
use tokio_postgres::types::Type as PgType;
use with_options::WithOptions;

use super::{SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError};
use crate::connector_common::{
    PgConnectionConfig, PostgresExternalTable, SslMode, TcpKeepaliveConfig, create_pg_client,
};
use crate::enforce_secret::EnforceSecret;
use crate::parser::scalar_adapter::{ScalarAdapter, validate_pg_type_to_rw_type};
use crate::sink::batching_log_sink::{BatchingLogSinker, BatchingSinkWriter};
use crate::sink::{Result, Sink, SinkParam, SinkWriterParam};

pub const POSTGRES_SINK: &str = "postgres";

/// Maximum number of bind parameters of a single statement. PostgreSQL itself allows 65535, but
/// the client encodes the parameter count of a `Bind` message as `i16`, so a larger batch fails
/// before ever reaching the server.
const MAX_STATEMENT_PARAMS: usize = i16::MAX as usize;

/// Upper bound of `max_batch_rows`: bigger batches only add buffer memory, since statements are
/// split at [`MAX_STATEMENT_PARAMS`] anyway.
const MAX_BATCH_ROWS_LIMIT: usize = 65536;

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
#[derive(Clone, Debug, Deserialize, WithOptions)]
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
    pub tcp_keepalive: TcpKeepaliveConfig,

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
    config
        .tcp_keepalive_enable
        .then(|| config.tcp_keepalive.clone())
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
    type LogSinker = BatchingLogSinker<PostgresSinkWriter>;

    const SINK_NAME: &'static str = POSTGRES_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !(1..=MAX_BATCH_ROWS_LIMIT).contains(&self.config.max_batch_rows) {
            return Err(SinkError::Config(anyhow!(
                "`max_batch_rows` must be between 1 and {}, got {}",
                MAX_BATCH_ROWS_LIMIT,
                self.config.max_batch_rows
            )));
        }

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
        let writer = PostgresSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?;
        Ok(BatchingLogSinker::new(writer))
    }
}

#[derive(Clone, Copy)]
enum StatementKind {
    Insert,
    Upsert,
    Delete,
}

impl StatementKind {
    fn as_str(self) -> &'static str {
        match self {
            StatementKind::Insert => "insert",
            StatementKind::Upsert => "upsert",
            StatementKind::Delete => "delete",
        }
    }
}

enum PendingOp {
    Upsert(PgRow),
    Delete,
}

/// Rows accumulated across chunks, written out on flush.
enum PendingRows {
    Insert(Vec<PgRow>),
    /// Keeps only the last operation per key: PostgreSQL rejects an `INSERT .. ON CONFLICT
    /// DO UPDATE` affecting the same row twice. Unlike `ChangeBuffer`, insert-then-delete
    /// still emits the delete: at-least-once replay may regroup a committed batch with new
    /// chunks, so same-key duplicates are legal and the downstream state is unknown.
    Upsert {
        /// Rows absorbed since the last flush; drives the flush threshold so log-store
        /// truncation keeps pace even when dedup keeps the map small.
        absorbed: usize,
        /// Last op per key, tagged with its arrival sequence: keys distinct to RisingWave may
        /// be equal to PostgreSQL (e.g. `char(n)` padding), so execution order is observable.
        rows: HashMap<OwnedRow, (usize, PendingOp)>,
    },
}

impl PendingRows {
    fn absorbed(&self) -> usize {
        match self {
            PendingRows::Insert(rows) => rows.len(),
            PendingRows::Upsert { absorbed, .. } => *absorbed,
        }
    }

    fn absorb(&mut self, chunk: &StreamChunk, key_indices: &[usize], schema_types: &[PgType]) {
        match self {
            PendingRows::Insert(rows) => {
                rows.reserve(chunk.cardinality());
                for (op, row) in chunk.rows() {
                    if op == Op::Insert {
                        rows.push(convert_row_to_pg_row(row, schema_types));
                    } else {
                        tracing::error!(
                            "row ignored, append-only sink should not receive update insert, update delete and delete operations"
                        );
                    }
                }
            }
            PendingRows::Upsert { absorbed, rows } => {
                rows.reserve(chunk.cardinality());
                for (op, row) in chunk.rows() {
                    let key = row.project(key_indices).into_owned_row();
                    let pending_op = match op {
                        Op::Insert | Op::UpdateInsert => {
                            PendingOp::Upsert(convert_row_to_pg_row(row, schema_types))
                        }
                        Op::Delete | Op::UpdateDelete => PendingOp::Delete,
                    };
                    rows.insert(key, (*absorbed, pending_op));
                    *absorbed += 1;
                }
            }
        }
    }

    /// Takes all pending rows out, split into deletes (rebuilt from the keys) and upserts, each
    /// in chronological order.
    fn take(&mut self, key_types: &[PgType]) -> (Vec<PgRow>, Vec<PgRow>) {
        match self {
            PendingRows::Insert(rows) => (vec![], std::mem::take(rows)),
            PendingRows::Upsert { absorbed, rows } => {
                *absorbed = 0;
                let mut deletes = Vec::with_capacity(rows.len());
                let mut upserts = Vec::with_capacity(rows.len());
                for (key, (seq, op)) in rows.drain() {
                    match op {
                        PendingOp::Upsert(row) => upserts.push((seq, row)),
                        PendingOp::Delete => {
                            deletes.push((seq, convert_row_to_pg_row(&key, key_types)))
                        }
                    }
                }
                deletes.sort_unstable_by_key(|(seq, _)| *seq);
                upserts.sort_unstable_by_key(|(seq, _)| *seq);
                (
                    deletes.into_iter().map(|(_, row)| row).collect(),
                    upserts.into_iter().map(|(_, row)| row).collect(),
                )
            }
        }
    }
}

pub struct PostgresSinkWriter {
    client: tokio_postgres::Client,
    schema: Schema,
    schema_name: String,
    table_name: String,
    /// Columns identifying a downstream row: the sink pk, or all columns when there is no pk.
    /// Never empty.
    key_indices: Vec<usize>,
    key_types: Vec<PgType>,
    schema_types: Vec<PgType>,
    max_batch_rows: usize,
    /// Prepared statements keyed by tuple count; only power-of-two sizes, so the caches stay
    /// small.
    write_statements: HashMap<usize, tokio_postgres::Statement>,
    delete_statements: HashMap<usize, tokio_postgres::Statement>,
    pending: PendingRows,
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

        // Rewrite schema types for serialization
        let schema_types = {
            let name_to_type = PostgresExternalTable::type_mapping(
                &pg_conn,
                &config.schema,
                &config.table,
                is_append_only,
            )
            .await?;
            let mut schema_types = Vec::with_capacity(schema.fields.len());
            for field in &schema.fields {
                let actual_data_type = name_to_type.get(&field.name).cloned().ok_or_else(|| {
                    SinkError::Config(anyhow!("Column `{}` not found in sink schema", field.name))
                })?;
                schema_types.push(actual_data_type);
            }
            schema_types
        };

        let key_indices = if pk_indices.is_empty() {
            (0..schema.len()).collect_vec()
        } else {
            pk_indices
        };
        let key_types = key_indices
            .iter()
            .map(|i| schema_types[*i].clone())
            .collect_vec();

        // validate() rejects out-of-range values at DDL time; clamp here so pre-existing sinks
        // keep running after an upgrade.
        let max_batch_rows = config.max_batch_rows.clamp(1, MAX_BATCH_ROWS_LIMIT);
        if max_batch_rows != config.max_batch_rows {
            tracing::warn!(
                configured = config.max_batch_rows,
                effective = max_batch_rows,
                "max_batch_rows out of range, clamped"
            );
        }

        let pending = if is_append_only {
            PendingRows::Insert(Vec::new())
        } else {
            PendingRows::Upsert {
                absorbed: 0,
                rows: HashMap::new(),
            }
        };

        let writer = Self {
            client,
            schema,
            schema_name: config.schema,
            table_name: config.table,
            key_indices,
            key_types,
            schema_types,
            max_batch_rows,
            write_statements: HashMap::new(),
            delete_statements: HashMap::new(),
            pending,
        };
        Ok(writer)
    }

    fn write_kind(&self) -> StatementKind {
        match &self.pending {
            PendingRows::Insert(_) => StatementKind::Insert,
            PendingRows::Upsert { .. } => StatementKind::Upsert,
        }
    }

    fn create_sql(&self, kind: StatementKind, n_tuples: usize) -> String {
        match kind {
            StatementKind::Insert => {
                create_insert_sql(&self.schema, &self.schema_name, &self.table_name, n_tuples)
            }
            StatementKind::Upsert => create_upsert_sql(
                &self.schema,
                &self.schema_name,
                &self.table_name,
                &self.key_indices,
                n_tuples,
            ),
            StatementKind::Delete => create_delete_sql(
                &self.schema,
                &self.schema_name,
                &self.table_name,
                &self.key_indices,
                n_tuples,
            ),
        }
    }

    fn statement_cache(
        &mut self,
        kind: StatementKind,
    ) -> &mut HashMap<usize, tokio_postgres::Statement> {
        match kind {
            StatementKind::Insert | StatementKind::Upsert => &mut self.write_statements,
            StatementKind::Delete => &mut self.delete_statements,
        }
    }

    async fn cached_statement(
        &mut self,
        kind: StatementKind,
        n_tuples: usize,
    ) -> Result<tokio_postgres::Statement> {
        if let Some(statement) = self.statement_cache(kind).get(&n_tuples) {
            return Ok(statement.clone());
        }
        let sql = self.create_sql(kind, n_tuples);
        let statement = self.client.prepare(&sql).await.with_context(|| {
            format!(
                "failed to prepare {} statement for {} rows",
                kind.as_str(),
                n_tuples
            )
        })?;
        self.statement_cache(kind)
            .insert(n_tuples, statement.clone());
        Ok(statement)
    }

    /// Pairs each sub-batch with a prepared statement of matching tuple count. Batches are split
    /// into power-of-two sizes so that once warm, every size hits the statement cache.
    async fn prepare_batches<'a>(
        &mut self,
        kind: StatementKind,
        rows: &'a [PgRow],
    ) -> Result<Vec<(tokio_postgres::Statement, &'a [PgRow])>> {
        let params_per_tuple = match kind {
            StatementKind::Insert | StatementKind::Upsert => self.schema.len(),
            StatementKind::Delete => self.key_indices.len(),
        };
        let cap = tuples_per_statement(self.max_batch_rows, params_per_tuple);
        let mut batches = Vec::new();
        for tuples in split_power_of_two(rows, cap) {
            let statement = self.cached_statement(kind, tuples.len()).await?;
            batches.push((statement, tuples));
        }
        Ok(batches)
    }

    /// Writes out all pending rows in a single transaction: all deletes first, then upserts in
    /// chronological order. Deletes are not interleaved by time because every upsert surviving
    /// dedup is live in RisingWave and must reach the target even if a PG-equal key was deleted
    /// after it.
    async fn flush(&mut self) -> Result<()> {
        let (deletes, upserts) = self.pending.take(&self.key_types);
        if deletes.is_empty() && upserts.is_empty() {
            return Ok(());
        }

        // Statements are prepared before the transaction; after warm-up every size hits the cache.
        let write_kind = self.write_kind();
        let delete_batches = self
            .prepare_batches(StatementKind::Delete, &deletes)
            .await?;
        let upsert_batches = self.prepare_batches(write_kind, &upserts).await?;

        let transaction = self.client.transaction().await?;
        // Deletes are awaited before upserts are sent, so that no delete can land after a
        // PG-equal upsert and erase a live row; costs one extra round trip on mixed flushes.
        let result = async {
            execute_batches(&transaction, &delete_batches).await?;
            execute_batches(&transaction, &upsert_batches).await
        }
        .await;
        if let Err(e) = result {
            // Retry any failed batch row by row: keys distinct to RisingWave but equal to
            // PostgreSQL fail a multi-row upsert with SQLSTATE 21000 yet apply cleanly one row at
            // a time; other errors recover on retry or resurface localized to a single row.
            let context = || {
                format!(
                    "failed to execute batched {} statements ({} delete rows, {} write rows)",
                    write_kind.as_str(),
                    deletes.len(),
                    upserts.len()
                )
            };
            if let Err(rollback_err) = transaction.rollback().await {
                tracing::warn!(
                    error = %rollback_err.as_report(),
                    "failed to roll back failed batch"
                );
                return Err(anyhow::Error::new(e).context(context()).into());
            }
            tracing::warn!(error = %e.as_report(), "{}, retrying row by row", context());
            return self.flush_row_by_row(&deletes, &upserts).await;
        }
        transaction.commit().await?;

        Ok(())
    }

    /// Fallback for a failed batched flush: batched deletes first, then one upsert per statement.
    async fn flush_row_by_row(&mut self, deletes: &[PgRow], upserts: &[PgRow]) -> Result<()> {
        let delete_batches = self.prepare_batches(StatementKind::Delete, deletes).await?;
        let statement = self.cached_statement(self.write_kind(), 1).await?;

        let transaction = self.client.transaction().await?;
        execute_batches(&transaction, &delete_batches)
            .await
            .with_context(|| {
                format!(
                    "failed to execute delete statements on {} rows",
                    deletes.len()
                )
            })?;
        // Polled concurrently to pipeline; statements still execute in wire order, so the
        // chronologically last of several PG-equal keys wins.
        let executions = upserts
            .iter()
            .map(|row| transaction.execute_raw(&statement, row));
        futures::future::try_join_all(executions)
            .await
            .context("failed to execute single-row write statements")?;
        transaction.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl BatchingSinkWriter for PostgresSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.pending
            .absorb(&chunk, &self.key_indices, &self.schema_types);
        Ok(())
    }

    async fn try_commit(&mut self) -> Result<bool> {
        if self.pending.absorbed() >= self.max_batch_rows {
            self.flush().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Barriers bound the sink's visibility latency, so they always flush.
    async fn commit_on_barrier(&mut self) -> Result<bool> {
        self.flush().await?;
        Ok(true)
    }
}

/// Number of tuples a single statement may carry, bounded by the parameter limit of the protocol.
fn tuples_per_statement(max_batch_rows: usize, params_per_tuple: usize) -> usize {
    max_batch_rows
        .min(MAX_STATEMENT_PARAMS / params_per_tuple.max(1))
        .max(1)
}

/// Splits `rows` into power-of-two-sized chunks no larger than `cap`, largest first.
fn split_power_of_two<T>(rows: &[T], cap: usize) -> Vec<&[T]> {
    let mut chunks = Vec::new();
    let mut rest = rows;
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(prev_power_of_two(rest.len().min(cap)));
        chunks.push(chunk);
        rest = tail;
    }
    chunks
}

/// Largest power of two not exceeding `n`. `n` must be positive.
fn prev_power_of_two(n: usize) -> usize {
    1 << (usize::BITS - 1 - n.leading_zeros())
}

async fn execute_batches(
    transaction: &tokio_postgres::Transaction<'_>,
    batches: &[(tokio_postgres::Statement, &[PgRow])],
) -> std::result::Result<(), tokio_postgres::Error> {
    // Polling all statements concurrently pipelines them into a single round trip; they execute
    // in first-poll order, i.e. the order of `batches` — an unstated implementation detail of
    // `futures` and `tokio-postgres`. If it ever breaks, only the winner among PG-equal upsert
    // keys can change, which is best-effort anyway.
    let executions = batches.iter().map(|(statement, tuples)| {
        let mut params = Vec::with_capacity(tuples.len() * tuples.first().map_or(0, |t| t.len()));
        params.extend(tuples.iter().flatten());
        transaction.execute_raw(statement, params)
    });
    futures::future::try_join_all(executions).await?;
    Ok(())
}

/// `($1, $2), ($3, $4), ...`
fn create_parameter_tuples(params_per_tuple: usize, n_tuples: usize) -> String {
    (0..n_tuples)
        .map(|tuple| {
            let parameters = (0..params_per_tuple)
                .map(|i| format!("${}", tuple * params_per_tuple + i + 1))
                .join(", ");
            format!("({parameters})")
        })
        .join(", ")
}

fn create_insert_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    n_tuples: usize,
) -> String {
    let normalized_table_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let columns: String = schema
        .fields()
        .iter()
        .map(|field| quote_identifier(&field.name))
        .join(", ");
    let values = create_parameter_tuples(schema.len(), n_tuples);
    format!("INSERT INTO {normalized_table_name} ({columns}) VALUES {values}")
}

fn create_delete_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    key_indices: &[usize],
    n_tuples: usize,
) -> String {
    let normalized_table_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let pk = {
        let pk_symbols = key_indices
            .iter()
            .map(|key_index| quote_identifier(&schema.fields()[*key_index].name))
            .join(", ");
        format!("({})", pk_symbols)
    };
    let parameters = create_parameter_tuples(key_indices.len(), n_tuples);
    format!("DELETE FROM {normalized_table_name} WHERE {pk} in ({parameters})")
}

fn create_upsert_sql(
    schema: &Schema,
    schema_name: &str,
    table_name: &str,
    key_indices: &[usize],
    n_tuples: usize,
) -> String {
    let insert_sql = create_insert_sql(schema, schema_name, table_name, n_tuples);
    let pk_columns = key_indices
        .iter()
        .map(|key_index| quote_identifier(&schema.fields()[*key_index].name))
        .collect_vec()
        .join(", ");
    let update_parameters: String = (0..schema.len())
        .filter(|i| !key_indices.contains(i))
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
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;

    use super::*;

    fn check(actual: impl Display, expect: Expect) {
        let actual = actual.to_string();
        expect.assert_eq(&actual);
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ])
    }

    #[test]
    fn test_create_insert_sql() {
        let schema = test_schema();
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_insert_sql(&schema, schema_name, table_name, 3);
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("a", "b") VALUES ($1, $2), ($3, $4), ($5, $6)"#
            ]],
        );
    }

    #[test]
    fn test_create_delete_sql() {
        let schema = test_schema();
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, schema_name, table_name, &[1], 3);
        check(
            sql,
            expect![[
                r#"DELETE FROM "test_schema"."test_table" WHERE ("b") in (($1), ($2), ($3))"#
            ]],
        );
        let sql = create_delete_sql(&schema, schema_name, table_name, &[0, 1], 3);
        check(
            sql,
            expect![[
                r#"DELETE FROM "test_schema"."test_table" WHERE ("a", "b") in (($1, $2), ($3, $4), ($5, $6))"#
            ]],
        );
    }

    #[test]
    fn test_create_upsert_sql() {
        let schema = test_schema();
        let schema_name = "test_schema";
        let table_name = "test_table";
        let sql = create_upsert_sql(&schema, schema_name, table_name, &[1], 3);
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("a", "b") VALUES ($1, $2), ($3, $4), ($5, $6) on conflict ("b") do update set "a" = EXCLUDED."a""#
            ]],
        );

        let composite = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "user_id".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "client_id".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "value".to_owned(),
            },
        ]);
        let sql = create_upsert_sql(&composite, schema_name, table_name, &[0, 1], 2);
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("user_id", "client_id", "value") VALUES ($1, $2, $3), ($4, $5, $6) on conflict ("user_id", "client_id") do update set "value" = EXCLUDED."value""#
            ]],
        );

        // All columns in the pk: nothing to update on conflict.
        let all_pk = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "user_id".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "client_id".to_owned(),
            },
        ]);
        let sql = create_upsert_sql(&all_pk, schema_name, table_name, &[0, 1], 2);
        check(
            sql,
            expect![[
                r#"INSERT INTO "test_schema"."test_table" ("user_id", "client_id") VALUES ($1, $2), ($3, $4) on conflict ("user_id", "client_id") do nothing"#
            ]],
        );
    }

    #[test]
    fn test_split_power_of_two() {
        let check_split = |len: usize, cap: usize, expect: &[usize]| {
            let rows = vec![(); len];
            let sizes = split_power_of_two(&rows, cap)
                .iter()
                .map(|c| c.len())
                .collect_vec();
            assert_eq!(sizes, expect, "len={len} cap={cap}");
        };
        check_split(1024, 1024, &[1024]);
        check_split(922, 1024, &[512, 256, 128, 16, 8, 2]);
        check_split(37, 1024, &[32, 4, 1]);
        check_split(1, 1, &[1]);
        check_split(1000, 327, &[256, 256, 256, 128, 64, 32, 8]);

        assert_eq!(prev_power_of_two(1), 1);
        assert_eq!(prev_power_of_two(3), 2);
        assert_eq!(prev_power_of_two(1023), 512);
        assert_eq!(prev_power_of_two(1024), 1024);
    }

    #[tokio::test]
    async fn test_validate_max_batch_rows_range() {
        let properties = BTreeMap::from(
            [
                ("host", "localhost"),
                ("port", "5432"),
                ("user", "u"),
                ("password", "p"),
                ("database", "d"),
                ("table", "t"),
                ("type", "upsert"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );
        // The range check fails before any connection is attempted.
        for bad in ["0", "65537"] {
            let mut properties = properties.clone();
            properties.insert("max_batch_rows".to_owned(), bad.to_owned());
            let config = PostgresConfig::from_btreemap(properties).unwrap();
            let sink = PostgresSink::new(config, test_schema(), vec![0], false).unwrap();
            let err = sink.validate().await.unwrap_err();
            assert!(err.to_string().contains("max_batch_rows"), "{}", err);
        }
    }

    #[test]
    fn test_tuples_per_statement() {
        assert_eq!(tuples_per_statement(1024, 2), 1024);
        // Absurd `max_batch_rows` values are capped by the parameter limit.
        assert_eq!(tuples_per_statement(usize::MAX, 1), 32767);
        assert_eq!(tuples_per_statement(usize::MAX, 2), 16383);
        assert_eq!(tuples_per_statement(1_000_000, 3), 10922);
        assert_eq!(tuples_per_statement(0, 2), 1);

        // The largest allowed statement stays within the parameter limit.
        let schema = test_schema();
        let n_tuples = tuples_per_statement(usize::MAX, schema.len());
        assert_eq!(n_tuples * schema.len(), 32766);
        assert!(n_tuples * schema.len() <= MAX_STATEMENT_PARAMS);
        let sql = create_insert_sql(&schema, "test_schema", "test_table", n_tuples);
        assert!(sql.ends_with("($32765, $32766)"));
    }

    fn render_pending(pending: &PendingRows) -> String {
        match pending {
            PendingRows::Insert(rows) => rows
                .iter()
                .map(|row| format!("insert {:?}", row))
                .join("\n"),
            PendingRows::Upsert { rows, .. } => rows
                .iter()
                .map(|(key, (seq, op))| match op {
                    PendingOp::Upsert(row) => format!("{:?} => #{} upsert {:?}", key, seq, row),
                    PendingOp::Delete => format!("{:?} => #{} delete", key, seq),
                })
                .sorted()
                .join("\n"),
        }
    }

    fn first_columns(rows: &[PgRow]) -> Vec<String> {
        rows.iter().map(|row| format!("{:?}", row[0])).collect()
    }

    #[test]
    fn test_pending_upsert_keep_last() {
        let types = vec![PgType::INT4, PgType::INT4];
        let key_indices = [0];
        let key_types = vec![PgType::INT4];
        let mut pending = PendingRows::Upsert {
            absorbed: 0,
            rows: HashMap::new(),
        };

        // Delete then insert on the same key collapses into an upsert, insert then delete into a
        // delete, and the last of several upserts wins.
        pending.absorb(
            &StreamChunk::from_pretty(
                " i i
                 - 1 10
                 + 1 11
                 + 2 20
                 - 2 20
                 + 3 30",
            ),
            &key_indices,
            &types,
        );
        pending.absorb(
            &StreamChunk::from_pretty(
                "  i i
                 U- 3 30
                 U+ 3 31",
            ),
            &key_indices,
            &types,
        );
        // Seven rows absorbed, three distinct keys left after dedup.
        assert_eq!(pending.absorbed(), 7);
        check(
            render_pending(&pending),
            expect![[r#"
                OwnedRow([Some(Int32(1))]) => #1 upsert [Some(Builtin(Int32(1))), Some(Builtin(Int32(11)))]
                OwnedRow([Some(Int32(2))]) => #3 delete
                OwnedRow([Some(Int32(3))]) => #6 upsert [Some(Builtin(Int32(3))), Some(Builtin(Int32(31)))]"#]],
        );

        let (deletes, upserts) = pending.take(&key_types);
        assert_eq!(deletes.len(), 1);
        assert_eq!(upserts.len(), 2);
        assert_eq!(pending.absorbed(), 0);
    }

    #[test]
    fn test_pending_take_chronological() {
        let types = vec![PgType::INT4, PgType::INT4];
        let mut pending = PendingRows::Upsert {
            absorbed: 0,
            rows: HashMap::new(),
        };

        let inserts = (1..=64).map(|k| format!("+ {k} {k}")).join("\n");
        pending.absorb(
            &StreamChunk::from_pretty(&format!(" i i\n{inserts}")),
            &[0],
            &types,
        );
        // Re-upserting a key moves it to the end.
        pending.absorb(
            &StreamChunk::from_pretty(
                " i i
                 - 3 3
                 + 1 100
                 - 2 2",
            ),
            &[0],
            &types,
        );

        let (deletes, upserts) = pending.take(&[PgType::INT4]);
        let expected_upserts = (4..=64)
            .chain([1])
            .map(|k| format!("Some(Builtin(Int32({k})))"))
            .collect_vec();
        assert_eq!(first_columns(&upserts), expected_upserts);
        assert_eq!(
            first_columns(&deletes),
            ["Some(Builtin(Int32(3)))", "Some(Builtin(Int32(2)))"]
        );
    }

    #[test]
    fn test_permuted_key_indices() {
        // Regression guard: key types and values must follow the declared key order, not schema
        // order.
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "b".to_owned(),
            },
        ]);
        let sql = create_delete_sql(&schema, "test_schema", "test_table", &[1, 0], 2);
        check(
            sql,
            expect![[
                r#"DELETE FROM "test_schema"."test_table" WHERE ("b", "a") in (($1, $2), ($3, $4))"#
            ]],
        );

        let schema_types = vec![PgType::INT4, PgType::TEXT];
        let mut pending = PendingRows::Upsert {
            absorbed: 0,
            rows: HashMap::new(),
        };
        pending.absorb(
            &StreamChunk::from_pretty(
                " i T
                 - 1 x",
            ),
            &[1, 0],
            &schema_types,
        );
        let (deletes, upserts) = pending.take(&[PgType::TEXT, PgType::INT4]);
        assert!(upserts.is_empty());
        check(
            format!("{:?}", deletes),
            expect![[r#"[[Some(Builtin(Utf8("x"))), Some(Builtin(Int32(1)))]]"#]],
        );
    }

    #[test]
    fn test_pending_insert_no_dedup() {
        let types = vec![PgType::INT4, PgType::INT4];
        let mut pending = PendingRows::Insert(Vec::new());
        // Non-insert ops are dropped by append-only sinks.
        pending.absorb(
            &StreamChunk::from_pretty(
                " i i
                 + 1 10
                 + 1 10
                 - 1 10",
            ),
            &[0],
            &types,
        );
        check(
            render_pending(&pending),
            expect![[r#"
                insert [Some(Builtin(Int32(1))), Some(Builtin(Int32(10)))]
                insert [Some(Builtin(Int32(1))), Some(Builtin(Int32(10)))]"#]],
        );
        let (deletes, upserts) = pending.take(&[PgType::INT4]);
        assert!(deletes.is_empty());
        assert_eq!(upserts.len(), 2);
    }
}
