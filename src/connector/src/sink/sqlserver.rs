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

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use phf::{Set, phf_set};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Decimal};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use simd_json::prelude::ArrayTrait;
use tiberius::numeric::Numeric;
use tiberius::{AuthMethod, Client, ColumnData, Config, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use with_options::WithOptions;

use super::{
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkWriterMetrics,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{Result, Sink, SinkParam, SinkWriterParam};

pub const SQLSERVER_SINK: &str = "sqlserver";

fn default_max_batch_rows() -> usize {
    1024
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SqlServerConfig {
    #[serde(rename = "sqlserver.host")]
    pub host: String,
    #[serde(rename = "sqlserver.port")]
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    #[serde(rename = "sqlserver.user")]
    pub user: String,
    #[serde(rename = "sqlserver.password")]
    pub password: String,
    #[serde(rename = "sqlserver.database")]
    pub database: String,
    #[serde(rename = "sqlserver.schema", default = "sql_server_default_schema")]
    pub schema: String,
    #[serde(rename = "sqlserver.table")]
    pub table: String,
    #[serde(
        rename = "sqlserver.max_batch_rows",
        default = "default_max_batch_rows"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(SqlServerConfig);

pub fn sql_server_default_schema() -> String {
    "dbo".to_owned()
}

impl SqlServerConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SqlServerConfig>(serde_json::to_value(properties).unwrap())
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

    pub fn full_object_path(&self) -> String {
        format!("[{}].[{}].[{}]", self.database, self.schema, self.table)
    }
}

impl EnforceSecret for SqlServerConfig {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "sqlserver.password"
    };
}
#[derive(Debug)]
pub struct SqlServerSink {
    pub config: SqlServerConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

struct SqlServerColumnMetadata {
    name: String,
    is_pk: bool,
    data_type: String,
}

impl EnforceSecret for SqlServerSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::sink::ConnectorResult<()> {
        for prop in prop_iter {
            SqlServerConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}
impl SqlServerSink {
    pub fn new(
        mut config: SqlServerConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        // Rewrite config because tiberius allows a maximum of 2100 params in one query request.
        const TIBERIUS_PARAM_MAX: usize = 2000;
        let params_per_op = schema.fields().len();
        let tiberius_max_batch_rows = if params_per_op == 0 {
            config.max_batch_rows
        } else {
            ((TIBERIUS_PARAM_MAX as f64 / params_per_op as f64).floor()) as usize
        };
        if tiberius_max_batch_rows == 0 {
            return Err(SinkError::SqlServer(anyhow!(format!(
                "too many column {}",
                params_per_op
            ))));
        }
        config.max_batch_rows = std::cmp::min(config.max_batch_rows, tiberius_max_batch_rows);
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for SqlServerSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = SqlServerConfig::from_btreemap(param.properties)?;
        SqlServerSink::new(config, schema, pk_indices, param.sink_type.is_append_only())
    }
}

impl Sink for SqlServerSink {
    type LogSinker = LogSinkerOf<SqlServerSinkWriter>;

    const SINK_NAME: &'static str = SQLSERVER_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::SqlServerSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;

        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert SQL Server sink (please define in `primary_key` field)"
            )));
        }

        for f in self.schema.fields() {
            check_data_type_compatibility(&f.data_type)?;
        }

        let mut sql_client = SqlServerClient::new(&self.config).await?;
        validate_sql_server_write_permission(&mut sql_client, &self.config, self.is_append_only)
            .await?;
        let sql_server_table_metadata =
            query_sql_server_table_metadata(&mut sql_client, &self.config).await?;
        let sql_server_pk_count = sql_server_table_metadata
            .iter()
            .filter(|metadata| metadata.is_pk)
            .count();
        let sql_server_table_metadata = sql_server_table_metadata
            .into_iter()
            .map(|metadata| (metadata.name.clone(), metadata))
            .collect::<HashMap<_, _>>();

        // Validate Column name, Primary Key and data type.
        for (idx, col) in self.schema.fields().iter().enumerate() {
            let rw_is_pk = self.pk_indices.contains(&idx);
            match sql_server_table_metadata.get(&normalize_sql_server_column_name(&col.name)) {
                None => {
                    return Err(SinkError::SqlServer(anyhow!(format!(
                        "column {} not found in the downstream SQL Server table {}",
                        col.name,
                        self.config.full_object_path()
                    ))));
                }
                Some(sql_server_col) => {
                    validate_data_type_compatibility(
                        &col.name,
                        &col.data_type,
                        &sql_server_col.data_type,
                    )?;
                    if self.is_append_only {
                        continue;
                    }
                    if rw_is_pk && !sql_server_col.is_pk {
                        return Err(SinkError::SqlServer(anyhow!(format!(
                            "column {} specified in primary_key mismatches with the downstream SQL Server table {} PK",
                            col.name,
                            self.config.full_object_path(),
                        ))));
                    }
                    if !rw_is_pk && sql_server_col.is_pk {
                        return Err(SinkError::SqlServer(anyhow!(format!(
                            "column {} unspecified in primary_key mismatches with the downstream SQL Server table {} PK",
                            col.name,
                            self.config.full_object_path(),
                        ))));
                    }
                }
            }
        }

        if !self.is_append_only && sql_server_pk_count != self.pk_indices.len() {
            let sql_server_pk_columns = sql_server_table_metadata
                .values()
                .filter(|metadata| metadata.is_pk)
                .map(|metadata| metadata.name.as_str())
                .collect::<Vec<_>>()
                .join(",");
            let rw_pk_columns = self
                .pk_indices
                .iter()
                .map(|idx| self.schema[*idx].name.as_str())
                .collect::<Vec<_>>()
                .join(",");
            return Err(SinkError::SqlServer(anyhow!(format!(
                "primary key does not match between RisingWave sink ({}: [{}]) and SQL Server table {} ({}: [{}])",
                self.pk_indices.len(),
                rw_pk_columns,
                self.config.full_object_path(),
                sql_server_pk_count,
                sql_server_pk_columns,
            ))));
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SqlServerSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(SinkWriterMetrics::new(&writer_param)))
    }
}

enum SqlOp {
    Insert(OwnedRow),
    Merge(OwnedRow),
    Delete(OwnedRow),
}

pub struct SqlServerSinkWriter {
    config: SqlServerConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    downstream_column_data_types: Vec<String>,
    sql_client: SqlServerClient,
    ops: Vec<SqlOp>,
}

impl SqlServerSinkWriter {
    async fn new(
        config: SqlServerConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let mut sql_client = SqlServerClient::new(&config).await?;
        let downstream_column_data_types =
            query_downstream_column_metadata(&mut sql_client, &config, &schema)
                .await?
                .into_iter()
                .map(|metadata| metadata.data_type)
                .collect();
        let writer = Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            downstream_column_data_types,
            sql_client,
            ops: vec![],
        };
        Ok(writer)
    }

    async fn delete_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Delete(row.into_owned_row()));
        Ok(())
    }

    async fn upsert_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Merge(row.into_owned_row()));
        Ok(())
    }

    async fn insert_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Insert(row.into_owned_row()));
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        use std::fmt::Write;
        if self.ops.is_empty() {
            return Ok(());
        }
        let mut query_str = String::new();
        let col_num = self.schema.fields.len();
        let mut next_param_id = 1;
        let non_pk_col_indices = (0..col_num)
            .filter(|idx| !self.pk_indices.contains(idx))
            .collect::<Vec<usize>>();
        let all_col_names = self
            .schema
            .fields
            .iter()
            .map(|f| format!("[{}]", f.name))
            .collect::<Vec<_>>()
            .join(",");
        let all_source_col_names = self
            .schema
            .fields
            .iter()
            .map(|f| format!("[SOURCE].[{}]", f.name))
            .collect::<Vec<_>>()
            .join(",");
        let pk_match = self
            .pk_indices
            .iter()
            .map(|idx| {
                format!(
                    "[SOURCE].[{}]=[TARGET].[{}]",
                    self.schema[*idx].name, self.schema[*idx].name
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let param_placeholders = |param_id: &mut usize| {
            (0..col_num)
                .map(|_| param_placeholder(param_id))
                .collect::<Vec<_>>()
                .join(",")
        };
        let set_all_source_col = non_pk_col_indices
            .iter()
            .map(|idx| {
                format!(
                    "[{}]=[SOURCE].[{}]",
                    self.schema[*idx].name, self.schema[*idx].name
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        // TODO: avoid repeating the SQL
        for op in &self.ops {
            match op {
                SqlOp::Insert(_) => {
                    write!(
                        &mut query_str,
                        "INSERT INTO {} ({}) VALUES ({});",
                        self.config.full_object_path(),
                        all_col_names,
                        param_placeholders(&mut next_param_id),
                    )
                    .unwrap();
                }
                SqlOp::Merge(_) => {
                    write!(
                        &mut query_str,
                        r#"MERGE {} WITH (HOLDLOCK) AS [TARGET]
                        USING (VALUES ({})) AS [SOURCE] ({})
                        ON {}
                        WHEN MATCHED THEN UPDATE SET {}
                        WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
                        self.config.full_object_path(),
                        param_placeholders(&mut next_param_id),
                        all_col_names,
                        pk_match,
                        set_all_source_col,
                        all_col_names,
                        all_source_col_names,
                    )
                    .unwrap();
                }
                SqlOp::Delete(_) => {
                    write!(
                        &mut query_str,
                        r#"DELETE FROM {} WHERE {};"#,
                        self.config.full_object_path(),
                        self.pk_indices
                            .iter()
                            .map(|idx| {
                                let condition = format!(
                                    "[{}]={}",
                                    self.schema[*idx].name,
                                    param_placeholder(&mut next_param_id)
                                );
                                condition
                            })
                            .collect::<Vec<_>>()
                            .join(" AND "),
                    )
                    .unwrap();
                }
            }
        }

        let mut query = Query::new(query_str);
        for op in self.ops.drain(..) {
            match op {
                SqlOp::Insert(row) => {
                    bind_params(
                        &mut query,
                        row,
                        &self.schema,
                        &self.downstream_column_data_types,
                        0..col_num,
                    )?;
                }
                SqlOp::Merge(row) => {
                    bind_params(
                        &mut query,
                        row,
                        &self.schema,
                        &self.downstream_column_data_types,
                        0..col_num,
                    )?;
                }
                SqlOp::Delete(row) => {
                    bind_params(
                        &mut query,
                        row,
                        &self.schema,
                        &self.downstream_column_data_types,
                        self.pk_indices.iter().copied(),
                    )?;
                }
            }
        }
        query.execute(&mut self.sql_client.inner_client).await?;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for SqlServerSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    if self.is_append_only {
                        self.insert_one(row).await?;
                    } else {
                        self.upsert_one(row).await?;
                    }
                }
                Op::UpdateInsert => {
                    debug_assert!(!self.is_append_only);
                    self.upsert_one(row).await?;
                }
                Op::Delete => {
                    debug_assert!(!self.is_append_only);
                    self.delete_one(row).await?;
                }
                Op::UpdateDelete => {}
            }
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.flush().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct SqlServerClient {
    pub inner_client: Client<tokio_util::compat::Compat<TcpStream>>,
}

impl SqlServerClient {
    async fn new(msconfig: &SqlServerConfig) -> Result<Self> {
        let mut config = Config::new();
        config.host(&msconfig.host);
        config.port(msconfig.port);
        config.authentication(AuthMethod::sql_server(&msconfig.user, &msconfig.password));
        config.database(&msconfig.database);
        config.trust_cert();
        Self::new_with_config(config).await
    }

    pub async fn new_with_config(mut config: Config) -> Result<Self> {
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .context("failed to connect to sql server")
            .map_err(SinkError::SqlServer)?;
        tcp.set_nodelay(true)
            .context("failed to setting nodelay when connecting to sql server")
            .map_err(SinkError::SqlServer)?;

        let client = match Client::connect(config.clone(), tcp.compat_write()).await {
            // Connection successful.
            Ok(client) => client,
            // The server wants us to redirect to a different address
            Err(tiberius::error::Error::Routing { host, port }) => {
                config.host(&host);
                config.port(port);
                let tcp = TcpStream::connect(config.get_addr())
                    .await
                    .context("failed to connect to sql server after routing")
                    .map_err(SinkError::SqlServer)?;
                tcp.set_nodelay(true)
                    .context(
                        "failed to setting nodelay when connecting to sql server after routing",
                    )
                    .map_err(SinkError::SqlServer)?;
                // we should not have more than one redirect, so we'll short-circuit here.
                Client::connect(config, tcp.compat_write()).await?
            }
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            inner_client: client,
        })
    }
}

async fn query_sql_server_table_metadata(
    sql_client: &mut SqlServerClient,
    config: &SqlServerConfig,
) -> Result<Vec<SqlServerColumnMetadata>> {
    let mut sql_server_table_metadata = Vec::new();
    let query_table_metadata_error = || {
        SinkError::SqlServer(anyhow!(format!(
            "SQL Server table {} metadata error",
            config.full_object_path()
        )))
    };
    // Query primary-key membership through a subquery filtered by `pk.is_primary_key = 1`.
    // A column can appear in both the primary-key index and secondary indexes, and a naive
    // join from `sys.columns` to all `sys.index_columns` would emit extra index rows or mark
    // secondary-index-only columns as PK columns. Keep the PK filter inside the subquery so
    // each table column is returned once with `IsPk` set only by the primary-key index.
    static QUERY_TABLE_METADATA: &str = r#"
SELECT
    col.name AS ColumnName,
    CAST(CASE WHEN pk_col.column_id IS NULL THEN 0 ELSE 1 END AS int) AS IsPk,
    typ.name AS DataType
FROM
    sys.columns col
JOIN
    sys.types typ ON typ.user_type_id = col.user_type_id
LEFT JOIN
    (
        SELECT ic.object_id, ic.column_id
        FROM sys.indexes pk
        JOIN sys.index_columns ic ON ic.object_id = pk.object_id AND ic.index_id = pk.index_id
        WHERE pk.is_primary_key = 1
    ) pk_col ON pk_col.object_id = col.object_id AND pk_col.column_id = col.column_id
WHERE
    col.object_id = OBJECT_ID(@P1)
ORDER BY
    col.column_id;"#;
    let rows = sql_client
        .inner_client
        .query(QUERY_TABLE_METADATA, &[&config.full_object_path()])
        .await?
        .into_results()
        .await?;
    for row in rows.into_iter().flatten() {
        let mut iter = row.into_iter();
        let ColumnData::String(Some(col_name)) =
            iter.next().ok_or_else(query_table_metadata_error)?
        else {
            return Err(query_table_metadata_error());
        };
        let ColumnData::I32(Some(col_is_pk)) =
            iter.next().ok_or_else(query_table_metadata_error)?
        else {
            return Err(query_table_metadata_error());
        };
        let ColumnData::String(Some(data_type)) =
            iter.next().ok_or_else(query_table_metadata_error)?
        else {
            return Err(query_table_metadata_error());
        };
        sql_server_table_metadata.push(SqlServerColumnMetadata {
            name: normalize_sql_server_column_name(&col_name),
            is_pk: col_is_pk != 0,
            data_type: data_type.into_owned(),
        });
    }
    Ok(sql_server_table_metadata)
}

async fn validate_sql_server_write_permission(
    sql_client: &mut SqlServerClient,
    config: &SqlServerConfig,
    is_append_only: bool,
) -> Result<()> {
    let permission_query_error = || {
        SinkError::SqlServer(anyhow!(format!(
            "SQL Server table {} permission metadata error",
            config.full_object_path()
        )))
    };
    static QUERY_WRITE_PERMISSION: &str = r#"
SELECT
    CAST(HAS_PERMS_BY_NAME(@P1, 'OBJECT', 'INSERT') AS int) AS CanInsert,
    CAST(HAS_PERMS_BY_NAME(@P1, 'OBJECT', 'UPDATE') AS int) AS CanUpdate,
    CAST(HAS_PERMS_BY_NAME(@P1, 'OBJECT', 'DELETE') AS int) AS CanDelete;"#;
    let rows = sql_client
        .inner_client
        .query(QUERY_WRITE_PERMISSION, &[&config.full_object_path()])
        .await?
        .into_results()
        .await?;
    let mut rows = rows.into_iter().flatten();
    let row = rows.next().ok_or_else(permission_query_error)?;
    let mut iter = row.into_iter();
    let ColumnData::I32(can_insert) = iter.next().ok_or_else(permission_query_error)? else {
        return Err(permission_query_error());
    };
    let ColumnData::I32(can_update) = iter.next().ok_or_else(permission_query_error)? else {
        return Err(permission_query_error());
    };
    let ColumnData::I32(can_delete) = iter.next().ok_or_else(permission_query_error)? else {
        return Err(permission_query_error());
    };

    let missing_permissions = missing_sql_server_write_permissions(
        is_append_only,
        permission_is_granted(can_insert),
        permission_is_granted(can_update),
        permission_is_granted(can_delete),
    );
    if missing_permissions.is_empty() {
        return Ok(());
    }

    Err(SinkError::SqlServer(anyhow!(format!(
        "SQL Server user {} lacks required write permission(s) {} on table {}",
        config.user,
        missing_permissions.join(", "),
        config.full_object_path()
    ))))
}

fn permission_is_granted(permission_value: Option<i32>) -> bool {
    permission_value == Some(1)
}

fn missing_sql_server_write_permissions(
    is_append_only: bool,
    can_insert: bool,
    can_update: bool,
    can_delete: bool,
) -> Vec<&'static str> {
    let mut missing_permissions = vec![];
    if !can_insert {
        missing_permissions.push("INSERT");
    }
    if !is_append_only {
        if !can_update {
            missing_permissions.push("UPDATE");
        }
        if !can_delete {
            missing_permissions.push("DELETE");
        }
    }
    missing_permissions
}

async fn query_downstream_column_metadata(
    sql_client: &mut SqlServerClient,
    config: &SqlServerConfig,
    schema: &Schema,
) -> Result<Vec<SqlServerColumnMetadata>> {
    let sql_server_table_metadata = query_sql_server_table_metadata(sql_client, config)
        .await?
        .into_iter()
        .map(|metadata| (metadata.name.clone(), metadata))
        .collect::<HashMap<_, _>>();
    schema
        .fields()
        .iter()
        .map(|col| {
            sql_server_table_metadata
                .get(&normalize_sql_server_column_name(&col.name))
                .map(|metadata| SqlServerColumnMetadata {
                    name: metadata.name.clone(),
                    is_pk: metadata.is_pk,
                    data_type: metadata.data_type.clone(),
                })
                .ok_or_else(|| {
                    SinkError::SqlServer(anyhow!(format!(
                        "column {} not found in the downstream SQL Server table {}",
                        col.name,
                        config.full_object_path()
                    )))
                })
        })
        .collect()
}

fn param_placeholder(param_id: &mut usize) -> String {
    let placeholder = format!("@P{}", *param_id);
    *param_id += 1;
    placeholder
}

fn bind_params(
    query: &mut Query<'_>,
    row: impl Row,
    schema: &Schema,
    downstream_column_data_types: &[String],
    col_indices: impl Iterator<Item = usize>,
) -> Result<()> {
    use risingwave_common::types::ScalarRefImpl;
    for col_idx in col_indices {
        match row.datum_at(col_idx) {
            Some(data_ref) => match data_ref {
                ScalarRefImpl::Int16(v) => query.bind(v),
                ScalarRefImpl::Int32(v) => query.bind(v),
                ScalarRefImpl::Int64(v) => query.bind(v),
                ScalarRefImpl::Float32(v) => query.bind(v.into_inner()),
                ScalarRefImpl::Float64(v) => query.bind(v.into_inner()),
                ScalarRefImpl::Utf8(v) => query.bind(v.to_owned()),
                ScalarRefImpl::Bool(v) => query.bind(v),
                ScalarRefImpl::Decimal(v) => match v {
                    Decimal::Normalized(d) => {
                        query.bind(decimal_to_sql(&d));
                    }
                    Decimal::NaN | Decimal::PositiveInf | Decimal::NegativeInf => {
                        tracing::warn!(
                            "Inf, -Inf, Nan in RisingWave decimal is converted into SQL Server null!"
                        );
                        query.bind(None as Option<Numeric>);
                    }
                },
                ScalarRefImpl::Date(v) => query.bind(v.0),
                ScalarRefImpl::Timestamp(v) => query.bind(v.0),
                ScalarRefImpl::Timestamptz(v) => {
                    let downstream_data_type = &downstream_column_data_types[col_idx];
                    match downstream_data_type.as_str() {
                        "bigint" | "int" | "smallint" | "tinyint" => {
                            query.bind(v.timestamp_micros());
                        }
                        "datetimeoffset" => {
                            query.bind(v.to_datetime_utc().fixed_offset());
                        }
                        "datetime" | "datetime2" | "smalldatetime" => {
                            query.bind(v.to_datetime_utc().naive_utc());
                        }
                        _ => {
                            return Err(unexpected_downstream_timestamptz_type(
                                downstream_data_type,
                            ));
                        }
                    };
                }
                ScalarRefImpl::Time(v) => query.bind(v.0),
                ScalarRefImpl::Bytea(v) => query.bind(v.to_vec()),
                ScalarRefImpl::Interval(_) => return Err(data_type_not_supported("Interval")),
                ScalarRefImpl::Jsonb(_) => return Err(data_type_not_supported("Jsonb")),
                ScalarRefImpl::Variant(_) => return Err(data_type_not_supported("Variant")),
                ScalarRefImpl::Struct(_) => return Err(data_type_not_supported("Struct")),
                ScalarRefImpl::List(_) => return Err(data_type_not_supported("List")),
                ScalarRefImpl::Int256(_) => return Err(data_type_not_supported("Int256")),
                ScalarRefImpl::Serial(_) => return Err(data_type_not_supported("Serial")),
                ScalarRefImpl::Map(_) => return Err(data_type_not_supported("Map")),
                ScalarRefImpl::Vector(_) => return Err(data_type_not_supported("Vector")),
            },
            None => match schema[col_idx].data_type {
                DataType::Boolean => {
                    query.bind(None as Option<bool>);
                }
                DataType::Int16 => {
                    query.bind(None as Option<i16>);
                }
                DataType::Int32 => {
                    query.bind(None as Option<i32>);
                }
                DataType::Int64 => {
                    query.bind(None as Option<i64>);
                }
                DataType::Float32 => {
                    query.bind(None as Option<f32>);
                }
                DataType::Float64 => {
                    query.bind(None as Option<f64>);
                }
                DataType::Decimal => {
                    query.bind(None as Option<Numeric>);
                }
                DataType::Date => {
                    query.bind(None as Option<chrono::NaiveDate>);
                }
                DataType::Time => {
                    query.bind(None as Option<chrono::NaiveTime>);
                }
                DataType::Timestamp => {
                    query.bind(None as Option<chrono::NaiveDateTime>);
                }
                DataType::Timestamptz => {
                    let downstream_data_type = &downstream_column_data_types[col_idx];
                    match downstream_data_type.as_str() {
                        "bigint" | "int" | "smallint" | "tinyint" => {
                            query.bind(None as Option<i64>);
                        }
                        "datetimeoffset" => {
                            query.bind(None as Option<chrono::DateTime<chrono::FixedOffset>>);
                        }
                        "datetime" | "datetime2" | "smalldatetime" => {
                            query.bind(None as Option<chrono::NaiveDateTime>);
                        }
                        _ => {
                            return Err(unexpected_downstream_timestamptz_type(
                                downstream_data_type,
                            ));
                        }
                    };
                }
                DataType::Varchar => {
                    query.bind(None as Option<String>);
                }
                DataType::Bytea => {
                    query.bind(None as Option<Vec<u8>>);
                }
                DataType::Interval => return Err(data_type_not_supported("Interval")),
                DataType::Struct(_) => return Err(data_type_not_supported("Struct")),
                DataType::List(_) => return Err(data_type_not_supported("List")),
                DataType::Jsonb => return Err(data_type_not_supported("Jsonb")),
                DataType::Variant => return Err(data_type_not_supported("Variant")),
                DataType::Serial => return Err(data_type_not_supported("Serial")),
                DataType::Int256 => return Err(data_type_not_supported("Int256")),
                DataType::Map(_) => return Err(data_type_not_supported("Map")),
                DataType::Vector(_) => return Err(data_type_not_supported("Vector")),
            },
        };
    }
    Ok(())
}

fn data_type_not_supported(data_type_name: &str) -> SinkError {
    SinkError::SqlServer(anyhow!(format!(
        "{data_type_name} is not supported in SQL Server"
    )))
}

fn unexpected_downstream_timestamptz_type(sql_server_data_type: &str) -> SinkError {
    SinkError::SqlServer(anyhow!(format!(
        "unexpected downstream SQL Server type {sql_server_data_type} for Timestamptz"
    )))
}

fn check_data_type_compatibility(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal
        | DataType::Date
        | DataType::Varchar
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Bytea => Ok(()),
        DataType::Interval => Err(data_type_not_supported("Interval")),
        DataType::Struct(_) => Err(data_type_not_supported("Struct")),
        DataType::List(_) => Err(data_type_not_supported("List")),
        DataType::Jsonb => Err(data_type_not_supported("Jsonb")),
        DataType::Variant => Err(data_type_not_supported("Variant")),
        DataType::Serial => Err(data_type_not_supported("Serial")),
        DataType::Int256 => Err(data_type_not_supported("Int256")),
        DataType::Map(_) => Err(data_type_not_supported("Map")),
        DataType::Vector(_) => Err(data_type_not_supported("Vector")),
    }
}

fn normalize_sql_server_column_name(column_name: &str) -> String {
    // SQL Server identifiers are usually case-insensitive depending on database collation.
    // Match metadata by a case-insensitive key so validation follows that common behavior.
    column_name.to_lowercase()
}

fn validate_data_type_compatibility(
    column_name: &str,
    rw_data_type: &DataType,
    sql_server_data_type: &str,
) -> Result<()> {
    if sql_server_data_type_is_compatible(rw_data_type, sql_server_data_type) {
        return Ok(());
    }

    Err(SinkError::SqlServer(anyhow!(format!(
        "column {} data type {:?} is incompatible with downstream SQL Server type {}",
        column_name, rw_data_type, sql_server_data_type
    ))))
}

fn sql_server_data_type_is_compatible(rw_data_type: &DataType, sql_server_data_type: &str) -> bool {
    match rw_data_type {
        DataType::Boolean => sql_server_data_type == "bit",
        DataType::Int16 => matches!(sql_server_data_type, "smallint" | "int" | "bigint"),
        DataType::Int32 => matches!(sql_server_data_type, "int" | "bigint"),
        DataType::Int64 => sql_server_data_type == "bigint",
        DataType::Float32 => matches!(sql_server_data_type, "real" | "float"),
        DataType::Float64 => sql_server_data_type == "float",
        DataType::Decimal => matches!(sql_server_data_type, "decimal" | "numeric"),
        DataType::Date => sql_server_data_type == "date",
        DataType::Varchar => matches!(
            sql_server_data_type,
            "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext"
        ),
        DataType::Time => sql_server_data_type == "time",
        DataType::Timestamp => {
            matches!(
                sql_server_data_type,
                "datetime" | "datetime2" | "smalldatetime"
            )
        }
        DataType::Timestamptz => matches!(
            sql_server_data_type,
            "datetimeoffset"
                | "datetime"
                | "datetime2"
                | "smalldatetime"
                | "bigint"
                | "int"
                | "smallint"
                | "tinyint"
        ),
        DataType::Bytea => matches!(sql_server_data_type, "binary" | "varbinary" | "image"),
        DataType::Interval
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::Jsonb
        | DataType::Variant
        | DataType::Serial
        | DataType::Int256
        | DataType::Map(_)
        | DataType::Vector(_) => false,
    }
}

/// The implementation is copied from tiberius crate.
fn decimal_to_sql(decimal: &rust_decimal::Decimal) -> Numeric {
    let unpacked = decimal.unpack();

    let mut value = (((unpacked.hi as u128) << 64)
        + ((unpacked.mid as u128) << 32)
        + unpacked.lo as u128) as i128;

    if decimal.is_sign_negative() {
        value = -value;
    }

    Numeric::new_with_scale(value, decimal.scale() as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql_server_column_name() {
        assert_eq!(normalize_sql_server_column_name("EventDate"), "eventdate");
    }

    #[test]
    fn test_sql_server_data_type_compatibility() {
        assert!(sql_server_data_type_is_compatible(
            &DataType::Int16,
            "smallint"
        ));
        assert!(sql_server_data_type_is_compatible(&DataType::Int16, "int"));
        assert!(!sql_server_data_type_is_compatible(
            &DataType::Int32,
            "smallint"
        ));

        assert!(sql_server_data_type_is_compatible(
            &DataType::Timestamp,
            "datetime2"
        ));
        assert!(sql_server_data_type_is_compatible(
            &DataType::Timestamptz,
            "datetimeoffset"
        ));
        assert!(sql_server_data_type_is_compatible(
            &DataType::Timestamptz,
            "datetime2"
        ));
        assert!(sql_server_data_type_is_compatible(
            &DataType::Timestamptz,
            "bigint"
        ));
        assert!(sql_server_data_type_is_compatible(
            &DataType::Timestamptz,
            "int"
        ));
        assert!(!sql_server_data_type_is_compatible(
            &DataType::Timestamp,
            "datetimeoffset"
        ));

        assert!(sql_server_data_type_is_compatible(
            &DataType::Varchar,
            "nvarchar"
        ));
        assert!(!sql_server_data_type_is_compatible(
            &DataType::Varchar,
            "uniqueidentifier"
        ));
    }

    #[test]
    fn test_missing_sql_server_write_permissions() {
        assert_eq!(
            missing_sql_server_write_permissions(true, false, false, false),
            vec!["INSERT"]
        );
        assert!(missing_sql_server_write_permissions(true, true, false, false).is_empty());
        assert_eq!(
            missing_sql_server_write_permissions(false, false, false, false),
            vec!["INSERT", "UPDATE", "DELETE"]
        );
        assert_eq!(
            missing_sql_server_write_permissions(false, true, false, true),
            vec!["UPDATE"]
        );
        assert!(missing_sql_server_write_permissions(false, true, true, true).is_empty());
    }
}
