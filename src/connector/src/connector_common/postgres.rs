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
use std::fmt;

use anyhow::{Context, anyhow};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::{DataType, ScalarImpl, StructType};
use sea_schema::postgres::def::{ColumnType as SeaType, TableDef, TableInfo};
use sea_schema::postgres::discovery::SchemaDiscovery;
use sea_schema::sea_query::{Alias, IntoIden};
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{PgPool, Row};
use thiserror_ext::AsReport;
use tokio_postgres::types::Kind as PgKind;
use tokio_postgres::{Client as PgClient, NoTls};

use super::TcpKeepaliveConfig;
#[cfg(not(madsim))]
use super::maybe_tls_connector::MaybeMakeTlsConnector;
use crate::error::ConnectorResult;

/// SQL query to discover primary key columns directly from PostgreSQL system tables.
/// This bypasses querying `information_schema.table_constraints` to avoid permission issues.
/// Match `pg_class` and `pg_namespace` by exact catalog names instead of casting a
/// constructed string to `regclass`, as unquoted `regclass` input folds mixed-case
/// table names to lower case.
const DISCOVER_PRIMARY_KEY_QUERY: &str = r#"
    SELECT a.attname as column_name
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE n.nspname = $1
      AND c.relname = $2
      AND i.indisprimary = true
    ORDER BY array_position(i.indkey, a.attnum)
"#;

/// Discover pgvector columns with both `atttypmod` (dimension) and `format_type` text.
/// `vector(n)` is stored as `atttypmod = n`, while dimension-less `vector` uses `-1`.
/// We rely on this to keep user-defined type modifiers that are not preserved by sea-schema.
const DISCOVER_PGVECTOR_COLUMNS_QUERY: &str = r#"
    SELECT
      a.attname as column_name,
      a.atttypmod as atttypmod,
      format_type(a.atttypid, a.atttypmod) as formatted_type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE n.nspname = $1
      AND c.relname = $2
      AND t.typname = 'vector'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
"#;

const CHECK_TABLE_PRIVILEGE_QUERY: &str = r#"
    SELECT
      current_user::text AS user_name,
      n.oid IS NOT NULL AS schema_exists,
      c.oid IS NOT NULL AS table_exists,
      COALESCE(has_schema_privilege(current_user, n.oid, 'USAGE'), false) AS has_schema_usage,
      COALESCE(has_table_privilege(current_user, c.oid, $3), false) AS has_table_privilege,
      COALESCE(has_any_column_privilege(current_user, c.oid, $3), false) AS has_any_column_privilege
    FROM (SELECT 1) AS one
    LEFT JOIN pg_namespace n ON n.nspname = $1
    LEFT JOIN pg_class c ON c.relnamespace = n.oid
      AND c.relname = $2
      AND c.relkind IN ('r', 'p')
    LIMIT 1
"#;

/// Canonical Postgres connection parameters shared across sink, source CDC, batch
/// executor, and frontend `postgres_query` table function. Each caller constructs
/// this from its own user-facing config struct before invoking the shared helpers
/// like `create_pg_client` or `PostgresExternalTable::connect`.
#[derive(Debug, Clone)]
pub struct PgConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub ssl_mode: SslMode,
    pub ssl_root_cert: Option<String>,
}

impl PgConnectionConfig {
    fn to_sqlx_connect_options(&self) -> PgConnectOptions {
        let mut options = PgConnectOptions::new()
            .username(&self.user)
            .password(&self.password)
            .host(&self.host)
            .port(self.port)
            .database(&self.database)
            .ssl_mode(match self.ssl_mode {
                SslMode::Disabled => PgSslMode::Disable,
                SslMode::Preferred => PgSslMode::Prefer,
                SslMode::Required => PgSslMode::Require,
                SslMode::VerifyCa => PgSslMode::VerifyCa,
                SslMode::VerifyFull => PgSslMode::VerifyFull,
            });

        if matches!(self.ssl_mode, SslMode::VerifyCa | SslMode::VerifyFull)
            && let Some(root_cert) = &self.ssl_root_cert
        {
            options = options.ssl_root_cert(root_cert.as_str());
        }

        options
    }
}

pub fn pg_connection_config_from_properties(
    props: &BTreeMap<String, String>,
) -> ConnectorResult<PgConnectionConfig> {
    Ok(PgConnectionConfig {
        host: props
            .get("hostname")
            .context("missing `hostname` in postgres-cdc properties")?
            .clone(),
        port: {
            let raw = props
                .get("port")
                .context("missing `port` in postgres-cdc properties")?;
            raw.parse::<u16>()
                .with_context(|| format!("invalid postgres port `{}`", raw))?
        },
        user: props
            .get("username")
            .context("missing `username` in postgres-cdc properties")?
            .clone(),
        password: props.get("password").cloned().unwrap_or_default(),
        database: props
            .get("database.name")
            .context("missing `database.name` in postgres-cdc properties")?
            .clone(),
        ssl_mode: props
            .get("ssl.mode")
            .and_then(|v| v.parse::<SslMode>().ok())
            .unwrap_or_default(),
        ssl_root_cert: props.get("ssl.root.cert").cloned(),
    })
}

pub async fn create_pg_client_from_properties(
    props: &BTreeMap<String, String>,
    tcp_keepalive: Option<TcpKeepaliveConfig>,
) -> ConnectorResult<PgClient> {
    let config = pg_connection_config_from_properties(props)?;
    create_pg_client(&config, tcp_keepalive)
        .await
        .map_err(Into::into)
}

pub async fn discover_pgvector_dimensions(
    client: &PgClient,
    schema: &str,
    table: &str,
) -> ConnectorResult<HashMap<String, usize>> {
    let rows = client
        .query(DISCOVER_PGVECTOR_COLUMNS_QUERY, &[&schema, &table])
        .await?;

    let mut dims = HashMap::new();
    for row in rows {
        let col_name: String = row.get("column_name");
        let atttypmod: i32 = row.get("atttypmod");
        if atttypmod > 0
            && let Ok(dim) = usize::try_from(atttypmod)
        {
            dims.insert(col_name, dim);
        }
    }
    Ok(dims)
}

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    #[serde(alias = "disable")]
    Disabled,
    #[serde(alias = "prefer")]
    #[default]
    Preferred,
    #[serde(alias = "require")]
    Required,
    /// verify that the server is trustworthy by checking the certificate chain
    /// up to the root certificate stored on the client.
    #[serde(alias = "verify-ca")]
    VerifyCa,
    /// Besides verify the certificate, will also verify that the serverhost name
    /// matches the name stored in the server certificate.
    #[serde(alias = "verify-full")]
    VerifyFull,
}

pub struct PostgresExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

struct PostgresTablePrivilege {
    user_name: String,
    schema_exists: bool,
    table_exists: bool,
    has_schema_usage: bool,
    has_table_privilege: bool,
    has_any_column_privilege: bool,
}

impl PostgresExternalTable {
    /// Discover primary key columns directly from PostgreSQL system tables.
    /// This bypasses querying `information_schema.table_constraints` to avoid requiring table owner permissions.
    async fn discover_primary_key(
        connection: &PgPool,
        schema_name: &str,
        table_name: &str,
    ) -> ConnectorResult<Vec<String>> {
        let rows = sqlx::query(DISCOVER_PRIMARY_KEY_QUERY)
            .bind(schema_name)
            .bind(table_name)
            .fetch_all(connection)
            .await
            .context("Failed to discover primary key columns")?;

        let pk_columns = rows
            .into_iter()
            .map(|row| row.get::<String, _>("column_name"))
            .collect();

        Ok(pk_columns)
    }

    /// Discover schema with workaround for primary key discovery
    /// This method uses direct PostgreSQL system table queries for primary keys
    /// to avoid permission issues when querying `information_schema.table_constraints`
    async fn discover_pk_and_full_columns(
        config: &PgConnectionConfig,
        schema: &str,
        table: &str,
        required_table_privilege: Option<&str>,
    ) -> ConnectorResult<(Vec<sea_schema::postgres::def::ColumnInfo>, Vec<String>)> {
        let options = config.to_sqlx_connect_options();
        let connection = PgPool::connect_with(options).await?;

        // Keep using sea-schema for column discovery, then run targeted access diagnostics below.
        let schema_discovery = SchemaDiscovery::new(connection.clone(), schema);
        let empty_map: HashMap<String, Vec<String>> = HashMap::new();
        let columns = schema_discovery
            .discover_columns(
                Alias::new(schema).into_iden(),
                Alias::new(table).into_iden(),
                &empty_map,
            )
            .await?;

        let pgvector_columns = sqlx::query(DISCOVER_PGVECTOR_COLUMNS_QUERY)
            .bind(schema)
            .bind(table)
            .fetch_all(&connection)
            .await
            .context("Failed to discover PostgreSQL pgvector columns")?;
        let formatted_type_by_column: HashMap<String, String> = pgvector_columns
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("column_name"),
                    row.get::<String, _>("formatted_type"),
                )
            })
            .collect();

        // sea-schema reports pgvector as `Unknown("vector")` and drops the dimension.
        // Patch it with PostgreSQL's formatted type text so we can derive vector(n).
        let mut columns = columns;
        if let Some(privilege) = required_table_privilege {
            Self::ensure_table_privilege(&connection, schema, table, privilege).await?;
        }

        for col in &mut columns {
            if let SeaType::Unknown(name) = &col.col_type
                && name.eq_ignore_ascii_case("vector")
                && let Some(formatted_type) = formatted_type_by_column.get(&col.name)
            {
                col.col_type = SeaType::Unknown(formatted_type.clone());
            }
        }

        // Use direct system table query for primary key discovery
        let pk_columns = Self::discover_primary_key(&connection, schema, table).await?;

        Ok((columns, pk_columns))
    }

    async fn ensure_table_privilege(
        connection: &PgPool,
        schema: &str,
        table: &str,
        required_privilege: &str,
    ) -> ConnectorResult<()> {
        let row = sqlx::query(CHECK_TABLE_PRIVILEGE_QUERY)
            .bind(schema)
            .bind(table)
            .bind(required_privilege)
            .fetch_one(connection)
            .await
            .context("Failed to check PostgreSQL table privileges")?;

        let privilege_status = PostgresTablePrivilege {
            user_name: row.get("user_name"),
            schema_exists: row.get("schema_exists"),
            table_exists: row.get("table_exists"),
            has_schema_usage: row.get("has_schema_usage"),
            has_table_privilege: row.get("has_table_privilege"),
            has_any_column_privilege: row.get("has_any_column_privilege"),
        };

        if !privilege_status.schema_exists {
            return Err(anyhow!("PostgreSQL schema `{schema}` does not exist").into());
        }

        if !privilege_status.table_exists {
            return Err(anyhow!("PostgreSQL table `{schema}`.`{table}` does not exist").into());
        }

        if !privilege_status.has_schema_usage {
            return Err(anyhow!(
                "PostgreSQL table {} exists, but the connection user `{}` does not have USAGE privilege on schema `{}`. Grant privileges on the upstream PostgreSQL database: {}",
                format_pg_table_name(schema, table),
                privilege_status.user_name,
                schema,
                format_grant_usage(schema, &privilege_status.user_name),
            )
            .into());
        }

        if !privilege_status.has_table_privilege {
            let column_privilege_msg = if privilege_status.has_any_column_privilege {
                " The user has column-level privilege on at least one column, but RisingWave requires table-level privilege for CDC schema discovery and snapshot reads."
            } else {
                ""
            };
            return Err(anyhow!(
                "PostgreSQL table {} exists, but the connection user `{}` does not have {} privilege on it.{} Grant privileges on the upstream PostgreSQL database: {}",
                format_pg_table_name(schema, table),
                privilege_status.user_name,
                required_privilege,
                column_privilege_msg,
                format_required_table_grants(
                    schema,
                    table,
                    &privilege_status.user_name,
                    required_privilege
                ),
            )
            .into());
        }

        Ok(())
    }

    async fn discover_schema(
        config: &PgConnectionConfig,
        schema: &str,
        table: &str,
    ) -> ConnectorResult<TableDef> {
        let options = config.to_sqlx_connect_options();
        let connection = PgPool::connect_with(options).await?;
        let schema_discovery = SchemaDiscovery::new(connection, schema);
        // fetch column schema and primary key
        let empty_map = HashMap::new();
        let table_schema = schema_discovery
            .discover_table(
                TableInfo {
                    name: table.to_owned(),
                    of_type: None,
                },
                &empty_map,
            )
            .await?;
        Ok(table_schema)
    }

    pub async fn connect(
        config: &PgConnectionConfig,
        schema: &str,
        table: &str,
        is_append_only: bool,
        required_table_privilege: Option<&str>,
    ) -> ConnectorResult<Self> {
        tracing::debug!("connect to postgres external table");

        let (columns, pk_names) =
            Self::discover_pk_and_full_columns(config, schema, table, required_table_privilege)
                .await?;

        let mut column_descs = vec![];
        for col in &columns {
            let rw_data_type = sea_type_to_rw_type(&col.col_type)?;
            let column_desc = if let Some(ref default_expr) = col.default {
                // parse the value of "column_default" field in information_schema.columns,
                // non number data type will be stored as "'value'::type"
                let val_text = default_expr
                    .0
                    .split("::")
                    .map(|s| s.trim_matches('\''))
                    .next()
                    .expect("default value expression");

                match ScalarImpl::from_text(val_text, &rw_data_type) {
                    Ok(scalar) => ColumnDesc::named_with_default_value(
                        col.name.clone(),
                        ColumnId::placeholder(),
                        rw_data_type.clone(),
                        Some(scalar),
                    ),
                    Err(err) => {
                        tracing::warn!(
                            error=%err.as_report(),
                            "failed to parse the PostgreSQL default value expression; only constants are supported",
                        );
                        ColumnDesc::named(col.name.clone(), ColumnId::placeholder(), rw_data_type)
                    }
                }
            } else {
                ColumnDesc::named(col.name.clone(), ColumnId::placeholder(), rw_data_type)
            };
            column_descs.push(column_desc);
        }

        // Check primary key existence using the directly discovered pk_names
        if !is_append_only && pk_names.is_empty() {
            return Err(anyhow!(
                "Postgres table should define the primary key for non-append-only tables"
            )
            .into());
        }

        Ok(Self {
            column_descs,
            pk_names,
        })
    }

    // return the mapping from column name to pg type, the pg type is used for writing data to postgres
    pub async fn type_mapping(
        config: &PgConnectionConfig,
        schema: &str,
        table: &str,
        is_append_only: bool,
    ) -> ConnectorResult<HashMap<String, tokio_postgres::types::Type>> {
        tracing::debug!("connect to postgres external table to get type mapping");
        let table_schema = Self::discover_schema(config, schema, table).await?;
        let mut column_name_to_pg_type = HashMap::new();
        for col in &table_schema.columns {
            let pg_type = sea_type_to_pg_type(&col.col_type)?;
            column_name_to_pg_type.insert(col.name.clone(), pg_type);
        }
        if !is_append_only && table_schema.primary_key_constraints.is_empty() {
            return Err(anyhow!(
                "Postgres table should define the primary key for non-append-only tables"
            )
            .into());
        }
        Ok(column_name_to_pg_type)
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
}

fn format_pg_table_name(schema: &str, table: &str) -> String {
    format!(
        "{}.{}",
        quote_pg_identifier(schema),
        quote_pg_identifier(table)
    )
}

fn format_grant_usage(schema: &str, user_name: &str) -> String {
    format!(
        "GRANT USAGE ON SCHEMA {} TO {};",
        quote_pg_identifier(schema),
        quote_pg_identifier(user_name)
    )
}

fn format_grant_table_privilege(
    schema: &str,
    table: &str,
    user_name: &str,
    privilege: &str,
) -> String {
    format!(
        "GRANT {} ON TABLE {} TO {};",
        privilege,
        format_pg_table_name(schema, table),
        quote_pg_identifier(user_name)
    )
}

fn format_required_table_grants(
    schema: &str,
    table: &str,
    user_name: &str,
    privilege: &str,
) -> String {
    format!(
        "{} {}",
        format_grant_usage(schema, user_name),
        format_grant_table_privilege(schema, table, user_name, privilege)
    )
}

fn quote_pg_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SslMode::Disabled => "disabled",
            SslMode::Preferred => "preferred",
            SslMode::Required => "required",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        })
    }
}

impl std::str::FromStr for SslMode {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_value(serde_json::Value::String(s.to_owned()))
    }
}

pub async fn create_pg_client(
    config: &PgConnectionConfig,
    tcp_keepalive: Option<TcpKeepaliveConfig>,
) -> anyhow::Result<PgClient> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .user(&config.user)
        .password(&config.password)
        .host(&config.host)
        .port(config.port)
        .dbname(&config.database);

    // Configure TCP keepalive if provided
    if let Some(keepalive) = tcp_keepalive {
        pg_config.keepalives(true);
        pg_config.keepalives_idle(std::time::Duration::from_secs(
            keepalive.tcp_keepalive_idle as u64,
        ));
        #[cfg(not(target_os = "windows"))]
        {
            pg_config.keepalives_interval(std::time::Duration::from_secs(
                keepalive.tcp_keepalive_interval as u64,
            ));
            pg_config.keepalives_retries(keepalive.tcp_keepalive_count);
        }
        tracing::info!(
            "TCP keepalive enabled: idle={}s, interval={}s, retries={}",
            keepalive.tcp_keepalive_idle,
            keepalive.tcp_keepalive_interval,
            keepalive.tcp_keepalive_count
        );
    }

    let verify_hostname = matches!(config.ssl_mode, SslMode::VerifyFull);

    #[cfg(not(madsim))]
    let connector = match config.ssl_mode {
        SslMode::Disabled => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Disable);
            MaybeMakeTlsConnector::NoTls(NoTls)
        }
        SslMode::Preferred => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Prefer);
            match SslConnector::builder(SslMethod::tls()) {
                Ok(mut builder) => {
                    // disable certificate verification for `prefer`
                    builder.set_verify(SslVerifyMode::NONE);
                    MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "SSL connector error");
                    MaybeMakeTlsConnector::NoTls(NoTls)
                }
            }
        }
        SslMode::Required => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            // disable certificate verification for `require`
            builder.set_verify(SslVerifyMode::NONE);
            MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
        }

        SslMode::VerifyCa | SslMode::VerifyFull => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            if let Some(ssl_root_cert) = &config.ssl_root_cert {
                builder.set_ca_file(ssl_root_cert).map_err(|e| {
                    anyhow!(format!("bad ssl root cert error: {}", e.to_report_string()))
                })?;
            }
            let mut connector = MakeTlsConnector::new(builder.build());
            if !verify_hostname {
                connector.set_callback(|c, _| {
                    c.set_verify_hostname(false);
                    Ok(())
                });
            }
            MaybeMakeTlsConnector::Tls(connector)
        }
    };
    #[cfg(madsim)]
    let connector = NoTls;

    let (client, connection) = pg_config.connect(connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e.as_report(), "postgres connection error");
        }
    });

    Ok(client)
}

pub fn postgres_point_type() -> DataType {
    DataType::Struct(StructType::new(vec![
        ("x", DataType::Float64),
        ("y", DataType::Float64),
    ]))
}

// Used for both source and sink connector
pub fn sea_type_to_rw_type(col_type: &SeaType) -> ConnectorResult<DataType> {
    let dtype = match col_type {
        SeaType::SmallInt | SeaType::SmallSerial => DataType::Int16,
        SeaType::Integer | SeaType::Serial => DataType::Int32,
        SeaType::BigInt | SeaType::BigSerial => DataType::Int64,
        SeaType::Money | SeaType::Decimal(_) | SeaType::Numeric(_) => DataType::Decimal,
        SeaType::Real => DataType::Float32,
        SeaType::DoublePrecision => DataType::Float64,
        SeaType::Varchar(_) | SeaType::Char(_) | SeaType::Text => DataType::Varchar,
        SeaType::Bytea => DataType::Bytea,
        SeaType::Timestamp(_) => DataType::Timestamp,
        SeaType::TimestampWithTimeZone(_) => DataType::Timestamptz,
        SeaType::Date => DataType::Date,
        SeaType::Time(_) | SeaType::TimeWithTimeZone(_) => DataType::Time,
        SeaType::Interval(_) => DataType::Interval,
        SeaType::Boolean => DataType::Boolean,
        SeaType::Point => postgres_point_type(),
        SeaType::Uuid => DataType::Varchar,
        SeaType::Xml => DataType::Varchar,
        SeaType::Json => DataType::Jsonb,
        SeaType::JsonBinary => DataType::Jsonb,
        SeaType::Array(def) => {
            let item_type = match def.col_type.as_ref() {
                Some(ty) => sea_type_to_rw_type(ty.as_ref())?,
                None => {
                    return Err(anyhow!("ARRAY type missing element type").into());
                }
            };

            DataType::list(item_type)
        }
        SeaType::PgLsn => DataType::Int64,
        SeaType::Cidr
        | SeaType::Inet
        | SeaType::MacAddr
        | SeaType::MacAddr8
        | SeaType::Int4Range
        | SeaType::Int8Range
        | SeaType::NumRange
        | SeaType::TsRange
        | SeaType::TsTzRange
        | SeaType::DateRange
        | SeaType::Enum(_) => DataType::Varchar,
        SeaType::Line
        | SeaType::Lseg
        | SeaType::Box
        | SeaType::Path
        | SeaType::Polygon
        | SeaType::Circle
        | SeaType::Bit(_)
        | SeaType::VarBit(_)
        | SeaType::TsVector
        | SeaType::TsQuery => {
            bail!("{:?} data type is not supported", col_type);
        }
        SeaType::Unknown(name) => {
            if let Some(dim) = parse_pgvector_dimension(name)? {
                DataType::Vector(dim)
            } else if matches!(name.to_ascii_lowercase().as_str(), "geometry" | "geography") {
                DataType::Bytea
            } else {
                // NOTES: user-defined enum type is classified as `Unknown`
                tracing::warn!("unknown PostgreSQL data type `{name}`; mapping it to varchar");
                DataType::Varchar
            }
        }
    };

    Ok(dtype)
}

fn parse_pgvector_dimension(type_name: &str) -> ConnectorResult<Option<usize>> {
    let normalized = type_name.trim().to_ascii_lowercase();
    if normalized == "vector" {
        bail!("pgvector type `vector` is missing dimension, expected `vector(n)`")
    }
    if !normalized.starts_with("vector(") || !normalized.ends_with(')') {
        return Ok(None);
    }

    let dim_text = normalized
        .trim_start_matches("vector(")
        .trim_end_matches(')')
        .trim();
    let dim = dim_text
        .parse::<usize>()
        .map_err(|_| anyhow!("invalid pgvector dimension in type `{type_name}`"))?;

    if !(1..=DataType::VEC_MAX_SIZE).contains(&dim) {
        bail!(
            "pgvector dimension out of range in type `{}`: expect 1..={}",
            type_name,
            DataType::VEC_MAX_SIZE
        );
    }

    Ok(Some(dim))
}

// Used for sink connector
// We use `sea-schema` for table schema discovery.
// So we have to map `sea-schema` pg types
// to `tokio-postgres` pg types (which we use for query binding).
fn sea_type_to_pg_type(sea_type: &SeaType) -> ConnectorResult<tokio_postgres::types::Type> {
    use tokio_postgres::types::Type as PgType;
    match sea_type {
        SeaType::SmallInt => Ok(PgType::INT2),
        SeaType::Integer => Ok(PgType::INT4),
        SeaType::BigInt => Ok(PgType::INT8),
        SeaType::Decimal(_) => Ok(PgType::NUMERIC),
        SeaType::Numeric(_) => Ok(PgType::NUMERIC),
        SeaType::Real => Ok(PgType::FLOAT4),
        SeaType::DoublePrecision => Ok(PgType::FLOAT8),
        SeaType::Varchar(_) => Ok(PgType::VARCHAR),
        SeaType::Char(_) => Ok(PgType::CHAR),
        SeaType::Text => Ok(PgType::TEXT),
        SeaType::Bytea => Ok(PgType::BYTEA),
        SeaType::Timestamp(_) => Ok(PgType::TIMESTAMP),
        SeaType::TimestampWithTimeZone(_) => Ok(PgType::TIMESTAMPTZ),
        SeaType::Date => Ok(PgType::DATE),
        SeaType::Time(_) => Ok(PgType::TIME),
        SeaType::TimeWithTimeZone(_) => Ok(PgType::TIMETZ),
        SeaType::Interval(_) => Ok(PgType::INTERVAL),
        SeaType::Boolean => Ok(PgType::BOOL),
        SeaType::Point => Ok(PgType::POINT),
        SeaType::Uuid => Ok(PgType::UUID),
        SeaType::Json => Ok(PgType::JSON),
        SeaType::JsonBinary => Ok(PgType::JSONB),
        SeaType::Array(t) => {
            let Some(t) = t.col_type.as_ref() else {
                bail!("missing array type")
            };
            match t.as_ref() {
                // RW only supports 1 level of nesting.
                SeaType::SmallInt => Ok(PgType::INT2_ARRAY),
                SeaType::Integer => Ok(PgType::INT4_ARRAY),
                SeaType::BigInt => Ok(PgType::INT8_ARRAY),
                SeaType::Decimal(_) => Ok(PgType::NUMERIC_ARRAY),
                SeaType::Numeric(_) => Ok(PgType::NUMERIC_ARRAY),
                SeaType::Real => Ok(PgType::FLOAT4_ARRAY),
                SeaType::DoublePrecision => Ok(PgType::FLOAT8_ARRAY),
                SeaType::Varchar(_) => Ok(PgType::VARCHAR_ARRAY),
                SeaType::Char(_) => Ok(PgType::CHAR_ARRAY),
                SeaType::Text => Ok(PgType::TEXT_ARRAY),
                SeaType::Bytea => Ok(PgType::BYTEA_ARRAY),
                SeaType::Timestamp(_) => Ok(PgType::TIMESTAMP_ARRAY),
                SeaType::TimestampWithTimeZone(_) => Ok(PgType::TIMESTAMPTZ_ARRAY),
                SeaType::Date => Ok(PgType::DATE_ARRAY),
                SeaType::Time(_) => Ok(PgType::TIME_ARRAY),
                SeaType::TimeWithTimeZone(_) => Ok(PgType::TIMETZ_ARRAY),
                SeaType::Interval(_) => Ok(PgType::INTERVAL_ARRAY),
                SeaType::Boolean => Ok(PgType::BOOL_ARRAY),
                SeaType::Point => Ok(PgType::POINT_ARRAY),
                SeaType::Uuid => Ok(PgType::UUID_ARRAY),
                SeaType::Json => Ok(PgType::JSON_ARRAY),
                SeaType::JsonBinary => Ok(PgType::JSONB_ARRAY),
                SeaType::Array(_) => bail!("nested array type is not supported"),
                SeaType::Unknown(name) => {
                    // Treat as enum type
                    Ok(PgType::new(
                        name.clone(),
                        0,
                        PgKind::Array(PgType::new(
                            name.clone(),
                            0,
                            PgKind::Enum(vec![]),
                            "".into(),
                        )),
                        "".into(),
                    ))
                }
                _ => bail!("unsupported array type: {:?}", t),
            }
        }
        SeaType::Unknown(name) => {
            // Treat as enum type
            Ok(PgType::new(
                name.clone(),
                0,
                PgKind::Enum(vec![]),
                "".into(),
            ))
        }
        _ => bail!("unsupported type: {:?}", sea_type),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        format_grant_table_privilege, format_grant_usage, format_pg_table_name,
        format_required_table_grants, parse_pgvector_dimension,
    };

    #[test]
    fn test_parse_pgvector_dimension() {
        assert_eq!(parse_pgvector_dimension("vector(3)").unwrap(), Some(3));
        assert_eq!(parse_pgvector_dimension("VECTOR(768)").unwrap(), Some(768));
        assert_eq!(parse_pgvector_dimension("varchar").unwrap(), None);
    }

    #[test]
    fn test_parse_pgvector_dimension_requires_size() {
        let err = parse_pgvector_dimension("vector").unwrap_err();
        assert!(err.to_string().contains("missing dimension"));
    }

    #[test]
    fn test_format_postgres_privilege_grants_quote_identifiers() {
        assert_eq!(
            format_pg_table_name("public", "GlobalBrandContentAnalysis"),
            r#""public"."GlobalBrandContentAnalysis""#
        );
        assert_eq!(
            format_grant_usage("tenant schema", r#"cdc"user"#),
            r#"GRANT USAGE ON SCHEMA "tenant schema" TO "cdc""user";"#
        );
        assert_eq!(
            format_grant_table_privilege("tenant schema", "Orders", r#"cdc"user"#, "SELECT"),
            r#"GRANT SELECT ON TABLE "tenant schema"."Orders" TO "cdc""user";"#
        );
        assert_eq!(
            format_required_table_grants("tenant schema", "Orders", r#"cdc"user"#, "SELECT"),
            r#"GRANT USAGE ON SCHEMA "tenant schema" TO "cdc""user"; GRANT SELECT ON TABLE "tenant schema"."Orders" TO "cdc""user";"#
        );
    }
}
