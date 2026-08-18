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

use std::collections::HashMap;

use anyhow::{Context, anyhow};
use chrono::{DateTime, NaiveDateTime};
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut, stream};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use mysql_common::params::Params;
use mysql_common::value::Value;
use risingwave_common::bail;
use risingwave_common::catalog::{CDC_OFFSET_COLUMN_NAME, ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Decimal, F32, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use sea_schema::mysql::def::{ColumnDefault, ColumnType, IndexInfo, NumericAttr};
use sea_schema::mysql::discovery::SchemaDiscovery;
use sea_schema::mysql::query::SchemaQueryBuilder;
use sea_schema::sea_query::{Alias, IntoIden};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;
use sqlx::mysql::MySqlConnectOptions;
use thiserror_ext::AsReport;

use crate::connector_common::SslMode;
// Re-export SslMode for convenience
pub use crate::connector_common::SslMode as MySqlSslMode;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::mysql_row_to_owned_row_with_strict_pk;
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, DebeziumOffset,
    ExternalTableConfig, ExternalTableReader, SchemaTableName,
};

/// Build MySQL connection pool with proper SSL configuration.
///
/// This helper function creates a `mysql_async::Pool` with all necessary configurations
/// including SSL settings. Use this function to ensure consistent MySQL connection setup
/// across the codebase.
///
/// # Arguments
/// * `host` - MySQL server hostname or IP address
/// * `port` - MySQL server port
/// * `username` - MySQL username
/// * `password` - MySQL password
/// * `database` - Database name
/// * `ssl_mode` - SSL mode configuration (disabled, preferred, required, verify-ca, verify-full)
///
/// # Returns
/// Returns a configured `mysql_async::Pool` ready for use
pub fn build_mysql_connection_pool(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database: &str,
    ssl_mode: SslMode,
) -> mysql_async::Pool {
    let mut opts_builder = mysql_async::OptsBuilder::default()
        .user(Some(username))
        .pass(Some(password))
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database));

    opts_builder = match ssl_mode {
        SslMode::Disabled | SslMode::Preferred => opts_builder.ssl_opts(None),
        // verify-ca and verify-full are same as required for mysql now
        SslMode::Required | SslMode::VerifyCa | SslMode::VerifyFull => {
            let ssl_without_verify = mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
            opts_builder.ssl_opts(Some(ssl_without_verify))
        }
    };

    mysql_async::Pool::new(opts_builder)
}

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MySqlOffset {
    pub filename: String,
    pub position: u64,
}

impl MySqlOffset {
    pub fn new(filename: String, position: u64) -> Self {
        Self { filename, position }
    }
}

impl MySqlOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        Ok(Self {
            filename: dbz_offset
                .source_offset
                .file
                .context("binlog file not found in offset")?,
            position: dbz_offset
                .source_offset
                .pos
                .context("binlog position not found in offset")?,
        })
    }
}

pub struct MySqlExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl MySqlExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        tracing::debug!("connect to mysql");
        let options = MySqlConnectOptions::new()
            .username(&config.username)
            .password(&config.password)
            .host(&config.host)
            .port(config.port.parse::<u16>().unwrap())
            .database(&config.database)
            .ssl_mode(match config.ssl_mode {
                SslMode::Disabled => sqlx::mysql::MySqlSslMode::Disabled,
                SslMode::Preferred => sqlx::mysql::MySqlSslMode::Preferred,
                SslMode::Required => sqlx::mysql::MySqlSslMode::Required,
                _ => {
                    return Err(anyhow!("unsupported SSL mode").into());
                }
            });

        let connection = MySqlPool::connect_with(options).await?;
        let mut schema_discovery = SchemaDiscovery::new(connection, config.database.as_str());

        // discover system version first
        let system_info = schema_discovery.discover_system().await?;
        schema_discovery.query = SchemaQueryBuilder::new(system_info.clone());
        let schema = Alias::new(config.database.as_str()).into_iden();
        let table = Alias::new(config.table.as_str()).into_iden();
        let columns = schema_discovery
            .discover_columns(schema.clone(), table.clone(), &system_info)
            .await?;
        let indexes = schema_discovery.discover_indexes(schema, table).await?;
        let mut column_descs = vec![];
        for col in columns {
            let data_type = mysql_type_to_rw_type(&col.col_type)?;
            // column name in mysql is case-insensitive, convert to lowercase
            let col_name = col.name.to_lowercase();
            let column_desc = if let Some(default) = col.default {
                let snapshot_value = derive_default_value(default.clone(), &data_type)
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            column = col_name,
                            ?default,
                            %data_type,
                            error = %e.as_report(),
                            "failed to derive column default value, fallback to `NULL`",
                        );
                        None
                    });

                ColumnDesc::named_with_default_value(
                    col_name.clone(),
                    ColumnId::placeholder(),
                    data_type.clone(),
                    snapshot_value,
                )
            } else {
                ColumnDesc::named(col_name.clone(), ColumnId::placeholder(), data_type)
            };

            column_descs.push(column_desc);
        }

        let pk_names = primary_key_names(&indexes)
            .ok_or_else(|| anyhow!("MySQL table doesn't define the primary key"))?;
        Ok(Self {
            column_descs,
            pk_names,
        })
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
}

fn primary_key_names(indexes: &[IndexInfo]) -> Option<Vec<String>> {
    indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
        .map(|index| {
            index
                .parts
                .iter()
                .map(|part| part.column.to_lowercase())
                .collect()
        })
        .filter(|names: &Vec<_>| !names.is_empty())
}

fn derive_default_value(default: ColumnDefault, data_type: &DataType) -> ConnectorResult<Datum> {
    let datum = match default {
        ColumnDefault::Null => None,
        ColumnDefault::Int(val) => match data_type {
            DataType::Int16 => Some(ScalarImpl::Int16(val as _)),
            DataType::Int32 => Some(ScalarImpl::Int32(val as _)),
            DataType::Int64 => Some(ScalarImpl::Int64(val)),
            DataType::Varchar => {
                // should be the Enum type which is mapped to Varchar
                Some(ScalarImpl::from(val.to_string()))
            }
            _ => bail!("unexpected default value type for integer"),
        },
        ColumnDefault::Real(val) => match data_type {
            DataType::Float32 => Some(ScalarImpl::Float32(F32::from(val as f32))),
            DataType::Float64 => Some(ScalarImpl::Float64(val.into())),
            DataType::Decimal => Some(ScalarImpl::Decimal(
                Decimal::try_from(val).context("failed to convert default value to decimal")?,
            )),
            _ => bail!("unexpected default value type for real"),
        },
        ColumnDefault::String(mut val) => {
            // mysql timestamp is mapped to timestamptz, we use UTC timezone to
            // interpret its value
            if data_type == &DataType::Timestamptz {
                val = timestamp_val_to_timestamptz(val.as_str())?;
            }
            Some(ScalarImpl::from_text(val.as_str(), data_type).map_err(|e| anyhow!(e)).context(
                "failed to parse mysql default value expression, only constant is supported",
            )?)
        }
        ColumnDefault::CurrentTimestamp | ColumnDefault::CustomExpr(_) => {
            bail!("MySQL CURRENT_TIMESTAMP and custom expression default value not supported")
        }
    };
    Ok(datum)
}

pub fn timestamp_val_to_timestamptz(value_text: &str) -> ConnectorResult<String> {
    let format = "%Y-%m-%d %H:%M:%S";
    let naive_datetime = NaiveDateTime::parse_from_str(value_text, format)
        .map_err(|err| anyhow!("failed to parse mysql timestamp value").context(err))?;
    let postgres_timestamptz: DateTime<chrono::Utc> =
        DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive_datetime, chrono::Utc);
    Ok(postgres_timestamptz
        .format("%Y-%m-%d %H:%M:%S%:z")
        .to_string())
}

pub fn type_name_to_mysql_type(ty_name: &str) -> Option<ColumnType> {
    // Debezium schema change message may include extra qualifiers, e.g. `BIGINT UNSIGNED`,
    // `BIGINT(20) UNSIGNED`, `INT UNSIGNED ZEROFILL`, etc.
    let ty = ty_name.trim().to_lowercase();
    let tokens = ty
        .split(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | ','))
        .filter(|token| !token.is_empty())
        .collect_vec();
    let base = tokens.first().copied().unwrap_or_default();
    let second = tokens.get(1).copied();
    let is_unsigned = tokens.contains(&"unsigned");
    let is_zero_fill = tokens.contains(&"zerofill");

    let make_numeric_attr = || {
        let mut attr = NumericAttr::default();
        if is_unsigned {
            attr.unsigned = Some(true);
        }
        if is_zero_fill {
            attr.zero_fill = Some(true);
        }
        attr
    };

    match (base, second) {
        ("character", Some("varying")) => return Some(ColumnType::Varchar(Default::default())),
        ("double", Some("precision")) => return Some(ColumnType::Double(make_numeric_attr())),
        ("long", Some("varchar")) => return Some(ColumnType::MediumText(Default::default())),
        ("long", Some("varbinary")) => return Some(ColumnType::MediumBlob),
        _ => {}
    }

    match base {
        "serial" => Some(ColumnType::Serial),
        "bit" => Some(ColumnType::Bit(make_numeric_attr())),
        "tinyint" | "int1" => Some(ColumnType::TinyInt(make_numeric_attr())),
        "bool" | "boolean" => Some(ColumnType::Bool),
        "smallint" | "int2" => Some(ColumnType::SmallInt(make_numeric_attr())),
        "mediumint" | "middleint" | "int3" => Some(ColumnType::MediumInt(make_numeric_attr())),
        "int" | "integer" | "int4" => Some(ColumnType::Int(make_numeric_attr())),
        "bigint" | "int8" => Some(ColumnType::BigInt(make_numeric_attr())),
        "decimal" | "dec" | "fixed" | "numeric" => Some(ColumnType::Decimal(make_numeric_attr())),
        "float" | "float4" => Some(ColumnType::Float(make_numeric_attr())),
        "double" | "float8" | "real" => Some(ColumnType::Double(make_numeric_attr())),
        "time" => Some(ColumnType::Time(Default::default())),
        "datetime" => Some(ColumnType::DateTime(Default::default())),
        "timestamp" => Some(ColumnType::Timestamp(Default::default())),
        "year" => Some(ColumnType::Year),
        "char" | "character" => Some(ColumnType::Char(Default::default())),
        "nchar" => Some(ColumnType::NChar(Default::default())),
        "varchar" => Some(ColumnType::Varchar(Default::default())),
        "nvarchar" => Some(ColumnType::NVarchar(Default::default())),
        "binary" => Some(ColumnType::Binary(Default::default())),
        "varbinary" => Some(ColumnType::Varbinary(Default::default())),
        "text" => Some(ColumnType::Text(Default::default())),
        "tinytext" => Some(ColumnType::TinyText(Default::default())),
        "mediumtext" => Some(ColumnType::MediumText(Default::default())),
        "longtext" => Some(ColumnType::LongText(Default::default())),
        "blob" => Some(ColumnType::Blob(Default::default())),
        "tinyblob" => Some(ColumnType::TinyBlob),
        "mediumblob" => Some(ColumnType::MediumBlob),
        "longblob" => Some(ColumnType::LongBlob),
        "enum" => Some(ColumnType::Enum(Default::default())),
        "set" => Some(ColumnType::Set(Default::default())),
        "json" => Some(ColumnType::Json),
        "date" => Some(ColumnType::Date),
        "geometry" => Some(ColumnType::Geometry(Default::default())),
        "point" => Some(ColumnType::Point(Default::default())),
        "linestring" => Some(ColumnType::LineString(Default::default())),
        "polygon" => Some(ColumnType::Polygon(Default::default())),
        "multipoint" => Some(ColumnType::MultiPoint(Default::default())),
        "multilinestring" => Some(ColumnType::MultiLineString(Default::default())),
        "multipolygon" => Some(ColumnType::MultiPolygon(Default::default())),
        "geometrycollection" => Some(ColumnType::GeometryCollection(Default::default())),
        _ => None,
    }
}

fn mysql_type_is_unsigned_bigint(col_type: &ColumnType) -> bool {
    match col_type {
        // MySQL SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
        ColumnType::Serial => true,
        ColumnType::BigInt(attr) => attr.unsigned == Some(true),
        _ => false,
    }
}

pub fn mysql_type_to_rw_type(col_type: &ColumnType) -> ConnectorResult<DataType> {
    let dtype = match col_type {
        ColumnType::Serial => DataType::Decimal,
        ColumnType::Bit(attr) => {
            if let Some(1) = attr.maximum {
                DataType::Boolean
            } else {
                return Err(
                    anyhow!("BIT({}) type not supported", attr.maximum.unwrap_or(0)).into(),
                );
            }
        }
        // Unsigned integer family needs promotion to avoid overflow.
        ColumnType::TinyInt(_) => DataType::Int16,
        ColumnType::SmallInt(attr) => {
            if attr.unsigned == Some(true) {
                DataType::Int32
            } else {
                DataType::Int16
            }
        }
        ColumnType::Bool => DataType::Boolean,
        ColumnType::MediumInt(_) => DataType::Int32,
        ColumnType::Int(attr) => {
            if attr.unsigned == Some(true) {
                DataType::Int64
            } else {
                DataType::Int32
            }
        }
        ColumnType::BigInt(attr) => {
            if attr.unsigned == Some(true) {
                DataType::Decimal
            } else {
                DataType::Int64
            }
        }
        ColumnType::Decimal(_) => DataType::Decimal,
        ColumnType::Float(_) => DataType::Float32,
        ColumnType::Double(_) => DataType::Float64,
        ColumnType::Date => DataType::Date,
        ColumnType::Time(_) => DataType::Time,
        ColumnType::DateTime(_) => DataType::Timestamp,
        ColumnType::Timestamp(_) => DataType::Timestamptz,
        ColumnType::Year => DataType::Int32,
        ColumnType::Char(_)
        | ColumnType::NChar(_)
        | ColumnType::Varchar(_)
        | ColumnType::NVarchar(_) => DataType::Varchar,
        ColumnType::Binary(_) | ColumnType::Varbinary(_) => DataType::Bytea,
        ColumnType::Text(_)
        | ColumnType::TinyText(_)
        | ColumnType::MediumText(_)
        | ColumnType::LongText(_) => DataType::Varchar,
        ColumnType::Blob(_)
        | ColumnType::TinyBlob
        | ColumnType::MediumBlob
        | ColumnType::LongBlob => DataType::Bytea,
        ColumnType::Enum(_) => DataType::Varchar,
        ColumnType::Json => DataType::Jsonb,
        ColumnType::Set(_) => {
            return Err(anyhow!("SET type not supported").into());
        }
        ColumnType::Geometry(_) => {
            return Err(anyhow!("GEOMETRY type not supported").into());
        }
        ColumnType::Point(_) => {
            return Err(anyhow!("POINT type not supported").into());
        }
        ColumnType::LineString(_) => {
            return Err(anyhow!("LINE string type not supported").into());
        }
        ColumnType::Polygon(_) => {
            return Err(anyhow!("POLYGON type not supported").into());
        }
        ColumnType::MultiPoint(_) => {
            return Err(anyhow!("MULTI POINT type not supported").into());
        }
        ColumnType::MultiLineString(_) => {
            return Err(anyhow!("MULTI LINE STRING type not supported").into());
        }
        ColumnType::MultiPolygon(_) => {
            return Err(anyhow!("MULTI POLYGON type not supported").into());
        }
        ColumnType::GeometryCollection(_) => {
            return Err(anyhow!("GEOMETRY COLLECTION type not supported").into());
        }
        ColumnType::Unknown(_) => {
            return Err(anyhow!("Unknown MySQL data type").into());
        }
    };

    Ok(dtype)
}

pub struct MySqlExternalTableReader {
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    field_names: String,
    pool: mysql_async::Pool,
    upstream_mysql_pk_infos: Vec<(String, ColumnType)>, // (column_name, column_type)
    mysql_version: (u8, u8),
    is_mariadb: bool,
}

impl ExternalTableReader for MySqlExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut conn = self.pool.get_conn().await?;

        // Choose SQL command based on MySQL version
        let sql = if !self.is_mariadb && self.is_mysql_8_4_or_later() {
            "SHOW BINARY LOG STATUS"
        } else {
            "SHOW MASTER STATUS"
        };

        tracing::debug!(
            "Using SQL command: {} for MySQL version {}.{} (is_mariadb={})",
            sql,
            self.mysql_version.0,
            self.mysql_version.1,
            self.is_mariadb
        );
        let mut rs = conn.query::<mysql_async::Row, _>(sql).await?;
        let row = Itertools::exactly_one(rs.iter_mut())
            .ok()
            .context("expect exactly one row when reading binlog offset")?;
        drop(conn);
        Ok(CdcOffset::MySql(MySqlOffset {
            filename: row.take("File").unwrap(),
            position: row.take("Position").unwrap(),
        }))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }

    async fn disconnect(self) -> ConnectorResult<()> {
        self.pool.disconnect().await.map_err(|e| e.into())
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        // TODO(zw): feat: impl
        stream::empty::<ConnectorResult<CdcTableSnapshotSplit>>().boxed()
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        todo!("implement MySQL CDC parallelized backfill")
    }
}

impl MySqlExternalTableReader {
    /// Get MySQL version from the connection
    async fn get_mysql_version(pool: &mysql_async::Pool) -> ConnectorResult<(u8, u8, bool)> {
        let mut conn = pool.get_conn().await?;
        let result: Option<String> = conn.query_first("SELECT VERSION()").await?;

        if let Some(version_str) = result {
            let parts: Vec<&str> = version_str.split('.').collect();
            if parts.len() >= 2 {
                let major_version = parts[0]
                    .parse::<u8>()
                    .context("Failed to parse major version")?;
                let minor_version = parts[1]
                    .parse::<u8>()
                    .context("Failed to parse minor version")?;
                let is_mariadb = version_str.to_lowercase().contains("mariadb");
                return Ok((major_version, minor_version, is_mariadb));
            }
        }
        Err(anyhow!("Failed to get MySQL version").into())
    }

    /// Check if MySQL version is 8.4 or later
    fn is_mysql_8_4_or_later(&self) -> bool {
        let (major, minor) = self.mysql_version;
        major > 8 || (major == 8 && minor >= 4)
    }

    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
    ) -> ConnectorResult<Self> {
        let database = config.database.clone();
        let table = config.table.clone();
        let pool = build_mysql_connection_pool(
            &config.host,
            config.port.parse::<u16>().unwrap(),
            &config.username,
            &config.password,
            &config.database,
            config.ssl_mode,
        );

        let field_names = rw_schema
            .fields
            .iter()
            .filter(|f| f.name != CDC_OFFSET_COLUMN_NAME)
            .map(|f| Self::quote_column(f.name.as_str()))
            .join(",");

        // Query MySQL primary key infos for type casting.
        let upstream_mysql_pk_infos =
            Self::query_upstream_pk_infos(&pool, &database, &table).await?;
        Self::validate_upstream_pk_columns(&upstream_mysql_pk_infos, &rw_schema, &pk_indices)?;
        // Get MySQL version
        let (major_version, minor_version, is_mariadb) = Self::get_mysql_version(&pool).await?;
        let mysql_version = (major_version, minor_version);
        tracing::info!(
            "MySQL version detected: {}.{} (is_mariadb={})",
            mysql_version.0,
            mysql_version.1,
            is_mariadb
        );

        Ok(Self {
            rw_schema,
            pk_indices,
            field_names,
            pool,
            upstream_mysql_pk_infos,
            mysql_version,
            is_mariadb,
        })
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        // schema name is the database name in mysql
        format!("`{}`.`{}`", table_name.schema_name, table_name.table_name)
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::MySql(MySqlOffset::parse_debezium_offset(
                offset,
            )?))
        })
    }

    /// Query upstream primary key data types, used for generating filter conditions with proper type casting.
    async fn query_upstream_pk_infos(
        pool: &mysql_async::Pool,
        database: &str,
        table: &str,
    ) -> ConnectorResult<Vec<(String, ColumnType)>> {
        let mut conn = pool.get_conn().await?;

        // Query primary key columns and their data types
        let sql = format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{}'
            AND TABLE_NAME = '{}'
            AND COLUMN_KEY = 'PRI'
            ORDER BY ORDINAL_POSITION",
            database, table
        );

        let rs = conn.query::<mysql_async::Row, _>(sql).await?;

        let mut column_infos = Vec::new();
        for row in &rs {
            let column_name: String = row.get(0).unwrap();
            let column_type: String = row.get(1).unwrap();
            let column_type =
                type_name_to_mysql_type(&column_type).unwrap_or(ColumnType::Unknown(column_type));
            column_infos.push((column_name, column_type));
        }

        drop(conn);

        Ok(column_infos)
    }

    fn validate_upstream_pk_columns(
        upstream_pk_infos: &[(String, ColumnType)],
        rw_schema: &Schema,
        pk_indices: &[usize],
    ) -> ConnectorResult<()> {
        let expected_pk_names = pk_indices
            .iter()
            .map(|&index| rw_schema.fields[index].name.as_str())
            .collect_vec();
        let upstream_pk_names = upstream_pk_infos
            .iter()
            .map(|(name, _)| name.as_str())
            .collect_vec();

        if upstream_pk_names.is_empty() {
            return Err(anyhow!(
                "upstream MySQL primary key metadata is empty; expected columns {:?}",
                expected_pk_names
            )
            .into());
        }
        if upstream_pk_names.len() != expected_pk_names.len()
            || upstream_pk_names
                .iter()
                .zip_eq_fast(&expected_pk_names)
                .any(|(upstream, expected)| !upstream.eq_ignore_ascii_case(expected))
        {
            return Err(anyhow!(
                "upstream MySQL primary key columns {:?} do not match expected columns {:?}",
                upstream_pk_names,
                expected_pk_names
            )
            .into());
        }
        Ok(())
    }

    /// Check whether a column is `BIGINT UNSIGNED`.
    ///
    /// Frontend up-casts narrower unsigned integer types, and non-integer unsigned types
    /// (`FLOAT`/`DOUBLE`/`DECIMAL UNSIGNED`) keep their own comparison semantics. Only
    /// `BIGINT UNSIGNED` can be represented as a negative `i64` in RisingWave and needs
    /// unsigned `u64` comparison/conversion.
    fn needs_unsigned_i64_compare(&self, column_name: &str) -> ConnectorResult<bool> {
        self.upstream_mysql_pk_infos
            .iter()
            .find(|(col_name, _)| col_name.eq_ignore_ascii_case(column_name))
            .map(|(_, col_type)| mysql_type_is_unsigned_bigint(col_type))
            .ok_or_else(|| {
                anyhow!(
                    "primary key column `{column_name}` not found in upstream MySQL primary key info"
                )
                .into()
            })
    }

    /// For each given primary key column (by name), whether it needs unsigned `i64` comparison.
    pub(crate) fn pk_column_unsigned_i64_compare_flags(
        &self,
        pk_names: &[String],
    ) -> ConnectorResult<Vec<bool>> {
        pk_names
            .iter()
            .map(|name| self.needs_unsigned_i64_compare(name))
            .collect()
    }

    /// Convert negative i64 to unsigned u64 based on column type
    fn convert_negative_to_unsigned(&self, negative_val: i64) -> u64 {
        negative_val as u64
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let order_key = primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",");
        let sql = if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {} LIMIT {limit}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                order_key,
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {limit}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                filter_expr,
                order_key,
            )
        };
        let mut conn = self.pool.get_conn().await?;
        // Set session timezone to UTC
        conn.exec_drop("SET time_zone = \"+00:00\"", ()).await?;

        if let Some(start_pk_row) = start_pk_row {
            let field_map = self
                .rw_schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type.clone()))
                .collect::<HashMap<_, _>>();

            // fill in start primary key params
            let params: Vec<_> = primary_keys
                .iter()
                .zip_eq_fast(start_pk_row.into_iter())
                .map(|(pk, datum)| {
                    if let Some(value) = datum {
                        let ty = field_map.get(pk.as_str()).unwrap();
                        let val = match ty {
                            DataType::Boolean => Value::from(value.into_bool()),
                            DataType::Int16 => Value::from(value.into_int16()),
                            DataType::Int32 => Value::from(value.into_int32()),
                            DataType::Int64 => {
                                let int64_val = value.into_int64();
                                if int64_val < 0 && self.needs_unsigned_i64_compare(pk.as_str())? {
                                    Value::from(self.convert_negative_to_unsigned(int64_val))
                                } else {
                                    Value::from(int64_val)
                                }
                            }
                            DataType::Float32 => Value::from(value.into_float32().into_inner()),
                            DataType::Float64 => Value::from(value.into_float64().into_inner()),
                            DataType::Varchar => Value::from(String::from(value.into_utf8())),
                            DataType::Date => Value::from(value.into_date().0),
                            DataType::Time => Value::from(value.into_time().0),
                            DataType::Timestamp => Value::from(value.into_timestamp().0),
                            DataType::Decimal => Value::from(value.into_decimal().to_string()),
                            DataType::Timestamptz => {
                                // Convert timestamptz to NaiveDateTime for MySQL TIMESTAMP comparison
                                // MySQL expects NaiveDateTime for TIMESTAMP parameters
                                let ts = value.into_timestamptz();
                                let datetime_utc = ts.to_datetime_utc();
                                let naive_datetime = datetime_utc.naive_utc();
                                Value::from(naive_datetime)
                            }
                            _ => bail!("unsupported primary key data type: {}", ty),
                        };
                        ConnectorResult::Ok((pk.to_lowercase(), val))
                    } else {
                        bail!("primary key {} cannot be null", pk);
                    }
                })
                .try_collect::<_, _, ConnectorError>()?;

            tracing::debug!("snapshot read params: {:?}", &params);
            let rs_stream = sql
                .with(Params::from(params))
                .stream::<mysql_async::Row, _>(&mut conn)
                .await?;

            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                mysql_row_to_owned_row_with_strict_pk(&mut row, &self.rw_schema, &self.pk_indices)
                    .map_err(ConnectorError::from)
            });
            pin_mut!(row_stream);
            #[for_await]
            for row in row_stream {
                let row = row?;
                yield row;
            }
        } else {
            let rs_stream = sql.stream::<mysql_async::Row, _>(&mut conn).await?;
            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                mysql_row_to_owned_row_with_strict_pk(&mut row, &self.rw_schema, &self.pk_indices)
                    .map_err(ConnectorError::from)
            });
            pin_mut!(row_stream);
            #[for_await]
            for row in row_stream {
                let row = row?;
                yield row;
            }
        }
        drop(conn);
    }

    // mysql cannot leverage the given key to narrow down the range of scan,
    // we need to rewrite the comparison conditions by our own.
    // (a, b) > (x, y) => (`a` > x) OR ((`a` = x) AND (`b` > y))
    fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!(
            "({} > :{})",
            Self::quote_column(&columns[0]),
            columns[0].to_lowercase()
        ));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, col) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!(
                        "({} = :{})",
                        Self::quote_column(col),
                        col.to_lowercase()
                    ));
                } else {
                    condition.push_str(&format!(
                        " AND ({} = :{})",
                        Self::quote_column(col),
                        col.to_lowercase()
                    ));
                }
            }
            // '>' condition
            condition.push_str(&format!(
                " AND ({} > :{})",
                Self::quote_column(&columns[i - 1]),
                columns[i - 1].to_lowercase()
            ));
            conditions.push(format!("({})", condition));
        }
        if columns.len() > 1 {
            conditions.join(" OR ")
        } else {
            conditions.join("")
        }
    }

    fn quote_column(column: &str) -> String {
        format!("`{}`", column)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::types::DataType;
    use sea_schema::mysql::def::{ColumnType, IndexInfo, IndexOrder, IndexPart, IndexType};

    use super::{
        mysql_type_is_unsigned_bigint, mysql_type_to_rw_type, primary_key_names,
        type_name_to_mysql_type,
    };
    use crate::source::cdc::external::mysql::MySqlExternalTable;
    use crate::source::cdc::external::{
        CdcOffset, ExternalTableConfig, ExternalTableReader, MySqlExternalTableReader, MySqlOffset,
        SchemaTableName,
    };

    fn parse_mysql_type_name(ty_name: &str) -> ColumnType {
        type_name_to_mysql_type(ty_name).unwrap()
    }

    #[test]
    fn test_primary_key_names_preserve_index_order() {
        let indexes = vec![IndexInfo {
            unique: true,
            name: "PRIMARY".to_owned(),
            parts: ["RelatedID", "TypeID", "ClientID"]
                .into_iter()
                .map(|column| IndexPart {
                    column: column.to_owned(),
                    order: IndexOrder::Ascending,
                    sub_part: None,
                })
                .collect(),
            nullable: false,
            idx_type: IndexType::BTree,
            comment: String::new(),
            functional: false,
        }];

        assert_eq!(
            primary_key_names(&indexes),
            Some(vec![
                "relatedid".to_owned(),
                "typeid".to_owned(),
                "clientid".to_owned(),
            ])
        );
    }

    #[test]
    fn test_mysql_unsigned_bigint_type_detection() {
        for ty_name in [
            "SERIAL",
            "BIGINT UNSIGNED",
            "BIGINT(20) UNSIGNED",
            "BIGINT UNSIGNED ZEROFILL",
            "INT8 UNSIGNED",
        ] {
            assert!(
                mysql_type_is_unsigned_bigint(&parse_mysql_type_name(ty_name)),
                "{ty_name}"
            );
        }

        for ty_name in [
            "BIGINT",
            "INTEGER UNSIGNED",
            "INT4 UNSIGNED",
            "MEDIUMINT UNSIGNED",
            "DECIMAL UNSIGNED",
            "FLOAT8 UNSIGNED",
        ] {
            assert!(
                !mysql_type_is_unsigned_bigint(&parse_mysql_type_name(ty_name)),
                "{ty_name}"
            );
        }
    }

    #[test]
    fn test_mysql_type_aliases() {
        assert!(matches!(
            parse_mysql_type_name("INTEGER UNSIGNED"),
            ColumnType::Int(attr) if attr.unsigned == Some(true)
        ));
        assert!(matches!(
            parse_mysql_type_name("INT1 UNSIGNED"),
            ColumnType::TinyInt(attr) if attr.unsigned == Some(true)
        ));
        assert!(matches!(
            parse_mysql_type_name("INT2 UNSIGNED"),
            ColumnType::SmallInt(attr) if attr.unsigned == Some(true)
        ));
        assert!(matches!(
            parse_mysql_type_name("INT3 UNSIGNED"),
            ColumnType::MediumInt(attr) if attr.unsigned == Some(true)
        ));
        assert!(matches!(
            parse_mysql_type_name("INT4 UNSIGNED"),
            ColumnType::Int(attr) if attr.unsigned == Some(true)
        ));
        assert!(matches!(
            parse_mysql_type_name("INT8 UNSIGNED"),
            ColumnType::BigInt(attr) if attr.unsigned == Some(true)
        ));
        assert!(matches!(
            parse_mysql_type_name("MIDDLEINT"),
            ColumnType::MediumInt(_)
        ));
        assert!(matches!(
            parse_mysql_type_name("NUMERIC"),
            ColumnType::Decimal(_)
        ));
        assert!(matches!(
            parse_mysql_type_name("CHARACTER VARYING(64)"),
            ColumnType::Varchar(_)
        ));
        assert!(matches!(
            parse_mysql_type_name("LONG VARBINARY"),
            ColumnType::MediumBlob
        ));
    }

    #[test]
    fn test_mysql_serial_maps_as_unsigned_bigint() {
        let col_type = parse_mysql_type_name("SERIAL");
        assert!(mysql_type_is_unsigned_bigint(&col_type));

        let serial_type = mysql_type_to_rw_type(&col_type).unwrap();
        let unsigned_bigint_type =
            mysql_type_to_rw_type(&parse_mysql_type_name("BIGINT UNSIGNED")).unwrap();
        assert_eq!(serial_type, DataType::Decimal);
        assert_eq!(serial_type, unsigned_bigint_type);
    }

    #[test]
    fn test_validate_upstream_pk_columns() {
        let rw_schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(DataType::Varchar, "tenant"),
        ]);
        let int_type = || ColumnType::BigInt(Default::default());

        MySqlExternalTableReader::validate_upstream_pk_columns(
            &[
                ("ID".to_owned(), int_type()),
                ("tenant".to_owned(), int_type()),
            ],
            &rw_schema,
            &[0, 1],
        )
        .unwrap();

        for invalid_pk_infos in [
            vec![],
            vec![
                ("tenant".to_owned(), int_type()),
                ("id".to_owned(), int_type()),
            ],
            vec![("id".to_owned(), int_type())],
            vec![
                ("id".to_owned(), int_type()),
                ("tenant".to_owned(), int_type()),
                ("extra".to_owned(), int_type()),
            ],
        ] {
            assert!(
                MySqlExternalTableReader::validate_upstream_pk_columns(
                    &invalid_pk_infos,
                    &rw_schema,
                    &[0, 1],
                )
                .is_err(),
                "unexpectedly accepted {invalid_pk_infos:?}"
            );
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_mysql_schema() {
        let config = ExternalTableConfig {
            connector: "mysql-cdc".to_owned(),
            host: "localhost".to_owned(),
            port: "8306".to_owned(),
            username: "root".to_owned(),
            password: "123456".to_owned(),
            database: "mydb".to_owned(),
            schema: "".to_owned(),
            table: "part".to_owned(),
            ssl_mode: Default::default(),
            ssl_root_cert: None,
            encrypt: "false".to_owned(),
        };

        let table = MySqlExternalTable::connect(config).await.unwrap();
        println!("columns: {:?}", table.column_descs);
        println!("primary keys: {:?}", table.pk_names);
    }

    #[test]
    fn test_mysql_filter_expr() {
        let cols = vec!["id".to_owned()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(`id` > :id)");

        let cols = vec!["aa".to_owned(), "bb".to_owned(), "cc".to_owned()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(`aa` > :aa) OR ((`aa` = :aa) AND (`bb` > :bb)) OR ((`aa` = :aa) AND (`bb` = :bb) AND (`cc` > :cc))"
        );
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let off1_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 1062363217, "snapshot": true }, "isHeartbeat": false }"#;
        let off2_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 659687560, "snapshot": true }, "isHeartbeat": false }"#;
        let off3_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;
        let off4_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;

        let off0 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off0_str).unwrap());
        let off1 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off1_str).unwrap());
        let off2 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off2_str).unwrap());
        let off3 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off3_str).unwrap());
        let off4 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off4_str).unwrap());

        assert!(off0 <= off1);
        assert!(off1 > off2);
        assert!(off2 < off3);
        assert_eq!(off3, off4);
    }

    // manual test case
    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let columns = [
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Decimal),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };
        let props: HashMap<String, String> = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mytest",
                "table.name" => "t1"));

        let config =
            serde_json::from_value::<ExternalTableConfig>(serde_json::to_value(props).unwrap())
                .unwrap();
        let reader = MySqlExternalTableReader::new(config, rw_schema, vec![0])
            .await
            .unwrap();
        let offset = reader.current_cdc_offset().await.unwrap();
        println!("BinlogOffset: {:?}", offset);

        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let parser = MySqlExternalTableReader::get_cdc_offset_parser();
        println!("parsed offset: {:?}", parser(off0_str).unwrap());
        let table_name = SchemaTableName {
            schema_name: "mytest".to_owned(),
            table_name: "t1".to_owned(),
        };

        let stream = reader.snapshot_read(table_name, None, vec!["v1".to_owned()], 1000);
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
