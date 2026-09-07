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
use futures::TryStreamExt;
use risingwave_common::types::DataType;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Configuration for connecting to a SQL Server instance.
///
/// Mirrors `PgConnectionConfig` for PostgreSQL. Used by the `mssql_query`
/// table-valued function and any future SQL Server sink/CDC source that needs
/// a generic SQL Server client (independent of the Debezium-based CDC pipeline).
#[derive(Debug, Clone)]
pub struct MssqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    /// SQL Server "encrypt" connection string option: `true` means full TLS
    /// encryption for all traffic; `false` means only the login handshake is
    /// encrypted (the underlying `tiberius` [`EncryptionLevel::Off`] semantics).
    pub encrypt: bool,
    /// SQL Server `TrustServerCertificate` connection string option. When
    /// `true`, the server certificate is accepted as-is without chain
    /// validation. Implies `encrypt = true` since TLS is required for
    /// certificate trust to apply.
    pub trust_cert: bool,
}

/// Create an authenticated `tiberius::Client` connected to a SQL Server
/// instance. Uses tokio's TCP stream wrapped with `TokioAsyncWriteCompatExt`
/// to bridge between tokio's async I/O and tiberius's `futures-io` traits.
///
/// Encryption semantics:
/// - `trust_cert = true` forces TLS on (matches SQL Server behavior where
///   `TrustServerCertificate=true` is only meaningful with encryption).
/// - Otherwise encryption follows the `encrypt` flag directly.
///
/// Note: this does not handle the `Routing { host, port }` redirect case
/// (Azure SQL gateway redirect). Callers that need to follow a redirect should
/// use `SqlServerClient::new_with_config` in `crate::sink::sqlserver` instead.
pub async fn create_mssql_client(
    config: &MssqlConnectionConfig,
) -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let mut tiberius_config = Config::new();
    tiberius_config.host(&config.host);
    tiberius_config.port(config.port);
    tiberius_config.database(&config.database);
    // trust_cert=true implies encrypt=true (TLS is required for cert trust).
    tiberius_config.encryption(if config.trust_cert || config.encrypt {
        EncryptionLevel::Required
    } else {
        EncryptionLevel::Off
    });
    if config.trust_cert {
        tiberius_config.trust_cert();
    }
    tiberius_config.authentication(AuthMethod::sql_server(&config.user, &config.password));

    let tcp = TcpStream::connect(tiberius_config.get_addr())
        .await
        .with_context(|| {
            format!(
                "failed to connect to sql server at {}:{}",
                config.host, config.port
            )
        })?;
    let client = Client::connect(tiberius_config, tcp.compat_write())
        .await
        .with_context(|| {
            format!(
                "failed to authenticate against sql server {}:{} as user {}",
                config.host, config.port, config.user
            )
        })?;

    Ok(client)
}

/// Discover the projected schema of an arbitrary user query against SQL Server.
///
/// Wraps the user query in `sp_describe_first_result_set`, which returns one
/// row per result column with `name` and `system_type_name` (e.g. `int`,
/// `nvarchar(50)`, `decimal(18,2)`). Each row is mapped to a RisingWave
/// [`DataType`].
///
/// The user query is passed as a literal to `EXEC sp_describe_first_result_set`,
/// so any single-quote in the query is escaped by doubling it (standard T-SQL
/// string escaping).
pub async fn describe_mssql_query(
    config: &MssqlConnectionConfig,
    user_query: &str,
) -> anyhow::Result<Vec<(String, DataType)>> {
    let mut client = create_mssql_client(config).await?;

    let escaped = user_query.replace('\'', "''");
    let describe_query = format!("EXEC sp_describe_first_result_set N'{}'", escaped);

    let stream = client.query(describe_query.as_str(), &[]).await?;
    let mut row_stream = stream.into_row_stream();

    let mut rw_types = vec![];
    while let Some(row) = row_stream.try_next().await? {
        // Column ordinal 0 -> `is_hidden` (bit). 0 = visible, 1 = hidden
        // (a column hidden from the client, e.g. an unused join key).
        // Column ordinal 1 -> `column_ordinal` (int). 1-based position
        // of this column in the result set; used to disambiguate unnamed
        // columns (`COUNT(*)` etc.) since `name` is empty for those.
        // Column ordinal 2 -> `name` (sysname / nvarchar). Empty for
        // hidden or computed-but-unnamed columns.
        // Column ordinal 5 -> `system_type_name` (e.g. "int", "nvarchar(50)",
        // "decimal(18,2)").
        let is_hidden = row.try_get::<bool, _>(0)?.unwrap_or(false);
        if is_hidden {
            // Skip hidden columns: they are not visible in the result set.
            continue;
        }
        let column_ordinal: i32 = row.try_get::<i32, _>(1)?.unwrap_or(0);
        let name: &str = row.try_get::<&str, _>(2)?.unwrap_or("");
        let type_name: &str = row.try_get::<&str, _>(5)?.unwrap_or("");

        let data_type = mssql_type_to_rw_type_str(type_name)
            .with_context(|| format!("unsupported column type {:?}", type_name))?;
        // Visible but unnamed columns (e.g. `SELECT COUNT(*) FROM t`) get a
        // synthetic `column_N` name where N is the 1-based column ordinal.
        // We keep the same default-name convention as the CDC source.
        let column_name = if name.is_empty() {
            format!("column_{column_ordinal}")
        } else {
            name.to_owned()
        };
        rw_types.push((column_name, data_type));
    }

    if rw_types.is_empty() {
        // `sp_describe_first_result_set` returns zero rows when the statement
        // produces no result set, or when every column is hidden. Refuse to
        // build a zero-column table function: downstream code (e.g. the
        // executor's per-row decoder) would silently drop every row because
        // there are no fields to decode into.
        return Err(anyhow!(
            "the query does not produce any result column: {:?}",
            user_query
        ));
    }

    Ok(rw_types)
}

/// Map a SQL Server `system_type_name` (as returned by
/// `sp_describe_first_result_set`) to a RisingWave [`DataType`].
///
/// The name is the human-readable form including precision/length parameters,
/// e.g. `int`, `nvarchar(50)`, `decimal(18,2)`, `datetime2(7)`. We strip the
/// parenthesized suffix and dispatch on the base type, mirroring the mapping
/// used by `mssql_type_to_rw_type` in
/// `src/connector/src/source/cdc/external/sql_server.rs` (which sees the bare
/// type from `INFORMATION_SCHEMA.COLUMNS`).
fn mssql_type_to_rw_type_str(type_name: &str) -> anyhow::Result<DataType> {
    let lower = type_name.to_lowercase();
    // Strip precision/scale/length suffix, e.g. "nvarchar(50)" -> "nvarchar".
    let base = lower.split_once('(').map(|(b, _)| b).unwrap_or(&lower);
    let dtype = match base.trim() {
        "bit" => DataType::Boolean,
        "binary" | "varbinary" => DataType::Bytea,
        "tinyint" | "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "float" => DataType::Float64,
        "decimal" | "numeric" | "money" | "smallmoney" => DataType::Decimal,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "datetime" | "datetime2" | "smalldatetime" => DataType::Timestamp,
        "datetimeoffset" => DataType::Timestamptz,
        "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" | "xml"
        | "uniqueidentifier" => DataType::Varchar,
        unknown => {
            return Err(anyhow!(
                "unsupported sql server type: {:?} (from system_type_name {:?})",
                unknown,
                type_name
            ));
        }
    };
    Ok(dtype)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::mssql_type_to_rw_type_str;

    /// Verify that the common base SQL Server types map to the expected
    /// RisingWave `DataType`s: int/bigint/bit/float/date/datetime2/datetimeoffset/uniqueidentifier.
    #[test]
    fn mssql_type_to_rw_type_str_base_types() {
        assert_eq!(mssql_type_to_rw_type_str("int").unwrap(), DataType::Int32);
        assert_eq!(
            mssql_type_to_rw_type_str("bigint").unwrap(),
            DataType::Int64
        );
        assert_eq!(mssql_type_to_rw_type_str("bit").unwrap(), DataType::Boolean);
        assert_eq!(
            mssql_type_to_rw_type_str("float").unwrap(),
            DataType::Float64
        );
        assert_eq!(mssql_type_to_rw_type_str("date").unwrap(), DataType::Date);
        assert_eq!(
            mssql_type_to_rw_type_str("datetime2").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            mssql_type_to_rw_type_str("datetimeoffset").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            mssql_type_to_rw_type_str("uniqueidentifier").unwrap(),
            DataType::Varchar
        );
    }

    /// Verify that parameterized SQL Server types (`nvarchar(50)`,
    /// `VARCHAR(255)`, `decimal(18, 2)`, `datetime2(7)`) are correctly
    /// stripped of their parenthesized suffix and mapped.
    #[test]
    fn mssql_type_to_rw_type_str_parameterized() {
        assert_eq!(
            mssql_type_to_rw_type_str("nvarchar(50)").unwrap(),
            DataType::Varchar
        );
        assert_eq!(
            mssql_type_to_rw_type_str("VARCHAR(255)").unwrap(),
            DataType::Varchar
        );
        assert_eq!(
            mssql_type_to_rw_type_str("decimal(18, 2)").unwrap(),
            DataType::Decimal
        );
        assert_eq!(
            mssql_type_to_rw_type_str("datetime2(7)").unwrap(),
            DataType::Timestamp
        );
    }

    /// Verify that unknown SQL Server types (`geometry`, `hierarchyid`,
    /// `sql_variant`) yield an error rather than silently mapping to
    /// the wrong type.
    #[test]
    fn mssql_type_to_rw_type_str_unknown() {
        assert!(mssql_type_to_rw_type_str("geometry").is_err());
        assert!(mssql_type_to_rw_type_str("hierarchyid").is_err());
        assert!(mssql_type_to_rw_type_str("sql_variant").is_err());
    }
}
