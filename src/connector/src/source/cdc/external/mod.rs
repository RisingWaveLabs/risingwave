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

pub mod mock_external_table;
pub mod postgres;
pub mod sql_server;

pub mod mysql;

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use futures::pin_mut;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::catalog::table::CdcTableType as PbCdcTableType;
use risingwave_pb::secret::PbSecretRef;
use serde::{Deserialize, Serialize};

use crate::WithPropertiesExt;
use crate::connector_common::{PgConnectionConfig, PostgresExternalTable, SslMode};
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::CdcSourceType;
use crate::source::cdc::external::mock_external_table::MockExternalTableReader;
use crate::source::cdc::external::mysql::{
    MySqlExternalTable, MySqlExternalTableReader, MySqlOffset,
};
use crate::source::cdc::external::postgres::{PostgresExternalTableReader, PostgresOffset};
use crate::source::cdc::external::sql_server::{
    SqlServerExternalTable, SqlServerExternalTableReader, SqlServerOffset,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExternalCdcTableType {
    Undefined,
    Mock,
    MySql,
    Postgres,
    SqlServer,
    Citus,
    Mongo,
}

impl ExternalCdcTableType {
    pub fn from_properties(with_properties: &impl WithPropertiesExt) -> Self {
        let connector = with_properties.get_connector().unwrap_or_default();
        match connector.as_str() {
            "mysql-cdc" => Self::MySql,
            "postgres-cdc" => Self::Postgres,
            "citus-cdc" => Self::Citus,
            "sqlserver-cdc" => Self::SqlServer,
            "mongodb-cdc" => Self::Mongo,
            _ => Self::Undefined,
        }
    }

    pub fn can_backfill(&self) -> bool {
        matches!(self, Self::MySql | Self::Postgres | Self::SqlServer)
    }

    pub fn enable_transaction_metadata(&self) -> bool {
        // In Debezium, transactional metadata cause delay of the newest events, as the `END` message is never sent unless a new transaction starts.
        // So we only allow transactional metadata for MySQL and Postgres.
        // See more in https://debezium.io/documentation/reference/2.6/connectors/sqlserver.html#sqlserver-transaction-metadata
        matches!(self, Self::MySql | Self::Postgres)
    }

    pub fn get_cdc_offset_parser(&self) -> ConnectorResult<CdcOffsetParseFunc> {
        match self {
            Self::MySql => Ok(MySqlExternalTableReader::get_cdc_offset_parser()),
            Self::Postgres => Ok(PostgresExternalTableReader::get_cdc_offset_parser()),
            Self::SqlServer => Ok(SqlServerExternalTableReader::get_cdc_offset_parser()),
            Self::Mock => Ok(MockExternalTableReader::get_cdc_offset_parser()),
            _ => bail!("invalid external table type: {:?}", *self),
        }
    }

    pub async fn create_table_reader(
        &self,
        config: ExternalTableConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::MySql => Ok(ExternalTableReaderImpl::MySql(
                MySqlExternalTableReader::new(config, schema, pk_indices).await?,
            )),
            Self::Postgres => Ok(ExternalTableReaderImpl::Postgres(
                PostgresExternalTableReader::new(config, schema, pk_indices, schema_table_name)
                    .await?,
            )),
            Self::SqlServer => Ok(ExternalTableReaderImpl::SqlServer(
                SqlServerExternalTableReader::new(config, schema, pk_indices).await?,
            )),
            // citus is never supported for cdc backfill (create source + create table).
            Self::Mock => Ok(ExternalTableReaderImpl::Mock(MockExternalTableReader::new())),
            _ => bail!("invalid external table type: {:?}", *self),
        }
    }
}

impl From<ExternalCdcTableType> for PbCdcTableType {
    fn from(cdc_table_type: ExternalCdcTableType) -> Self {
        match cdc_table_type {
            ExternalCdcTableType::Postgres => Self::Postgres,
            ExternalCdcTableType::MySql => Self::Mysql,
            ExternalCdcTableType::SqlServer => Self::Sqlserver,

            ExternalCdcTableType::Citus => Self::Citus,
            ExternalCdcTableType::Mongo => Self::Mongo,
            ExternalCdcTableType::Undefined | ExternalCdcTableType::Mock => Self::Unspecified,
        }
    }
}

impl From<PbCdcTableType> for ExternalCdcTableType {
    fn from(cdc_table_type: PbCdcTableType) -> Self {
        match cdc_table_type {
            PbCdcTableType::Postgres => Self::Postgres,
            PbCdcTableType::Mysql => Self::MySql,
            PbCdcTableType::Sqlserver => Self::SqlServer,
            PbCdcTableType::Mongo => Self::Mongo,
            PbCdcTableType::Citus => Self::Citus,
            PbCdcTableType::Unspecified => Self::Undefined,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTableName {
    // namespace of the table, e.g. database in mysql, schema in postgres
    pub schema_name: String,
    pub table_name: String,
}

pub const TABLE_NAME_KEY: &str = "table.name";
pub const SCHEMA_NAME_KEY: &str = "schema.name";
pub const DATABASE_NAME_KEY: &str = "database.name";

impl SchemaTableName {
    pub fn from_properties(properties: &BTreeMap<String, String>) -> Self {
        let table_type = ExternalCdcTableType::from_properties(properties);
        let table_name = properties.get(TABLE_NAME_KEY).cloned().unwrap_or_default();

        let schema_name = match table_type {
            ExternalCdcTableType::MySql => properties
                .get(DATABASE_NAME_KEY)
                .cloned()
                .unwrap_or_default(),
            ExternalCdcTableType::Postgres | ExternalCdcTableType::Citus => {
                properties.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default()
            }
            ExternalCdcTableType::SqlServer => {
                properties.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default()
            }
            _ => {
                unreachable!("invalid external table type: {:?}", table_type);
            }
        };

        Self {
            schema_name,
            table_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CdcOffset {
    MySql(MySqlOffset),
    Postgres(PostgresOffset),
    SqlServer(SqlServerOffset),
}

// Example debezium offset for Postgres:
// {
//     "sourcePartition":
//     {
//         "server": "RW_CDC_1004"
//     },
//     "sourceOffset":
//     {
//         "last_snapshot_record": false,
//         "lsn": 29973552,
//         "txId": 1046,
//         "ts_usec": 1670826189008456,
//         "snapshot": true
//     }
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumOffset {
    #[serde(rename = "sourcePartition")]
    pub source_partition: HashMap<String, String>,
    #[serde(rename = "sourceOffset")]
    pub source_offset: DebeziumSourceOffset,
    #[serde(rename = "isHeartbeat")]
    pub is_heartbeat: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DebeziumSourceOffset {
    // postgres snapshot progress
    pub last_snapshot_record: Option<bool>,
    // mysql snapshot progress
    pub snapshot: Option<bool>,

    // mysql binlog offset
    pub file: Option<String>,
    pub pos: Option<u64>,

    // postgres offset
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<i64>,
    pub tx_usec: Option<u64>,
    pub lsn_commit: Option<u64>,
    pub lsn_proc: Option<u64>,

    // sql server offset
    pub commit_lsn: Option<String>,
    pub change_lsn: Option<String>,
}

pub type CdcOffsetParseFunc = Box<dyn Fn(&str) -> ConnectorResult<CdcOffset> + Send>;

pub trait ExternalTableReader: Sized {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset>;

    // Currently, MySQL cdc uses a connection pool to manage connections to MySQL, and other CDC processes do not require the disconnect step for now.

    async fn disconnect(self) -> ConnectorResult<()> {
        Ok(())
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>>;

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

pub struct CdcTableSnapshotSplitOption {
    pub backfill_num_rows_per_split: u64,
    pub backfill_as_even_splits: bool,
    pub backfill_split_pk_column_index: u32,
}

pub enum ExternalTableReaderImpl {
    MySql(MySqlExternalTableReader),
    Postgres(PostgresExternalTableReader),
    SqlServer(SqlServerExternalTableReader),
    Mock(MockExternalTableReader),
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct ExternalTableConfig {
    pub connector: String,

    #[serde(rename = "hostname")]
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    #[serde(rename = "database.name")]
    pub database: String,
    #[serde(rename = "schema.name", default = "Default::default")]
    pub schema: String,
    #[serde(rename = "table.name")]
    pub table: String,
    /// `ssl.mode` specifies the SSL/TLS encryption level for secure communication with Postgres.
    /// Choices include `disabled`, `preferred`, and `required`.
    /// This field is optional.
    #[serde(rename = "ssl.mode", default = "postgres_ssl_mode_default")]
    #[serde(alias = "debezium.database.sslmode")]
    pub ssl_mode: SslMode,

    #[serde(rename = "ssl.root.cert")]
    #[serde(alias = "debezium.database.sslrootcert")]
    pub ssl_root_cert: Option<String>,

    /// `encrypt` specifies whether connect to SQL Server using SSL.
    /// Only "true" means using SSL. All other values are treated as "false".
    #[serde(rename = "database.encrypt", default = "Default::default")]
    pub encrypt: String,
}

fn postgres_ssl_mode_default() -> SslMode {
    // NOTE(StrikeW): Default to `disabled` for backward compatibility
    SslMode::Disabled
}

impl ExternalTableConfig {
    pub fn try_from_btreemap(
        connect_properties: BTreeMap<String, String>,
        secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> ConnectorResult<Self> {
        let options_with_secret =
            LocalSecretManager::global().fill_secrets(connect_properties, secret_refs)?;
        let json_value = serde_json::to_value(options_with_secret)?;
        let config = serde_json::from_value::<ExternalTableConfig>(json_value)?;
        Ok(config)
    }

    /// Project the Postgres-specific subset of this config so it can drive the
    /// shared `create_pg_client` / `PostgresExternalTable` helpers. Only meaningful
    /// for the Postgres CDC connector; other connectors ignore the fields.
    pub fn pg_connection_config(&self) -> ConnectorResult<PgConnectionConfig> {
        let port = self
            .port
            .parse::<u16>()
            .with_context(|| format!("invalid postgres port `{}`", self.port))?;
        Ok(PgConnectionConfig {
            host: self.host.clone(),
            port,
            user: self.username.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            ssl_mode: self.ssl_mode.clone(),
            ssl_root_cert: self.ssl_root_cert.clone(),
        })
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.current_cdc_offset().await,
            ExternalTableReaderImpl::Postgres(postgres) => postgres.current_cdc_offset().await,
            ExternalTableReaderImpl::SqlServer(sql_server) => sql_server.current_cdc_offset().await,
            ExternalTableReaderImpl::Mock(mock) => mock.current_cdc_offset().await,
        }
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

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        self.get_parallel_cdc_splits_inner(options)
    }

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.split_snapshot_read_inner(table_name, left, right, split_columns)
    }
}

impl ExternalTableReaderImpl {
    /// For each given primary key column (by name), returns whether comparing the RisingWave
    /// `i64` value needs upstream unsigned `BIGINT` semantics. Only MySQL `BIGINT UNSIGNED` can
    /// overflow into a negative `i64`; other connectors are always false.
    pub fn pk_column_unsigned_i64_compare_flags(
        &self,
        pk_names: &[String],
    ) -> ConnectorResult<Vec<bool>> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.pk_column_unsigned_i64_compare_flags(pk_names)
            }
            _ => Ok(vec![false; pk_names.len()]),
        }
    }

    pub fn get_cdc_offset_parser(&self) -> CdcOffsetParseFunc {
        match self {
            ExternalTableReaderImpl::MySql(_) => MySqlExternalTableReader::get_cdc_offset_parser(),
            ExternalTableReaderImpl::Postgres(_) => {
                PostgresExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::SqlServer(_) => {
                SqlServerExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::Mock(_) => MockExternalTableReader::get_cdc_offset_parser(),
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::SqlServer(sql_server) => {
                sql_server.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
        };

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits_inner(&self, options: CdcTableSnapshotSplitOption) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Postgres(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::SqlServer(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Mock(e) => e.get_parallel_cdc_splits(options),
        };
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::SqlServer(sql_server) => {
                sql_server.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.split_snapshot_read(table_name, left, right, split_columns)
            }
        };

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }
}

pub enum ExternalTableImpl {
    MySql(MySqlExternalTable),
    Postgres(PostgresExternalTable),
    SqlServer(SqlServerExternalTable),
}

impl ExternalTableImpl {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let cdc_source_type = CdcSourceType::from(config.connector.as_str());
        match cdc_source_type {
            CdcSourceType::Mysql => Ok(ExternalTableImpl::MySql(
                MySqlExternalTable::connect(config).await?,
            )),
            CdcSourceType::Postgres => {
                let pg_conn = config.pg_connection_config()?;
                Ok(ExternalTableImpl::Postgres(
                    PostgresExternalTable::connect(
                        &pg_conn,
                        &config.schema,
                        &config.table,
                        false,
                        Some("SELECT"),
                    )
                    .await?,
                ))
            }
            CdcSourceType::SqlServer => Ok(ExternalTableImpl::SqlServer(
                SqlServerExternalTable::connect(config).await?,
            )),
            _ => Err(anyhow!("Unsupported cdc connector type: {}", config.connector).into()),
        }
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        match self {
            ExternalTableImpl::MySql(mysql) => mysql.column_descs(),
            ExternalTableImpl::Postgres(postgres) => postgres.column_descs(),
            ExternalTableImpl::SqlServer(sql_server) => sql_server.column_descs(),
        }
    }

    pub fn pk_names(&self) -> &Vec<String> {
        match self {
            ExternalTableImpl::MySql(mysql) => mysql.pk_names(),
            ExternalTableImpl::Postgres(postgres) => postgres.pk_names(),
            ExternalTableImpl::SqlServer(sql_server) => sql_server.pk_names(),
        }
    }
}

pub const CDC_TABLE_SPLIT_ID_START: i64 = 1;
