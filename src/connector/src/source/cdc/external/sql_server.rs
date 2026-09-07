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

use std::cmp::Ordering;

use anyhow::{Context, anyhow};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, pin_mut, stream};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use serde::{Deserialize, Serialize};
use tiberius::{Config, Query, QueryItem};

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::{ScalarImplTiberiusWrapper, sql_server_row_to_owned_row_with_strict_pk};
use crate::sink::sqlserver::SqlServerClient;
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, DebeziumOffset,
    ExternalTableConfig, ExternalTableReader, SchemaTableName,
};

// The maximum commit_lsn value in Sql Server
const MAX_COMMIT_LSN: &str = "ffffffff:ffffffff:ffff";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlServerOffset {
    // https://learn.microsoft.com/en-us/answers/questions/1328359/how-to-accurately-sequence-change-data-capture-dat
    pub change_lsn: String,
    pub commit_lsn: String,
}

// only compare the lsn field
impl PartialOrd for SqlServerOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.change_lsn.partial_cmp(&other.change_lsn) {
            Some(Ordering::Equal) => self.commit_lsn.partial_cmp(&other.commit_lsn),
            other => other,
        }
    }
}

impl SqlServerOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        Ok(Self {
            change_lsn: dbz_offset
                .source_offset
                .change_lsn
                .context("invalid sql server change_lsn")?,
            commit_lsn: dbz_offset
                .source_offset
                .commit_lsn
                .context("invalid sql server commit_lsn")?,
        })
    }
}

pub struct SqlServerExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl SqlServerExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        tracing::debug!("connect to sql server");

        let mut client_config = Config::new();

        client_config.host(&config.host);
        client_config.database(&config.database);
        client_config.port(config.port.parse::<u16>().unwrap());
        client_config.authentication(tiberius::AuthMethod::sql_server(
            &config.username,
            &config.password,
        ));
        // TODO(kexiang): use trust_cert_ca, trust_cert is not secure
        if config.encrypt == "true" {
            client_config.encryption(tiberius::EncryptionLevel::Required);
        }
        client_config.trust_cert();

        let mut client = SqlServerClient::new_with_config(client_config).await?;

        let mut column_descs = vec![];
        let mut pk_names = vec![];
        {
            let sql = Query::new(format!(
                "SELECT
                    COLUMN_NAME,
                    DATA_TYPE
                FROM
                    INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '{}'
                    AND TABLE_NAME = '{}'",
                config.schema.clone(),
                config.table.clone(),
            ));

            let mut stream = sql.query(&mut client.inner_client).await?;
            while let Some(item) = stream.try_next().await? {
                match item {
                    QueryItem::Metadata(_) => {}
                    QueryItem::Row(row) => {
                        let col_name: &str = row.try_get(0)?.unwrap();
                        let col_type: &str = row.try_get(1)?.unwrap();
                        column_descs.push(ColumnDesc::named(
                            col_name,
                            ColumnId::placeholder(),
                            mssql_type_to_rw_type(col_type, col_name)?,
                        ));
                    }
                }
            }
        }
        {
            let sql = Query::new(format!(
                "SELECT kcu.COLUMN_NAME
                FROM
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                JOIN
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND
                    tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND
                    tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                    tc.TABLE_SCHEMA = '{}' AND tc.TABLE_NAME = '{}'",
                config.schema, config.table,
            ));

            let mut stream = sql.query(&mut client.inner_client).await?;
            while let Some(item) = stream.try_next().await? {
                match item {
                    QueryItem::Metadata(_) => {}
                    QueryItem::Row(row) => {
                        let pk_name: &str = row.try_get(0)?.unwrap();
                        pk_names.push(pk_name.to_owned());
                    }
                }
            }
        }

        // The table does not exist
        if column_descs.is_empty() {
            bail!(
                "Sql Server table '{}'.'{}' not found in '{}'",
                config.schema,
                config.table,
                config.database
            );
        }

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

/// Map a SQL Server column type string (as returned by
/// `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE`) to a RisingWave [`DataType`].
/// Case-insensitive. Includes aliases for both the modern names
/// (`integer`, `smallmoney`) and the legacy names (`int`, `money`) so
/// the CDC source accepts the same set of aliases as the planning-time
/// mapping in `connector_common::sql_server::mssql_type_to_rw_type_str`.
/// Unknown types yield `ConnectorError`.
fn mssql_type_to_rw_type(col_type: &str, col_name: &str) -> ConnectorResult<DataType> {
    let dtype = match col_type.to_lowercase().as_str() {
        "bit" => DataType::Boolean,
        "binary" | "varbinary" => DataType::Bytea,
        "tinyint" | "smallint" => DataType::Int16,
        "integer" | "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "float" => DataType::Float64,
        "decimal" | "numeric" => DataType::Decimal,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "datetime" | "datetime2" | "smalldatetime" => DataType::Timestamp,
        "datetimeoffset" => DataType::Timestamptz,
        "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" | "xml"
        | "uniqueidentifier" => DataType::Varchar,
        "money" | "smallmoney" => DataType::Decimal,
        mssql_type => {
            return Err(anyhow!(
                "Unsupported Sql Server data type: {:?}, column name: {}",
                mssql_type,
                col_name
            )
            .into());
        }
    };
    Ok(dtype)
}

#[derive(Debug)]
pub struct SqlServerExternalTableReader {
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    field_names: String,
    client: tokio::sync::Mutex<SqlServerClient>,
}

impl ExternalTableReader for SqlServerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut client = self.client.lock().await;
        // start a transaction to read max start_lsn.
        let row = client
            .inner_client
            .simple_query(String::from("SELECT sys.fn_cdc_get_max_lsn()"))
            .await?
            .into_row()
            .await?
            .expect("No result returned by `SELECT sys.fn_cdc_get_max_lsn()`");
        // An example of change_lsn or commit_lsn: "00000027:00000ac0:0002" from debezium
        // sys.fn_cdc_get_max_lsn() returns a 10 bytes array, we convert it to a hex string here.
        let max_lsn = match row.try_get::<&[u8], usize>(0)? {
            Some(bytes) => {
                let mut hex_string = String::with_capacity(bytes.len() * 2 + 2);
                assert_eq!(
                    bytes.len(),
                    10,
                    "sys.fn_cdc_get_max_lsn() should return a 10 bytes array."
                );
                for byte in &bytes[0..4] {
                    hex_string.push_str(&format!("{:02x}", byte));
                }
                hex_string.push(':');
                for byte in &bytes[4..8] {
                    hex_string.push_str(&format!("{:02x}", byte));
                }
                hex_string.push(':');
                for byte in &bytes[8..10] {
                    hex_string.push_str(&format!("{:02x}", byte));
                }
                hex_string
            }
            None => bail!(
                "None is returned by `SELECT sys.fn_cdc_get_max_lsn()`, please ensure Sql Server Agent is running."
            ),
        };

        tracing::debug!("current max_lsn: {}", max_lsn);

        Ok(CdcOffset::SqlServer(SqlServerOffset {
            change_lsn: max_lsn,
            commit_lsn: MAX_COMMIT_LSN.into(),
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
        todo!("implement SqlServer CDC parallelized backfill")
    }
}

impl SqlServerExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
    ) -> ConnectorResult<Self> {
        tracing::info!(
            ?rw_schema,
            ?pk_indices,
            "create sql server external table reader"
        );
        let mut client_config = Config::new();

        client_config.host(&config.host);
        client_config.database(&config.database);
        client_config.port(config.port.parse::<u16>().unwrap());
        client_config.authentication(tiberius::AuthMethod::sql_server(
            &config.username,
            &config.password,
        ));
        // TODO(kexiang): use trust_cert_ca, trust_cert is not secure
        if config.encrypt == "true" {
            client_config.encryption(tiberius::EncryptionLevel::Required);
        }
        client_config.trust_cert();

        let client = SqlServerClient::new_with_config(client_config).await?;

        let field_names = rw_schema
            .fields
            .iter()
            .map(|f| Self::quote_column(&f.name))
            .join(",");

        Ok(Self {
            rw_schema,
            pk_indices,
            field_names,
            client: tokio::sync::Mutex::new(client),
        })
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::SqlServer(
                SqlServerOffset::parse_debezium_offset(offset)?,
            ))
        })
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
        let mut sql = Query::new(if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                order_key,
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                filter_expr,
                order_key,
            )
        });

        let mut client = self.client.lock().await;

        // FIXME(kexiang): Set session timezone to UTC
        if let Some(pk_row) = start_pk_row {
            let params: Vec<Option<ScalarImpl>> = pk_row.into_iter().collect();
            for (index, param) in params.into_iter().enumerate() {
                let param = param.with_context(|| {
                    format!(
                        "SQL Server snapshot primary-key position at index {index} cannot be NULL"
                    )
                })?;
                sql.bind(ScalarImplTiberiusWrapper::from(param));
            }
        }

        let stream = sql.query(&mut client.inner_client).await?.into_row_stream();

        let row_stream = stream.map(|res| {
            // convert sql server row into OwnedRow
            let mut row = res?;
            sql_server_row_to_owned_row_with_strict_pk(&mut row, &self.rw_schema, &self.pk_indices)
                .map_err(ConnectorError::from)
        });

        pin_mut!(row_stream);

        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!(
            "\"{}\".\"{}\"",
            table_name.schema_name, table_name.table_name
        )
    }

    // sql server cannot leverage the given key to narrow down the range of scan,
    // we need to rewrite the comparison conditions by our own.
    // (a, b) > (x, y) => ("a" > @P1) OR (("a" = @P1) AND ("b" > @P2))
    fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!("({} > @P{})", Self::quote_column(&columns[0]), 1));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, col) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!("({} = @P{})", Self::quote_column(col), j + 1));
                } else {
                    condition.push_str(&format!(
                        " AND ({} = @P{})",
                        Self::quote_column(col),
                        j + 1
                    ));
                }
            }
            // '>' condition
            condition.push_str(&format!(
                " AND ({} > @P{})",
                Self::quote_column(&columns[i - 1]),
                i
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
        format!("\"{}\"", column)
    }
}

#[cfg(test)]
mod tests {
    use crate::source::cdc::external::SqlServerExternalTableReader;

    #[test]
    fn test_sql_server_filter_expr() {
        let cols = vec!["id".to_owned()];
        let expr = SqlServerExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"id\" > @P1)");

        let cols = vec!["aa".to_owned(), "bb".to_owned(), "cc".to_owned()];
        let expr = SqlServerExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(\"aa\" > @P1) OR ((\"aa\" = @P1) AND (\"bb\" > @P2)) OR ((\"aa\" = @P1) AND (\"bb\" = @P2) AND (\"cc\" > @P3))"
        );
    }
}
