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

use std::cmp::Ordering;
use std::sync::LazyLock;

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut};
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio_postgres::types::{PgLsn, Type as PgType};

use crate::connector_common::create_pg_client;
use crate::connector_common::postgres::postgres_point_type;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::scalar_adapter::ScalarAdapter;
use crate::parser::{
    postgres_cell_to_scalar_impl_strict, postgres_row_to_owned_row_with_strict_pk,
};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CDC_TABLE_SPLIT_ID_START, CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption,
    DebeziumOffset, ExternalTableConfig, ExternalTableReader, SchemaTableName,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostgresOffset {
    pub txid: i64,
    // In postgres, an LSN is a 64-bit integer, representing a byte position in the write-ahead log stream.
    // It is printed as two hexadecimal numbers of up to 8 digits each, separated by a slash; for example, 16/B374D848
    pub lsn: u64,
    // Additional LSN fields for improved tracking
    #[serde(default)]
    pub lsn_commit: Option<u64>,
    #[serde(default)]
    pub lsn_proc: Option<u64>,
}

impl PartialOrd for PostgresOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PostgresOffset {}
impl PartialEq for PostgresOffset {
    fn eq(&self, other: &Self) -> bool {
        match (
            self.lsn_commit,
            self.lsn_proc,
            other.lsn_commit,
            other.lsn_proc,
        ) {
            (_, Some(_), _, Some(_)) => {
                self.lsn_commit == other.lsn_commit && self.lsn_proc == other.lsn_proc
            }
            _ => self.lsn == other.lsn,
        }
    }
}

// only compare the lsn field, prefer lsn_commit and lsn_proc if both available
impl Ord for PostgresOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.lsn_commit,
            self.lsn_proc,
            other.lsn_commit,
            other.lsn_proc,
        ) {
            (_, Some(self_proc), _, Some(other_proc)) => {
                // if both have `lsn_commit` and `lsn_proc`, compare `lsn_commit` first, then `lsn_proc`
                // if `lsn_commit` is None, fall back to `lsn_proc`
                match self.lsn_commit.cmp(&other.lsn_commit) {
                    Ordering::Equal => self_proc.cmp(&other_proc),
                    other_result => other_result,
                }
            }
            _ => {
                // Fall back to lsn comparison when either lsn_commit or lsn_proc is missing
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::warn!(
                        suppressed_count,
                        self_lsn = self.lsn,
                        other_lsn = other.lsn,
                        "lsn_commit and lsn_proc are missing, fall back to lsn comparison"
                    );
                }
                self.lsn.cmp(&other.lsn)
            }
        }
    }
}

impl PostgresOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        let lsn = dbz_offset
            .source_offset
            .lsn
            .context("invalid postgres lsn")?;

        // `lsn_commit` may not be present in the offset for the first tx.
        let lsn_commit = dbz_offset.source_offset.lsn_commit;

        let lsn_proc = dbz_offset
            .source_offset
            .lsn_proc
            .context("invalid postgres lsn_proc")?;

        Ok(Self {
            txid: dbz_offset
                .source_offset
                .txid
                .context("invalid postgres txid")?,
            lsn,
            lsn_commit,
            lsn_proc: Some(lsn_proc),
        })
    }
}

pub struct PostgresExternalTableReader {
    rw_schema: Schema,
    field_names: String,
    pk_indices: Vec<usize>,
    client: tokio::sync::Mutex<tokio_postgres::Client>,
    schema_table_name: SchemaTableName,
}

impl ExternalTableReader for PostgresExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut client = self.client.lock().await;
        // start a transaction to read current lsn and txid
        let trxn = client.transaction().await?;
        let row = trxn.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
        let mut pg_offset = PostgresOffset::default();
        let pg_lsn = row.get::<_, PgLsn>(0);
        tracing::debug!("current lsn: {}", pg_lsn);
        pg_offset.lsn = pg_lsn.into();

        let txid_row = trxn.query_one("SELECT txid_current()", &[]).await?;
        let txid: i64 = txid_row.get::<_, i64>(0);
        pg_offset.txid = txid;

        // commit the transaction
        trxn.commit().await?;

        Ok(CdcOffset::Postgres(pg_offset))
    }

    async fn estimated_row_count(
        &self,
        table_name: &SchemaTableName,
    ) -> ConnectorResult<Option<u64>> {
        assert_eq!(table_name, &self.schema_table_name);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT c.reltuples::double precision \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                   AND c.relkind IN ('r', 'p')",
                &[&table_name.schema_name, &table_name.table_name],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let estimate = row.get::<_, f64>(0);
        Ok((estimate.is_finite() && estimate >= 0.0).then_some(estimate.round() as u64))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        assert_eq!(table_name, self.schema_table_name);
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits(&self, options: CdcTableSnapshotSplitOption) {
        let backfill_num_rows_per_split = options.backfill_num_rows_per_split;
        if backfill_num_rows_per_split == 0 {
            return Err(anyhow::anyhow!(
                "invalid backfill_num_rows_per_split, must be greater than 0"
            )
            .into());
        }
        if options.backfill_split_pk_column_index as usize >= self.pk_indices.len() {
            return Err(anyhow::anyhow!(format!(
                "invalid backfill_split_pk_column_index {}, out of bound",
                options.backfill_split_pk_column_index
            ))
            .into());
        }
        let split_column = self.split_column(&options);
        let row_stream = if options.backfill_as_even_splits
            && is_supported_even_split_data_type(&split_column.data_type)
        {
            // For certain types, use evenly-sized partition to optimize performance.
            tracing::info!(?self.schema_table_name, ?self.rw_schema, ?self.pk_indices, ?split_column, "Get parallel cdc table snapshot even splits.");
            self.as_even_splits(options)
        } else {
            tracing::info!(?self.schema_table_name, ?self.rw_schema, ?self.pk_indices, ?split_column, "Get parallel cdc table snapshot uneven splits.");
            self.as_uneven_splits(options)
        };
        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        assert_eq!(table_name, self.schema_table_name);
        self.split_snapshot_read_inner(table_name, left, right, split_columns)
    }
}

impl PostgresExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
    ) -> ConnectorResult<Self> {
        tracing::info!(
            ?rw_schema,
            ?pk_indices,
            "create postgres external table reader"
        );
        // No TCP keepalive for CDC source
        let client = create_pg_client(&config.pg_connection_config()?, None).await?;

        // Discover user-defined composite columns and arrays of composites.
        // tokio-postgres cannot decode composite values natively, so for these
        // columns we cast to text in the snapshot SELECT. Scalar composites
        // become `(a,b,c)`; arrays become `text[]` so tokio-postgres can
        // decode them directly as `Vec<Option<String>>` matching the RW
        // `List(Varchar)` column. Other varchar columns stay as-is to
        // preserve RW's own text rendering (e.g. numeric -> "NaN").
        let composite_columns = client
            .query(
                "SELECT a.attname, (t.typcategory = 'A') AS is_array \
                 FROM pg_attribute a \
                 JOIN pg_class c ON a.attrelid = c.oid \
                 JOIN pg_namespace n ON c.relnamespace = n.oid \
                 JOIN pg_type t ON a.atttypid = t.oid \
                 LEFT JOIN pg_type t_elem ON t.typelem = t_elem.oid \
                 WHERE n.nspname = $1 \
                   AND c.relname = $2 \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped \
                   AND (t.typtype = 'c' \
                        OR (t.typcategory = 'A' AND t_elem.typtype = 'c'))",
                &[
                    &schema_table_name.schema_name,
                    &schema_table_name.table_name,
                ],
            )
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1)))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_else(|err| {
                tracing::warn!(
                    error = %err.as_report(),
                    schema = %schema_table_name.schema_name,
                    table = %schema_table_name.table_name,
                    "failed to discover postgres composite columns; falling back to no text cast"
                );
                std::collections::HashMap::new()
            });

        let field_names = rw_schema
            .fields
            .iter()
            .map(|f| {
                let quoted = Self::quote_column(&f.name);
                match composite_columns.get(&f.name) {
                    Some(false) if matches!(f.data_type, DataType::Varchar) => {
                        format!("{quoted}::text AS {quoted}")
                    }
                    Some(true) if matches!(f.data_type, DataType::List(_)) => {
                        format!(
                            "CASE WHEN {quoted} IS NULL THEN NULL \
                             ELSE ARRAY(SELECT t::text FROM unnest({quoted}) t)::text[] \
                             END AS {quoted}"
                        )
                    }
                    _ => quoted,
                }
            })
            .join(",");

        Ok(Self {
            rw_schema,
            field_names,
            pk_indices,
            client: tokio::sync::Mutex::new(client),
            schema_table_name,
        })
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!(
            "\"{}\".\"{}\"",
            table_name.schema_name, table_name.table_name
        )
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::Postgres(PostgresOffset::parse_debezium_offset(
                offset,
            )?))
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        scan_limit: u32,
    ) {
        let order_key = Self::get_order_key(&primary_keys);
        let client = self.client.lock().await;
        client.execute("set time zone '+00:00'", &[]).await?;

        let stream = match start_pk_row {
            Some(ref pk_row) => {
                // prepare the scan statement, since we may need to convert the RW data type to postgres data type
                // e.g. varchar to uuid
                let prepared_scan_stmt = {
                    let primary_keys = self
                        .pk_indices
                        .iter()
                        .map(|i| self.rw_schema.fields[*i].name.clone())
                        .collect_vec();

                    let order_key = Self::get_order_key(&primary_keys);
                    let scan_sql = format!(
                        "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {scan_limit}",
                        self.field_names,
                        Self::get_normalized_table_name(&table_name),
                        Self::filter_expression(&primary_keys),
                        order_key,
                    );
                    client.prepare(&scan_sql).await?
                };

                let params: Vec<Option<ScalarAdapter>> = pk_row
                    .iter()
                    .zip_eq_fast(prepared_scan_stmt.params())
                    .map(|(datum, ty)| {
                        datum
                            .map(|scalar| ScalarAdapter::from_scalar(scalar, ty))
                            .transpose()
                    })
                    .try_collect()?;

                client.query_raw(&prepared_scan_stmt, &params).await?
            }
            None => {
                let sql = format!(
                    "SELECT {} FROM {} ORDER BY {} LIMIT {scan_limit}",
                    self.field_names,
                    Self::get_normalized_table_name(&table_name),
                    order_key,
                );
                let params: Vec<Option<ScalarAdapter>> = vec![];
                client.query_raw(&sql, &params).await?
            }
        };

        let row_stream = stream.map(|row| {
            let row = row?;
            postgres_row_to_owned_row_with_strict_pk(row, &self.rw_schema, &self.pk_indices)
                .map_err(ConnectorError::from)
        });

        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    // row filter expression: (v1, v2, v3) > ($1, $2, $3)
    fn filter_expression(columns: &[String]) -> String {
        let mut col_expr = String::new();
        let mut arg_expr = String::new();
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                col_expr.push_str(", ");
                arg_expr.push_str(", ");
            }
            col_expr.push_str(&Self::quote_column(column));
            arg_expr.push_str(format!("${}", i + 1).as_str());
        }
        format!("({}) > ({})", col_expr, arg_expr)
    }

    // row filter expression: (v1, v2, v3) >= ($1, $2, $3) AND (v1, v2, v3) < ($1, $2, $3)
    fn split_filter_expression(
        columns: &[String],
        is_first_split: bool,
        is_last_split: bool,
    ) -> String {
        let mut left_col_expr = String::new();
        let mut left_arg_expr = String::new();
        let mut right_col_expr = String::new();
        let mut right_arg_expr = String::new();
        let mut c = 1;
        if !is_first_split {
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    left_col_expr.push_str(", ");
                    left_arg_expr.push_str(", ");
                }
                left_col_expr.push_str(&Self::quote_column(column));
                left_arg_expr.push_str(format!("${}", c).as_str());
                c += 1;
            }
        }
        if !is_last_split {
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    right_col_expr.push_str(", ");
                    right_arg_expr.push_str(", ");
                }
                right_col_expr.push_str(&Self::quote_column(column));
                right_arg_expr.push_str(format!("${}", c).as_str());
                c += 1;
            }
        }
        if is_first_split && is_last_split {
            "1 = 1".to_owned()
        } else if is_first_split {
            format!("({}) < ({})", right_col_expr, right_arg_expr,)
        } else if is_last_split {
            format!("({}) >= ({})", left_col_expr, left_arg_expr,)
        } else {
            format!(
                "({}) >= ({}) AND ({}) < ({})",
                left_col_expr, left_arg_expr, right_col_expr, right_arg_expr,
            )
        }
    }

    fn get_order_key(primary_keys: &Vec<String>) -> String {
        primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",")
    }

    fn quote_column(column: &str) -> String {
        format!("\"{}\"", column)
    }

    async fn min_and_max(
        &self,
        split_column: &Field,
    ) -> ConnectorResult<Option<(ScalarImpl, ScalarImpl)>> {
        let sql = format!(
            "SELECT MIN({}), MAX({}) FROM {}",
            split_column.name,
            split_column.name,
            Self::get_normalized_table_name(&self.schema_table_name),
        );
        let client = self.client.lock().await;
        let rows = client.query(&sql, &[]).await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let row = &rows[0];
            let min = postgres_cell_to_scalar_impl_strict(
                row,
                &split_column.data_type,
                0,
                &split_column.name,
            )?;
            let max = postgres_cell_to_scalar_impl_strict(
                row,
                &split_column.data_type,
                1,
                &split_column.name,
            )?;
            match (min, max) {
                (Some(min), Some(max)) => Ok(Some((min, max))),
                _ => Ok(None),
            }
        }
    }

    async fn next_split_right_bound_exclusive(
        &self,
        left_value: &ScalarImpl,
        max_value: &ScalarImpl,
        max_split_size: u64,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let sql = format!(
            "WITH t as (SELECT {} FROM {} WHERE {} >= $1 ORDER BY {} ASC LIMIT {}) SELECT CASE WHEN MAX({}) < $2 THEN MAX({}) ELSE NULL END FROM t",
            Self::quote_column(&split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            Self::quote_column(&split_column.name),
            Self::quote_column(&split_column.name),
            max_split_size,
            Self::quote_column(&split_column.name),
            Self::quote_column(&split_column.name),
        );
        let client = self.client.lock().await;
        let prepared_stmt = client.prepare(&sql).await?;
        let params: Vec<Option<ScalarAdapter>> = vec![
            Some(ScalarAdapter::from_scalar(
                left_value.as_scalar_ref_impl(),
                &prepared_stmt.params()[0],
            )?),
            Some(ScalarAdapter::from_scalar(
                max_value.as_scalar_ref_impl(),
                &prepared_stmt.params()[1],
            )?),
        ];
        let stream = client.query_raw(&prepared_stmt, &params).await?;
        let datum_stream = stream.map(|row| {
            let row = row?;
            Ok::<_, ConnectorError>(postgres_cell_to_scalar_impl_strict(
                &row,
                &split_column.data_type,
                0,
                &split_column.name,
            )?)
        });
        pin_mut!(datum_stream);
        #[for_await]
        for datum in datum_stream {
            let right = datum?;
            return Ok(Some(right.to_owned_datum()));
        }
        Ok(None)
    }

    async fn next_greater_bound(
        &self,
        start_offset: &ScalarImpl,
        max_value: &ScalarImpl,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let sql = format!(
            "SELECT MIN({}) FROM {} WHERE {} > $1 AND {} <$2",
            Self::quote_column(&split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            Self::quote_column(&split_column.name),
            Self::quote_column(&split_column.name),
        );
        let client = self.client.lock().await;
        let prepared_stmt = client.prepare(&sql).await?;
        let params: Vec<Option<ScalarAdapter>> = vec![
            Some(ScalarAdapter::from_scalar(
                start_offset.as_scalar_ref_impl(),
                &prepared_stmt.params()[0],
            )?),
            Some(ScalarAdapter::from_scalar(
                max_value.as_scalar_ref_impl(),
                &prepared_stmt.params()[1],
            )?),
        ];
        let stream = client.query_raw(&prepared_stmt, &params).await?;
        let datum_stream = stream.map(|row| {
            let row = row?;
            Ok::<_, ConnectorError>(postgres_cell_to_scalar_impl_strict(
                &row,
                &split_column.data_type,
                0,
                &split_column.name,
            )?)
        });
        pin_mut!(datum_stream);
        #[for_await]
        for datum in datum_stream {
            let right = datum?;
            return Ok(Some(right));
        }
        Ok(None)
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        assert_eq!(
            split_columns.len(),
            1,
            "multiple split columns is not supported yet"
        );
        assert_eq!(left.len(), 1, "multiple split columns is not supported yet");
        assert_eq!(
            right.len(),
            1,
            "multiple split columns is not supported yet"
        );
        let is_first_split = left[0].is_none();
        let is_last_split = right[0].is_none();
        let split_column_names = split_columns.iter().map(|c| c.name.clone()).collect_vec();
        let client = self.client.lock().await;
        client.execute("set time zone '+00:00'", &[]).await?;
        // prepare the scan statement, since we may need to convert the RW data type to postgres data type
        // e.g. varchar to uuid
        let prepared_scan_stmt = {
            let scan_sql = format!(
                "SELECT {} FROM {} WHERE {}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                Self::split_filter_expression(&split_column_names, is_first_split, is_last_split),
            );
            client.prepare(&scan_sql).await?
        };

        let mut params: Vec<Option<ScalarAdapter>> = vec![];
        if !is_first_split {
            let left_params: Vec<Option<ScalarAdapter>> = left
                .iter()
                .zip_eq_fast(prepared_scan_stmt.params().iter().take(left.len()))
                .map(|(datum, ty)| {
                    datum
                        .map(|scalar| ScalarAdapter::from_scalar(scalar, ty))
                        .transpose()
                })
                .try_collect()?;
            params.extend(left_params);
        }
        if !is_last_split {
            let right_params: Vec<Option<ScalarAdapter>> = right
                .iter()
                .zip_eq_fast(prepared_scan_stmt.params().iter().skip(params.len()))
                .map(|(datum, ty)| {
                    datum
                        .map(|scalar| ScalarAdapter::from_scalar(scalar, ty))
                        .transpose()
                })
                .try_collect()?;
            params.extend(right_params);
        }

        let stream = client.query_raw(&prepared_scan_stmt, &params).await?;
        let row_stream = stream.map(|row| {
            let row = row?;
            postgres_row_to_owned_row_with_strict_pk(row, &self.rw_schema, &self.pk_indices)
                .map_err(ConnectorError::from)
        });

        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn as_uneven_splits(&self, options: CdcTableSnapshotSplitOption) {
        let split_column = self.split_column(&options);
        let mut split_id = CDC_TABLE_SPLIT_ID_START;
        let Some((min_value, max_value)) = self.min_and_max(&split_column).await? else {
            let left_bound_row = OwnedRow::new(vec![None]);
            let right_bound_row = OwnedRow::new(vec![None]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };
            yield split;
            return Ok(());
        };
        // left bound will never be NULL value.
        let mut next_left_bound_inclusive = min_value.clone();
        loop {
            let left_bound_inclusive: Datum = if next_left_bound_inclusive == min_value {
                None
            } else {
                Some(next_left_bound_inclusive.clone())
            };
            let right_bound_exclusive;
            let mut next_right = self
                .next_split_right_bound_exclusive(
                    &next_left_bound_inclusive,
                    &max_value,
                    options.backfill_num_rows_per_split,
                    &split_column,
                )
                .await?;
            if let Some(Some(ref inner)) = next_right
                && *inner == next_left_bound_inclusive
            {
                next_right = self
                    .next_greater_bound(&next_left_bound_inclusive, &max_value, &split_column)
                    .await?;
            }
            if let Some(next_right) = next_right {
                match next_right {
                    None => {
                        // NULL found.
                        right_bound_exclusive = None;
                    }
                    Some(next_right) => {
                        next_left_bound_inclusive = next_right.clone();
                        right_bound_exclusive = Some(next_right);
                    }
                }
            } else {
                // Not found.
                right_bound_exclusive = None;
            };
            let is_completed = right_bound_exclusive.is_none();
            if is_completed && left_bound_inclusive.is_none() {
                assert_eq!(split_id, 1);
            }
            tracing::info!(
                split_id,
                ?left_bound_inclusive,
                ?right_bound_exclusive,
                "New CDC table snapshot split."
            );
            let left_bound_row = OwnedRow::new(vec![left_bound_inclusive]);
            let right_bound_row = OwnedRow::new(vec![right_bound_exclusive]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };
            try_increase_split_id(&mut split_id)?;
            yield split;
            if is_completed {
                break;
            }
        }
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn as_even_splits(&self, options: CdcTableSnapshotSplitOption) {
        let split_column = self.split_column(&options);
        let mut split_id = 1;
        let Some((min_value, max_value)) = self.min_and_max(&split_column).await? else {
            let left_bound_row = OwnedRow::new(vec![None]);
            let right_bound_row = OwnedRow::new(vec![None]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };
            yield split;
            return Ok(());
        };
        let min_value = min_value.as_integral();
        let max_value = max_value.as_integral();
        let saturated_split_max_size = options
            .backfill_num_rows_per_split
            .try_into()
            .unwrap_or(i64::MAX);
        let mut left = None;
        let mut right = Some(min_value.saturating_add(saturated_split_max_size));
        loop {
            let mut is_completed = false;
            if right.as_ref().map(|r| *r >= max_value).unwrap_or(true) {
                right = None;
                is_completed = true;
            }
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: OwnedRow::new(vec![
                    left.map(|l| to_int_scalar(l, &split_column.data_type)),
                ]),
                right_bound_exclusive: OwnedRow::new(vec![
                    right.map(|r| to_int_scalar(r, &split_column.data_type)),
                ]),
            };
            try_increase_split_id(&mut split_id)?;
            yield split;
            if is_completed {
                break;
            }
            left = right;
            right = left.map(|l| l.saturating_add(saturated_split_max_size));
        }
    }

    fn split_column(&self, options: &CdcTableSnapshotSplitOption) -> Field {
        self.rw_schema.fields[self.pk_indices[options.backfill_split_pk_column_index as usize]]
            .clone()
    }
}

fn to_int_scalar(i: i64, data_type: &DataType) -> ScalarImpl {
    match data_type {
        DataType::Int16 => ScalarImpl::Int16(i.try_into().unwrap()),
        DataType::Int32 => ScalarImpl::Int32(i.try_into().unwrap()),
        DataType::Int64 => ScalarImpl::Int64(i),
        _ => {
            panic!("Can't convert int {} to ScalarImpl::{}", i, data_type)
        }
    }
}

fn try_increase_split_id(split_id: &mut i64) -> ConnectorResult<()> {
    match split_id.checked_add(1) {
        Some(s) => {
            *split_id = s;
            Ok(())
        }
        None => Err(anyhow::anyhow!("too many CDC snapshot splits").into()),
    }
}

/// Use the first column of primary keys to split table.
fn is_supported_even_split_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

pub fn type_name_to_pg_type(ty_name: &str) -> Option<PgType> {
    let ty_name_lower = ty_name.to_lowercase();
    // Handle array types (prefixed with _)
    if let Some(base_type) = ty_name_lower.strip_prefix('_') {
        match base_type {
            "int2" => Some(PgType::INT2_ARRAY),
            "int4" => Some(PgType::INT4_ARRAY),
            "int8" => Some(PgType::INT8_ARRAY),
            "bit" => Some(PgType::BIT_ARRAY),
            "float4" => Some(PgType::FLOAT4_ARRAY),
            "float8" => Some(PgType::FLOAT8_ARRAY),
            "numeric" => Some(PgType::NUMERIC_ARRAY),
            "bool" => Some(PgType::BOOL_ARRAY),
            "xml" | "macaddr" | "macaddr8" | "cidr" | "inet" | "int4range" | "int8range"
            | "numrange" | "tsrange" | "tstzrange" | "daterange" | "citext" => {
                Some(PgType::VARCHAR_ARRAY)
            }
            "varchar" => Some(PgType::VARCHAR_ARRAY),
            "text" => Some(PgType::TEXT_ARRAY),
            "bytea" => Some(PgType::BYTEA_ARRAY),
            "geometry" | "geography" => Some(PgType::BYTEA_ARRAY), // PostGIS spatial arrays
            "date" => Some(PgType::DATE_ARRAY),
            "time" => Some(PgType::TIME_ARRAY),
            "timetz" => Some(PgType::TIMETZ_ARRAY),
            "timestamp" => Some(PgType::TIMESTAMP_ARRAY),
            "timestamptz" => Some(PgType::TIMESTAMPTZ_ARRAY),
            "interval" => Some(PgType::INTERVAL_ARRAY),
            "json" => Some(PgType::JSON_ARRAY),
            "jsonb" => Some(PgType::JSONB_ARRAY),
            "uuid" => Some(PgType::UUID_ARRAY),
            "point" => Some(PgType::POINT_ARRAY),
            "oid" => Some(PgType::OID_ARRAY),
            "money" => Some(PgType::MONEY_ARRAY),
            _ => None,
        }
    } else {
        // Handle non-array types
        match ty_name_lower.as_str() {
            "int2" => Some(PgType::INT2),
            "bit" => Some(PgType::BIT),
            "int" | "int4" => Some(PgType::INT4),
            "int8" => Some(PgType::INT8),
            "float4" => Some(PgType::FLOAT4),
            "float8" => Some(PgType::FLOAT8),
            "numeric" => Some(PgType::NUMERIC),
            "money" => Some(PgType::MONEY),
            "boolean" | "bool" => Some(PgType::BOOL),
            "inet" | "xml" | "varchar" | "character varying" | "int4range" | "int8range"
            | "numrange" | "tsrange" | "tstzrange" | "daterange" | "macaddr" | "macaddr8"
            | "cidr" => Some(PgType::VARCHAR),
            "char" | "character" | "bpchar" => Some(PgType::BPCHAR),
            "citext" | "text" => Some(PgType::TEXT),
            "bytea" => Some(PgType::BYTEA),
            "geometry" | "geography" => Some(PgType::BYTEA), // PostGIS spatial types
            "date" => Some(PgType::DATE),
            "time" => Some(PgType::TIME),
            "timetz" => Some(PgType::TIMETZ),
            "timestamp" => Some(PgType::TIMESTAMP),
            "timestamptz" => Some(PgType::TIMESTAMPTZ),
            "interval" => Some(PgType::INTERVAL),
            "json" => Some(PgType::JSON),
            "jsonb" => Some(PgType::JSONB),
            "uuid" => Some(PgType::UUID),
            "point" => Some(PgType::POINT),
            "oid" => Some(PgType::OID),
            _ => None,
        }
    }
}

// Keep this canonical mapping aligned with `postgres_source_column_type_compatible` in
// `src/common/src/catalog/cdc_type_compatibility.rs`, which validates user-declared RW column
// types.
pub fn pg_type_to_rw_type(pg_type: &PgType) -> ConnectorResult<DataType> {
    let data_type = match *pg_type {
        PgType::BOOL => DataType::Boolean,
        PgType::BIT => DataType::Boolean,
        PgType::INT2 => DataType::Int16,
        PgType::INT4 => DataType::Int32,
        PgType::INT8 => DataType::Int64,
        PgType::FLOAT4 => DataType::Float32,
        PgType::FLOAT8 => DataType::Float64,
        PgType::NUMERIC | PgType::MONEY => DataType::Decimal,
        PgType::DATE => DataType::Date,
        PgType::TIME => DataType::Time,
        PgType::TIMETZ => DataType::Time,
        PgType::POINT => postgres_point_type(),
        PgType::TIMESTAMP => DataType::Timestamp,
        PgType::TIMESTAMPTZ => DataType::Timestamptz,
        PgType::INTERVAL => DataType::Interval,
        PgType::VARCHAR | PgType::TEXT | PgType::BPCHAR | PgType::UUID => DataType::Varchar,
        PgType::BYTEA => DataType::Bytea,
        PgType::JSON | PgType::JSONB => DataType::Jsonb,
        // Array types
        PgType::BOOL_ARRAY => DataType::Boolean.list(),
        PgType::BIT_ARRAY => DataType::Boolean.list(),
        PgType::INT2_ARRAY => DataType::Int16.list(),
        PgType::INT4_ARRAY => DataType::Int32.list(),
        PgType::INT8_ARRAY => DataType::Int64.list(),
        PgType::FLOAT4_ARRAY => DataType::Float32.list(),
        PgType::FLOAT8_ARRAY => DataType::Float64.list(),
        PgType::NUMERIC_ARRAY => DataType::Decimal.list(),
        PgType::VARCHAR_ARRAY => DataType::Varchar.list(),
        PgType::TEXT_ARRAY => DataType::Varchar.list(),
        PgType::BYTEA_ARRAY => DataType::Bytea.list(),
        PgType::DATE_ARRAY => DataType::Date.list(),
        PgType::TIME_ARRAY => DataType::Time.list(),
        PgType::TIMESTAMP_ARRAY => DataType::Timestamp.list(),
        PgType::TIMESTAMPTZ_ARRAY => DataType::Timestamptz.list(),
        PgType::INTERVAL_ARRAY => DataType::Interval.list(),
        PgType::JSON_ARRAY => DataType::Jsonb.list(),
        PgType::JSONB_ARRAY => DataType::Jsonb.list(),
        PgType::UUID_ARRAY => DataType::Varchar.list(),
        PgType::OID => DataType::Int64,
        PgType::OID_ARRAY => DataType::Int64.list(),
        PgType::MONEY_ARRAY => DataType::Decimal.list(),
        // Debezium does not implement POINT_ARRAY schema conversion.
        // https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresValueConverter.java#L339-L348
        PgType::POINT_ARRAY => {
            return Err(anyhow::anyhow!("unsupported postgres type: {}", pg_type).into());
        }
        _ => {
            return Err(anyhow::anyhow!("unsupported postgres type: {}", pg_type).into());
        }
    };
    Ok(data_type)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use crate::connector_common::PostgresExternalTable;
    use crate::source::cdc::external::postgres::{PostgresExternalTableReader, PostgresOffset};
    use crate::source::cdc::external::{ExternalTableConfig, ExternalTableReader, SchemaTableName};

    #[ignore]
    #[tokio::test]
    async fn test_postgres_schema() {
        let config = ExternalTableConfig {
            connector: "postgres-cdc".to_owned(),
            host: "localhost".to_owned(),
            port: "8432".to_owned(),
            username: "myuser".to_owned(),
            password: "123456".to_owned(),
            database: "mydb".to_owned(),
            schema: "public".to_owned(),
            table: "mytest".to_owned(),
            ssl_mode: Default::default(),
            ssl_root_cert: None,
            encrypt: "false".to_owned(),
        };

        let table = PostgresExternalTable::connect(
            &config.pg_connection_config().unwrap(),
            &config.schema,
            &config.table,
            false,
            Some("SELECT"),
        )
        .await
        .unwrap();

        println!("columns: {:?}", table.column_descs());
        println!("primary keys: {:?}", table.pk_names());
    }

    #[test]
    fn test_postgres_offset() {
        let off1 = PostgresOffset {
            txid: 4,
            lsn: 2,
            ..Default::default()
        };
        let off2 = PostgresOffset {
            txid: 1,
            lsn: 3,
            ..Default::default()
        };
        let off3 = PostgresOffset {
            txid: 5,
            lsn: 1,
            ..Default::default()
        };

        assert!(off1 < off2);
        assert!(off3 < off1);
        assert!(off2 > off3);
    }

    #[test]
    fn test_postgres_offset_partial_ord_with_lsn_commit() {
        // Test comparison with both lsn_commit and lsn_proc fields
        let off1 = PostgresOffset {
            txid: 1,
            lsn: 100,
            lsn_commit: Some(200),
            lsn_proc: Some(150),
        };
        let off2 = PostgresOffset {
            txid: 2,
            lsn: 300,
            lsn_commit: Some(250),
            lsn_proc: Some(200),
        };

        // Should compare using lsn_commit first when both have both fields
        assert!(off1 < off2);

        // Test with same lsn_commit but different lsn_proc
        let off3 = PostgresOffset {
            txid: 3,
            lsn: 500,
            lsn_commit: Some(200), // same as off1
            lsn_proc: Some(160),   // higher than off1
        };

        // Should compare lsn_proc when lsn_commit is equal
        assert!(off1 < off3);

        // Test with missing lsn_proc - should fall back to lsn comparison
        let off4 = PostgresOffset {
            txid: 4,
            lsn: 400,
            lsn_commit: Some(100), // lower than off1's lsn_commit
            lsn_proc: None,        // missing lsn_proc
        };

        // Should fall back to lsn comparison (off1.lsn=100 < off4.lsn=400)
        assert!(off1 < off4);

        // Test with missing lsn_commit - should fall back to lsn comparison
        let off5 = PostgresOffset {
            txid: 5,
            lsn: 50,             // lower than off1.lsn
            lsn_commit: None,    // missing lsn_commit
            lsn_proc: Some(300), // higher than off1's lsn_proc
        };

        // Should fall back to lsn comparison (off5.lsn=50 < off1.lsn=100)
        assert!(off5 < off1);

        // Additional test cases: equal lsn_commit values with different lsn_proc
        let off6 = PostgresOffset {
            txid: 6,
            lsn: 600,
            lsn_commit: Some(500),
            lsn_proc: Some(300),
        };
        let off7 = PostgresOffset {
            txid: 7,
            lsn: 700,
            lsn_commit: Some(500), // same as off6
            lsn_proc: Some(400),   // higher than off6
        };

        // Should compare lsn_proc since lsn_commit is equal
        assert!(off6 < off7);

        // Test reverse order
        let off8 = PostgresOffset {
            txid: 8,
            lsn: 800,
            lsn_commit: Some(500), // same as others
            lsn_proc: Some(200),   // lower than off6
        };

        assert!(off8 < off6);
        assert!(off8 < off7);

        // Test equal lsn_commit and lsn_proc
        let off9 = PostgresOffset {
            txid: 9,
            lsn: 900,
            lsn_commit: Some(500), // same as off6
            lsn_proc: Some(300),   // same as off6
        };

        // Should be equal
        assert_eq!(off6.partial_cmp(&off9), Some(Ordering::Equal));
    }

    #[test]
    fn test_debezium_offset_parsing() {
        // Test parsing with all required fields present
        let debezium_offset_with_fields = r#"{
            "sourcePartition": {"server": "RW_CDC_1004"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 29973552,
                "txId": 1046,
                "ts_usec": 1670826189008456,
                "snapshot": true,
                "lsn_commit": 29973600,
                "lsn_proc": 29973580
            },
            "isHeartbeat": false
        }"#;

        let offset = PostgresOffset::parse_debezium_offset(debezium_offset_with_fields).unwrap();
        assert_eq!(offset.txid, 1046);
        assert_eq!(offset.lsn, 29973552);
        assert_eq!(offset.lsn_commit, Some(29973600));
        assert_eq!(offset.lsn_proc, Some(29973580));

        // Test parsing should fail when required fields are missing
        let debezium_offset_missing_fields = r#"{
            "sourcePartition": {"server": "RW_CDC_1004"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 29973552,
                "txId": 1046,
                "ts_usec": 1670826189008456,
                "snapshot": true
            },
            "isHeartbeat": false
        }"#;

        let result = PostgresOffset::parse_debezium_offset(debezium_offset_missing_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid postgres lsn_proc"));
    }

    #[test]
    fn test_filter_expression() {
        let cols = vec!["v1".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\") > ($1)");

        let cols = vec!["v1".to_owned(), "v2".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\", \"v2\") > ($1, $2)");

        let cols = vec!["v1".to_owned(), "v2".to_owned(), "v3".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\", \"v2\", \"v3\") > ($1, $2, $3)");
    }

    #[test]
    fn test_split_filter_expression() {
        let cols = vec!["v1".to_owned()];
        let expr = PostgresExternalTableReader::split_filter_expression(&cols, true, true);
        assert_eq!(expr, "1 = 1");

        let expr = PostgresExternalTableReader::split_filter_expression(&cols, true, false);
        assert_eq!(expr, "(\"v1\") < ($1)");

        let expr = PostgresExternalTableReader::split_filter_expression(&cols, false, true);
        assert_eq!(expr, "(\"v1\") >= ($1)");

        let expr = PostgresExternalTableReader::split_filter_expression(&cols, false, false);
        assert_eq!(expr, "(\"v1\") >= ($1) AND (\"v1\") < ($2)");
    }

    // manual test
    #[ignore]
    #[tokio::test]
    async fn test_pg_table_reader() {
        let columns = [
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Varchar),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Decimal),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };

        let props: HashMap<String, String> = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8432",
                "username" => "myuser",
                "password" => "123456",
                "database.name" => "mydb",
                "schema.name" => "public",
                "table.name" => "t1"));

        let config =
            serde_json::from_value::<ExternalTableConfig>(serde_json::to_value(props).unwrap())
                .unwrap();
        let schema_table_name = SchemaTableName {
            schema_name: "public".to_owned(),
            table_name: "t1".to_owned(),
        };
        let reader = PostgresExternalTableReader::new(
            config,
            rw_schema,
            vec![0, 1],
            schema_table_name.clone(),
        )
        .await
        .unwrap();

        let offset = reader.current_cdc_offset().await.unwrap();
        println!("CdcOffset: {:?}", offset);

        let start_pk = OwnedRow::new(vec![Some(ScalarImpl::from(3)), Some(ScalarImpl::from("c"))]);
        let stream = reader.snapshot_read(
            schema_table_name,
            Some(start_pk),
            vec!["v1".to_owned(), "v2".to_owned()],
            1000,
        );

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
