// Copyright 2022 RisingWave Labs
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

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::Row;
use mysql_async::prelude::*;
use prost::Message;
use risingwave_common::global_jvm::Jvm;
use risingwave_common::id::SourceId;
use risingwave_common::metrics::{LabelGuardedIntGauge, LabelGuardedIntGaugeVec};
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_pb::connector_service::{SourceType, ValidateSourceRequest, ValidateSourceResponse};
use tiberius::Config;
use tokio_postgres::types::PgLsn;

use crate::connector_common::{SslMode, create_pg_client, pg_connection_config_from_properties};
use crate::error::ConnectorResult;
use crate::sink::sqlserver::SqlServerClient;
use crate::source::cdc::external::mysql::build_mysql_connection_pool;
use crate::source::cdc::split::{extract_binlog_file_seq, parse_sql_server_lsn_str};
use crate::source::cdc::{
    CdcProperties, CdcSourceTypeTrait, Citus, DebeziumCdcSplit, Mongodb, Mysql, Oracle, Postgres,
    SqlServer, table_schema_exclude_additional_columns,
};
use crate::source::monitor::metrics::EnumeratorMetrics;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub const DATABASE_SERVERS_KEY: &str = "database.servers";

#[derive(Debug)]
pub struct DebeziumSplitEnumerator<T: CdcSourceTypeTrait> {
    /// The `source_id` in the catalog
    source_id: SourceId,
    worker_node_addrs: Vec<HostAddr>,
    metrics: Arc<EnumeratorMetrics>,
    pg_cdc_upstream_max_lsn: Option<LabelGuardedIntGauge>,
    pg_cdc_confirmed_flush_lsn: Option<LabelGuardedIntGauge>,
    mysql_cdc_binlog_file_seq_min: Option<LabelGuardedIntGauge>,
    mysql_cdc_binlog_file_seq_max: Option<LabelGuardedIntGauge>,
    sqlserver_cdc_upstream_min_lsn: Option<LabelGuardedIntGauge>,
    sqlserver_cdc_upstream_max_lsn: Option<LabelGuardedIntGauge>,
    /// Properties specified in the WITH clause by user for database connection
    properties: Arc<BTreeMap<String, String>>,
    _phantom: PhantomData<T>,
}

fn get_or_create_guarded_int_gauge<'a>(
    metric: &'a mut Option<LabelGuardedIntGauge>,
    metric_vec: &LabelGuardedIntGaugeVec,
    labels: &[String],
) -> &'a LabelGuardedIntGauge {
    metric.get_or_insert_with(|| metric_vec.with_guarded_label_values(labels))
}

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitEnumerator for DebeziumSplitEnumerator<T>
where
    Self: ListCdcSplits<CdcSourceType = T> + CdcMonitor,
{
    type Properties = CdcProperties<T>;
    type Split = DebeziumCdcSplit<T>;

    async fn new(
        props: CdcProperties<T>,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let server_addrs = props
            .properties
            .get(DATABASE_SERVERS_KEY)
            .map(|s| {
                s.split(',')
                    .map(HostAddr::from_str)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        assert_eq!(
            props.get_source_type_pb(),
            SourceType::from(T::source_type())
        );

        let jvm = Jvm::get_or_init()?;
        let source_id = context.info.source_id;

        // Extract fields before moving props
        let source_type_pb = props.get_source_type_pb();

        // Create Arc once and share it
        let properties_arc = Arc::new(props.properties);
        let properties_arc_for_validation = properties_arc.clone();
        let table_schema_for_validation = props.table_schema;

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            execute_with_jni_env(jvm, |env| {
                let validate_source_request = ValidateSourceRequest {
                    source_id: source_id.as_raw_id() as u64,
                    source_type: source_type_pb as _,
                    properties: (*properties_arc_for_validation).clone(),
                    table_schema: Some(table_schema_exclude_additional_columns(
                        &table_schema_for_validation,
                    )),
                    is_source_job: props.is_cdc_source_job,
                    is_backfill_table: props.is_backfill_table,
                };

                let validate_source_request_bytes =
                    env.byte_array_from_slice(&Message::encode_to_vec(&validate_source_request))?;

                let validate_source_response_bytes = call_static_method!(
                    env,
                    {com.risingwave.connector.source.JniSourceValidateHandler},
                    {byte[] validate(byte[] validateSourceRequestBytes)},
                    &validate_source_request_bytes
                )?;

                let validate_source_response: ValidateSourceResponse = Message::decode(
                    risingwave_jni_core::to_guarded_slice(&validate_source_response_bytes, env)?
                        .deref(),
                )?;

                if let Some(error) = validate_source_response.error {
                    return Err(anyhow!(error.error_message).context("source validation failed"));
                }

                Ok(())
            })
        })
        .await
        .context("failed to validate source")??;

        tracing::debug!("validate cdc source properties success");
        Ok(Self {
            source_id,
            worker_node_addrs: server_addrs,
            metrics: context.metrics.clone(),
            pg_cdc_upstream_max_lsn: None,
            pg_cdc_confirmed_flush_lsn: None,
            mysql_cdc_binlog_file_seq_min: None,
            mysql_cdc_binlog_file_seq_max: None,
            sqlserver_cdc_upstream_min_lsn: None,
            sqlserver_cdc_upstream_max_lsn: None,
            properties: properties_arc,
            _phantom: PhantomData,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<DebeziumCdcSplit<T>>> {
        Ok(self.list_cdc_splits())
    }

    async fn on_tick(&mut self) -> ConnectorResult<()> {
        self.monitor_cdc().await
    }
}

impl<T: CdcSourceTypeTrait> DebeziumSplitEnumerator<T> {
    fn sql_server_lsn_to_i64(lsn: &str) -> Option<i64> {
        parse_sql_server_lsn_str(lsn).map(|v| v.min(i64::MAX as u128) as i64)
    }

    fn pg_cdc_lsn_metric_labels(&self, slot_name: &str) -> Vec<String> {
        vec![self.source_id.to_string(), slot_name.to_owned()]
    }

    async fn monitor_postgres_confirmed_flush_lsn(&mut self) -> ConnectorResult<()> {
        // Query upstream LSNs and update metrics.
        let lsns = self.query_postgres_lsns().await.with_context(|| {
            format!(
                "failed to query PostgreSQL LSNs for source {}",
                self.source_id
            )
        })?;
        match lsns {
            Some((confirmed_flush_lsn, upstream_max_lsn, slot_name)) => {
                let labels = self.pg_cdc_lsn_metric_labels(&slot_name);

                get_or_create_guarded_int_gauge(
                    &mut self.pg_cdc_upstream_max_lsn,
                    &self.metrics.pg_cdc_upstream_max_lsn,
                    &labels,
                )
                .set(upstream_max_lsn as i64);

                if let Some(lsn) = confirmed_flush_lsn {
                    get_or_create_guarded_int_gauge(
                        &mut self.pg_cdc_confirmed_flush_lsn,
                        &self.metrics.pg_cdc_confirmed_flush_lsn,
                        &labels,
                    )
                    .set(lsn as i64);
                    tracing::debug!(
                        "Updated confirmed_flush_lsn for source {} slot {}: {}",
                        self.source_id,
                        slot_name,
                        lsn
                    );
                } else {
                    tracing::warn!(
                        "confirmed_flush_lsn is NULL for source {} slot {}",
                        self.source_id,
                        slot_name
                    );
                }
            }
            None => {
                tracing::warn!(
                    "no replication slot was found while querying LSNs for source {}",
                    self.source_id
                );
            }
        };
        Ok(())
    }

    /// Query LSNs from PostgreSQL, return (`confirmed_flush_lsn`, `upstream_max_lsn`, `slot_name`).
    async fn query_postgres_lsns(&self) -> ConnectorResult<Option<(Option<u64>, u64, String)>> {
        let pg_conn = pg_connection_config_from_properties(&self.properties)?;

        let slot_name = self
            .properties
            .get("slot.name")
            .ok_or_else(|| anyhow::anyhow!("missing `slot.name` in CDC properties"))?;

        // No TCP keepalive for CDC enumerator
        let client = create_pg_client(&pg_conn, None)
            .await
            .context("failed to create the PostgreSQL client")?;

        let query = "SELECT confirmed_flush_lsn, pg_current_wal_lsn() \
            FROM pg_replication_slots WHERE slot_name = $1";
        let row = client
            .query_opt(query, &[&slot_name])
            .await
            .context("failed to query PostgreSQL LSNs")?;
        match row {
            Some(row) => {
                let confirmed_flush_lsn: Option<PgLsn> = row.get(0);
                let upstream_max_lsn: PgLsn = row.get(1);
                Ok(Some((
                    confirmed_flush_lsn.map(Into::into),
                    upstream_max_lsn.into(),
                    slot_name.clone(),
                )))
            }
            None => {
                tracing::warn!("no replication slot found with name: {}", slot_name);
                Ok(None)
            }
        }
    }

    /// Query min/max LSNs from SQL Server CDC.
    async fn query_sql_server_lsns(&self) -> ConnectorResult<Option<(String, String)>> {
        let hostname = self
            .properties
            .get("hostname")
            .ok_or_else(|| anyhow!("missing `hostname` in CDC properties"))?;
        let port = self
            .properties
            .get("port")
            .ok_or_else(|| anyhow!("missing `port` in CDC properties"))?
            .parse::<u16>()
            .context("failed to parse `port` as a u16")?;
        let username = self
            .properties
            .get("username")
            .ok_or_else(|| anyhow!("missing `username` in CDC properties"))?;
        let password = self
            .properties
            .get("password")
            .ok_or_else(|| anyhow!("missing `password` in CDC properties"))?;
        let database = self
            .properties
            .get("database.name")
            .ok_or_else(|| anyhow!("missing `database.name` in CDC properties"))?;

        let mut config = Config::new();
        config.host(hostname);
        config.port(port);
        config.database(database);
        config.authentication(tiberius::AuthMethod::sql_server(username, password));
        config.trust_cert();

        let mut client = SqlServerClient::new_with_config(config).await?;
        let row = client
            .inner_client
            .simple_query(
                "SELECT \
                    sys.fn_cdc_get_max_lsn() AS max_lsn, \
                    (SELECT MIN(sys.fn_cdc_get_min_lsn(capture_instance)) FROM cdc.change_tables) AS min_lsn"
                    .to_owned(),
            )
            .await?
            .into_row()
            .await?
            .ok_or_else(|| anyhow!("No result returned when querying SQL Server max/min LSN"))?;

        let lsn_bytes_to_hex = |bytes: &[u8]| -> ConnectorResult<String> {
            if bytes.len() != 10 {
                return Err(anyhow!(
                    "SQL Server LSN should be 10 bytes, got {} bytes",
                    bytes.len()
                )
                .into());
            }
            let mut hex_string = String::with_capacity(22);
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
            Ok(hex_string)
        };

        let max_lsn = row
            .try_get::<&[u8], usize>(0)?
            .map(lsn_bytes_to_hex)
            .transpose()?
            .ok_or_else(|| anyhow!("SQL Server max_lsn is NULL"))?;
        let min_lsn = row
            .try_get::<&[u8], usize>(1)?
            .map(lsn_bytes_to_hex)
            .transpose()?
            .ok_or_else(|| anyhow!("SQL Server min_lsn is NULL"))?;

        Ok(Some((min_lsn, max_lsn)))
    }

    async fn monitor_sql_server_lsns(&mut self) -> ConnectorResult<()> {
        let lsns = self.query_sql_server_lsns().await.with_context(|| {
            format!(
                "failed to query SQL Server LSNs for source {}",
                self.source_id
            )
        })?;
        if let Some((min_lsn, max_lsn)) = lsns {
            let labels = vec![self.source_id.to_string()];

            if let Some(value) = Self::sql_server_lsn_to_i64(&min_lsn) {
                get_or_create_guarded_int_gauge(
                    &mut self.sqlserver_cdc_upstream_min_lsn,
                    &self.metrics.sqlserver_cdc_upstream_min_lsn,
                    &labels,
                )
                .set(value);
            }
            if let Some(value) = Self::sql_server_lsn_to_i64(&max_lsn) {
                get_or_create_guarded_int_gauge(
                    &mut self.sqlserver_cdc_upstream_max_lsn,
                    &self.metrics.sqlserver_cdc_upstream_max_lsn,
                    &labels,
                )
                .set(value);
            }
        }

        Ok(())
    }
}

pub trait ListCdcSplits {
    type CdcSourceType: CdcSourceTypeTrait;
    /// Generates a single split for shared source.
    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>>;
}

/// Trait for CDC-specific monitoring behavior
#[async_trait]
pub trait CdcMonitor {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()>;
}

#[async_trait]
impl<T: CdcSourceTypeTrait> CdcMonitor for DebeziumSplitEnumerator<T> {
    default async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

impl DebeziumSplitEnumerator<Mysql> {
    async fn monitor_mysql_binlog_files(&mut self) -> ConnectorResult<()> {
        // Get hostname and port for metrics labels
        let hostname = self
            .properties
            .get("hostname")
            .map(|s| s.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("missing required property 'hostname' for MySQL CDC source")
            })?;
        let port = self
            .properties
            .get("port")
            .map(|s| s.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("missing required property 'port' for MySQL CDC source")
            })?;

        // Query binlog files and update metrics
        let binlog_files = self.query_binlog_files().await.with_context(|| {
            format!(
                "failed to query binlog files for MySQL CDC source {} ({}:{})",
                self.source_id, hostname, port
            )
        })?;
        if let Some((oldest_file, oldest_size)) = binlog_files.first()
            && let Some(seq) = extract_binlog_file_seq(oldest_file)
        {
            let labels = vec![
                self.source_id.to_string(),
                hostname.to_owned(),
                port.to_owned(),
            ];
            get_or_create_guarded_int_gauge(
                &mut self.mysql_cdc_binlog_file_seq_min,
                &self.metrics.mysql_cdc_binlog_file_seq_min,
                &labels,
            )
            .set(seq as i64);
            tracing::debug!(
                "MySQL CDC source {} ({}:{}): oldest binlog = {}, seq = {}, size = {}",
                self.source_id,
                hostname,
                port,
                oldest_file,
                seq,
                oldest_size
            );
        }
        if let Some((newest_file, newest_size)) = binlog_files.last()
            && let Some(seq) = extract_binlog_file_seq(newest_file)
        {
            let labels = vec![
                self.source_id.to_string(),
                hostname.to_owned(),
                port.to_owned(),
            ];
            get_or_create_guarded_int_gauge(
                &mut self.mysql_cdc_binlog_file_seq_max,
                &self.metrics.mysql_cdc_binlog_file_seq_max,
                &labels,
            )
            .set(seq as i64);
            tracing::debug!(
                "MySQL CDC source {} ({}:{}): newest binlog = {}, seq = {}, size = {}",
                self.source_id,
                hostname,
                port,
                newest_file,
                seq,
                newest_size
            );
        }
        tracing::debug!(
            "MySQL CDC source {} ({}:{}): total {} binlog files",
            self.source_id,
            hostname,
            port,
            binlog_files.len()
        );
        Ok(())
    }

    /// Query binlog files from MySQL, returns Vec<(filename, size)>
    async fn query_binlog_files(&self) -> ConnectorResult<Vec<(String, u64)>> {
        // Extract connection parameters from CDC properties
        let hostname = self
            .properties
            .get("hostname")
            .ok_or_else(|| anyhow::anyhow!("missing `hostname` in CDC properties"))?;
        let port = self
            .properties
            .get("port")
            .ok_or_else(|| anyhow::anyhow!("missing `port` in CDC properties"))?
            .parse::<u16>()
            .context("failed to parse `port` as a u16")?;
        let username = self
            .properties
            .get("username")
            .ok_or_else(|| anyhow::anyhow!("missing `username` in CDC properties"))?;
        let password = self
            .properties
            .get("password")
            .ok_or_else(|| anyhow::anyhow!("missing `password` in CDC properties"))?;
        let database = self
            .properties
            .get("database.name")
            .ok_or_else(|| anyhow::anyhow!("missing `database.name` in CDC properties"))?;

        // Get SSL mode configuration (default to Disabled if not specified)
        let ssl_mode = self
            .properties
            .get("ssl.mode")
            .and_then(|s| s.parse().ok())
            .unwrap_or(SslMode::Preferred);

        // Build MySQL connection pool with proper SSL configuration
        let pool =
            build_mysql_connection_pool(hostname, port, username, password, database, ssl_mode);
        let mut conn = pool
            .get_conn()
            .await
            .context("failed to connect to MySQL")?;

        // Query binlog files using SHOW BINARY LOGS.
        // MySQL 8.0+ may return 3 columns (Log_name, File_size, Encrypted), while some variants
        // only return the first 2. Decode the row manually so we don't panic on column-count
        // differences.
        let rows: Vec<Row> = conn
            .query("SHOW BINARY LOGS")
            .await
            .context("failed to execute `SHOW BINARY LOGS`")?;
        let query_result = rows
            .into_iter()
            .map(|mut row| -> ConnectorResult<(String, u64)> {
                let log_name = row
                    .take_opt::<String, _>(0)
                    .transpose()
                    .context("`SHOW BINARY LOGS`: failed to decode `Log_name`")?
                    .ok_or_else(|| anyhow!("`SHOW BINARY LOGS`: missing `Log_name` column"))?;
                let file_size = row
                    .take_opt::<u64, _>(1)
                    .transpose()
                    .context("`SHOW BINARY LOGS`: failed to decode `File_size`")?
                    .ok_or_else(|| anyhow!("`SHOW BINARY LOGS`: missing `File_size` column"))?;
                Ok((log_name, file_size))
            })
            .collect::<ConnectorResult<Vec<_>>>()?;

        drop(conn);
        pool.disconnect().await.ok();

        Ok(query_result)
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Mysql> {
    type CdcSourceType = Mysql;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

#[async_trait]
impl CdcMonitor for DebeziumSplitEnumerator<Mysql> {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        // For MySQL CDC, query the upstream MySQL binlog files and monitor them.
        self.monitor_mysql_binlog_files().await?;
        Ok(())
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Postgres> {
    type CdcSourceType = Postgres;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

#[async_trait]
impl CdcMonitor for DebeziumSplitEnumerator<Postgres> {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        // For PostgreSQL CDC, query the upstream Postgres confirmed flush lsn and monitor it.
        self.monitor_postgres_confirmed_flush_lsn().await?;
        Ok(())
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Citus> {
    type CdcSourceType = Citus;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        self.worker_node_addrs
            .iter()
            .enumerate()
            .map(|(id, addr)| {
                DebeziumCdcSplit::<Self::CdcSourceType>::new(
                    id as u32,
                    None,
                    Some(addr.to_string()),
                )
            })
            .collect_vec()
    }
}
impl ListCdcSplits for DebeziumSplitEnumerator<Mongodb> {
    type CdcSourceType = Mongodb;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<SqlServer> {
    type CdcSourceType = SqlServer;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Oracle> {
    type CdcSourceType = Oracle;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id.as_raw_id(),
            None,
            None,
        )]
    }
}

#[async_trait]
impl CdcMonitor for DebeziumSplitEnumerator<SqlServer> {
    async fn monitor_cdc(&mut self) -> ConnectorResult<()> {
        self.monitor_sql_server_lsns().await
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;
    use risingwave_common::metrics::LabelGuardedIntGaugeVec;

    use super::get_or_create_guarded_int_gauge;

    #[test]
    fn cached_guarded_metric_survives_repeated_collections() {
        let metric_vec = LabelGuardedIntGaugeVec::test_int_gauge_vec::<2>();
        let labels = vec!["source_id".to_owned(), "slot_name".to_owned()];
        let mut metric = None;

        get_or_create_guarded_int_gauge(&mut metric, &metric_vec, &labels).set(1);

        assert_eq!(1, metric_vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(1, metric_vec.collect().pop().unwrap().get_metric().len());

        drop(metric);
        assert_eq!(1, metric_vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(0, metric_vec.collect().pop().unwrap().get_metric().len());
    }
}
