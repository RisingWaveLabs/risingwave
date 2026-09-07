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

use core::str::FromStr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::RowSetResult;
use pgwire::pg_server::BoxedError;
use pgwire::types::{Format, FormatIterator, Row};
use pin_project_lite::pin_project;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Field;
use risingwave_common::config::FrontendConfig;
use risingwave_common::id::ObjectId;
use risingwave_common::row::Row as _;
use risingwave_common::types::{
    DataType, Interval, ScalarRefImpl, Timestamptz, write_date_time_tz,
};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::sink::elasticsearch_opensearch::elasticsearch::ES_SINK;
use risingwave_connector::sink::file_sink::fs::FS_SINK;
use risingwave_connector::source::iceberg::ICEBERG_CONNECTOR;
use risingwave_connector::source::{BATCH_POSIX_FS_CONNECTOR, KAFKA_CONNECTOR, POSIX_FS_CONNECTOR};
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_sqlparser::ast::{
    CompatibleFormatEncode, FormatEncodeOptions, ObjectName, Query, Select, SelectItem, SetExpr,
    TableFactor, TableWithJoins,
};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::time::{Duration, sleep};

use crate::catalog::root_catalog::SchemaPath;
use crate::error::ErrorCode::ProtocolError;
use crate::error::{ErrorCode, Result as RwResult, RwError};
use crate::session::{SessionImpl, current};
use crate::{Binder, HashSet, TableCatalog};

pub fn ensure_local_fs_connector_allowed(session: &SessionImpl, connector: &str) -> RwResult<()> {
    let is_local_fs_connector = is_local_fs_connector(connector);

    if !is_local_fs_connector
        || is_local_fs_connector_enabled(session.env().frontend_config(), connector)
    {
        return Ok(());
    }

    Err(RwError::from(ProtocolError(format!(
        "local filesystem connector '{}' is disabled. Set `frontend.unsafe_enable_local_fs_connector = true` in `risingwave.toml` to enable it.",
        connector
    ))))
}

fn is_local_fs_connector_enabled(frontend_config: &FrontendConfig, connector: &str) -> bool {
    !is_local_fs_connector(connector) || frontend_config.unsafe_enable_local_fs_connector
}

fn is_local_fs_connector(connector: &str) -> bool {
    connector.eq_ignore_ascii_case(POSIX_FS_CONNECTOR)
        || connector.eq_ignore_ascii_case(BATCH_POSIX_FS_CONNECTOR)
        || connector.eq_ignore_ascii_case(FS_SINK)
}

pin_project! {
    /// Wrapper struct that converts a stream of DataChunk to a stream of RowSet based on formatting
    /// parameters.
    ///
    /// This is essentially `StreamExt::map(self, move |res| res.map(|chunk| to_pg_rows(chunk,
    /// format)))` but we need a nameable type as part of [`super::PgResponseStream`], but we cannot
    /// name the type of a closure.
    pub struct DataChunkToRowSetAdapter<VS>
    where
        VS: Stream<Item = Result<DataChunk, BoxedError>>,
    {
        #[pin]
        chunk_stream: VS,
        column_types: Vec<DataType>,
        pub formats: Vec<Format>,
        session_data: StaticSessionData,
    }
}

// Static session data frozen at the time of the creation of the stream
pub struct StaticSessionData {
    pub timezone: String,
}

impl<VS> DataChunkToRowSetAdapter<VS>
where
    VS: Stream<Item = Result<DataChunk, BoxedError>>,
{
    pub fn new(
        chunk_stream: VS,
        column_types: Vec<DataType>,
        formats: Vec<Format>,
        session: Arc<SessionImpl>,
    ) -> Self {
        let session_data = StaticSessionData {
            timezone: session.config().timezone(),
        };
        Self {
            chunk_stream,
            column_types,
            formats,
            session_data,
        }
    }
}

impl<VS> Stream for DataChunkToRowSetAdapter<VS>
where
    VS: Stream<Item = Result<DataChunk, BoxedError>>,
{
    type Item = RowSetResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.chunk_stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(chunk) => match chunk {
                Some(chunk_result) => match chunk_result {
                    Ok(chunk) => Poll::Ready(Some(
                        to_pg_rows(this.column_types, chunk, this.formats, this.session_data)
                            .map_err(|err| err.into()),
                    )),
                    Err(err) => Poll::Ready(Some(Err(err))),
                },
                None => Poll::Ready(None),
            },
        }
    }
}

/// Format scalars according to postgres convention.
pub fn pg_value_format(
    data_type: &DataType,
    d: ScalarRefImpl<'_>,
    format: Format,
    session_data: &StaticSessionData,
) -> RwResult<Bytes> {
    // format == false means TEXT format
    // format == true means BINARY format
    match format {
        Format::Text => {
            if *data_type == DataType::Timestamptz {
                Ok(timestamptz_to_string_with_session_data(d, session_data))
            } else {
                Ok(d.text_format(data_type).into())
            }
        }
        Format::Binary => Ok(d
            .binary_format(data_type)
            .context("failed to format binary value")?),
    }
}

fn timestamptz_to_string_with_session_data(
    d: ScalarRefImpl<'_>,
    session_data: &StaticSessionData,
) -> Bytes {
    let tz = d.into_timestamptz();
    let time_zone = Timestamptz::lookup_time_zone(&session_data.timezone).unwrap();
    let instant_local = tz.to_datetime_in_zone(time_zone);
    let mut result_string = BytesMut::new();
    write_date_time_tz(instant_local, &mut result_string).unwrap();
    result_string.into()
}

fn to_pg_rows(
    column_types: &[DataType],
    chunk: DataChunk,
    formats: &[Format],
    session_data: &StaticSessionData,
) -> RwResult<Vec<Row>> {
    assert_eq!(chunk.dimension(), column_types.len());
    if cfg!(debug_assertions) {
        let chunk_data_types = chunk.data_types();
        for (ty1, ty2) in chunk_data_types.iter().zip_eq_fast(column_types) {
            debug_assert!(
                ty1.equals_datatype(ty2),
                "chunk_data_types: {chunk_data_types:?}, column_types: {column_types:?}"
            )
        }
    }

    chunk
        .rows()
        .map(|r| {
            let format_iter = FormatIterator::new(formats, chunk.dimension())
                .map_err(ErrorCode::InternalError)?;
            let row = r
                .iter()
                .zip_eq_fast(column_types)
                .zip_eq_fast(format_iter)
                .map(|((data, t), format)| match data {
                    Some(data) => Some(pg_value_format(t, data, format, session_data)).transpose(),
                    None => Ok(None),
                })
                .try_collect()?;
            Ok(Row::new(row))
        })
        .try_collect()
}

/// Convert from [`Field`] to [`PgFieldDescriptor`].
pub fn to_pg_field(f: &Field) -> PgFieldDescriptor {
    PgFieldDescriptor::new(
        f.name.clone(),
        f.data_type().to_oid(),
        f.data_type().type_len(),
    )
}

#[easy_ext::ext(SourceSchemaCompatExt)]
impl CompatibleFormatEncode {
    /// Convert `self` to [`FormatEncodeOptions`] and warn the user if the syntax is deprecated.
    pub fn into_v2_with_warning(self) -> FormatEncodeOptions {
        match self {
            CompatibleFormatEncode::RowFormat(inner) => {
                // TODO: should be warning
                current::notice_to_user(
                    "RisingWave will stop supporting the syntax \"ROW FORMAT\" in future versions, which will be changed to \"FORMAT ... ENCODE ...\" syntax.",
                );
                inner.into_format_encode_v2()
            }
            CompatibleFormatEncode::V2(inner) => inner,
        }
    }
}

pub fn gen_query_from_table_name(from_name: ObjectName) -> Query {
    let table_factor = TableFactor::Table {
        name: from_name,
        alias: None,
        as_of: None,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard(None)],
        ..Default::default()
    };
    let body = SetExpr::Select(Box::new(select));
    Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }
}

pub fn convert_unix_millis_to_logstore_u64(unix_millis: u64) -> u64 {
    Epoch::from_unix_millis(unix_millis).0
}

pub fn convert_logstore_u64_to_unix_millis(logstore_u64: u64) -> u64 {
    Epoch::from(logstore_u64).as_unix_millis()
}

pub fn convert_interval_to_u64_seconds(interval: &String) -> RwResult<u64> {
    let seconds = (Interval::from_str(interval)
        .map_err(|err| {
            ErrorCode::InternalError(format!(
                "Convert interval to u64 error, please check format, error: {:?}",
                err.to_report_string()
            ))
        })?
        .epoch_in_micros()
        / 1000000) as u64;
    Ok(seconds)
}

pub fn ensure_connection_type_allowed(
    connection_type: PbConnectionType,
    allowed_types: &HashSet<PbConnectionType>,
) -> RwResult<()> {
    if !allowed_types.contains(&connection_type) {
        return Err(RwError::from(ProtocolError(format!(
            "connection type {:?} is not allowed, allowed types: {:?}",
            connection_type, allowed_types
        ))));
    }
    Ok(())
}

fn connection_type_to_connector(connection_type: &PbConnectionType) -> &str {
    match connection_type {
        PbConnectionType::Kafka => KAFKA_CONNECTOR,
        PbConnectionType::Iceberg => ICEBERG_CONNECTOR,
        PbConnectionType::Elasticsearch => ES_SINK,
        _ => unreachable!(),
    }
}

pub fn check_connector_match_connection_type(
    connector: &str,
    connection_type: &PbConnectionType,
) -> RwResult<()> {
    if !connector.eq(connection_type_to_connector(connection_type)) {
        return Err(RwError::from(ProtocolError(format!(
            "connector {} and connection type {:?} are not compatible",
            connector, connection_type
        ))));
    }
    Ok(())
}

pub fn get_table_catalog_by_table_name(
    session: &SessionImpl,
    table_name: &ObjectName,
) -> RwResult<(Arc<TableCatalog>, String)> {
    let db_name = &session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
    let reader = session.env().catalog_reader().read_guard();
    match reader.get_created_table_by_name(db_name, schema_path, &real_table_name) {
        Ok((table, schema_name)) => Ok((table.clone(), schema_name.to_owned())),
        Err(err) => {
            if let Some(table) = session
                .staging_catalog_manager()
                .get_table(&real_table_name)
            {
                // During CREATE TABLE (iceberg engine), the table is only in staging, but we
                // still need a stable schema name for internal sink planning.
                let schema_name = reader
                    .get_schema_by_id(table.database_id, table.schema_id)
                    .map(|schema| schema.name.clone())?;
                Ok((Arc::new(table.clone()), schema_name))
            } else {
                Err(err.into())
            }
        }
    }
}

pub fn reject_internal_table_dependency(
    table: &TableCatalog,
    statement_name: &str,
) -> RwResult<()> {
    if table.is_internal_table() {
        return Err(RwError::from(ErrorCode::InvalidInputSyntax(format!(
            "{statement_name} does not support internal table \"{}\"",
            table.name()
        ))));
    }
    Ok(())
}

pub fn reject_internal_table_dependencies<'a, I>(
    session: &SessionImpl,
    dependent_relations: I,
    statement_name: &str,
) -> RwResult<()>
where
    I: IntoIterator<Item = &'a ObjectId>,
{
    let catalog_reader = session.env().catalog_reader().read_guard();
    for object_id in dependent_relations {
        if let Ok(table) = catalog_reader.get_any_table_by_id(object_id.as_table_id()) {
            reject_internal_table_dependency(table.as_ref(), statement_name)?;
        }
    }
    Ok(())
}

/// Execute an async operation with a notification if it takes too long.
/// This is useful for operations that might be delayed due to high barrier latency.
///
/// The notification timeout duration is controlled by the `slow_ddl_notification_secs` session variable.
///
/// # Arguments
/// * `operation_fut` - The async operation to execute
/// * `session` - The session to send notifications to
/// * `operation_name` - The name of the operation for the notification message (e.g., "DROP TABLE")
///
/// # Example
/// ```ignore
/// execute_with_long_running_notification(
///     catalog_writer.drop_table(source_id, table_id, cascade),
///     &session,
///     "DROP TABLE",
/// ).await?;
/// ```
#[derive(Clone, Copy)]
pub enum LongRunningNotificationAction {
    SuggestRecover,
    DiagnoseBarrierLatency,
    MonitorBackfillJob,
}

impl LongRunningNotificationAction {
    fn build_message(self, operation_name: &str, notify_timeout_secs: u32) -> String {
        match self {
            LongRunningNotificationAction::SuggestRecover => format!(
                "{} has taken more than {} secs, likely due to high barrier latency.\n\
                You may trigger cluster recovery to let {} take effect immediately.\n\
                Run RECOVER in a separate session to trigger recovery.\n\
                See: https://docs.risingwave.com/sql/commands/sql-recover#recover",
                operation_name, notify_timeout_secs, operation_name
            ),
            LongRunningNotificationAction::DiagnoseBarrierLatency => format!(
                "{} has taken more than {} secs, likely due to high barrier latency.\n\
                See: https://docs.risingwave.com/performance/metrics#barrier-monitoring for steps to diagnose high barrier latency.",
                operation_name, notify_timeout_secs
            ),
            LongRunningNotificationAction::MonitorBackfillJob => format!(
                "{} has taken more than {} secs, barrier latency might be high. Please check barrier latency metrics to confirm.\n\
                You can also run SHOW JOBS to track the progress of the job.\n\
                See: https://docs.risingwave.com/performance/metrics#barrier-monitoring and https://docs.risingwave.com/sql/commands/sql-show-jobs",
                operation_name, notify_timeout_secs
            ),
        }
    }
}

pub async fn execute_with_long_running_notification<F, T>(
    operation_fut: F,
    session: &SessionImpl,
    operation_name: &str,
    action: LongRunningNotificationAction,
) -> RwResult<T>
where
    F: std::future::Future<Output = RwResult<T>>,
{
    let notify_timeout_secs = session.config().slow_ddl_notification_secs();

    // If timeout is 0, disable notifications and just execute the operation
    if notify_timeout_secs == 0 {
        return operation_fut.await;
    }

    let notify_fut = sleep(Duration::from_secs(notify_timeout_secs as u64));
    tokio::pin!(operation_fut);

    select! {
        _ = notify_fut => {
            session.notice_to_user(action.build_message(operation_name, notify_timeout_secs));
            operation_fut.await
        }
        result = &mut operation_fut => {
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use postgres_types::{ToSql, Type};
    use risingwave_common::array::*;

    use super::*;

    #[test]
    fn test_to_pg_field() {
        let field = Field::with_name(DataType::Int32, "v1");
        let pg_field = to_pg_field(&field);
        assert_eq!(pg_field.get_name(), "v1");
        assert_eq!(pg_field.get_type_oid(), DataType::Int32.to_oid());
    }

    #[test]
    fn test_to_pg_rows() {
        let chunk = DataChunk::from_pretty(
            "i I f    T
             1 6 6.01 aaa
             2 . .    .
             3 7 7.01 vvv
             4 . .    .  ",
        );
        let static_session = StaticSessionData {
            timezone: "UTC".into(),
        };
        let rows = to_pg_rows(
            &[
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Varchar,
            ],
            chunk,
            &[],
            &static_session,
        );
        let expected: Vec<Vec<Option<Bytes>>> = vec![
            vec![
                Some("1".into()),
                Some("6".into()),
                Some("6.01".into()),
                Some("aaa".into()),
            ],
            vec![Some("2".into()), None, None, None],
            vec![
                Some("3".into()),
                Some("7".into()),
                Some("7.01".into()),
                Some("vvv".into()),
            ],
            vec![Some("4".into()), None, None, None],
        ];
        let vec = rows
            .unwrap()
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }

    #[test]
    fn test_to_pg_rows_mix_format() {
        let chunk = DataChunk::from_pretty(
            "i I f    T
             1 6 6.01 aaa
            ",
        );
        let static_session = StaticSessionData {
            timezone: "UTC".into(),
        };
        let rows = to_pg_rows(
            &[
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Varchar,
            ],
            chunk,
            &[Format::Binary, Format::Binary, Format::Binary, Format::Text],
            &static_session,
        );
        let mut raw_params = vec![BytesMut::new(); 3];
        1_i32.to_sql(&Type::ANY, &mut raw_params[0]).unwrap();
        6_i64.to_sql(&Type::ANY, &mut raw_params[1]).unwrap();
        6.01_f32.to_sql(&Type::ANY, &mut raw_params[2]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let expected: Vec<Vec<Option<Bytes>>> = vec![vec![
            Some(raw_params[0].clone()),
            Some(raw_params[1].clone()),
            Some(raw_params[2].clone()),
            Some("aaa".into()),
        ]];
        let vec = rows
            .unwrap()
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }

    #[test]
    fn test_value_format() {
        use DataType as T;
        use ScalarRefImpl as S;
        let static_session = StaticSessionData {
            timezone: "UTC".into(),
        };

        let f = |t, d, f| pg_value_format(t, d, f, &static_session).unwrap();
        assert_eq!(&f(&T::Float32, S::Float32(1_f32.into()), Format::Text), "1");
        assert_eq!(
            &f(&T::Float32, S::Float32(f32::NAN.into()), Format::Text),
            "NaN"
        );
        assert_eq!(
            &f(&T::Float64, S::Float64(f64::NAN.into()), Format::Text),
            "NaN"
        );
        assert_eq!(
            &f(&T::Float32, S::Float32(f32::INFINITY.into()), Format::Text),
            "Infinity"
        );
        assert_eq!(
            &f(
                &T::Float32,
                S::Float32(f32::NEG_INFINITY.into()),
                Format::Text
            ),
            "-Infinity"
        );
        assert_eq!(
            &f(&T::Float64, S::Float64(f64::INFINITY.into()), Format::Text),
            "Infinity"
        );
        assert_eq!(
            &f(
                &T::Float64,
                S::Float64(f64::NEG_INFINITY.into()),
                Format::Text
            ),
            "-Infinity"
        );
        assert_eq!(&f(&T::Boolean, S::Bool(true), Format::Text), "t");
        assert_eq!(&f(&T::Boolean, S::Bool(false), Format::Text), "f");
        assert_eq!(
            &f(
                &T::Timestamptz,
                S::Timestamptz(Timestamptz::from_micros(-1).unwrap()),
                Format::Text
            ),
            "1969-12-31 23:59:59.999999+00:00"
        );
    }

    #[test]
    fn test_local_fs_connector_config_gate() {
        let frontend_config = FrontendConfig::default();
        let default_enabled = cfg!(debug_assertions);
        assert_eq!(
            is_local_fs_connector_enabled(&frontend_config, POSIX_FS_CONNECTOR),
            default_enabled
        );
        assert_eq!(
            is_local_fs_connector_enabled(&frontend_config, BATCH_POSIX_FS_CONNECTOR),
            default_enabled
        );
        assert_eq!(
            is_local_fs_connector_enabled(&frontend_config, FS_SINK),
            default_enabled
        );
        assert!(is_local_fs_connector_enabled(
            &frontend_config,
            KAFKA_CONNECTOR
        ));

        let disabled_config = FrontendConfig {
            unsafe_enable_local_fs_connector: false,
            ..Default::default()
        };
        assert!(!is_local_fs_connector_enabled(
            &disabled_config,
            POSIX_FS_CONNECTOR
        ));
        assert!(!is_local_fs_connector_enabled(
            &disabled_config,
            BATCH_POSIX_FS_CONNECTOR
        ));
        assert!(!is_local_fs_connector_enabled(&disabled_config, FS_SINK));

        let enabled_config = FrontendConfig {
            unsafe_enable_local_fs_connector: true,
            ..Default::default()
        };
        assert!(is_local_fs_connector_enabled(
            &enabled_config,
            POSIX_FS_CONNECTOR
        ));
        assert!(is_local_fs_connector_enabled(
            &enabled_config,
            BATCH_POSIX_FS_CONNECTOR
        ));
        assert!(is_local_fs_connector_enabled(&enabled_config, FS_SINK));
    }
}
