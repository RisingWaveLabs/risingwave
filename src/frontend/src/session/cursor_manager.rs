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

use core::mem;
use core::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::{Format, Row};
use risingwave_batch::task::{ShutdownSender, ShutdownToken};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::error::BoxedError;
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::{DataType, ScalarImpl, StructType, StructValue};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockVersionId;

use super::SessionImpl;
use crate::catalog::TableId;
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::error::{ErrorCode, Result};
use crate::expr::{ExprType, FunctionCall, InputRef, Literal};
use crate::handler::HandlerArgs;
use crate::handler::declare_cursor::create_chunk_stream_for_cursor;
use crate::handler::query::{RwBatchQueryPlanResult, gen_batch_plan_fragmenter};
use crate::handler::util::{
    DataChunkToRowSetAdapter, StaticSessionData, convert_logstore_u64_to_unix_millis,
    pg_value_format, to_pg_field,
};
use crate::monitor::{CursorMetrics, PeriodicCursorMetrics};
use crate::optimizer::PlanRoot;
use crate::optimizer::plan_node::{BatchFilter, BatchLogSeqScan, BatchSeqScan, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::{
    DistributedQueryStream, LocalQueryStream, QueryManager, ReadSnapshot, SchedulerError,
};
use crate::utils::Condition;
use crate::{OptimizerContext, OptimizerContextRef, PgResponseStream, TableCatalog};

/// The local or distributed query stream owned by a cursor.
enum CursorQueryStreamInner {
    /// A query executed by the frontend's local batch executor.
    Local {
        /// Data chunks produced by the local batch executor.
        stream: LocalQueryStream,
        /// Sender used to cancel the local executor if this stream is dropped before EOF.
        shutdown_tx: ShutdownSender,
    },
    /// A query scheduled through the distributed query manager.
    ///
    /// The cursor takes ownership of the query lifecycle only after scheduling finishes and
    /// returns a [`DistributedQueryStream`]. From that point, the cursor ensures the query is
    /// cleaned up. While scheduling is still awaiting the stream, the query may already be
    /// registered but not yet owned by the cursor. `DistributedQueryRegistrationAtomicGuard`
    /// makes this intermediate state cancellation-safe and ensures cleanup unless the completed
    /// stream is handed off to the cursor.
    Distributed {
        /// Data chunks produced by the distributed query execution.
        stream: DistributedQueryStream,
        /// Manager used to cancel the distributed query if this stream is dropped before EOF.
        query_manager: QueryManager,
        /// Identifier passed to [`QueryManager`] when cancelling the distributed query.
        query_id: QueryId,
    },
}

/// A cursor-owned query stream that cancels unfinished execution when dropped.
pub struct CursorQueryStream {
    /// Concrete local or distributed query stream and its cancellation resources.
    inner: CursorQueryStreamInner,
    /// Whether the concrete stream has reached EOF and therefore no longer needs cancellation.
    finished: bool,
}

impl CursorQueryStream {
    /// Wraps a local query stream and the sender that cancels its execution.
    ///
    /// The local executor observes the token paired with `shutdown_tx`, allowing the cursor to
    /// stop unfinished execution when it is closed.
    pub fn local(stream: LocalQueryStream, shutdown_tx: ShutdownSender) -> Self {
        Self {
            inner: CursorQueryStreamInner::Local {
                stream,
                shutdown_tx,
            },
            finished: false,
        }
    }

    /// Wraps a distributed query stream and remembers some metadata needed to cancel it when the
    /// cursor closes.
    pub fn distributed(stream: DistributedQueryStream, query_manager: QueryManager) -> Self {
        let query_id = stream.query_id().clone();
        Self {
            inner: CursorQueryStreamInner::Distributed {
                stream,
                query_manager,
                query_id,
            },
            finished: false,
        }
    }
}

impl Stream for CursorQueryStream {
    type Item = std::result::Result<DataChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = match &mut this.inner {
            CursorQueryStreamInner::Local { stream, .. } => stream.poll_next_unpin(cx),
            CursorQueryStreamInner::Distributed { stream, .. } => stream.poll_next_unpin(cx),
        };
        if matches!(&result, Poll::Ready(None)) {
            this.finished = true;
        }
        result
    }
}

impl Drop for CursorQueryStream {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        match &self.inner {
            CursorQueryStreamInner::Local { shutdown_tx, .. } => {
                shutdown_tx.cancel();
            }
            CursorQueryStreamInner::Distributed {
                query_manager,
                query_id,
                ..
            } => query_manager.cancel_cursor_query(query_id, "cursor closed"),
        }
    }
}

pub enum CursorDataChunkStream {
    LocalDataChunk(Option<CursorQueryStream>),
    DistributedDataChunk(Option<CursorQueryStream>),
    PgResponse(PgResponseStream),
}

pub struct FetchCursorCancelHandle {
    cancel_tx: ShutdownSender,
    cancel_rx: ShutdownToken,
}

impl FetchCursorCancelHandle {
    pub fn new() -> Self {
        let (cancel_tx, cancel_rx) = ShutdownToken::new();
        Self {
            cancel_tx,
            cancel_rx,
        }
    }

    fn register(&self, session: &SessionImpl) {
        session.set_cancel_query_flag(self.cancel_tx.clone());
    }

    async fn cancelled(&mut self) {
        self.cancel_rx.cancelled().await;
    }

    fn is_cancelled(&self) -> bool {
        self.cancel_rx.is_cancelled()
    }
}

impl CursorDataChunkStream {
    pub fn init_row_stream(
        &mut self,
        fields: &Vec<Field>,
        formats: &Vec<Format>,
        session: Arc<SessionImpl>,
    ) {
        let columns_type = fields.iter().map(|f| f.data_type()).collect();
        match self {
            CursorDataChunkStream::LocalDataChunk(data_chunk)
            | CursorDataChunkStream::DistributedDataChunk(data_chunk) => {
                let data_chunk = mem::take(data_chunk).unwrap();
                let row_stream = PgResponseStream::Rows(
                    DataChunkToRowSetAdapter::new(
                        data_chunk,
                        columns_type,
                        formats.clone(),
                        session,
                    )
                    .boxed(),
                );
                *self = CursorDataChunkStream::PgResponse(row_stream);
            }
            _ => {}
        }
    }

    pub async fn next(&mut self) -> Result<Option<std::result::Result<Vec<Row>, BoxedError>>> {
        match self {
            CursorDataChunkStream::PgResponse(row_stream) => Ok(row_stream.next().await),
            _ => Err(ErrorCode::InternalError(
                "Only 'CursorDataChunkStream' can call next and return rows".to_owned(),
            )
            .into()),
        }
    }
}

pub enum Cursor {
    Subscription(SubscriptionCursor),
    Query(QueryCursor),
}
impl Cursor {
    pub async fn next(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        match self {
            Cursor::Subscription(cursor) => cursor
                .next(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
                .inspect_err(|_| cursor.cursor_metrics.subscription_cursor_error_count.inc()),
            Cursor::Query(cursor) => {
                cursor
                    .next(count, formats, handler_args, timeout_seconds)
                    .await
            }
        }
    }

    pub fn get_fields(&mut self) -> Vec<Field> {
        match self {
            Cursor::Subscription(cursor) => cursor.fields_manager.get_output_fields(),
            Cursor::Query(cursor) => cursor.fields.clone(),
        }
    }
}

pub struct QueryCursor {
    chunk_stream: CursorDataChunkStream,
    fields: Vec<Field>,
    remaining_rows: VecDeque<Row>,
}

impl QueryCursor {
    pub fn new(chunk_stream: CursorDataChunkStream, fields: Vec<Field>) -> Result<Self> {
        Ok(Self {
            chunk_stream,
            fields,
            remaining_rows: VecDeque::<Row>::new(),
        })
    }

    pub async fn next_once(&mut self) -> Result<Option<Row>> {
        while self.remaining_rows.is_empty() {
            let rows = self.chunk_stream.next().await?;
            let rows = match rows {
                None => return Ok(None),
                Some(row) => row?,
            };
            self.remaining_rows = rows.into_iter().collect();
        }
        let row = self.remaining_rows.pop_front().unwrap();
        Ok(Some(row))
    }

    pub async fn next(
        &mut self,
        count: u32,
        formats: &Vec<Format>,
        handler_args: HandlerArgs,
        timeout_seconds: Option<u64>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        // `FETCH NEXT` is equivalent to `FETCH 1`.
        // min with 100 to avoid allocating too many memory at once.
        let timeout_instant = timeout_seconds.map(|s| Instant::now() + Duration::from_secs(s));
        let session = handler_args.session;
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let mut cur = 0;
        let desc = self.fields.iter().map(to_pg_field).collect();
        self.chunk_stream
            .init_row_stream(&self.fields, formats, session);
        while cur < count
            && let Some(row) = self.next_once().await?
        {
            cur += 1;
            ans.push(row);
            if let Some(timeout_instant) = timeout_instant
                && Instant::now() > timeout_instant
            {
                break;
            }
        }
        Ok((ans, desc))
    }
}

enum State {
    InitLogStoreQuery {
        // The rw_timestamp used to initiate the query to read from subscription logstore.
        seek_timestamp: u64,

        // If specified, the expected_timestamp must be an exact match for the next rw_timestamp.
        expected_timestamp: Option<u64>,
    },
    Fetch {
        // Whether the query is reading from snapshot
        // true: read from the upstream table snapshot
        // false: read from subscription logstore
        from_snapshot: bool,

        // The rw_timestamp used to initiate the query to read from subscription logstore.
        rw_timestamp: u64,

        // The row stream to from the batch query read.
        // It is returned from the batch execution.
        chunk_stream: CursorDataChunkStream,

        // A cache to store the remaining rows from the row stream.
        remaining_rows: VecDeque<Row>,

        expected_timestamp: Option<u64>,

        init_query_timer: Instant,
    },
    Invalid,
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            State::InitLogStoreQuery {
                seek_timestamp,
                expected_timestamp,
            } => write!(
                f,
                "InitLogStoreQuery {{ seek_timestamp: {}, expected_timestamp: {:?} }}",
                seek_timestamp, expected_timestamp
            ),
            State::Fetch {
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                remaining_rows,
                init_query_timer,
                ..
            } => write!(
                f,
                "Fetch {{ from_snapshot: {}, rw_timestamp: {}, expected_timestamp: {:?}, cached rows: {}, query init at {}ms before }}",
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                remaining_rows.len(),
                init_query_timer.elapsed().as_millis()
            ),
            State::Invalid => write!(f, "Invalid"),
        }
    }
}

struct FieldsManager {
    columns_catalog: Vec<ColumnCatalog>,
    // All row fields, including hidden pk, op, rw_timestamp and all non-hidden columns in the upstream table.
    row_fields: Vec<Field>,
    // Row output column indices based on `row_fields`.
    row_output_col_indices: Vec<usize>,
    // Row pk indices based on `row_fields`.
    row_pk_indices: Vec<usize>,
    // Stream chunk row indices based on `row_fields`.
    stream_chunk_row_indices: Vec<usize>,
    // The op index based on `row_fields`.
    op_index: usize,
}

impl FieldsManager {
    // pub const OP_FIELD: Field = Field::with_name(DataType::Varchar, "op".to_owned());
    // pub const RW_TIMESTAMP_FIELD: Field = Field::with_name(DataType::Int64, "rw_timestamp".to_owned());

    pub fn new(catalog: &TableCatalog) -> Self {
        let mut row_fields = Vec::new();
        let mut row_output_col_indices = Vec::new();
        let mut row_pk_indices = Vec::new();
        let mut stream_chunk_row_indices = Vec::new();
        let mut output_idx = 0_usize;
        let pk_set: HashSet<usize> = catalog
            .pk
            .iter()
            .map(|col_order| col_order.column_index)
            .collect();

        for (index, v) in catalog.columns.iter().enumerate() {
            if pk_set.contains(&index) {
                row_pk_indices.push(output_idx);
                stream_chunk_row_indices.push(output_idx);
                row_fields.push(Field::with_name(v.data_type().clone(), v.name()));
                if !v.is_hidden {
                    row_output_col_indices.push(output_idx);
                }
                output_idx += 1;
            } else if !v.is_hidden {
                row_output_col_indices.push(output_idx);
                stream_chunk_row_indices.push(output_idx);
                row_fields.push(Field::with_name(v.data_type().clone(), v.name()));
                output_idx += 1;
            }
        }

        row_fields.push(Field::with_name(DataType::Varchar, "op".to_owned()));
        row_output_col_indices.push(output_idx);
        let op_index = output_idx;
        output_idx += 1;
        row_fields.push(Field::with_name(DataType::Int64, "rw_timestamp".to_owned()));
        row_output_col_indices.push(output_idx);
        Self {
            columns_catalog: catalog.columns.clone(),
            row_fields,
            row_output_col_indices,
            row_pk_indices,
            stream_chunk_row_indices,
            op_index,
        }
    }

    pub fn try_refill_fields(&mut self, catalog: &TableCatalog) -> bool {
        if self.columns_catalog.ne(&catalog.columns) {
            *self = Self::new(catalog);
            true
        } else {
            false
        }
    }

    pub fn process_output_desc_row(&self, mut rows: Vec<Row>) -> (Vec<Row>, Option<Row>) {
        let last_row = rows.last_mut().map(|row| {
            let mut row = row.clone();
            row.project(&self.row_pk_indices)
        });
        let rows = rows
            .iter_mut()
            .map(|row| row.project(&self.row_output_col_indices))
            .collect();
        (rows, last_row)
    }

    pub fn get_output_fields(&self) -> Vec<Field> {
        self.row_output_col_indices
            .iter()
            .map(|&idx| self.row_fields[idx].clone())
            .collect()
    }

    // In the beginning (declare cur), we will give it an empty formats,
    // this formats is not a real, when we fetch, We fill it with the formats returned from the pg client.
    pub fn get_row_stream_fields_and_formats(
        &self,
        formats: &Vec<Format>,
        from_snapshot: bool,
    ) -> (Vec<Field>, Vec<Format>) {
        let mut fields = Vec::new();
        let need_format = !(formats.is_empty() || formats.len() == 1);
        let mut new_formats = formats.clone();
        let stream_chunk_row_indices_iter = if from_snapshot {
            self.stream_chunk_row_indices.iter().chain(None)
        } else {
            self.stream_chunk_row_indices
                .iter()
                .chain(Some(&self.op_index))
        };
        for index in stream_chunk_row_indices_iter {
            fields.push(self.row_fields[*index].clone());
            if need_format && !self.row_output_col_indices.contains(index) {
                new_formats.insert(*index, Format::Text);
            }
        }
        (fields, new_formats)
    }
}

pub struct SubscriptionCursor {
    cursor_name: String,
    subscription: Arc<SubscriptionCatalog>,
    dependent_table_id: TableId,
    cursor_need_drop_time: Instant,
    state: State,
    // fields will be set in the table's catalog when the cursor is created,
    // and will be reset each time it is created chunk_stream, this is to avoid changes in the catalog due to alter.
    fields_manager: FieldsManager,
    cursor_metrics: Arc<CursorMetrics>,
    last_fetch: Instant,
    seek_pk_row: Option<Row>,
}

impl SubscriptionCursor {
    pub async fn new(
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_args: &HandlerArgs,
        cursor_metrics: Arc<CursorMetrics>,
    ) -> Result<Self> {
        let (state, fields_manager) = if let Some(start_timestamp) = start_timestamp {
            let table_catalog = handler_args.session.get_table_by_id(dependent_table_id)?;
            (
                State::InitLogStoreQuery {
                    seek_timestamp: start_timestamp,
                    expected_timestamp: None,
                },
                FieldsManager::new(&table_catalog),
            )
        } else {
            // The query stream needs to initiated on cursor creation to make sure
            // future fetch on the cursor starts from the snapshot when the cursor is declared.
            //
            // TODO: is this the right behavior? Should we delay the query stream initiation till the first fetch?
            let (chunk_stream, init_query_timer, table_catalog) =
                Self::initiate_query(None, dependent_table_id, handler_args.clone(), None).await?;
            let pinned_epoch = match handler_args.session.get_pinned_snapshot().ok_or_else(
                || ErrorCode::InternalError("Fetch Cursor can't find snapshot epoch".to_owned()),
            )? {
                ReadSnapshot::FrontendPinned { snapshot, .. } => {
                    snapshot
                        .version()
                        .state_table_info
                        .info()
                        .get(&dependent_table_id)
                        .ok_or_else(|| {
                            anyhow!("dependent_table_id {dependent_table_id} not exists")
                        })?
                        .committed_epoch
                }
                ReadSnapshot::Other(_) => {
                    return Err(ErrorCode::InternalError("Fetch Cursor can't start from specified query epoch. May run `set query_epoch = 0;`".to_owned()).into());
                }
                ReadSnapshot::ReadUncommitted => {
                    return Err(ErrorCode::InternalError(
                        "Fetch Cursor don't support read uncommitted".to_owned(),
                    )
                    .into());
                }
            };
            let start_timestamp = pinned_epoch;

            (
                State::Fetch {
                    from_snapshot: true,
                    rw_timestamp: start_timestamp,
                    chunk_stream,
                    remaining_rows: VecDeque::new(),
                    expected_timestamp: None,
                    init_query_timer,
                },
                FieldsManager::new(&table_catalog),
            )
        };

        let cursor_need_drop_time =
            Instant::now() + Duration::from_secs(subscription.retention_seconds);
        Ok(Self {
            cursor_name,
            subscription,
            dependent_table_id,
            cursor_need_drop_time,
            state,
            fields_manager,
            cursor_metrics,
            last_fetch: Instant::now(),
            seek_pk_row: None,
        })
    }

    async fn next_row(
        &mut self,
        handler_args: &HandlerArgs,
        formats: &Vec<Format>,
    ) -> Result<Option<Row>> {
        loop {
            match &mut self.state {
                State::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                } => {
                    let from_snapshot = false;

                    // Initiate a new batch query to continue fetching
                    match Self::get_next_rw_timestamp(
                        *seek_timestamp,
                        self.dependent_table_id,
                        *expected_timestamp,
                        handler_args.clone(),
                        &self.subscription,
                    )
                    .await
                    {
                        Ok((Some(rw_timestamp), expected_timestamp)) => {
                            let (mut chunk_stream, init_query_timer, catalog) =
                                Self::initiate_query(
                                    Some(rw_timestamp),
                                    self.dependent_table_id,
                                    handler_args.clone(),
                                    None,
                                )
                                .await?;
                            let table_schema_changed =
                                self.fields_manager.try_refill_fields(&catalog);
                            let (fields, formats) = self
                                .fields_manager
                                .get_row_stream_fields_and_formats(formats, from_snapshot);
                            chunk_stream.init_row_stream(
                                &fields,
                                &formats,
                                handler_args.session.clone(),
                            );

                            self.cursor_need_drop_time = Instant::now()
                                + Duration::from_secs(self.subscription.retention_seconds);
                            let mut remaining_rows = VecDeque::new();
                            Self::try_refill_remaining_rows(&mut chunk_stream, &mut remaining_rows)
                                .await?;
                            // Transition to the Fetch state
                            self.state = State::Fetch {
                                from_snapshot,
                                rw_timestamp,
                                chunk_stream,
                                remaining_rows,
                                expected_timestamp,
                                init_query_timer,
                            };
                            if table_schema_changed {
                                return Ok(None);
                            }
                        }
                        Ok((None, _)) => return Ok(None),
                        Err(e) => {
                            self.state = State::Invalid;
                            return Err(e);
                        }
                    }
                }
                State::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    chunk_stream,
                    remaining_rows,
                    expected_timestamp,
                    init_query_timer,
                } => {
                    let session_data = StaticSessionData {
                        timezone: handler_args.session.config().timezone(),
                    };
                    let from_snapshot = *from_snapshot;
                    let rw_timestamp = *rw_timestamp;

                    // Try refill remaining rows
                    Self::try_refill_remaining_rows(chunk_stream, remaining_rows).await?;

                    if let Some(row) = remaining_rows.pop_front() {
                        // 1. Fetch the next row
                        if from_snapshot {
                            return Ok(Some(Self::build_row(
                                row.take(),
                                None,
                                formats,
                                &session_data,
                            )?));
                        } else {
                            return Ok(Some(Self::build_row(
                                row.take(),
                                Some(rw_timestamp),
                                formats,
                                &session_data,
                            )?));
                        }
                    } else {
                        self.cursor_metrics
                            .subscription_cursor_query_duration
                            .with_label_values(&[&self.subscription.name])
                            .observe(init_query_timer.elapsed().as_millis() as _);
                        // 2. Reach EOF for the current query.
                        if let Some(expected_timestamp) = expected_timestamp {
                            self.state = State::InitLogStoreQuery {
                                seek_timestamp: *expected_timestamp,
                                expected_timestamp: Some(*expected_timestamp),
                            };
                        } else {
                            self.state = State::InitLogStoreQuery {
                                seek_timestamp: rw_timestamp + 1,
                                expected_timestamp: None,
                            };
                        }
                    }
                }
                State::Invalid => {
                    // TODO: auto close invalid cursor?
                    return Err(ErrorCode::InternalError(
                        "Cursor is in invalid state. Please close and re-create the cursor."
                            .to_owned(),
                    )
                    .into());
                }
            }
        }
    }

    pub async fn next(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        let timeout_instant = timeout_seconds.map(|s| Instant::now() + Duration::from_secs(s));
        if Instant::now() > self.cursor_need_drop_time {
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_owned(),
            )
            .into());
        }

        let session = &handler_args.session;
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let mut cur = 0;
        if let State::Fetch {
            from_snapshot,
            chunk_stream,
            ..
        } = &mut self.state
        {
            let (fields, fotmats) = self
                .fields_manager
                .get_row_stream_fields_and_formats(formats, *from_snapshot);
            chunk_stream.init_row_stream(&fields, &fotmats, session.clone());
        }
        while cur < count {
            if cancel_handle.is_cancelled() {
                return Err(SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into());
            }
            let fetch_cursor_timer = Instant::now();
            let row = self.next_row(&handler_args, formats).await?;
            self.cursor_metrics
                .subscription_cursor_fetch_duration
                .with_label_values(&[&self.subscription.name])
                .observe(fetch_cursor_timer.elapsed().as_millis() as _);
            match row {
                Some(row) => {
                    cur += 1;
                    ans.push(row);
                }
                None => {
                    let timeout_seconds = timeout_seconds.unwrap_or(0);
                    if cur > 0 || timeout_seconds == 0 {
                        break;
                    }
                    let State::InitLogStoreQuery { seek_timestamp, .. } = &self.state else {
                        // Triggered when previous next_row returns None while self.state is State::Fetch.
                        continue;
                    };
                    // This is the only point where subscription cursor fetch waits without an
                    // inner query. Register the FETCH-level cancel token so CancelRequest can
                    // interrupt this wait. The token also marks the whole FETCH as cancelled, so
                    // we won't start another inner query after a cancellation.
                    cancel_handle.register(session);
                    let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds));
                    tokio::pin!(timeout);
                    tokio::select! {
                        biased;
                        _ = cancel_handle.cancelled() => {
                            return Err(SchedulerError::QueryCancelled(
                                "Cancelled by user".to_owned(),
                            )
                            .into());
                        }
                        result = session
                            .env
                            .hummock_snapshot_manager()
                            .wait_table_change_log_notification(
                                self.dependent_table_id,
                                *seek_timestamp,
                            ) => {
                            result?;
                        }
                        _ = &mut timeout => {
                            tracing::debug!("Cursor wait next epoch timeout");
                            break;
                        }
                    }
                    if cancel_handle.is_cancelled() {
                        return Err(
                            SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into(),
                        );
                    }
                }
            }
            // Timeout, return with current value
            if let Some(timeout_instant) = timeout_instant
                && Instant::now() > timeout_instant
            {
                break;
            }
        }
        self.last_fetch = Instant::now();
        let (rows, seek_pk_row) = self.fields_manager.process_output_desc_row(ans);
        if let Some(seek_pk_row) = seek_pk_row {
            self.seek_pk_row = Some(seek_pk_row);
        }
        let desc = self
            .fields_manager
            .get_output_fields()
            .iter()
            .map(to_pg_field)
            .collect();

        Ok((rows, desc))
    }

    async fn get_next_rw_timestamp(
        seek_timestamp: u64,
        table_id: TableId,
        expected_timestamp: Option<u64>,
        handler_args: HandlerArgs,
        dependent_subscription: &SubscriptionCatalog,
    ) -> Result<(Option<u64>, Option<u64>)> {
        let session = handler_args.session;
        // Test subscription existence
        session.get_subscription_by_schema_id_name(
            dependent_subscription.schema_id,
            &dependent_subscription.name,
        )?;

        // The epoch here must be pulled every time, otherwise there will be cache consistency issues
        let Some(new_epochs) = session
            .list_change_log_epochs(table_id, seek_timestamp, 2)
            .await?
        else {
            return Ok((None, None));
        };
        if let Some(expected_timestamp) = expected_timestamp
            && (new_epochs.is_empty() || &expected_timestamp != new_epochs.first().unwrap())
        {
            return Err(ErrorCode::CatalogError(
                format!(
                    " No data found for rw_timestamp {:?}, data may have been recycled, please recreate cursor",
                    convert_logstore_u64_to_unix_millis(expected_timestamp)
                )
                .into(),
            )
            .into());
        }
        Ok((new_epochs.get(0).cloned(), new_epochs.get(1).cloned()))
    }

    pub fn gen_batch_plan_result(
        &self,
        handler_args: HandlerArgs,
    ) -> Result<RwBatchQueryPlanResult> {
        match self.state {
            // Only used to return generated plans, so rw_timestamp are meaningless
            State::InitLogStoreQuery { .. } => Self::init_batch_plan_for_subscription_cursor(
                Some(0),
                self.dependent_table_id,
                handler_args,
                self.seek_pk_row.clone(),
            ),
            State::Fetch {
                from_snapshot,
                rw_timestamp,
                ..
            } => {
                if from_snapshot {
                    Self::init_batch_plan_for_subscription_cursor(
                        None,
                        self.dependent_table_id,
                        handler_args,
                        self.seek_pk_row.clone(),
                    )
                } else {
                    Self::init_batch_plan_for_subscription_cursor(
                        Some(rw_timestamp),
                        self.dependent_table_id,
                        handler_args,
                        self.seek_pk_row.clone(),
                    )
                }
            }
            State::Invalid => Err(ErrorCode::InternalError(
                "Cursor is in invalid state. Please close and re-create the cursor.".to_owned(),
            )
            .into()),
        }
    }

    fn init_batch_plan_for_subscription_cursor(
        rw_timestamp: Option<u64>,
        dependent_table_id: TableId,
        handler_args: HandlerArgs,
        seek_pk_row: Option<Row>,
    ) -> Result<RwBatchQueryPlanResult> {
        let session = handler_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let context = OptimizerContext::from_handler_args(handler_args);
        let version_id = {
            let version = session.env.hummock_snapshot_manager.acquire();
            let version = version.version();
            if !version
                .state_table_info
                .info()
                .contains_key(&dependent_table_id)
            {
                return Err(anyhow!("table id {dependent_table_id} has been dropped").into());
            }
            version.id
        };
        Self::create_batch_plan_for_cursor(
            table_catalog,
            &session,
            context.into(),
            rw_timestamp.map(|rw_timestamp| (rw_timestamp, rw_timestamp)),
            version_id,
            seek_pk_row,
        )
    }

    async fn initiate_query(
        rw_timestamp: Option<u64>,
        dependent_table_id: TableId,
        handler_args: HandlerArgs,
        seek_pk_row: Option<Row>,
    ) -> Result<(CursorDataChunkStream, Instant, Arc<TableCatalog>)> {
        let init_query_timer = Instant::now();
        let session = handler_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let plan_result = Self::init_batch_plan_for_subscription_cursor(
            rw_timestamp,
            dependent_table_id,
            handler_args.clone(),
            seek_pk_row,
        )?;
        let plan_fragmenter_result = gen_batch_plan_fragmenter(&handler_args.session, plan_result)?;
        let (chunk_stream, _) =
            create_chunk_stream_for_cursor(handler_args.session, plan_fragmenter_result).await?;
        Ok((chunk_stream, init_query_timer, table_catalog))
    }

    async fn try_refill_remaining_rows(
        chunk_stream: &mut CursorDataChunkStream,
        remaining_rows: &mut VecDeque<Row>,
    ) -> Result<()> {
        if remaining_rows.is_empty()
            && let Some(row_set) = chunk_stream.next().await?
        {
            remaining_rows.extend(row_set?);
        }
        Ok(())
    }

    pub fn build_row(
        mut row: Vec<Option<Bytes>>,
        rw_timestamp: Option<u64>,
        formats: &Vec<Format>,
        session_data: &StaticSessionData,
    ) -> Result<Row> {
        let row_len = row.len();
        let new_row = if let Some(rw_timestamp) = rw_timestamp {
            let rw_timestamp_formats = formats.get(row_len).unwrap_or(&Format::Text);
            let rw_timestamp = convert_logstore_u64_to_unix_millis(rw_timestamp);
            let rw_timestamp = pg_value_format(
                &DataType::Int64,
                risingwave_common::types::ScalarRefImpl::Int64(rw_timestamp as i64),
                *rw_timestamp_formats,
                session_data,
            )?;
            vec![Some(rw_timestamp)]
        } else {
            let op_formats = formats.get(row_len).unwrap_or(&Format::Text);
            let op = pg_value_format(
                &DataType::Varchar,
                risingwave_common::types::ScalarRefImpl::Utf8("Insert"),
                *op_formats,
                session_data,
            )?;
            vec![Some(op), None]
        };
        row.extend(new_row);
        Ok(Row::new(row))
    }

    pub fn build_desc(mut descs: Vec<Field>, from_snapshot: bool) -> Vec<Field> {
        if from_snapshot {
            descs.push(Field::with_name(DataType::Varchar, "op"));
        }
        descs.push(Field::with_name(DataType::Int64, "rw_timestamp"));
        descs
    }

    pub fn create_batch_plan_for_cursor(
        table_catalog: Arc<TableCatalog>,
        session: &SessionImpl,
        context: OptimizerContextRef,
        epoch_range: Option<(u64, u64)>,
        version_id: HummockVersionId,
        seek_pk_rows: Option<Row>,
    ) -> Result<RwBatchQueryPlanResult> {
        // pk + all column without hidden
        let output_col_idx = table_catalog
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, v)| {
                if !v.is_hidden || table_catalog.pk.iter().any(|pk| pk.column_index == index) {
                    Some(index)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let max_split_range_gap = context.session_ctx().config().max_split_range_gap() as u64;
        let pks = table_catalog.pk();
        let pks = pks
            .iter()
            .map(|f| {
                let pk = table_catalog.columns.get(f.column_index).unwrap();
                (pk.data_type(), f.column_index)
            })
            .collect_vec();
        let (scan, predicate) = if let Some(seek_pk_rows) = seek_pk_rows {
            let mut pk_rows = vec![];
            let mut values = vec![];
            for (seek_pk, (data_type, column_index)) in
                seek_pk_rows.take().into_iter().zip_eq_fast(pks.into_iter())
            {
                if let Some(seek_pk) = seek_pk {
                    pk_rows.push(InputRef {
                        index: column_index,
                        data_type: data_type.clone(),
                    });
                    let value_string = String::from_utf8(seek_pk.clone().into()).unwrap();
                    let value_data = ScalarImpl::from_text(&value_string, data_type).unwrap();
                    values.push((Some(value_data), data_type.clone()));
                }
            }
            if pk_rows.is_empty() {
                (None, None)
            } else {
                let (right_data, right_types): (Vec<_>, Vec<_>) = values.into_iter().unzip();
                let right_data = ScalarImpl::Struct(StructValue::new(right_data));
                let right_type = DataType::Struct(StructType::row_expr_type(right_types));
                let left = FunctionCall::new_unchecked(
                    ExprType::Row,
                    pk_rows.into_iter().map(|pk| pk.into()).collect(),
                    right_type.clone(),
                );
                let right = Literal::new(Some(right_data), right_type);
                let (scan, predicate) = Condition {
                    conjunctions: vec![
                        FunctionCall::new(ExprType::GreaterThan, vec![left.into(), right.into()])?
                            .into(),
                    ],
                }
                .split_to_scan_ranges(&table_catalog, max_split_range_gap)?;
                if scan.len() > 1 {
                    return Err(ErrorCode::InternalError(
                        "Seek pk row should only generate one scan range".to_owned(),
                    )
                    .into());
                }
                (scan.first().cloned(), Some(predicate))
            }
        } else {
            (None, None)
        };

        let (seq_scan, out_fields, out_names) = if let Some(epoch_range) = epoch_range {
            let core = generic::LogScan::new(
                table_catalog.name.clone(),
                output_col_idx,
                table_catalog.clone(),
                context,
                epoch_range,
                version_id,
            );
            let batch_log_seq_scan = BatchLogSeqScan::new(core, scan);
            let out_fields = batch_log_seq_scan.core().out_fields();
            let out_names = batch_log_seq_scan.core().column_names();
            (batch_log_seq_scan.into(), out_fields, out_names)
        } else {
            let core = generic::TableScan::new(
                output_col_idx,
                table_catalog.clone(),
                vec![],
                vec![],
                context,
                Condition {
                    conjunctions: vec![],
                },
                None,
            );
            let scans = match scan {
                Some(scan) => vec![scan],
                None => vec![],
            };
            let table_scan = BatchSeqScan::new(core, scans, None);
            let out_fields = table_scan.core().out_fields();
            let out_names = table_scan.core().column_names();
            (table_scan.into(), out_fields, out_names)
        };

        let plan = if let Some(predicate) = predicate
            && !predicate.always_true()
        {
            BatchFilter::new(generic::Filter::new(predicate, seq_scan)).into()
        } else {
            seq_scan
        };

        // order by pk, so don't need to sort
        let order = Order::new(table_catalog.pk().to_vec());

        // Here we just need a plan_root to call the method, only out_fields and out_names will be used
        let plan_root = PlanRoot::new_with_batch_plan(
            plan,
            RequiredDist::single(),
            order,
            out_fields,
            out_names,
        );
        let schema = plan_root.schema();
        let (batch_log_seq_scan, query_mode) = match session.config().query_mode() {
            QueryMode::Auto | QueryMode::Local => {
                (plan_root.gen_batch_local_plan()?, QueryMode::Local)
            }
            QueryMode::Distributed => (
                plan_root.gen_batch_distributed_plan()?,
                QueryMode::Distributed,
            ),
        };
        Ok(RwBatchQueryPlanResult {
            plan: batch_log_seq_scan,
            query_mode,
            schema,
            stmt_type: StatementType::SELECT,
        })
    }

    pub fn idle_duration(&self) -> Duration {
        self.last_fetch.elapsed()
    }

    pub fn subscription_name(&self) -> &str {
        self.subscription.name.as_str()
    }

    pub fn state_info_string(&self) -> String {
        format!("{}", self.state)
    }
}

pub struct CursorManager {
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    cursor_metrics: Arc<CursorMetrics>,
}

impl CursorManager {
    pub fn new(cursor_metrics: Arc<CursorMetrics>) -> Self {
        Self {
            cursor_map: tokio::sync::Mutex::new(HashMap::new()),
            cursor_metrics,
        }
    }

    pub async fn add_subscription_cursor(
        &self,
        cursor_name: String,
        start_timestamp: Option<u64>,
        dependent_table_id: TableId,
        subscription: Arc<SubscriptionCatalog>,
        handler_args: &HandlerArgs,
    ) -> Result<()> {
        let create_cursor_timer = Instant::now();
        let subscription_name = subscription.name.clone();
        let cursor = SubscriptionCursor::new(
            cursor_name,
            start_timestamp,
            subscription,
            dependent_table_id,
            handler_args,
            self.cursor_metrics.clone(),
        )
        .await?;
        let mut cursor_map = self.cursor_map.lock().await;
        self.cursor_metrics
            .subscription_cursor_declare_duration
            .with_label_values(&[&subscription_name])
            .observe(create_cursor_timer.elapsed().as_millis() as _);

        cursor_map.retain(|_, v| {
            if let Cursor::Subscription(cursor) = v
                && matches!(cursor.state, State::Invalid)
            {
                false
            } else {
                true
            }
        });

        cursor_map
            .try_insert(cursor.cursor_name.clone(), Cursor::Subscription(cursor))
            .map_err(|error| {
                ErrorCode::CatalogError(
                    format!("cursor `{}` already exists", error.entry.key()).into(),
                )
            })?;
        Ok(())
    }

    pub async fn add_query_cursor(
        &self,
        cursor_name: String,
        chunk_stream: CursorDataChunkStream,
        fields: Vec<Field>,
    ) -> Result<()> {
        let cursor = QueryCursor::new(chunk_stream, fields)?;
        self.cursor_map
            .lock()
            .await
            .try_insert(cursor_name, Cursor::Query(cursor))
            .map_err(|error| {
                ErrorCode::CatalogError(
                    format!("cursor `{}` already exists", error.entry.key()).into(),
                )
            })?;

        Ok(())
    }

    pub async fn remove_cursor(&self, cursor_name: &str) -> Result<()> {
        self.cursor_map
            .lock()
            .await
            .remove(cursor_name)
            .ok_or_else(|| {
                ErrorCode::CatalogError(format!("cursor `{}` don't exists", cursor_name).into())
            })?;
        Ok(())
    }

    pub async fn remove_all_cursor(&self) {
        self.cursor_map.lock().await.clear();
    }

    pub async fn remove_all_query_cursor(&self) {
        self.cursor_map
            .lock()
            .await
            .retain(|_, v| matches!(v, Cursor::Subscription(_)));
    }

    pub async fn get_rows_with_cursor(
        &self,
        cursor_name: &str,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            cursor
                .next(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    pub async fn get_fields_with_cursor(&self, cursor_name: &str) -> Result<Vec<Field>> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            Ok(cursor.get_fields())
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    pub async fn get_periodic_cursor_metrics(&self) -> PeriodicCursorMetrics {
        let mut subscription_cursor_nums = 0;
        let mut invalid_subscription_cursor_nums = 0;
        let mut subscription_cursor_last_fetch_duration = HashMap::new();
        for cursor in self.cursor_map.lock().await.values() {
            if let Cursor::Subscription(subscription_cursor) = cursor {
                subscription_cursor_nums += 1;
                if matches!(subscription_cursor.state, State::Invalid) {
                    invalid_subscription_cursor_nums += 1;
                } else {
                    let fetch_duration =
                        subscription_cursor.last_fetch.elapsed().as_millis() as f64;
                    subscription_cursor_last_fetch_duration.insert(
                        subscription_cursor.subscription.name.clone(),
                        fetch_duration,
                    );
                }
            }
        }
        PeriodicCursorMetrics {
            subscription_cursor_nums,
            invalid_subscription_cursor_nums,
            subscription_cursor_last_fetch_duration,
        }
    }

    pub async fn iter_query_cursors(&self, mut f: impl FnMut(&String, &QueryCursor)) {
        self.cursor_map
            .lock()
            .await
            .iter()
            .for_each(|(cursor_name, cursor)| {
                if let Cursor::Query(cursor) = cursor {
                    f(cursor_name, cursor)
                }
            });
    }

    pub async fn iter_subscription_cursors(&self, mut f: impl FnMut(&String, &SubscriptionCursor)) {
        self.cursor_map
            .lock()
            .await
            .iter()
            .for_each(|(cursor_name, cursor)| {
                if let Cursor::Subscription(cursor) = cursor {
                    f(cursor_name, cursor)
                }
            });
    }

    pub async fn gen_batch_plan_with_subscription_cursor(
        &self,
        cursor_name: &str,
        handler_args: HandlerArgs,
    ) -> Result<RwBatchQueryPlanResult> {
        match self.cursor_map.lock().await.get(cursor_name).ok_or_else(|| {
            ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name))
        })? {
            Cursor::Subscription(cursor) => {
                cursor.gen_batch_plan_result(handler_args.clone())
            },
            Cursor::Query(_) => Err(ErrorCode::InternalError("The plan of the cursor is the same as the query statement of the as when it was created.".to_owned()).into()),
        }
    }
}

#[cfg(test)]
mod cursor_lifecycle_tests {
    use risingwave_common::array::DataChunkTestExt;
    use tokio::sync::mpsc;

    use super::*;

    /// Verifies that dropping an unfinished local stream owned by a query cursor signals its
    /// executor-shutdown token instead of leaving background work alive.
    #[tokio::test]
    async fn test_dropping_unfinished_local_cursor_queries_cancels_executors() {
        let (query_shutdown_tx, query_shutdown_rx) = ShutdownToken::new();
        let (query_chunk_tx, query_chunk_rx) = mpsc::channel(1);
        query_chunk_tx
            .try_send(Ok(DataChunk::from_pretty("i\n1")))
            .unwrap();
        let query_stream = CursorQueryStream::local(
            tokio_stream::wrappers::ReceiverStream::new(query_chunk_rx),
            query_shutdown_tx,
        );
        drop(query_stream);
        // Dropping an ordinary query cursor stream must terminate its still-open executor.
        assert!(query_shutdown_rx.is_cancelled());
    }
}
