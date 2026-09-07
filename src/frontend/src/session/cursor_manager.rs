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

use core::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::anyhow;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::{Format, FormatIterator, Row};
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
use crate::handler::query::{
    BatchPlanFragmenterResult, RwBatchQueryPlanResult, gen_batch_plan_fragmenter,
};
use crate::handler::util::{
    StaticSessionData, convert_logstore_u64_to_unix_millis, pg_value_format, to_pg_field,
    to_pg_rows,
};
use crate::monitor::{CursorMetrics, PeriodicCursorMetrics};
use crate::optimizer::PlanRoot;
use crate::optimizer::plan_node::{BatchFilter, BatchLogSeqScan, BatchSeqScan, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::{
    DistributedQueryStream, LocalQueryStream, QueryManager, ReadSnapshot, SchedulerError,
};
use crate::utils::{Condition, WithOptions};
use crate::{OptimizerContext, OptimizerContextRef, TableCatalog};

/// Cancellation resources shared by a cursor and its underlying query executors.
struct CursorLifecycle {
    /// Sender used to stop every query executor owned by this cursor.
    shutdown_tx: ShutdownSender,
    /// Cloneable token through which the cursor and its query executors observe cursor shutdown.
    shutdown_rx: ShutdownToken,
    /// Cloneable token through which this cursor observes termination of its frontend session.
    session_shutdown_rx: ShutdownToken,
}

impl CursorLifecycle {
    /// Creates lifecycle resources for a cursor in the given session.
    fn new(session_shutdown_rx: ShutdownToken) -> Self {
        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        Self {
            shutdown_tx,
            shutdown_rx,
            session_shutdown_rx,
        }
    }

    /// Returns a cursor-scoped token for cancelling an underlying query executor.
    fn query_shutdown_token(&self) -> ShutdownToken {
        self.shutdown_rx.clone()
    }

    /// Returns a cursor-scoped sender for cancelling an underlying local query executor.
    fn query_shutdown_sender(&self) -> ShutdownSender {
        self.shutdown_tx.clone()
    }

    /// Returns foreground-owned clones of the cursor and session shutdown tokens. Both tokens
    /// returned by this function will not only control terminating the single FETCH but also
    /// terminating the underlying query.
    fn shutdown_tokens(&self) -> (ShutdownToken, ShutdownToken) {
        (self.shutdown_rx.clone(), self.session_shutdown_rx.clone())
    }

    /// Signals this cursor's underlying query executor to stop cooperatively.
    fn shutdown(&self) {
        self.shutdown_tx.cancel();
    }
}

impl Drop for CursorLifecycle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

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

#[derive(Clone)]
/// Schema and provenance metadata associated with a raw cursor data chunk.
enum CursorDataChunkMetadata {
    /// A chunk from a regular query cursor.
    Query {
        /// Fields used to format every raw column in the query chunk.
        fields: Arc<Vec<Field>>,
    },
    /// A chunk from either the snapshot or log-store phase of a subscription cursor.
    Subscription {
        /// Field mapping that was current when this chunk was produced.
        fields: Arc<FieldsManager>,
        /// Whether this chunk came from the initial upstream-table snapshot.
        from_snapshot: bool,
        /// Snapshot epoch or log-store timestamp represented by this chunk.
        rw_timestamp: u64,
    },
}

#[derive(Clone)]
/// One raw chunk produced independently of any PostgreSQL `FETCH` format.
struct CursorDataChunk {
    /// Unformatted rows received from the local or distributed query executor.
    chunk: DataChunk,
    /// Schema and provenance metadata used to interpret the chunk during a later `FETCH`.
    metadata: CursorDataChunkMetadata,
}

/// One formatted row plus subscription-only position metadata committed with that row.
struct FormattedCursorRow {
    /// PostgreSQL row returned to the client.
    row: Row,
    /// Primary-key row used by `EXPLAIN FETCH` to describe the committed cursor position.
    subscription_seek_pk_row: Option<Row>,
}

impl CursorDataChunk {
    /// Converts this raw chunk into rows for one PostgreSQL `FETCH`.
    fn into_pg_rows(
        self,
        formats: &[Format],
        session_data: &StaticSessionData,
    ) -> Result<Vec<FormattedCursorRow>> {
        match self.metadata {
            CursorDataChunkMetadata::Query { fields } => {
                let column_types = fields.iter().map(|field| field.data_type()).collect_vec();
                Ok(
                    to_pg_rows(&column_types, self.chunk, formats, session_data)?
                        .into_iter()
                        .map(|row| FormattedCursorRow {
                            row,
                            subscription_seek_pk_row: None,
                        })
                        .collect(),
                )
            }
            CursorDataChunkMetadata::Subscription {
                fields,
                from_snapshot,
                rw_timestamp,
            } => {
                // Have to call this everytime to ensure the formatting is based on the latest
                // schema.
                let (row_fields, row_formats) =
                    fields.get_row_stream_fields_and_formats(formats, from_snapshot)?;
                let column_types = row_fields
                    .iter()
                    .map(|field| field.data_type())
                    .collect_vec();
                let raw_formats = if row_formats.is_empty() {
                    &[][..]
                } else {
                    &row_formats[..column_types.len()]
                };
                to_pg_rows(&column_types, self.chunk, raw_formats, session_data)?
                    .into_iter()
                    .map(|row| {
                        let mut seek_pk_row = row.clone();
                        let seek_pk_row = seek_pk_row.project(&fields.row_pk_indices);
                        let mut row = SubscriptionCursor::build_row(
                            row.take(),
                            (!from_snapshot).then_some(rw_timestamp),
                            &row_formats,
                            session_data,
                        )?;
                        Ok(FormattedCursorRow {
                            row: row.project(&fields.row_output_col_indices),
                            subscription_seek_pk_row: Some(seek_pk_row),
                        })
                    })
                    .try_collect()
            }
        }
    }
}

#[derive(Clone)]
/// A non-row control event that makes cursor-stream state transitions visible to foreground
/// `FETCH` processing.
///
/// Unlike [`Poll::Pending`], which may represent any incomplete asynchronous operation, each
/// barrier communicates a specific query lifecycle or data availability state.
enum CursorDataChunkBarrier {
    /// Marks completion of the single query owned by a regular query cursor.
    QueryEnd,
    /// Marks the start of one per-epoch subscription query and exposes its state to the foreground.
    ///
    /// A subscription query may span multiple `FETCH` commands, while one cursor may execute
    /// multiple such queries. Committing this state lets later `FETCH` commands resume the same
    /// query with its position, output schema, and retention deadline.
    SubscriptionQueryStarted {
        /// Whether the query reads the initial upstream-table snapshot.
        from_snapshot: bool,
        /// The snapshot epoch or log-store timestamp read by the query.
        rw_timestamp: u64,
        /// The next log-store timestamp expected after this query, used to detect a retention gap.
        expected_timestamp: Option<u64>,
        /// The time at which query initialization began.
        init_query_timer: Instant,
        /// The output fields to commit for chunks produced by the query.
        output_fields: Vec<Field>,
        /// The retention deadline shared by `FETCH` commands that consume this query.
        expires_at: Instant,
    },
    /// Marks completion of a subscription query and carries the position for initializing the
    /// next per-epoch query.
    SubscriptionNewEpoch {
        /// The timestamp from which to search for the next available epoch.
        seek_timestamp: u64,
        /// The exact timestamp required at that position to detect a retention gap, when known.
        expected_timestamp: Option<u64>,
    },
    /// Indicates that no subscription log-store epoch is currently available.
    ///
    /// This barrier is emitted before waiting for a table-change notification, allowing the
    /// foreground to distinguish an intentional idle wait from [`Poll::Pending`] caused by other
    /// asynchronous work.
    SubscriptionIdle,
    /// Marks the end of an idle wait after the upstream table reports a change.
    SubscriptionIdleEnded,
    /// Indicates that the subscription output schema changed before rows from the newly started
    /// query are consumed, ending the current `FETCH` at the schema boundary.
    SchemaChanged,
}

#[derive(Clone)]
/// A raw chunk or cursor control barrier.
enum CursorDataChunkEvent {
    /// A data chunk whose rows can be formatted by the current `FETCH`.
    Chunk(CursorDataChunk),
    /// A control barrier produced between data chunks.
    Barrier(CursorDataChunkBarrier),
}

/// A regular query cursor's demand-driven raw data stream.
struct QueryCursorDataChunkStream {
    /// Boxed coroutine that owns the query stream and emits chunks followed by `QueryEnd`.
    inner: BoxStream<'static, std::result::Result<CursorDataChunkEvent, BoxedError>>,
}

impl QueryCursorDataChunkStream {
    /// Wraps an already-created query stream without adding another task or channel.
    fn new(query_stream: CursorQueryStream, fields: Vec<Field>) -> Self {
        Self {
            inner: Self::event_stream(query_stream, fields).boxed(),
        }
    }

    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    async fn event_stream(mut query_stream: CursorQueryStream, fields: Vec<Field>) {
        let query_fields = Arc::new(fields);
        while let Some(chunk) = query_stream.next().await {
            yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: chunk?,
                metadata: CursorDataChunkMetadata::Query {
                    fields: query_fields.clone(),
                },
            });
        }
        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd);
    }
}

impl Stream for QueryCursorDataChunkStream {
    type Item = std::result::Result<CursorDataChunkEvent, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

/// A subscription cursor's demand-driven raw data stream.
struct SubscriptionCursorDataChunkStream {
    /// Boxed coroutine that owns all query execution and state transitions for the subscription.
    inner: BoxStream<'static, std::result::Result<CursorDataChunkEvent, BoxedError>>,
}

impl SubscriptionCursorDataChunkStream {
    /// Owns the complete subscription execution coroutine without spawning another task.
    #[allow(clippy::too_many_arguments)]
    fn new(
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_context: SubscriptionCursorHandlerContext,
        fields_manager: FieldsManager,
        state: SubscriptionCursorDataChunkStreamState,
        cursor_metrics: Arc<CursorMetrics>,
        query_shutdown_tx: ShutdownSender,
        query_shutdown_rx: ShutdownToken,
    ) -> Self {
        Self {
            inner: Self::event_stream(
                subscription,
                dependent_table_id,
                handler_context,
                fields_manager,
                state,
                cursor_metrics,
                query_shutdown_tx,
                query_shutdown_rx,
            )
            .boxed(),
        }
    }

    #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
    #[allow(clippy::too_many_arguments)]
    async fn event_stream(
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_context: SubscriptionCursorHandlerContext,
        mut fields_manager: FieldsManager,
        mut state: SubscriptionCursorDataChunkStreamState,
        cursor_metrics: Arc<CursorMetrics>,
        query_shutdown_tx: ShutdownSender,
        query_shutdown_rx: ShutdownToken,
    ) {
        loop {
            let current_state =
                mem::replace(&mut state, SubscriptionCursorDataChunkStreamState::Invalid);
            match current_state {
                SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                } => {
                    let handler_args = handler_context.handler_args()?;
                    let (rw_timestamp, next_expected_timestamp) =
                        match SubscriptionCursor::get_next_rw_timestamp(
                            seek_timestamp,
                            dependent_table_id,
                            expected_timestamp,
                            handler_args,
                            &subscription,
                        )
                        .await?
                        {
                            (Some(rw_timestamp), next_expected_timestamp) => {
                                (rw_timestamp, next_expected_timestamp)
                            }
                            (None, _) => {
                                state = SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                                    seek_timestamp,
                                    expected_timestamp,
                                };
                                yield CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionIdle,
                                );
                                let session = handler_context.handler_args()?.session;
                                session
                                    .env
                                    .hummock_snapshot_manager()
                                    .wait_table_change_log_notification(
                                        dependent_table_id,
                                        seek_timestamp,
                                    )
                                    .await?;
                                yield CursorDataChunkEvent::Barrier(
                                    CursorDataChunkBarrier::SubscriptionIdleEnded,
                                );
                                continue;
                            }
                        };

                    let handler_args = handler_context.handler_args()?;
                    let snapshot = ReadSnapshot::FrontendPinned {
                        snapshot: handler_args
                            .session
                            .env
                            .hummock_snapshot_manager()
                            .acquire(),
                    };
                    let (query_stream, init_query_timer, catalog) =
                        SubscriptionCursor::initiate_query(
                            Some(rw_timestamp),
                            dependent_table_id,
                            handler_args,
                            query_shutdown_tx.clone(),
                            query_shutdown_rx.clone(),
                            snapshot,
                        )
                        .await?;
                    let schema_changed = fields_manager.try_refill_fields(&catalog);
                    state = SubscriptionCursorDataChunkStreamState::Fetch {
                        from_snapshot: false,
                        rw_timestamp,
                        query_stream,
                        expected_timestamp: next_expected_timestamp,
                        init_query_timer,
                    };
                    yield CursorDataChunkEvent::Barrier(
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot: false,
                            rw_timestamp,
                            expected_timestamp: next_expected_timestamp,
                            init_query_timer,
                            output_fields: fields_manager.get_output_fields(),
                            expires_at: Instant::now()
                                + Duration::from_secs(subscription.retention_seconds),
                        },
                    );
                    if schema_changed {
                        yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged);
                    }
                }
                SubscriptionCursorDataChunkStreamState::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    mut query_stream,
                    expected_timestamp,
                    init_query_timer,
                } => match query_stream.next().await {
                    Some(Ok(chunk)) => {
                        state = SubscriptionCursorDataChunkStreamState::Fetch {
                            from_snapshot,
                            rw_timestamp,
                            query_stream,
                            expected_timestamp,
                            init_query_timer,
                        };
                        yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                            chunk,
                            metadata: CursorDataChunkMetadata::Subscription {
                                fields: Arc::new(fields_manager.clone()),
                                from_snapshot,
                                rw_timestamp,
                            },
                        });
                    }
                    Some(Err(error)) => Err(error)?,
                    None => {
                        cursor_metrics
                            .subscription_cursor_query_duration
                            .with_label_values(&[&subscription.name])
                            .observe(init_query_timer.elapsed().as_millis() as _);
                        let (seek_timestamp, expected_timestamp) =
                            if let Some(expected_timestamp) = expected_timestamp {
                                (expected_timestamp, Some(expected_timestamp))
                            } else {
                                (rw_timestamp + 1, None)
                            };
                        state = SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                            seek_timestamp,
                            expected_timestamp,
                        };
                        yield CursorDataChunkEvent::Barrier(
                            CursorDataChunkBarrier::SubscriptionNewEpoch {
                                seek_timestamp,
                                expected_timestamp,
                            },
                        );
                    }
                },
                SubscriptionCursorDataChunkStreamState::Invalid => return Ok(()),
            }
        }
    }
}

impl Stream for SubscriptionCursorDataChunkStream {
    type Item = std::result::Result<CursorDataChunkEvent, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

/// A raw event retained until a successful `FETCH` commits past it.
struct CachedCursorDataChunkEvent {
    /// Raw chunk or control barrier already polled from the irreversible underlying stream.
    event: CursorDataChunkEvent,
    /// Number of leading formatted output rows already committed for a chunk; zero for a barrier.
    row_offset_in_chunk: usize,
}

/// Rows formatted for the current `FETCH` and their next raw-chunk offset.
struct FormattedCursorDataChunk {
    /// Formatted-row offset immediately after the last row tentatively yielded by this `FETCH`.
    next_offset: usize,
    /// Formatted rows in this raw chunk that have not yet been yielded by this `FETCH`.
    rows: VecDeque<FormattedCursorRow>,
}

/// Tentative row and raw-event progress shared by every kind of `FETCH` command.
struct CursorPgResponseFetchStateInner {
    /// PostgreSQL result formats requested by this `FETCH`.
    formats: Vec<Format>,
    /// Session settings captured when this `FETCH` began and needed for row formatting.
    session_data: StaticSessionData,
    /// Tentative index of the cached event currently being processed or to be processed next.
    next_event_index: usize,
    /// Current raw chunk formatted using this `FETCH`'s formats, if one is being processed.
    current_formatted_chunk: Option<FormattedCursorDataChunk>,
    /// Number of rows tentatively returned to this `FETCH` so far.
    yielded_rows: usize,
    /// Whether this response stream must emit no more items for the active `FETCH`.
    ///
    /// This is set after a normal `FETCH` boundary, underlying stream EOF, or an error is
    /// returned. It does not imply that the `FETCH` succeeded or that the cursor itself is
    /// permanently terminated.
    fetch_stream_terminated: bool,
}

impl CursorPgResponseFetchStateInner {
    /// Creates uncommitted progress for a new `FETCH` command.
    fn new(formats: &[Format], session: &SessionImpl) -> Self {
        Self {
            formats: formats.to_vec(),
            session_data: StaticSessionData {
                timezone: session.config().timezone(),
            },
            next_event_index: 0,
            current_formatted_chunk: None,
            yielded_rows: 0,
            fetch_stream_terminated: false,
        }
    }
}

/// Cursor-kind-specific state accumulated tentatively during the active `FETCH` command.
///
/// This state is discarded if the command is aborted. If the command succeeds, its raw-event
/// progress and staged subscription metadata are committed to the cursor.
enum CursorPgResponseFetchState {
    /// Tentative progress for a regular query cursor.
    Query {
        /// Row and raw-event progress shared with subscription cursors.
        inner: CursorPgResponseFetchStateInner,
    },
    /// Tentative progress for a subscription cursor plus metadata staged for commit.
    Subscription {
        /// Row and raw-event progress shared with regular query cursors.
        inner: CursorPgResponseFetchStateInner,
        /// Whether this `FETCH` waits for newly produced data when the subscription is idle.
        wait_for_data_when_idle: bool,
        /// Subscription state staged for commit with this `FETCH`.
        subscription_state_to_commit: Option<SubscriptionCursorState>,
        /// Output fields staged for commit with this `FETCH`.
        output_fields_to_commit: Option<Vec<Field>>,
        /// Retention deadline staged for commit with this `FETCH`.
        expires_at_to_commit: Option<Instant>,
        /// Primary-key position staged for commit with this `FETCH`.
        seek_pk_row_to_commit: Option<Row>,
    },
}

impl CursorPgResponseFetchState {
    /// Returns the tentative progress shared by both cursor kinds.
    fn inner(&self) -> &CursorPgResponseFetchStateInner {
        match self {
            Self::Query { inner } | Self::Subscription { inner, .. } => inner,
        }
    }

    /// Returns mutable tentative progress shared by both cursor kinds.
    fn inner_mut(&mut self) -> &mut CursorPgResponseFetchStateInner {
        match self {
            Self::Query { inner } | Self::Subscription { inner, .. } => inner,
        }
    }

    /// Returns whether this `FETCH` waits for newly produced data when the subscription is idle.
    fn wait_for_data_when_idle(&self) -> bool {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot wait for new data"),
            Self::Subscription {
                wait_for_data_when_idle,
                ..
            } => *wait_for_data_when_idle,
        }
    }

    /// Stages a newly started subscription query's metadata for commit.
    fn update_when_subscription_query_started(
        &mut self,
        from_snapshot: bool,
        rw_timestamp: u64,
        expected_timestamp: Option<u64>,
        init_query_timer: Instant,
        output_fields: Vec<Field>,
        expires_at: Instant,
    ) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                subscription_state_to_commit,
                output_fields_to_commit,
                expires_at_to_commit,
                ..
            } => {
                *subscription_state_to_commit = Some(SubscriptionCursorState::Fetch {
                    from_snapshot,
                    rw_timestamp,
                    expected_timestamp,
                    init_query_timer,
                });
                *output_fields_to_commit = Some(output_fields);
                *expires_at_to_commit = Some(expires_at);
            }
        }
    }

    /// Stages the next subscription log-store lookup position for commit.
    fn update_when_subscription_new_epoch(
        &mut self,
        seek_timestamp: u64,
        expected_timestamp: Option<u64>,
    ) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                subscription_state_to_commit,
                ..
            } => {
                *subscription_state_to_commit = Some(SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp,
                    expected_timestamp,
                });
            }
        }
    }

    /// Stages the primary-key position of a tentatively yielded row for commit.
    fn update_when_subscription_row_yielded(&mut self, seek_pk_row: Row) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                seek_pk_row_to_commit,
                ..
            } => *seek_pk_row_to_commit = Some(seek_pk_row),
        }
    }
}

/// State shared by concrete query and subscription PostgreSQL response streams.
struct CursorPgResponseStreamInner<S> {
    /// Irreversible source of raw chunks and ordered cursor control barriers.
    data_stream: S,
    /// Raw events polled from `data_stream` but not yet fully committed by a successful `FETCH`.
    cached_events: VecDeque<CachedCursorDataChunkEvent>,
    /// Tentative progress for the active `FETCH`, or `None` between commands.
    fetch_state: Option<CursorPgResponseFetchState>,
    /// Output fields committed by successful preceding `FETCH` commands.
    output_fields: Vec<Field>,
    /// Whether this response stream has entered an unrecoverable terminal state. When `true` then
    /// the cursor can be considered as invalid.
    failed: bool,
}

/// One item produced by the shared response-stream polling core.
enum CursorPgResponsePollItem {
    /// A formatted PostgreSQL row.
    Row(FormattedCursorRow),
    /// A cursor-specific control barrier interpreted by the concrete response stream.
    Barrier(CursorDataChunkBarrier),
    /// The underlying raw data stream ended.
    DataChunkStreamEnd,
}

impl<S> CursorPgResponseStreamInner<S> {
    /// Creates shared response-stream state around one concrete raw-event stream.
    fn new(data_stream: S, output_fields: Vec<Field>) -> Self {
        Self {
            data_stream,
            cached_events: VecDeque::new(),
            fetch_state: None,
            output_fields,
            failed: false,
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.output_fields.clone()
    }

    /// Returns whether the raw data stream reported an internal failure.
    fn is_failed(&self) -> bool {
        self.failed
    }

    /// Returns whether this response stream must emit no more items for the active `FETCH`.
    fn is_fetch_stream_terminated(&self) -> bool {
        self.fetch_state
            .as_ref()
            .is_some_and(|state| state.inner().fetch_stream_terminated)
    }

    /// Prevents this response stream from emitting more items for the active `FETCH`.
    ///
    /// This does not commit tentative progress or imply permanent cursor termination.
    fn terminate_fetch_stream(&mut self) {
        self.fetch_state
            .as_mut()
            .expect("response stream must be inside a FETCH")
            .inner_mut()
            .fetch_stream_terminated = true;
    }

    /// Returns the active `FETCH` state.
    fn fetch_state(&self) -> &CursorPgResponseFetchState {
        self.fetch_state
            .as_ref()
            .expect("response stream must be inside a FETCH")
    }

    /// Returns mutable active `FETCH` state.
    fn fetch_state_mut(&mut self) -> &mut CursorPgResponseFetchState {
        self.fetch_state
            .as_mut()
            .expect("response stream must be inside a FETCH")
    }

    /// Commits raw-event progress and returns cursor-kind-specific metadata to its owner.
    ///
    /// Fully consumed cached events are removed. If the active chunk was only partly consumed,
    /// its raw event is retained and its committed `row_offset` is advanced so the next `FETCH`
    /// can reformat the chunk and resume at the first uncommitted row.
    fn commit_fetch(&mut self) -> Option<CursorPgResponseFetchState> {
        let mut fetch_state = self.fetch_state.take()?;
        let next_event_index = {
            let inner = fetch_state.inner_mut();
            if let Some(chunk) = inner.current_formatted_chunk.take() {
                let event = self
                    .cached_events
                    .get_mut(inner.next_event_index)
                    .expect("current formatted chunk must have a cached raw chunk");
                debug_assert!(matches!(event.event, CursorDataChunkEvent::Chunk(_)));
                if chunk.rows.is_empty() {
                    inner.next_event_index += 1;
                } else {
                    event.row_offset_in_chunk = chunk.next_offset;
                }
            }
            inner.next_event_index
        };
        drop(self.cached_events.drain(..next_event_index));
        Some(fetch_state)
    }

    /// Discards the active `FETCH`'s tentative progress and staged metadata.
    ///
    /// Cached raw events and the committed cursor position remain unchanged, allowing a later
    /// `FETCH` to resume from the last commit.
    fn abort_fetch(&mut self) {
        self.fetch_state = None;
    }
}

impl<S> CursorPgResponseStreamInner<S>
where
    S: Stream<Item = std::result::Result<CursorDataChunkEvent, BoxedError>> + Unpin,
{
    /// Polls one formatted row, control barrier, or raw-stream termination.
    fn poll_next_item(&mut self, cx: &mut Context<'_>) -> Poll<Result<CursorPgResponsePollItem>> {
        loop {
            let Some(fetch_state) = self.fetch_state.as_mut() else {
                return Poll::Ready(Err(ErrorCode::InternalError(
                    "cursor response stream polled outside a FETCH".to_owned(),
                )
                .into()));
            };

            {
                // The first level cache: consume the current formatted chunk's remaining rows
                // before poll the next chunk.
                let inner = fetch_state.inner_mut();
                if let Some(chunk) = inner.current_formatted_chunk.as_mut() {
                    if let Some(row) = chunk.rows.pop_front() {
                        chunk.next_offset += 1;
                        inner.yielded_rows += 1;
                        return Poll::Ready(Ok(CursorPgResponsePollItem::Row(row)));
                    } else {
                        // Dispose the completely-consumed formatted chunk and try to find the next raw chunk
                        // in the local cache, which is the second level cache.
                        fetch_state.inner_mut().current_formatted_chunk.take();
                        fetch_state.inner_mut().next_event_index += 1
                    }
                }
            }

            let event_index = fetch_state.inner().next_event_index;
            // All cached events have been consumed, poll a new one.
            if event_index == self.cached_events.len() {
                match self.data_stream.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(event))) => {
                        self.cached_events.push_back(CachedCursorDataChunkEvent {
                            event,
                            row_offset_in_chunk: 0,
                        });
                        continue;
                    }
                    Poll::Ready(Some(Err(error))) => {
                        self.failed = true;
                        fetch_state.inner_mut().fetch_stream_terminated = true;
                        return Poll::Ready(Err(error.into()));
                    }
                    Poll::Ready(None) => {
                        fetch_state.inner_mut().fetch_stream_terminated = true;
                        return Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd));
                    }
                }
            }
            debug_assert!(event_index < self.cached_events.len());
            let event = self.cached_events[event_index].event.clone();
            let row_offset = self.cached_events[event_index].row_offset_in_chunk;

            match event {
                CursorDataChunkEvent::Chunk(data) => {
                    let rows = match data.into_pg_rows(
                        &fetch_state.inner().formats,
                        &fetch_state.inner().session_data,
                    ) {
                        Ok(rows) => rows,
                        Err(error) => {
                            fetch_state.inner_mut().fetch_stream_terminated = true;
                            return Poll::Ready(Err(error));
                        }
                    };
                    fetch_state.inner_mut().current_formatted_chunk =
                        Some(FormattedCursorDataChunk {
                            next_offset: row_offset,
                            rows: rows.into_iter().skip(row_offset).collect(),
                        });
                }
                CursorDataChunkEvent::Barrier(barrier) => {
                    // Barrier is instantly consumed, not like Chunk, which we keep consuming it
                    // until all rows within it are consumed.
                    fetch_state.inner_mut().next_event_index += 1;
                    return Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier)));
                }
            }
        }
    }
}

/// A regular query cursor's PostgreSQL response stream with commit-on-success progress.
struct QueryCursorPgResponseStream {
    /// Shared raw-event cache and per-`FETCH` tentative progress.
    inner: CursorPgResponseStreamInner<QueryCursorDataChunkStream>,
}

impl QueryCursorPgResponseStream {
    /// Creates the foreground stream for a regular query cursor.
    fn new(data_stream: QueryCursorDataChunkStream, output_fields: Vec<Field>) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.inner.fields()
    }

    /// Begins tentative progress for one regular query `FETCH` command.
    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl) {
        self.inner.fetch_state = Some(CursorPgResponseFetchState::Query {
            inner: CursorPgResponseFetchStateInner::new(formats, session),
        });
    }

    /// Commits the current `FETCH` position.
    fn commit_fetch(&mut self) {
        _ = self.inner.commit_fetch();
    }

    /// Discards the active `FETCH` without advancing the committed cursor position.
    fn abort_fetch(&mut self) {
        self.inner.abort_fetch();
    }
}

impl Stream for QueryCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.is_fetch_stream_terminated() {
            return Poll::Ready(None);
        }
        match this.inner.poll_next_item(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Row(row))) => Poll::Ready(Some(Ok(row.row))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(
                CursorDataChunkBarrier::QueryEnd,
            ))) => {
                this.inner.terminate_fetch_stream();
                Poll::Ready(None)
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(_))) => {
                this.inner.terminate_fetch_stream();
                Poll::Ready(Some(Err(ErrorCode::InternalError(
                    "query cursor received a non-query barrier".to_owned(),
                )
                .into())))
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => {
                if this.inner.failed {
                    Poll::Ready(Some(Err(ErrorCode::InternalError(
                        "Cursor data stream has terminated with an error; close and recreate the cursor"
                            .to_owned(),
                    )
                    .into())))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

/// A subscription cursor's PostgreSQL response stream with commit-on-success progress and metadata.
struct SubscriptionCursorPgResponseStream {
    /// Shared raw-event cache, active per-`FETCH` state, and committed output fields.
    inner: CursorPgResponseStreamInner<SubscriptionCursorDataChunkStream>,
    /// Logical subscription position committed by the most recent successful `FETCH`.
    subscription_state: SubscriptionCursorState,
    /// Primary-key position committed by the most recent successful `FETCH`.
    seek_pk_row: Option<Row>,
    /// Retention deadline associated with the committed subscription position.
    expires_at: Instant,
    /// Whether the raw stream is specifically waiting for the next available epoch.
    ///
    /// This distinguishes that idle wait from an ordinary `Poll::Pending` produced while an
    /// active query is still executing.
    subscription_idle: bool,
}

impl SubscriptionCursorPgResponseStream {
    /// Creates the foreground stream for a subscription cursor.
    fn new(
        data_stream: SubscriptionCursorDataChunkStream,
        output_fields: Vec<Field>,
        subscription_state: SubscriptionCursorState,
        expires_at: Instant,
    ) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
            subscription_state,
            seek_pk_row: None,
            expires_at,
            subscription_idle: false,
        }
    }

    /// Returns the output fields committed by previous `FETCH` commands.
    fn fields(&self) -> Vec<Field> {
        self.inner.fields()
    }

    /// Returns the subscription state committed by the foreground stream.
    fn subscription_state(&self) -> &SubscriptionCursorState {
        &self.subscription_state
    }

    /// Returns whether the subscription's current logical position has expired.
    fn is_expired(&self, now: Instant) -> bool {
        now > self.expires_at
    }

    /// Returns whether the raw data stream reported a terminal internal failure.
    fn is_failed(&self) -> bool {
        self.inner.is_failed()
    }

    /// Begins tentative subscription `FETCH` progress.
    fn begin_fetch(
        &mut self,
        formats: &[Format],
        session: &SessionImpl,
        wait_for_data_when_idle: bool,
    ) {
        self.inner.fetch_state = Some(CursorPgResponseFetchState::Subscription {
            inner: CursorPgResponseFetchStateInner::new(formats, session),
            wait_for_data_when_idle,
            subscription_state_to_commit: None,
            output_fields_to_commit: None,
            expires_at_to_commit: None,
            seek_pk_row_to_commit: None,
        });
    }

    /// Commits the current `FETCH` position and its ordered raw-stream metadata.
    fn commit_fetch(&mut self) {
        let Some(fetch_state) = self.inner.commit_fetch() else {
            return;
        };
        let CursorPgResponseFetchState::Subscription {
            subscription_state_to_commit,
            output_fields_to_commit,
            expires_at_to_commit,
            seek_pk_row_to_commit,
            ..
        } = fetch_state
        else {
            unreachable!("subscription response stream must own subscription fetch state");
        };
        if let Some(state) = subscription_state_to_commit {
            self.subscription_state = state;
        }
        if let Some(output_fields) = output_fields_to_commit {
            self.inner.output_fields = output_fields;
        }
        if let Some(expires_at) = expires_at_to_commit {
            self.expires_at = expires_at;
        }
        if let Some(seek_pk_row) = seek_pk_row_to_commit {
            self.seek_pk_row = Some(seek_pk_row);
        }
    }

    /// Discards the active `FETCH` without committing its progress or staged metadata.
    fn abort_fetch(&mut self) {
        self.inner.abort_fetch();
    }
}

impl Stream for SubscriptionCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.inner.is_fetch_stream_terminated() {
                return Poll::Ready(None);
            }
            match this.inner.poll_next_item(cx) {
                Poll::Pending if this.subscription_idle => {
                    let fetch_state = this.inner.fetch_state();
                    if fetch_state.inner().yielded_rows > 0
                        || !fetch_state.wait_for_data_when_idle()
                    {
                        this.inner.terminate_fetch_stream();
                        return Poll::Ready(None);
                    }
                    return Poll::Pending;
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    if this.inner.failed {
                        this.subscription_state = SubscriptionCursorState::Invalid;
                    }
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Row(FormattedCursorRow {
                    row,
                    subscription_seek_pk_row,
                }))) => {
                    this.subscription_idle = false;
                    if let Some(seek_pk_row) = subscription_seek_pk_row {
                        this.inner
                            .fetch_state_mut()
                            .update_when_subscription_row_yielded(seek_pk_row);
                    }
                    return Poll::Ready(Some(Ok(row)));
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier))) => {
                    this.subscription_idle = false;
                    let should_finish = match barrier {
                        CursorDataChunkBarrier::QueryEnd
                        | CursorDataChunkBarrier::SchemaChanged => true,
                        CursorDataChunkBarrier::SubscriptionQueryStarted {
                            from_snapshot,
                            rw_timestamp,
                            expected_timestamp,
                            init_query_timer,
                            output_fields,
                            expires_at,
                        } => {
                            this.inner
                                .fetch_state_mut()
                                .update_when_subscription_query_started(
                                    from_snapshot,
                                    rw_timestamp,
                                    expected_timestamp,
                                    init_query_timer,
                                    output_fields,
                                    expires_at,
                                );
                            false
                        }
                        CursorDataChunkBarrier::SubscriptionNewEpoch {
                            seek_timestamp,
                            expected_timestamp,
                        } => {
                            this.inner
                                .fetch_state_mut()
                                .update_when_subscription_new_epoch(
                                    seek_timestamp,
                                    expected_timestamp,
                                );
                            false
                        }
                        CursorDataChunkBarrier::SubscriptionIdle => {
                            this.subscription_idle = true;
                            let fetch_state = this.inner.fetch_state();
                            fetch_state.inner().yielded_rows > 0
                                || !fetch_state.wait_for_data_when_idle()
                        }
                        CursorDataChunkBarrier::SubscriptionIdleEnded => false,
                    };
                    if should_finish {
                        this.inner.terminate_fetch_stream();
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => {
                    // An idle subscription stream remains pending rather than reaching EOF. EOF
                    // therefore means the stream failed or entered its `Invalid` state, so this
                    // cursor cannot continue.
                    if this.inner.failed {
                        return Poll::Ready(Some(Err(ErrorCode::InternalError(
                            "Cursor data stream has terminated with an error; close and recreate the cursor"
                                .to_owned(),
                        )
                        .into())));
                    }
                    this.inner.failed = true;
                    this.subscription_state = SubscriptionCursorState::Invalid;
                    return Poll::Ready(Some(Err(ErrorCode::InternalError(
                        "Subscription cursor data stream terminated unexpectedly; close and recreate the cursor"
                            .to_owned(),
                    )
                    .into())));
                }
            }
        }
    }
}

/// A PostgreSQL `CancelRequest` handle scoped to exactly one `FETCH` command.
pub struct FetchCursorCancelHandle {
    /// Sender registered with the session so a PostgreSQL `CancelRequest` can cancel this `FETCH`.
    cancel_tx: ShutdownSender,
    /// Token awaited by the active `FETCH` loop.
    cancel_rx: ShutdownToken,
}

impl FetchCursorCancelHandle {
    /// Creates an uncanceled per-`FETCH` handle.
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
}

/// A named SQL cursor managed by one frontend session.
pub enum Cursor {
    /// A cursor that continuously follows a subscription.
    Subscription(SubscriptionCursor),
    /// A cursor over one regular query execution.
    Query(QueryCursor),
}
impl Cursor {
    /// Executes one SQL `FETCH` command against this cursor.
    pub async fn fetch(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        match self {
            Cursor::Subscription(cursor) => cursor
                .fetch(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
                .inspect_err(|_| cursor.cursor_metrics.subscription_cursor_error_count.inc()),
            Cursor::Query(cursor) => {
                cursor
                    .fetch(count, formats, handler_args, timeout_seconds, cancel_handle)
                    .await
            }
        }
    }

    /// Returns the fields currently committed by the cursor foreground.
    pub fn get_fields(&mut self) -> Vec<Field> {
        match self {
            Cursor::Subscription(cursor) => cursor.pg_response_stream.fields(),
            Cursor::Query(cursor) => cursor.pg_response_stream.fields(),
        }
    }
}

/// A regular query cursor whose raw stream survives individual `FETCH` cancellation.
pub struct QueryCursor {
    /// Declared first so cursor drop signals shutdown before the raw stream is dropped.
    lifecycle: CursorLifecycle,
    /// Owns raw-event caching and tentative per-`FETCH` progress.
    pg_response_stream: QueryCursorPgResponseStream,
}

impl QueryCursor {
    /// Creates a lifecycle, schedules a planned query, and wraps its raw chunk stream.
    pub(crate) async fn new(
        session: Arc<SessionImpl>,
        plan_fragmenter_result: BatchPlanFragmenterResult,
    ) -> Result<Self> {
        let lifecycle = CursorLifecycle::new(session.get_cursor_manager().session_shutdown_token());
        let query_shutdown_tx = lifecycle.query_shutdown_sender();
        let query_shutdown_rx = lifecycle.query_shutdown_token();
        let snapshot = session.pinned_snapshot();
        let (query_stream, fields) = crate::handler::declare_cursor::create_cursor_query_stream(
            session,
            plan_fragmenter_result,
            query_shutdown_tx,
            query_shutdown_rx,
            snapshot,
        )
        .await?;
        let chunk_stream = QueryCursorDataChunkStream::new(query_stream, fields.clone());
        Ok(Self {
            lifecycle,
            pg_response_stream: QueryCursorPgResponseStream::new(chunk_stream, fields),
        })
    }

    /// Executes one SQL `FETCH` command without cancelling the underlying query on timeout or
    /// PostgreSQL `CancelRequest`.
    pub async fn fetch(
        &mut self,
        count: u32,
        formats: &Vec<Format>,
        handler_args: HandlerArgs,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        let session = handler_args.session;
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        let desc = self
            .pg_response_stream
            .fields()
            .iter()
            .map(to_pg_field)
            .collect();
        if count == 0 {
            return Ok((vec![], desc));
        }
        cancel_handle.register(&session);
        self.pg_response_stream.begin_fetch(formats, &session);
        let (mut cursor_shutdown_rx, mut session_shutdown_rx) = self.lifecycle.shutdown_tokens();
        let timeout_instant =
            timeout_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds.unwrap_or(0)));
        tokio::pin!(timeout);
        while ans.len() < count as usize {
            tokio::select! {
                biased;
                _ = cancel_handle.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    ).into());
                }
                _ = cursor_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Cursor was closed while FETCH was running".to_owned(),
                    ).into());
                }
                _ = session_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Session ended while FETCH was running".to_owned(),
                    ).into());
                }
                row = self.pg_response_stream.next() => match row {
                    Some(Ok(row)) => ans.push(row),
                    Some(Err(error)) => {
                        self.pg_response_stream.abort_fetch();
                        return Err(error);
                    }
                    None => break,
                },
                _ = &mut timeout, if timeout_seconds.is_some() => break,
            }
            if timeout_instant.is_some_and(|timeout_instant| Instant::now() >= timeout_instant) {
                break;
            }
        }
        self.pg_response_stream.commit_fetch();
        Ok((ans, desc))
    }
}

#[derive(Clone)]
/// Foreground-visible subscription position received from ordered raw-stream barriers.
enum SubscriptionCursorState {
    /// The cursor is looking for the next available subscription log-store epoch.
    InitLogStoreQuery {
        /// The timestamp from which the stream will search.
        seek_timestamp: u64,

        /// When present, the next available timestamp must exactly match this value.
        expected_timestamp: Option<u64>,
    },
    /// The cursor has started a snapshot or log-store query at this logical position.
    Fetch {
        /// Whether the query reads the initial upstream-table snapshot rather than the log store.
        from_snapshot: bool,

        /// The snapshot epoch or subscription log-store timestamp read by the query.
        rw_timestamp: u64,

        /// The next log-store timestamp expected after this query, when already known.
        expected_timestamp: Option<u64>,

        /// The time at which query initialization began, used only for diagnostics.
        init_query_timer: Instant,
    },
    /// The raw stream reported an unrecoverable error.
    Invalid,
}

impl Display for SubscriptionCursorState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionCursorState::InitLogStoreQuery {
                seek_timestamp,
                expected_timestamp,
            } => write!(
                f,
                "InitLogStoreQuery {{ seek_timestamp: {}, expected_timestamp: {:?} }}",
                seek_timestamp, expected_timestamp
            ),
            SubscriptionCursorState::Fetch {
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                init_query_timer,
                ..
            } => write!(
                f,
                "Fetch {{ from_snapshot: {}, rw_timestamp: {}, expected_timestamp: {:?}, query init at {}ms before }}",
                from_snapshot,
                rw_timestamp,
                expected_timestamp,
                init_query_timer.elapsed().as_millis()
            ),
            SubscriptionCursorState::Invalid => write!(f, "Invalid"),
        }
    }
}

#[derive(Clone)]
/// Maps raw subscription query columns to stable cursor output columns and metadata columns.
struct FieldsManager {
    /// Upstream columns used to detect schema changes between subscription queries.
    columns_catalog: Vec<ColumnCatalog>,
    /// All row fields, including hidden primary keys, operation, timestamp, and visible columns.
    row_fields: Vec<Field>,
    /// Cursor output column indices within [`Self::row_fields`].
    row_output_col_indices: Vec<usize>,
    /// Primary-key column indices within [`Self::row_fields`].
    row_pk_indices: Vec<usize>,
    /// Raw stream-chunk column indices within [`Self::row_fields`].
    stream_chunk_row_indices: Vec<usize>,
    /// Operation-column index within [`Self::row_fields`].
    op_index: usize,
}

impl FieldsManager {
    // pub const OP_FIELD: Field = Field::with_name(DataType::Varchar, "op".to_owned());
    // pub const RW_TIMESTAMP_FIELD: Field = Field::with_name(DataType::Int64, "rw_timestamp".to_owned());

    /// Builds field mappings for the current upstream table catalog.
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

    /// Rebuilds all mappings when the upstream columns changed.
    pub fn try_refill_fields(&mut self, catalog: &TableCatalog) -> bool {
        if self.columns_catalog.ne(&catalog.columns) {
            *self = Self::new(catalog);
            true
        } else {
            false
        }
    }

    /// Returns fields visible in PostgreSQL cursor responses.
    pub fn get_output_fields(&self) -> Vec<Field> {
        self.row_output_col_indices
            .iter()
            .map(|&idx| self.row_fields[idx].clone())
            .collect()
    }

    /// Maps `FETCH` result formats to raw query fields for snapshot or log-store chunks.
    ///
    /// An empty format list selects PostgreSQL's default text format for every output column.
    pub fn get_row_stream_fields_and_formats(
        &self,
        formats: &[Format],
        from_snapshot: bool,
    ) -> Result<(Vec<Field>, Vec<Format>)> {
        let raw_indices = self
            .stream_chunk_row_indices
            .iter()
            .copied()
            .chain((!from_snapshot).then_some(self.op_index))
            .collect_vec();
        let fields = raw_indices
            .iter()
            .map(|index| self.row_fields[*index].clone())
            .collect();

        if formats.is_empty() {
            return Ok((fields, vec![]));
        }
        let output_formats = FormatIterator::new(formats, self.row_output_col_indices.len())
            .map_err(ErrorCode::InternalError)?;
        let mut row_formats = vec![Format::Text; self.row_fields.len()];
        for (row_index, format) in self
            .row_output_col_indices
            .iter()
            .copied()
            .zip_eq_fast(output_formats)
        {
            row_formats[row_index] = format;
        }
        Ok((fields, row_formats))
    }
}

/// The complete state machine owned by a subscription cursor's raw stream.
enum SubscriptionCursorDataChunkStreamState {
    /// Searches for the next available subscription log-store epoch.
    InitLogStoreQuery {
        /// The timestamp from which the stream searches.
        seek_timestamp: u64,
        /// An exact timestamp required to detect retention gaps.
        expected_timestamp: Option<u64>,
    },
    /// Streams one active snapshot or log-store query into raw cursor events.
    Fetch {
        /// Whether this is the initial upstream-table snapshot.
        from_snapshot: bool,
        /// The snapshot epoch or log-store timestamp read by the query.
        rw_timestamp: u64,
        /// The query stream owned by the cursor's raw stream.
        query_stream: CursorQueryStream,
        /// The next timestamp expected after this query, when known.
        expected_timestamp: Option<u64>,
        /// The query initialization time used by cursor metrics.
        init_query_timer: Instant,
    },
    /// A temporary sentinel used while moving the current state through an async transition.
    Invalid,
}

/// Session context needed to plan later subscription batches without retaining the session while
/// the cursor stream is dormant.
struct SubscriptionCursorHandlerContext {
    /// Weak session reference so a dormant cursor cannot keep a terminated session alive.
    session: Weak<SessionImpl>,
    /// Original SQL text retained for planning later subscription queries.
    sql: Arc<str>,
    /// Normalized SQL text retained for query diagnostics and metrics.
    normalized_sql: String,
    /// Statement options retained for planning later subscription queries.
    with_options: WithOptions,
}

impl SubscriptionCursorHandlerContext {
    fn new(handler_args: &HandlerArgs) -> Self {
        Self {
            session: Arc::downgrade(&handler_args.session),
            sql: handler_args.sql.clone(),
            normalized_sql: handler_args.normalized_sql.clone(),
            with_options: handler_args.with_options.clone(),
        }
    }

    fn handler_args(&self) -> Result<HandlerArgs> {
        let session = self.session.upgrade().ok_or_else(|| {
            ErrorCode::InternalError("session ended while polling subscription cursor".to_owned())
        })?;
        Ok(HandlerArgs {
            session,
            sql: self.sql.clone(),
            normalized_sql: self.normalized_sql.clone(),
            with_options: self.with_options.clone(),
        })
    }
}

/// A subscription cursor with foreground state committed only from raw-stream barriers.
pub struct SubscriptionCursor {
    /// Declared first so cursor drop signals shutdown before its raw stream is dropped.
    lifecycle: CursorLifecycle,
    /// Name under which this cursor is registered in its session.
    cursor_name: String,
    /// Catalog entry for the subscription followed by this cursor.
    subscription: Arc<SubscriptionCatalog>,
    /// Upstream table read by snapshot and log-store queries.
    dependent_table_id: TableId,
    /// Raw-event cache, tentative progress, and foreground-committed subscription metadata.
    pg_response_stream: SubscriptionCursorPgResponseStream,
    /// Metrics updated by this cursor's query and `FETCH` lifecycle.
    cursor_metrics: Arc<CursorMetrics>,
    /// Completion time of the most recent successful `FETCH`.
    last_fetch: Instant,
}

impl SubscriptionCursor {
    /// Declares a subscription cursor and captures a `FULL` cursor's snapshot immediately.
    pub async fn new(
        cursor_name: String,
        start_timestamp: Option<u64>,
        subscription: Arc<SubscriptionCatalog>,
        dependent_table_id: TableId,
        handler_args: &HandlerArgs,
        cursor_metrics: Arc<CursorMetrics>,
    ) -> Result<Self> {
        let lifecycle = CursorLifecycle::new(
            handler_args
                .session
                .get_cursor_manager()
                .session_shutdown_token(),
        );
        let query_shutdown_tx = lifecycle.query_shutdown_sender();
        let query_shutdown_rx = lifecycle.query_shutdown_token();
        let (stream_state, cursor_state, fields_manager) =
            if let Some(start_timestamp) = start_timestamp {
                let table_catalog = handler_args.session.get_table_by_id(dependent_table_id)?;
                (
                    SubscriptionCursorDataChunkStreamState::InitLogStoreQuery {
                        seek_timestamp: start_timestamp,
                        expected_timestamp: None,
                    },
                    SubscriptionCursorState::InitLogStoreQuery {
                        seek_timestamp: start_timestamp,
                        expected_timestamp: None,
                    },
                    FieldsManager::new(&table_catalog),
                )
            } else {
                // `FULL` is the only subscription mode represented by `None`. Pin its snapshot
                // during `DECLARE`; every `SINCE` mode stays in `InitLogStoreQuery` without one.
                let snapshot = handler_args.session.pinned_snapshot();
                let pinned_epoch = Self::snapshot_epoch(&snapshot, dependent_table_id)?;
                let (query_stream, init_query_timer, table_catalog) = Self::initiate_query(
                    None,
                    dependent_table_id,
                    handler_args.clone(),
                    query_shutdown_tx.clone(),
                    query_shutdown_rx.clone(),
                    snapshot,
                )
                .await?;
                (
                    SubscriptionCursorDataChunkStreamState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: pinned_epoch,
                        query_stream,
                        expected_timestamp: None,
                        init_query_timer,
                    },
                    SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: pinned_epoch,
                        expected_timestamp: None,
                        init_query_timer,
                    },
                    FieldsManager::new(&table_catalog),
                )
            };

        let output_fields = fields_manager.get_output_fields();
        let expires_at = Instant::now() + Duration::from_secs(subscription.retention_seconds);
        let chunk_stream = SubscriptionCursorDataChunkStream::new(
            subscription.clone(),
            dependent_table_id,
            SubscriptionCursorHandlerContext::new(handler_args),
            fields_manager,
            stream_state,
            cursor_metrics.clone(),
            query_shutdown_tx,
            query_shutdown_rx,
        );

        Ok(Self {
            lifecycle,
            cursor_name,
            subscription,
            dependent_table_id,
            pg_response_stream: SubscriptionCursorPgResponseStream::new(
                chunk_stream,
                output_fields,
                cursor_state,
                expires_at,
            ),
            cursor_metrics,
            last_fetch: Instant::now(),
        })
    }

    fn snapshot_epoch(snapshot: &ReadSnapshot, dependent_table_id: TableId) -> Result<u64> {
        match snapshot {
            ReadSnapshot::FrontendPinned { snapshot, .. } => snapshot
                .version()
                .state_table_info
                .info()
                .get(&dependent_table_id)
                .ok_or_else(|| anyhow!("dependent_table_id {dependent_table_id} not exists").into())
                .map(|info| info.committed_epoch),
            ReadSnapshot::Other(_) => Err(ErrorCode::InternalError(
                "Fetch Cursor can't start from specified query epoch. May run `set query_epoch = 0;`"
                    .to_owned(),
            )
            .into()),
            ReadSnapshot::ReadUncommitted => Err(ErrorCode::InternalError(
                "Fetch Cursor don't support read uncommitted".to_owned(),
            )
            .into()),
        }
    }

    /// Executes one SQL `FETCH` while preserving raw-stream progress across timeout or cancellation.
    pub async fn fetch(
        &mut self,
        count: u32,
        handler_args: HandlerArgs,
        formats: &Vec<Format>,
        timeout_seconds: Option<u64>,
        cancel_handle: &mut FetchCursorCancelHandle,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if self.pg_response_stream.is_expired(Instant::now()) {
            self.lifecycle.shutdown();
            return Err(ErrorCode::InternalError(
                "The cursor has exceeded its maximum lifetime, please recreate it (close then declare cursor).".to_owned(),
            )
            .into());
        }

        let desc = self
            .pg_response_stream
            .fields()
            .iter()
            .map(to_pg_field)
            .collect();
        if count == 0 {
            return Ok((vec![], desc));
        }

        let session = handler_args.session;
        cancel_handle.register(&session);
        let wait_for_data_when_idle = timeout_seconds.unwrap_or(0) > 0;
        self.pg_response_stream
            .begin_fetch(formats, &session, wait_for_data_when_idle);
        let (mut cursor_shutdown_rx, mut session_shutdown_rx) = self.lifecycle.shutdown_tokens();
        let timeout_instant =
            timeout_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds.unwrap_or(0)));
        tokio::pin!(timeout);
        let mut ans = Vec::with_capacity(std::cmp::min(100, count) as usize);
        while ans.len() < count as usize {
            let fetch_row_timer = Instant::now();
            tokio::select! {
                biased;
                _ = cancel_handle.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    ).into());
                }
                _ = cursor_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Cursor was closed while FETCH was running".to_owned(),
                    ).into());
                }
                _ = session_shutdown_rx.cancelled() => {
                    self.pg_response_stream.abort_fetch();
                    return Err(ErrorCode::InternalError(
                        "Session ended while FETCH was running".to_owned(),
                    ).into());
                }
                row = self.pg_response_stream.next() => match row {
                    Some(Ok(row)) => {
                        self.cursor_metrics
                            .subscription_cursor_fetch_duration
                            .with_label_values(&[&self.subscription.name])
                            .observe(fetch_row_timer.elapsed().as_millis() as _);
                        ans.push(row);
                    }
                    Some(Err(error)) => {
                        self.pg_response_stream.abort_fetch();
                        return Err(error);
                    }
                    None => break,
                },
                _ = &mut timeout, if timeout_seconds.is_some() => break,
            }
            if timeout_instant.is_some_and(|timeout_instant| Instant::now() >= timeout_instant) {
                break;
            }
        }
        self.pg_response_stream.commit_fetch();
        self.last_fetch = Instant::now();
        Ok((ans, desc))
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

    /// Generates a diagnostic batch plan for the foreground-committed subscription position.
    pub fn gen_batch_plan_result(
        &self,
        handler_args: HandlerArgs,
    ) -> Result<RwBatchQueryPlanResult> {
        match self.pg_response_stream.subscription_state().clone() {
            // Only used to return generated plans, so rw_timestamp is meaningless.
            SubscriptionCursorState::InitLogStoreQuery { .. } => {
                Self::init_batch_plan_for_subscription_cursor(
                    Some(0),
                    self.dependent_table_id,
                    handler_args,
                    self.pg_response_stream.seek_pk_row.clone(),
                )
            }
            SubscriptionCursorState::Fetch {
                from_snapshot,
                rw_timestamp,
                ..
            } => {
                if from_snapshot {
                    Self::init_batch_plan_for_subscription_cursor(
                        None,
                        self.dependent_table_id,
                        handler_args,
                        self.pg_response_stream.seek_pk_row.clone(),
                    )
                } else {
                    Self::init_batch_plan_for_subscription_cursor(
                        Some(rw_timestamp),
                        self.dependent_table_id,
                        handler_args,
                        self.pg_response_stream.seek_pk_row.clone(),
                    )
                }
            }
            SubscriptionCursorState::Invalid => Err(ErrorCode::InternalError(
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
        query_shutdown_tx: ShutdownSender,
        query_shutdown_rx: ShutdownToken,
        snapshot: ReadSnapshot,
    ) -> Result<(CursorQueryStream, Instant, Arc<TableCatalog>)> {
        let init_query_timer = Instant::now();
        let session = handler_args.clone().session;
        let table_catalog = session.get_table_by_id(dependent_table_id)?;
        let plan_result = Self::init_batch_plan_for_subscription_cursor(
            rw_timestamp,
            dependent_table_id,
            handler_args.clone(),
            None,
        )?;
        let plan_fragmenter_result = gen_batch_plan_fragmenter(&handler_args.session, plan_result)?;
        let (query_stream, _) = crate::handler::declare_cursor::create_cursor_query_stream(
            handler_args.session,
            plan_fragmenter_result,
            query_shutdown_tx,
            query_shutdown_rx,
            snapshot,
        )
        .await?;
        Ok((query_stream, init_query_timer, table_catalog))
    }

    /// Adds subscription operation and timestamp metadata to one encoded PostgreSQL row.
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

    /// Adds subscription metadata fields to a PostgreSQL row description.
    pub fn build_desc(mut descs: Vec<Field>, from_snapshot: bool) -> Vec<Field> {
        if from_snapshot {
            descs.push(Field::with_name(DataType::Varchar, "op"));
        }
        descs.push(Field::with_name(DataType::Int64, "rw_timestamp"));
        descs
    }

    /// Builds the ordered snapshot or log-store scan plan used by a subscription cursor query.
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

    /// Returns the duration since the last successful `FETCH`.
    pub fn idle_duration(&self) -> Duration {
        self.last_fetch.elapsed()
    }

    /// Returns the catalog subscription name followed by this cursor.
    pub fn subscription_name(&self) -> &str {
        self.subscription.name.as_str()
    }

    /// Formats the foreground-committed stream position for diagnostics.
    pub fn state_info_string(&self) -> String {
        self.pg_response_stream.subscription_state().to_string()
    }
}

/// Owns every named cursor and the session-wide cursor shutdown signal.
pub struct CursorManager {
    /// Session-local cursors, locked across each `FETCH` to serialize cursor access.
    cursor_map: tokio::sync::Mutex<HashMap<String, Cursor>>,
    /// Metrics shared by all cursors in this frontend process.
    cursor_metrics: Arc<CursorMetrics>,
    /// Sender canceled when this frontend session begins shutting down.
    session_shutdown_tx: ShutdownSender,
    /// Token cloned into cursors so active `FETCH` commands observe session shutdown.
    session_shutdown_rx: ShutdownToken,
}

impl CursorManager {
    /// Creates an empty cursor manager for one frontend session.
    pub fn new(cursor_metrics: Arc<CursorMetrics>) -> Self {
        let (session_shutdown_tx, session_shutdown_rx) = ShutdownToken::new();
        Self {
            cursor_map: tokio::sync::Mutex::new(HashMap::new()),
            cursor_metrics,
            session_shutdown_tx,
            session_shutdown_rx,
        }
    }

    /// Returns a token observed by every cursor foreground in this session.
    pub fn session_shutdown_token(&self) -> ShutdownToken {
        self.session_shutdown_rx.clone()
    }

    /// Initiates session cursor shutdown without waiting for cleanup to finish.
    ///
    /// Active `FETCH` commands are signaled immediately. If an active command holds the cursor-map
    /// lock, a background task waits for it to release the lock and then drops every cursor-owned
    /// stream. Otherwise, all streams are dropped before this method returns.
    pub fn initiate_shutdown(self: &Arc<Self>) {
        if !self.session_shutdown_tx.cancel() {
            return;
        }
        if let Ok(mut cursor_map) = self.cursor_map.try_lock() {
            cursor_map.clear();
            return;
        }
        let cursor_manager = self.clone();
        tokio::spawn(async move {
            cursor_manager.cursor_map.lock().await.clear();
        });
    }

    /// Completes session shutdown only after all cursor-owned streams have been dropped.
    pub async fn shutdown_and_wait(&self) {
        self.session_shutdown_tx.cancel();
        self.cursor_map.lock().await.clear();
    }

    /// Declares and registers a subscription cursor.
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
                && cursor.pg_response_stream.is_failed()
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

    /// Registers a regular query cursor and its stable output fields.
    pub async fn add_query_cursor(&self, cursor_name: String, cursor: QueryCursor) -> Result<()> {
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

    /// Closes and removes one named cursor.
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

    /// Closes and removes every cursor in the session.
    pub async fn remove_all_cursor(&self) {
        self.cursor_map.lock().await.clear();
    }

    /// Closes regular query cursors while retaining subscription cursors.
    pub async fn remove_all_query_cursor(&self) {
        self.cursor_map
            .lock()
            .await
            .retain(|_, v| matches!(v, Cursor::Subscription(_)));
    }

    /// Executes one `FETCH` against a named cursor.
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
                .fetch(count, handler_args, formats, timeout_seconds, cancel_handle)
                .await
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    /// Returns the foreground-committed fields of a named cursor.
    pub async fn get_fields_with_cursor(&self, cursor_name: &str) -> Result<Vec<Field>> {
        if let Some(cursor) = self.cursor_map.lock().await.get_mut(cursor_name) {
            Ok(cursor.get_fields())
        } else {
            Err(ErrorCode::InternalError(format!("Cannot find cursor `{}`", cursor_name)).into())
        }
    }

    /// Collects periodic cursor counts and last-fetch durations.
    pub async fn get_periodic_cursor_metrics(&self) -> PeriodicCursorMetrics {
        let mut subscription_cursor_nums = 0;
        let mut invalid_subscription_cursor_nums = 0;
        let mut subscription_cursor_last_fetch_duration = HashMap::new();
        for cursor in self.cursor_map.lock().await.values() {
            if let Cursor::Subscription(subscription_cursor) = cursor {
                subscription_cursor_nums += 1;
                if subscription_cursor.pg_response_stream.is_failed() {
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

    /// Applies a callback to every regular query cursor while holding the manager lock.
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

    /// Applies a callback to every subscription cursor while holding the manager lock.
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

    /// Generates the batch plan corresponding to a subscription cursor's committed position.
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
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use risingwave_common::array::DataChunkTestExt;
    use risingwave_common::catalog::ColumnDesc;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_pb::hummock::StateTableInfoDelta;
    use risingwave_sqlparser::parser::Parser;
    use tokio::sync::{mpsc, oneshot};

    use super::*;

    impl QueryCursorDataChunkStream {
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn events_for_test(events: Vec<CursorDataChunkEvent>) {
            for event in events {
                yield event;
            }
        }

        /// Creates a synthetic query stream from the supplied events.
        fn for_test(events: Vec<CursorDataChunkEvent>) -> Self {
            Self {
                inner: Self::events_for_test(events).boxed(),
            }
        }

        /// Creates the canonical three-chunk query stream used by cursor tests.
        fn query_data_stream_for_test() -> (CursorLifecycle, Self, Vec<Field>) {
            let fields = vec![Field::with_name(DataType::Int32, "v")];
            let query_fields = Arc::new(fields.clone());
            let events = ["i\n1\n2", "i\n3\n4", "i\n5\n6"]
                .into_iter()
                .map(|chunk| {
                    CursorDataChunkEvent::Chunk(CursorDataChunk {
                        chunk: DataChunk::from_pretty(chunk),
                        metadata: CursorDataChunkMetadata::Query {
                            fields: query_fields.clone(),
                        },
                    })
                })
                .chain(std::iter::once(CursorDataChunkEvent::Barrier(
                    CursorDataChunkBarrier::QueryEnd,
                )))
                .collect();
            (lifecycle_for_test(), Self::for_test(events), fields)
        }

        /// Emits rows 1-2, signals `waiting`, and blocks before rows 3-6 and `QueryEnd`.
        /// This leaves a `FETCH` pending between chunks: cancellation is expected to roll its
        /// tentative position back to row 1, while a positive timeout commits rows 1-2 so the
        /// next `FETCH` starts at row 3. Releasing the gate lets the same stream finish.
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn gated_events_for_test(waiting: Arc<AtomicBool>, resume_rx: oneshot::Receiver<()>) {
            let fields = Arc::new(vec![Field::with_name(DataType::Int32, "v")]);
            yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: DataChunk::from_pretty("i\n1\n2"),
                metadata: CursorDataChunkMetadata::Query {
                    fields: fields.clone(),
                },
            });
            waiting.store(true, Ordering::Relaxed);
            resume_rx.await.unwrap();
            for chunk in ["i\n3\n4", "i\n5\n6"] {
                yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                    chunk: DataChunk::from_pretty(chunk),
                    metadata: CursorDataChunkMetadata::Query {
                        fields: fields.clone(),
                    },
                });
            }
            yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::QueryEnd);
        }

        fn gated_query_data_stream_for_test() -> (
            CursorLifecycle,
            Self,
            Vec<Field>,
            Arc<AtomicBool>,
            oneshot::Sender<()>,
        ) {
            let waiting = Arc::new(AtomicBool::new(false));
            let (resume_tx, resume_rx) = oneshot::channel();
            (
                lifecycle_for_test(),
                Self {
                    inner: Self::gated_events_for_test(waiting.clone(), resume_rx).boxed(),
                },
                vec![Field::with_name(DataType::Int32, "v")],
                waiting,
                resume_tx,
            )
        }
    }

    impl SubscriptionCursorDataChunkStream {
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn events_for_test(events: Vec<CursorDataChunkEvent>, polled: Arc<AtomicBool>) {
            polled.store(true, Ordering::Relaxed);
            for event in events {
                yield event;
            }
            std::future::pending::<()>().await;
        }

        /// Creates a synthetic subscription event stream and returns a flag that records whether
        /// the wrapper has been polled by `FETCH`. This tests on-demand event consumption, not
        /// query initiation: `FULL` initiates its snapshot query in [`SubscriptionCursor::new`].
        fn for_test(events: Vec<CursorDataChunkEvent>) -> (Self, Arc<AtomicBool>) {
            let polled = Arc::new(AtomicBool::new(false));
            (
                Self {
                    inner: Self::events_for_test(events, polled.clone()).boxed(),
                },
                polled,
            )
        }

        fn subscription_fields_for_test() -> FieldsManager {
            FieldsManager {
                columns_catalog: vec![],
                row_fields: vec![
                    Field::with_name(DataType::Int32, "v"),
                    Field::with_name(DataType::Varchar, "op"),
                    Field::with_name(DataType::Int64, "rw_timestamp"),
                ],
                row_output_col_indices: vec![0, 1, 2],
                row_pk_indices: vec![0],
                stream_chunk_row_indices: vec![0],
                op_index: 1,
            }
        }

        fn subscription_query_started_for_test(
            rw_timestamp: u64,
            fields: &FieldsManager,
        ) -> CursorDataChunkEvent {
            CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionQueryStarted {
                from_snapshot: false,
                rw_timestamp,
                expected_timestamp: None,
                init_query_timer: Instant::now(),
                output_fields: fields.get_output_fields(),
                expires_at: Instant::now() + Duration::from_secs(60),
            })
        }

        /// Creates the canonical subscription stream: one initial epoch, two later epochs, and
        /// a final idle barrier. Snapshot mode omits the first query-started barrier because its
        /// query was already started by `DECLARE`.
        fn subscription_data_stream_for_test(
            from_snapshot: bool,
        ) -> (CursorLifecycle, Self, Arc<AtomicBool>) {
            const EPOCHS: [u64; 3] = [1 << 16, 2 << 16, 3 << 16];

            let fields = Arc::new(Self::subscription_fields_for_test());
            let mut events = vec![];
            // A `FULL` cursor starts its initial snapshot query during `DECLARE`, so its stream
            // begins directly with snapshot data. A `SINCE` cursor starts its first log-store
            // query during `FETCH`, so emit its query-started event before the first chunk.
            if !from_snapshot {
                events.push(Self::subscription_query_started_for_test(
                    EPOCHS[0], &fields,
                ));
            }

            for (index, rw_timestamp) in EPOCHS.into_iter().enumerate() {
                let chunk = if from_snapshot && index == 0 {
                    DataChunk::from_pretty("i\n1\n2")
                } else {
                    DataChunk::from_pretty(&format!(
                        "i T\n{} Insert\n{} Insert",
                        index * 2 + 1,
                        index * 2 + 2
                    ))
                };
                events.push(CursorDataChunkEvent::Chunk(CursorDataChunk {
                    chunk,
                    metadata: CursorDataChunkMetadata::Subscription {
                        fields: fields.clone(),
                        from_snapshot: from_snapshot && index == 0,
                        rw_timestamp,
                    },
                }));
                events.push(CursorDataChunkEvent::Barrier(
                    CursorDataChunkBarrier::SubscriptionNewEpoch {
                        seek_timestamp: rw_timestamp + 1,
                        expected_timestamp: None,
                    },
                ));
                if let Some(next_epoch) = EPOCHS.get(index + 1) {
                    events.push(Self::subscription_query_started_for_test(
                        *next_epoch,
                        &fields,
                    ));
                }
            }
            events.push(CursorDataChunkEvent::Barrier(
                CursorDataChunkBarrier::SubscriptionIdle,
            ));

            let (stream, polled) = Self::for_test(events);
            (lifecycle_for_test(), stream, polled)
        }

        /// Builds a stream that starts idle, resumes into a new log-store query, emits one row,
        /// and then remains pending. Separate gates control leaving idle and producing query data.
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn idle_then_resume_for_test(
            waiting_at_idle: Arc<AtomicBool>,
            idle_resume_rx: oneshot::Receiver<()>,
            query_resume_rx: oneshot::Receiver<()>,
        ) {
            yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdle);
            waiting_at_idle.store(true, Ordering::Relaxed);
            idle_resume_rx.await.unwrap();
            yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdleEnded);
            query_resume_rx.await.unwrap();
            let fields = Self::subscription_fields_for_test();
            yield Self::subscription_query_started_for_test(1 << 16, &fields);
            yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: DataChunk::from_pretty(
                    "i T
                     1 Insert",
                ),
                metadata: CursorDataChunkMetadata::Subscription {
                    fields: Arc::new(Self::subscription_fields_for_test()),
                    from_snapshot: false,
                    rw_timestamp: 1 << 16,
                },
            });
            std::future::pending::<()>().await;
        }

        /// Emits rows 1-4 across two completed log-store epochs, starts the third epoch, signals
        /// `waiting`, and blocks before rows 5-6, the third epoch boundary, and `SubscriptionIdle`.
        /// Cancellation at the gate is expected to roll all tentative rows and epochs back to the
        /// original position; a positive timeout instead commits rows 1-4 and both completed
        /// epochs. Releasing the gate lets the same stream deliver the remaining rows.
        #[try_stream(ok = CursorDataChunkEvent, error = BoxedError)]
        async fn gated_multi_epoch_events_for_test(
            waiting_for_query: Arc<AtomicBool>,
            query_resume_rx: oneshot::Receiver<()>,
        ) {
            let fields = Arc::new(Self::subscription_fields_for_test());
            for (rw_timestamp, first_value) in [(1 << 16, 1), (2 << 16, 3)] {
                yield Self::subscription_query_started_for_test(rw_timestamp, &fields);
                yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                    chunk: DataChunk::from_pretty(&format!(
                        "i T\n{first_value} Insert\n{} Insert",
                        first_value + 1
                    )),
                    metadata: CursorDataChunkMetadata::Subscription {
                        fields: fields.clone(),
                        from_snapshot: false,
                        rw_timestamp,
                    },
                });
                yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                    seek_timestamp: rw_timestamp + 1,
                    expected_timestamp: None,
                });
            }
            yield Self::subscription_query_started_for_test(3 << 16, &fields);
            waiting_for_query.store(true, Ordering::Relaxed);
            query_resume_rx.await.unwrap();
            yield CursorDataChunkEvent::Chunk(CursorDataChunk {
                chunk: DataChunk::from_pretty("i T\n5 Insert\n6 Insert"),
                metadata: CursorDataChunkMetadata::Subscription {
                    fields,
                    from_snapshot: false,
                    rw_timestamp: 3 << 16,
                },
            });
            yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                seek_timestamp: (3 << 16) + 1,
                expected_timestamp: None,
            });
            yield CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionIdle);
            std::future::pending::<()>().await;
        }

        fn gated_subscription_data_stream_for_test()
        -> (CursorLifecycle, Self, Arc<AtomicBool>, oneshot::Sender<()>) {
            let waiting = Arc::new(AtomicBool::new(false));
            let (resume_tx, resume_rx) = oneshot::channel();
            (
                lifecycle_for_test(),
                Self {
                    inner: Self::gated_multi_epoch_events_for_test(waiting.clone(), resume_rx)
                        .boxed(),
                },
                waiting,
                resume_tx,
            )
        }
    }

    fn handler_args_for_test(session: Arc<SessionImpl>) -> HandlerArgs {
        let sql: Arc<str> = "select 1".into();
        let statement = Parser::parse_exactly_one(&sql).unwrap();
        HandlerArgs::new(session, &statement, sql).unwrap()
    }

    fn lifecycle_for_test() -> CursorLifecycle {
        let (_, session_shutdown_rx) = ShutdownToken::new();
        CursorLifecycle::new(session_shutdown_rx)
    }

    fn set_table_committed_epoch_for_test(
        session: &SessionImpl,
        table_id: TableId,
        committed_epoch: u64,
    ) {
        let snapshot_manager = session.env().hummock_snapshot_manager();
        let mut version = snapshot_manager.acquire().version().clone();
        version.id += 1;
        version.state_table_info.apply_delta(
            &HashMap::from_iter([(
                table_id,
                StateTableInfoDelta {
                    committed_epoch,
                    compaction_group_id: 0.into(),
                },
            )]),
            &HashSet::new(),
        );
        snapshot_manager.init(version);
    }

    impl SubscriptionCursor {
        fn subscription_cursor_for_test(
            lifecycle: CursorLifecycle,
            chunk_stream: SubscriptionCursorDataChunkStream,
            from_snapshot: bool,
        ) -> Self {
            let state = if from_snapshot {
                SubscriptionCursorState::Fetch {
                    from_snapshot: true,
                    rw_timestamp: 1 << 16,
                    expected_timestamp: None,
                    init_query_timer: Instant::now(),
                }
            } else {
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp: 1 << 16,
                    expected_timestamp: None,
                }
            };
            Self {
                lifecycle,
                cursor_name: "cur".to_owned(),
                subscription: Arc::new(SubscriptionCatalog {
                    name: "sub".to_owned(),
                    retention_seconds: 60,
                    ..Default::default()
                }),
                dependent_table_id: 0.into(),
                pg_response_stream: SubscriptionCursorPgResponseStream::new(
                    chunk_stream,
                    SubscriptionCursorDataChunkStream::subscription_fields_for_test()
                        .get_output_fields(),
                    state,
                    Instant::now() + Duration::from_secs(60),
                ),
                cursor_metrics: Arc::new(CursorMetrics::for_test()),
                last_fetch: Instant::now(),
            }
        }
    }

    fn assert_text_value(row: &Row, expected: &[u8]) {
        assert_eq!(row.values()[0].as_ref().unwrap().as_ref(), expected);
    }

    fn assert_binary_i32(row: &Row, expected: i32) {
        assert_eq!(
            row.values()[0].as_ref().unwrap().as_ref(),
            expected.to_be_bytes()
        );
    }

    async fn wait_for_flag_for_test(flag: &AtomicBool, message: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !flag.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(message);
    }

    async fn advance_time_for_test(duration: Duration) {
        #[cfg(madsim)]
        tokio::time::advance(duration);
        #[cfg(not(madsim))]
        tokio::time::advance(duration).await;
    }

    /// Covers ownership and teardown invariants for cursor-managed query streams throughout the
    /// cursor and session lifecycle.
    mod cursor_lifecycle_tests {
        use super::*;

        type TestChunkSender =
            mpsc::Sender<std::result::Result<DataChunk, risingwave_common::error::BoxedError>>;

        async fn add_pending_query_cursor_for_test(
            cursor_manager: &CursorManager,
            name: &str,
        ) -> (ShutdownToken, TestChunkSender) {
            let lifecycle = CursorLifecycle::new(cursor_manager.session_shutdown_token());
            let query_shutdown_rx = lifecycle.query_shutdown_token();
            let (chunk_tx, chunk_rx) = mpsc::channel(1);
            chunk_tx
                .try_send(Ok(DataChunk::from_pretty("i\n1")))
                .unwrap();
            let fields = vec![Field::with_name(DataType::Int32, "v")];
            let query_stream = CursorQueryStream::local(
                tokio_stream::wrappers::ReceiverStream::new(chunk_rx),
                lifecycle.query_shutdown_sender(),
            );
            cursor_manager
                .add_query_cursor(
                    name.to_owned(),
                    QueryCursor {
                        lifecycle,
                        pg_response_stream: QueryCursorPgResponseStream::new(
                            QueryCursorDataChunkStream::new(query_stream, fields.clone()),
                            fields,
                        ),
                    },
                )
                .await
                .unwrap();
            (query_shutdown_rx, chunk_tx)
        }

        async fn add_pending_subscription_cursor_for_test(
            cursor_manager: &CursorManager,
            args: &HandlerArgs,
            name: &str,
        ) -> (ShutdownToken, TestChunkSender) {
            let lifecycle = CursorLifecycle::new(cursor_manager.session_shutdown_token());
            let query_shutdown_rx = lifecycle.query_shutdown_token();
            let (chunk_tx, chunk_rx) = mpsc::channel(1);
            chunk_tx
                .try_send(Ok(DataChunk::from_pretty("i\n1")))
                .unwrap();
            let query_stream = CursorQueryStream::local(
                tokio_stream::wrappers::ReceiverStream::new(chunk_rx),
                lifecycle.query_shutdown_sender(),
            );
            let subscription = Arc::new(SubscriptionCatalog {
                name: name.to_owned(),
                retention_seconds: 60,
                ..Default::default()
            });
            let fields = SubscriptionCursorDataChunkStream::subscription_fields_for_test();
            let metrics = Arc::new(CursorMetrics::for_test());
            let stream = SubscriptionCursorDataChunkStream::new(
                subscription.clone(),
                0.into(),
                SubscriptionCursorHandlerContext::new(args),
                fields.clone(),
                SubscriptionCursorDataChunkStreamState::Fetch {
                    from_snapshot: true,
                    rw_timestamp: 1 << 16,
                    query_stream,
                    expected_timestamp: None,
                    init_query_timer: Instant::now(),
                },
                metrics.clone(),
                lifecycle.query_shutdown_sender(),
                lifecycle.query_shutdown_token(),
            );
            let cursor = SubscriptionCursor {
                lifecycle,
                cursor_name: name.to_owned(),
                subscription,
                dependent_table_id: 0.into(),
                pg_response_stream: SubscriptionCursorPgResponseStream::new(
                    stream,
                    fields.get_output_fields(),
                    SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: 1 << 16,
                        expected_timestamp: None,
                        init_query_timer: Instant::now(),
                    },
                    Instant::now() + Duration::from_secs(60),
                ),
                cursor_metrics: metrics,
                last_fetch: Instant::now(),
            };
            assert!(
                cursor_manager
                    .cursor_map
                    .lock()
                    .await
                    .insert(name.to_owned(), Cursor::Subscription(cursor))
                    .is_none()
            );
            (query_shutdown_rx, chunk_tx)
        }

        /// Verifies that dropping unfinished local streams owned by both query and subscription
        /// cursors signals their executor-shutdown tokens instead of leaving background work alive.
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

            let session = Arc::new(SessionImpl::mock());
            let args = handler_args_for_test(session);
            let subscription_lifecycle = lifecycle_for_test();
            let subscription_shutdown_rx = subscription_lifecycle.query_shutdown_token();
            let (subscription_chunk_tx, subscription_chunk_rx) = mpsc::channel(1);
            subscription_chunk_tx
                .try_send(Ok(DataChunk::from_pretty("i\n1")))
                .unwrap();
            let subscription_query_stream = CursorQueryStream::local(
                tokio_stream::wrappers::ReceiverStream::new(subscription_chunk_rx),
                subscription_lifecycle.query_shutdown_sender(),
            );
            let subscription = Arc::new(SubscriptionCatalog {
                name: "sub".to_owned(),
                retention_seconds: 60,
                ..Default::default()
            });
            let fields = SubscriptionCursorDataChunkStream::subscription_fields_for_test();
            let stream = SubscriptionCursorDataChunkStream::new(
                subscription,
                0.into(),
                SubscriptionCursorHandlerContext::new(&args),
                fields,
                SubscriptionCursorDataChunkStreamState::Fetch {
                    from_snapshot: true,
                    rw_timestamp: 1 << 16,
                    query_stream: subscription_query_stream,
                    expected_timestamp: None,
                    init_query_timer: Instant::now(),
                },
                Arc::new(CursorMetrics::for_test()),
                subscription_lifecycle.query_shutdown_sender(),
                subscription_lifecycle.query_shutdown_token(),
            );
            drop(stream);
            // The subscription wrapper must propagate its drop to the active snapshot query too.
            assert!(subscription_shutdown_rx.is_cancelled());
        }

        /// Verifies that session shutdown interrupts an in-flight `FETCH`, removes every query and
        /// subscription cursor, and terminates all of their unfinished local query executors.
        #[tokio::test]
        async fn test_session_shutdown_interrupts_fetch_and_drops_all_cursor_streams() {
            let session = Arc::new(SessionImpl::mock());
            let cursor_manager = session.get_cursor_manager();
            let args = handler_args_for_test(session.clone());

            let query_lifecycle = CursorLifecycle::new(cursor_manager.session_shutdown_token());
            let query_cursor_shutdown_rx = query_lifecycle.query_shutdown_token();
            let (query_chunk_tx, query_chunk_rx) = mpsc::channel(1);
            query_chunk_tx
                .try_send(Ok(DataChunk::from_pretty("i\n1")))
                .unwrap();
            let fields = vec![Field::with_name(DataType::Int32, "v")];
            let query_stream = CursorQueryStream::local(
                tokio_stream::wrappers::ReceiverStream::new(query_chunk_rx),
                query_lifecycle.query_shutdown_sender(),
            );
            let cursor = QueryCursor {
                lifecycle: query_lifecycle,
                pg_response_stream: QueryCursorPgResponseStream::new(
                    QueryCursorDataChunkStream::new(query_stream, fields.clone()),
                    fields,
                ),
            };
            cursor_manager
                .add_query_cursor("cur".to_owned(), cursor)
                .await
                .unwrap();

            // A `FULL` subscription owns and runs its snapshot query before any `FETCH`. Keep that
            // query pending so session shutdown must also terminate this second cursor-owned stream.
            let subscription_lifecycle =
                CursorLifecycle::new(cursor_manager.session_shutdown_token());
            let subscription_cursor_shutdown_rx = subscription_lifecycle.query_shutdown_token();
            let (subscription_chunk_tx, subscription_chunk_rx) = mpsc::channel(1);
            subscription_chunk_tx
                .try_send(Ok(DataChunk::from_pretty("i\n1")))
                .unwrap();
            let subscription_query_stream = CursorQueryStream::local(
                tokio_stream::wrappers::ReceiverStream::new(subscription_chunk_rx),
                subscription_lifecycle.query_shutdown_sender(),
            );
            let subscription = Arc::new(SubscriptionCatalog {
                name: "sub".to_owned(),
                retention_seconds: 60,
                ..Default::default()
            });
            let subscription_fields =
                SubscriptionCursorDataChunkStream::subscription_fields_for_test();
            let subscription_cursor_metrics = Arc::new(CursorMetrics::for_test());
            let subscription_chunk_stream = SubscriptionCursorDataChunkStream::new(
                subscription.clone(),
                0.into(),
                SubscriptionCursorHandlerContext::new(&args),
                subscription_fields.clone(),
                SubscriptionCursorDataChunkStreamState::Fetch {
                    from_snapshot: true,
                    rw_timestamp: 1 << 16,
                    query_stream: subscription_query_stream,
                    expected_timestamp: None,
                    init_query_timer: Instant::now(),
                },
                subscription_cursor_metrics.clone(),
                subscription_lifecycle.query_shutdown_sender(),
                subscription_lifecycle.query_shutdown_token(),
            );
            let subscription_cursor = SubscriptionCursor {
                lifecycle: subscription_lifecycle,
                cursor_name: "sub_cur".to_owned(),
                subscription,
                dependent_table_id: 0.into(),
                pg_response_stream: SubscriptionCursorPgResponseStream::new(
                    subscription_chunk_stream,
                    subscription_fields.get_output_fields(),
                    SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: 1 << 16,
                        expected_timestamp: None,
                        init_query_timer: Instant::now(),
                    },
                    Instant::now() + Duration::from_secs(60),
                ),
                cursor_metrics: subscription_cursor_metrics,
                last_fetch: Instant::now(),
            };
            assert!(
                cursor_manager
                    .cursor_map
                    .lock()
                    .await
                    .insert(
                        "sub_cur".to_owned(),
                        Cursor::Subscription(subscription_cursor),
                    )
                    .is_none()
            );

            let (query_cursor_2_shutdown_rx, _query_chunk_tx_2) =
                add_pending_query_cursor_for_test(&cursor_manager, "cur_2").await;
            let (subscription_cursor_2_shutdown_rx, _subscription_chunk_tx_2) =
                add_pending_subscription_cursor_for_test(&cursor_manager, &args, "sub_cur_2").await;

            for cursor_name in ["cur", "cur_2", "sub_cur", "sub_cur_2"] {
                let (rows, _) = tokio::time::timeout(
                    Duration::from_secs(1),
                    cursor_manager.get_rows_with_cursor(
                        cursor_name,
                        1,
                        args.clone(),
                        &vec![],
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    ),
                )
                .await
                .unwrap_or_else(|_| panic!("initial FETCH for {cursor_name} must finish"))
                .unwrap();
                assert_eq!(rows.len(), 1);
                assert_text_value(&rows[0], b"1");
            }
            // All four streams remain registered and pending after their initial buffered row.
            assert_eq!(cursor_manager.cursor_map.lock().await.len(), 4);

            let fetch_manager = cursor_manager.clone();
            let fetch = tokio::spawn(async move {
                fetch_manager
                    .get_rows_with_cursor(
                        "cur",
                        1,
                        args,
                        &vec![],
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if cursor_manager.cursor_map.try_lock().is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("FETCH must acquire the cursor map");

            cursor_manager.initiate_shutdown();
            let error = tokio::time::timeout(Duration::from_secs(1), fetch)
                .await
                .expect("session shutdown must wake FETCH")
                .unwrap()
                .expect_err("session shutdown must fail the active FETCH");
            // Session shutdown has a distinct error from cursor close and user cancellation.
            assert!(error.to_string().contains("Session ended"));
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if cursor_manager.cursor_map.lock().await.is_empty() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("session shutdown must drop every cursor-owned stream");
            // Clearing the map must drop both actively fetched and merely registered streams,
            // which propagates cancellation to every underlying executor.
            assert!(query_cursor_shutdown_rx.is_cancelled());
            assert!(subscription_cursor_shutdown_rx.is_cancelled());
            assert!(query_cursor_2_shutdown_rx.is_cancelled());
            assert!(subscription_cursor_2_shutdown_rx.is_cancelled());
        }

        /// Verifies the declaration-time difference between subscription modes: `FULL` retains its
        /// pinned snapshot epoch across a later catalog advance, while `SINCE` remains lazy until a
        /// positive-count `FETCH` starts its first log-store query.
        #[tokio::test]
        async fn test_subscription_declaration_lifecycles() {
            let session = Arc::new(SessionImpl::mock());
            let table_id = TableId::new(1);
            let declared_epoch = 1 << 16;
            let latest_epoch = 2 << 16;
            let snapshot_manager = session.env().hummock_snapshot_manager();
            snapshot_manager.add_table_for_test(table_id);
            set_table_committed_epoch_for_test(&session, table_id, declared_epoch);

            let declare_txn = session.txn_begin_implicit();
            let declared_snapshot = session.pinned_snapshot();
            drop(declare_txn);
            // Capture the epoch that a real `FULL` declaration binds to its snapshot query.
            assert_eq!(
                SubscriptionCursor::snapshot_epoch(&declared_snapshot, table_id).unwrap(),
                declared_epoch
            );

            // Model a `FULL` cursor whose snapshot stream was bound at declaration time. Advancing
            // the catalog before its first `FETCH` must not change the cursor's pinned epoch.
            let (lifecycle, stream, raw_stream_polled) =
                SubscriptionCursorDataChunkStream::subscription_data_stream_for_test(true);
            let mut full_cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, true);
            assert!(!raw_stream_polled.load(Ordering::Relaxed));

            set_table_committed_epoch_for_test(&session, table_id, latest_epoch);
            // Advancing the manager must not mutate the already-pinned declaration snapshot.
            assert_eq!(
                SubscriptionCursor::snapshot_epoch(&declared_snapshot, table_id).unwrap(),
                declared_epoch
            );
            let (rows, _) = full_cursor
                .fetch(
                    1,
                    handler_args_for_test(session.clone()),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");
            assert!(raw_stream_polled.load(Ordering::Relaxed));
            // The first `FETCH` still reports the epoch selected at `DECLARE`, not `latest_epoch`.
            assert!(matches!(
                full_cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: true,
                    rw_timestamp,
                    ..
                } if *rw_timestamp == declared_epoch
            ));

            let (lifecycle, stream, raw_stream_polled) =
                SubscriptionCursorDataChunkStream::subscription_data_stream_for_test(false);
            let mut since_cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, false);
            assert!(!raw_stream_polled.load(Ordering::Relaxed));
            let (rows, _) = since_cursor
                .fetch(
                    0,
                    handler_args_for_test(session.clone()),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert!(rows.is_empty());
            // `FETCH 0` is metadata-only and must not lazily start the `SINCE` query.
            assert!(!raw_stream_polled.load(Ordering::Relaxed));
            assert!(matches!(
                since_cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::InitLogStoreQuery { .. }
            ));

            let (rows, _) = since_cursor
                .fetch(
                    1,
                    handler_args_for_test(session),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");
            assert!(raw_stream_polled.load(Ordering::Relaxed));
            // Requesting data performs the lazy transition into the first log-store epoch.
            assert!(matches!(
                since_cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: false,
                    rw_timestamp,
                    ..
                } if *rw_timestamp == 1 << 16
            ));
        }

        /// Verifies that an expired subscription cursor rejects new reads and immediately shuts
        /// down its owned query rather than leaving the executor running after the error.
        #[tokio::test]
        async fn test_expired_subscription_cursor_shuts_down_its_query() {
            let session = Arc::new(SessionImpl::mock());
            let (lifecycle, stream, _) =
                SubscriptionCursorDataChunkStream::subscription_data_stream_for_test(true);
            let query_shutdown_rx = lifecycle.query_shutdown_token();
            let mut cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, true);
            cursor.pg_response_stream.expires_at = Instant::now() - Duration::from_secs(1);

            let error = cursor
                .fetch(
                    1,
                    handler_args_for_test(session),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .expect_err("an expired cursor must reject FETCH");
            assert!(error.to_string().contains("maximum lifetime"));
            // Expiration is terminal for the cursor, so its active query must be torn down too.
            assert!(query_shutdown_rx.is_cancelled());
        }

        /// Verifies that both a raw-stream error and an unexpected raw-stream end permanently
        /// invalidate a subscription cursor, record errors, and make later `FETCH` calls fail fast.
        #[tokio::test]
        async fn test_subscription_terminal_failures_invalidate_cursor() {
            for raw_error in [true, false] {
                let inner: BoxStream<
                    'static,
                    std::result::Result<CursorDataChunkEvent, BoxedError>,
                > = if raw_error {
                    futures::stream::iter([Err(std::io::Error::other("raw stream failed").into())])
                        .boxed()
                } else {
                    futures::stream::empty().boxed()
                };
                let stream = SubscriptionCursorDataChunkStream { inner };
                let lifecycle = lifecycle_for_test();
                let metrics = Arc::new(CursorMetrics::new(&prometheus::Registry::new()));
                let mut subscription =
                    SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, false);
                subscription.cursor_metrics = metrics.clone();
                let mut cursor = Cursor::Subscription(subscription);
                let session = Arc::new(SessionImpl::mock());

                let error = cursor
                    .fetch(
                        1,
                        handler_args_for_test(session.clone()),
                        &vec![],
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await
                    .expect_err("terminal subscription failure must fail FETCH");
                if raw_error {
                    assert!(error.to_string().contains("raw stream failed"));
                } else {
                    assert!(error.to_string().contains("terminated unexpectedly"));
                }
                let Cursor::Subscription(subscription) = &cursor else {
                    unreachable!()
                };
                // Either terminal source condition poisons the cursor because its ordered epoch
                // position can no longer be resumed safely.
                assert!(matches!(
                    subscription.pg_response_stream.subscription_state(),
                    SubscriptionCursorState::Invalid
                ));
                assert!(subscription.pg_response_stream.is_failed());
                assert_eq!(metrics.subscription_cursor_error_count.get(), 1);

                let error = cursor
                    .fetch(
                        1,
                        handler_args_for_test(session),
                        &vec![],
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await
                    .expect_err("an invalid subscription cursor must not resume");
                assert!(error.to_string().contains("close and recreate"));
                // Each rejected `FETCH`, including retries on an invalid cursor, is observable.
                assert_eq!(metrics.subscription_cursor_error_count.get(), 2);
            }
        }
    }

    /// Covers transactional `FETCH` progress and resumability for query and subscription cursors
    /// across interruptions and stream boundaries.
    mod fetch_progress_tests {
        use super::*;

        /// Verifies that a pre-cancelled query `FETCH` consumes nothing, a zero timeout returns
        /// only immediately available progress, and later format changes preserve row ordering
        /// through end-of-stream.
        #[tokio::test]
        async fn test_query_zero_timeout_after_cancel_preserves_progress_and_formats() {
            let session = Arc::new(SessionImpl::mock());
            let args = handler_args_for_test(session.clone());
            let (lifecycle, chunk_stream, fields) =
                QueryCursorDataChunkStream::query_data_stream_for_test();
            let mut cursor = QueryCursor {
                lifecycle,
                pg_response_stream: QueryCursorPgResponseStream::new(chunk_stream, fields),
            };

            let mut cancelled_fetch = FetchCursorCancelHandle::new();
            assert!(cancelled_fetch.cancel_tx.cancel());
            let error = cursor
                .fetch(3, &vec![], args.clone(), None, &mut cancelled_fetch)
                .await
                .expect_err("CancelRequest must terminate only this FETCH");
            assert!(error.to_string().contains("Cancelled by user"));
            session.clear_cancel_query_flag();

            // The cancelled attempt did not advance the cursor; nonblocking `FETCH` starts at 1.
            let (rows, _) = cursor
                .fetch(
                    3,
                    &vec![],
                    args.clone(),
                    Some(0),
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");

            // Changing to binary format must continue at row 2 without replaying or skipping data.
            let (rows, _) = cursor
                .fetch(
                    3,
                    &vec![Format::Binary],
                    args,
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            assert_binary_i32(&rows[0], 2);
            assert_binary_i32(&rows[1], 3);
            assert_binary_i32(&rows[2], 4);

            // Switching back to text drains only the two remaining rows.
            let (rows, _) = cursor
                .fetch(
                    10,
                    &vec![],
                    handler_args_for_test(session.clone()),
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_text_value(&rows[0], b"5");
            assert_text_value(&rows[1], b"6");

            let (rows, _) = cursor
                .fetch(
                    1,
                    &vec![],
                    handler_args_for_test(session),
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            // Repeated reads after `QueryEnd` stay empty rather than restarting the stream.
            assert!(rows.is_empty());
        }

        /// Verifies that cancellation while a query stream is pending between chunks rolls back
        /// tentative `FETCH` progress, and that later fetches resume the same stream without loss.
        #[tokio::test]
        async fn test_query_cancel_while_pending_resumes_without_loss() {
            let session = Arc::new(SessionImpl::mock());
            let (lifecycle, stream, fields, waiting, resume_tx) =
                QueryCursorDataChunkStream::gated_query_data_stream_for_test();
            let mut cursor = QueryCursor {
                lifecycle,
                pg_response_stream: QueryCursorPgResponseStream::new(stream, fields),
            };
            let fetch_session = session.clone();
            let fetch = tokio::spawn(async move {
                let result = cursor
                    .fetch(
                        6,
                        &vec![],
                        handler_args_for_test(fetch_session),
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await;
                (cursor, result)
            });
            wait_for_flag_for_test(&waiting, "FETCH must wait between query chunks").await;
            session.cancel_current_query();
            let (mut cursor, result) = fetch.await.unwrap();
            let error = result.expect_err("CancelRequest must fail the active FETCH");
            assert!(error.to_string().contains("Cancelled by user"));

            // Rows 1-2 were tentatively read before cancellation, so rollback must expose row 1
            // again to the next nonblocking `FETCH`.
            let (rows, _) = cursor
                .fetch(
                    6,
                    &vec![],
                    handler_args_for_test(session.clone()),
                    Some(0),
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");

            resume_tx.send(()).unwrap();
            // The cached row 2 and newly released rows 3-6 complete the original stream exactly
            // once, proving cancellation did not replace or terminate the cursor-owned query.
            let (rows, _) = cursor
                .fetch(
                    10,
                    &vec![Format::Binary],
                    handler_args_for_test(session),
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 5);
            for (row, expected) in rows.iter().zip_eq_fast([2, 3, 4, 5, 6]) {
                assert_binary_i32(row, expected);
            }
        }

        /// Verifies that a positive query `FETCH` timeout commits rows accumulated before the
        /// stream blocks, so the next `FETCH` continues after them rather than replaying them.
        #[tokio::test(start_paused = true)]
        async fn test_query_positive_timeout_commits_accumulated_rows() {
            let session = Arc::new(SessionImpl::mock());
            let (lifecycle, stream, fields, waiting, resume_tx) =
                QueryCursorDataChunkStream::gated_query_data_stream_for_test();
            let mut cursor = QueryCursor {
                lifecycle,
                pg_response_stream: QueryCursorPgResponseStream::new(stream, fields),
            };
            let fetch_session = session.clone();
            let fetch = tokio::spawn(async move {
                let result = cursor
                    .fetch(
                        6,
                        &vec![],
                        handler_args_for_test(fetch_session),
                        Some(1),
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await;
                (cursor, result)
            });
            wait_for_flag_for_test(&waiting, "FETCH must wait between query chunks").await;
            advance_time_for_test(Duration::from_secs(1)).await;
            let (mut cursor, result) = fetch.await.unwrap();
            let (rows, _) = result.unwrap();
            // A positive timeout returns and commits all rows available before the gate.
            assert_eq!(rows.len(), 2);
            assert_text_value(&rows[0], b"1");
            assert_text_value(&rows[1], b"2");

            resume_tx.send(()).unwrap();
            // Because timeout committed rows 1-2, resumption begins at row 3 with no replay.
            let (rows, _) = cursor
                .fetch(
                    10,
                    &vec![Format::Binary],
                    handler_args_for_test(session),
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
            for (row, expected) in rows.iter().zip_eq_fast([3, 4, 5, 6]) {
                assert_binary_i32(row, expected);
            }
        }

        /// Verifies subscription `FETCH` progression across several log-store epochs, including
        /// per-fetch format changes, foreground seek-position updates, row metrics, and stable
        /// nonblocking reads once the stream reaches `SubscriptionIdle`.
        #[tokio::test]
        async fn test_subscription_fetch_progress_across_epochs_formats_and_idle() {
            let session = Arc::new(SessionImpl::mock());
            let args = handler_args_for_test(session.clone());
            let (lifecycle, chunk_stream, raw_stream_polled) =
                SubscriptionCursorDataChunkStream::subscription_data_stream_for_test(false);
            let mut cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, chunk_stream, false);
            let cursor_metrics = Arc::new(CursorMetrics::new(&prometheus::Registry::new()));
            cursor.cursor_metrics = cursor_metrics.clone();
            let fetch_duration = cursor_metrics
                .subscription_cursor_fetch_duration
                .with_label_values(&[&cursor.subscription.name]);

            assert!(!raw_stream_polled.load(Ordering::Relaxed));
            let (rows, _) = cursor
                .fetch(
                    3,
                    args.clone(),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert!(raw_stream_polled.load(Ordering::Relaxed));
            assert_eq!(rows.len(), 3);
            for (row, expected) in rows.iter().zip_eq_fast([b"1", b"2", b"3"]) {
                assert_text_value(row, expected);
            }
            assert_eq!(fetch_duration.get_sample_count(), 3);
            // Row 3 belongs to the second epoch, so foreground state and resume PK must advance
            // together only after the successful `FETCH` commits.
            assert!(matches!(
                cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: false,
                    rw_timestamp,
                    ..
                } if *rw_timestamp == 2 << 16
            ));
            assert_text_value(
                cursor.pg_response_stream.seek_pk_row.as_ref().unwrap(),
                b"3",
            );

            let formats = vec![Format::Binary, Format::Text, Format::Binary];
            let (rows, _) = cursor
                .fetch(
                    3,
                    args.clone(),
                    &formats,
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            for (row, expected) in rows.iter().zip_eq_fast([4, 5, 6]) {
                assert_binary_i32(row, expected);
            }
            assert_eq!(fetch_duration.get_sample_count(), 6);
            // The resume key is encoded with the format of the successful `FETCH` that committed it.
            assert_text_value(
                cursor.pg_response_stream.seek_pk_row.as_ref().unwrap(),
                &6_i32.to_be_bytes(),
            );

            // Idle is a stable, non-terminal boundary: repeated nonblocking fetches return no rows
            // and therefore add no row-duration metric samples.
            for _ in 0..2 {
                let (rows, _) = cursor
                    .fetch(
                        1,
                        args.clone(),
                        &vec![],
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await
                    .unwrap();
                assert!(rows.is_empty());
            }
            assert_eq!(fetch_duration.get_sample_count(), 6);
        }

        /// Verifies that cancellation while the third subscription query is pending rolls back all
        /// tentative rows, epoch transitions, and seek-key progress, then resumes the same stream
        /// without losing or duplicating data.
        #[tokio::test]
        async fn test_subscription_cancel_while_pending_rolls_back_epochs_and_rows() {
            let session = Arc::new(SessionImpl::mock());
            let (lifecycle, stream, waiting, resume_tx) =
                SubscriptionCursorDataChunkStream::gated_subscription_data_stream_for_test();
            let mut cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, false);
            let fetch_session = session.clone();
            let fetch = tokio::spawn(async move {
                let result = cursor
                    .fetch(
                        6,
                        handler_args_for_test(fetch_session),
                        &vec![],
                        None,
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await;
                (cursor, result)
            });
            wait_for_flag_for_test(&waiting, "subscription FETCH must wait for its third query")
                .await;
            session.cancel_current_query();
            let (mut cursor, result) = fetch.await.unwrap();
            let error = result.expect_err("CancelRequest must fail the active FETCH");
            assert!(error.to_string().contains("Cancelled by user"));
            // Although rows 1-4 and two epoch barriers were consumed tentatively, cancellation
            // restores the foreground position to the original epoch with no committed seek key.
            assert!(cursor.pg_response_stream.seek_pk_row.is_none());
            assert!(matches!(
                cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::InitLogStoreQuery {
                    seek_timestamp,
                    ..
                } if *seek_timestamp == 1 << 16
            ));

            // Rollback makes row 1 visible again; committing it establishes the first resume key.
            let (rows, _) = cursor
                .fetch(
                    6,
                    handler_args_for_test(session.clone()),
                    &vec![],
                    Some(0),
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");
            assert_text_value(
                cursor.pg_response_stream.seek_pk_row.as_ref().unwrap(),
                b"1",
            );

            resume_tx.send(()).unwrap();
            // Cached rows 2-4 plus released rows 5-6 finish the original stream exactly once.
            let formats = vec![Format::Binary, Format::Text, Format::Binary];
            let (rows, _) = cursor
                .fetch(
                    10,
                    handler_args_for_test(session),
                    &formats,
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 5);
            for (row, expected) in rows.iter().zip_eq_fast([2, 3, 4, 5, 6]) {
                assert_binary_i32(row, expected);
            }
        }

        /// Verifies that a positive subscription `FETCH` timeout commits accumulated rows and
        /// completed epoch progress while retaining the in-progress third query for resumption.
        #[tokio::test(start_paused = true)]
        async fn test_subscription_positive_timeout_commits_accumulated_epochs() {
            let session = Arc::new(SessionImpl::mock());
            let (lifecycle, stream, waiting, resume_tx) =
                SubscriptionCursorDataChunkStream::gated_subscription_data_stream_for_test();
            let mut cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, false);
            let fetch_session = session.clone();
            let fetch = tokio::spawn(async move {
                let result = cursor
                    .fetch(
                        6,
                        handler_args_for_test(fetch_session),
                        &vec![],
                        Some(1),
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await;
                (cursor, result)
            });
            wait_for_flag_for_test(&waiting, "subscription FETCH must wait for its third query")
                .await;
            advance_time_for_test(Duration::from_secs(1)).await;
            let (mut cursor, result) = fetch.await.unwrap();
            let (rows, _) = result.unwrap();
            // Rows 1-4 and their two completed epochs are committed when the positive timeout fires.
            assert_eq!(rows.len(), 4);
            for (row, expected) in rows.iter().zip_eq_fast([b"1", b"2", b"3", b"4"]) {
                assert_text_value(row, expected);
            }
            assert_text_value(
                cursor.pg_response_stream.seek_pk_row.as_ref().unwrap(),
                b"4",
            );
            // The third query has started, but its rows remain pending; this is the exact state the
            // next `FETCH` must resume rather than starting a replacement query.
            assert!(matches!(
                cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::Fetch { rw_timestamp, .. }
                    if *rw_timestamp == 3 << 16
            ));

            resume_tx.send(()).unwrap();
            // Committed rows are not replayed; only the third query's rows remain.
            let formats = vec![Format::Binary, Format::Text, Format::Binary];
            let (rows, _) = cursor
                .fetch(
                    10,
                    handler_args_for_test(session),
                    &formats,
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_binary_i32(&rows[0], 5);
            assert_binary_i32(&rows[1], 6);
        }

        /// Verifies that cancelling a subscription `FETCH` while it waits at idle aborts only that
        /// foreground wait; after notification, the same raw stream can start the next query and
        /// deliver its first row.
        #[tokio::test]
        async fn test_subscription_cancel_while_idle_resumes_the_same_stream() {
            let waiting = Arc::new(AtomicBool::new(false));
            let (idle_resume_tx, idle_resume_rx) = oneshot::channel();
            let (query_resume_tx, query_resume_rx) = oneshot::channel();
            let stream = SubscriptionCursorDataChunkStream {
                inner: SubscriptionCursorDataChunkStream::idle_then_resume_for_test(
                    waiting.clone(),
                    idle_resume_rx,
                    query_resume_rx,
                )
                .boxed(),
            };
            let lifecycle = lifecycle_for_test();
            let mut cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, false);
            let session = Arc::new(SessionImpl::mock());

            let (rows, _) = cursor
                .fetch(
                    1,
                    handler_args_for_test(session.clone()),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            // A non-waiting `FETCH` observes idle as an empty successful boundary.
            assert!(rows.is_empty());

            let fetch_session = session.clone();
            let fetch = tokio::spawn(async move {
                let result = cursor
                    .fetch(
                        1,
                        handler_args_for_test(fetch_session),
                        &vec![],
                        Some(60),
                        &mut FetchCursorCancelHandle::new(),
                    )
                    .await;
                (cursor, result)
            });
            wait_for_flag_for_test(&waiting, "subscription FETCH must wait at idle").await;
            session.cancel_current_query();
            let (mut cursor, result) = fetch.await.unwrap();
            let error = result.expect_err("CancelRequest must interrupt the idle wait");
            assert!(error.to_string().contains("Cancelled by user"));

            // Wake the existing idle stream and its next query; cancellation must not have dropped
            // either continuation.
            idle_resume_tx.send(()).unwrap();
            query_resume_tx.send(()).unwrap();
            let (rows, _) = cursor
                .fetch(
                    1,
                    handler_args_for_test(session),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_text_value(&rows[0], b"1");
            // The resumed stream advances normally into its first log-store epoch.
            assert!(matches!(
                cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: false,
                    rw_timestamp,
                    ..
                } if *rw_timestamp == 1 << 16
            ));
        }

        /// Verifies that a changed upstream schema ends one `FETCH`, both with accumulated rows
        /// and with zero rows at the boundary, before the next `FETCH` uses the new row description.
        #[tokio::test]
        async fn test_subscription_schema_change_ends_fetch_and_updates_fields() {
            let initial_catalog = TableCatalog {
                columns: vec![ColumnCatalog::visible(ColumnDesc::named(
                    "v",
                    1.into(),
                    DataType::Int32,
                ))],
                pk: vec![ColumnOrder::new(0, OrderType::ascending())],
                ..Default::default()
            };
            let changed_catalog = TableCatalog {
                columns: vec![
                    ColumnCatalog::visible(ColumnDesc::named("v", 1.into(), DataType::Int32)),
                    ColumnCatalog::visible(ColumnDesc::named("v2", 2.into(), DataType::Int32)),
                ],
                pk: vec![ColumnOrder::new(0, OrderType::ascending())],
                ..Default::default()
            };
            let initial_fields = FieldsManager::new(&initial_catalog);
            let mut changed_fields = initial_fields.clone();
            // Re-reading the same catalog is a no-op, while the added visible column must rebuild
            // the output description before the next row is exposed.
            assert!(!changed_fields.try_refill_fields(&initial_catalog));
            assert!(changed_fields.try_refill_fields(&changed_catalog));
            assert_eq!(
                changed_fields
                    .get_output_fields()
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["v", "v2", "op", "rw_timestamp"]
            );

            let initial_timestamp = 1 << 16;
            let changed_timestamp = 2 << 16;
            let changed_output_fields = changed_fields.get_output_fields();
            let events = vec![
                CursorDataChunkEvent::Chunk(CursorDataChunk {
                    chunk: DataChunk::from_pretty(
                        "i
                     1
                     2",
                    ),
                    metadata: CursorDataChunkMetadata::Subscription {
                        fields: Arc::new(initial_fields.clone()),
                        from_snapshot: true,
                        rw_timestamp: initial_timestamp,
                    },
                }),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionNewEpoch {
                    seek_timestamp: initial_timestamp + 1,
                    expected_timestamp: None,
                }),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SubscriptionQueryStarted {
                    from_snapshot: false,
                    rw_timestamp: changed_timestamp,
                    expected_timestamp: None,
                    init_query_timer: Instant::now(),
                    output_fields: changed_output_fields,
                    expires_at: Instant::now() + Duration::from_secs(60),
                }),
                CursorDataChunkEvent::Barrier(CursorDataChunkBarrier::SchemaChanged),
                CursorDataChunkEvent::Chunk(CursorDataChunk {
                    chunk: DataChunk::from_pretty(
                        "i i T
                     2 20 Insert",
                    ),
                    metadata: CursorDataChunkMetadata::Subscription {
                        fields: Arc::new(changed_fields.clone()),
                        from_snapshot: false,
                        rw_timestamp: changed_timestamp,
                    },
                }),
            ];
            let accumulated_rows_events = events.clone();
            let lifecycle = lifecycle_for_test();
            let (chunk_stream, _) = SubscriptionCursorDataChunkStream::for_test(events);
            let mut cursor = SubscriptionCursor {
                lifecycle,
                cursor_name: "cur".to_owned(),
                subscription: Arc::new(SubscriptionCatalog {
                    name: "sub".to_owned(),
                    retention_seconds: 60,
                    ..Default::default()
                }),
                dependent_table_id: 0.into(),
                pg_response_stream: SubscriptionCursorPgResponseStream::new(
                    chunk_stream,
                    initial_fields.get_output_fields(),
                    SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: initial_timestamp,
                        expected_timestamp: None,
                        init_query_timer: Instant::now(),
                    },
                    Instant::now() + Duration::from_secs(60),
                ),
                cursor_metrics: Arc::new(CursorMetrics::for_test()),
                last_fetch: Instant::now(),
            };
            let session = Arc::new(SessionImpl::mock());
            let args = handler_args_for_test(session);
            let formats = vec![];

            let (rows, fields) = cursor
                .fetch(
                    2,
                    args.clone(),
                    &formats,
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(fields.len(), 3);
            assert_text_value(&rows[0], b"1");
            assert_text_value(&rows[1], b"2");

            // With no accumulated rows in this `FETCH`, the schema barrier returns an empty result
            // using the old description while atomically installing the new description internally.
            let (rows, fields) = cursor
                .fetch(
                    1,
                    args.clone(),
                    &formats,
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert!(rows.is_empty(), "schema change must end the current FETCH");
            assert_eq!(fields.len(), 3);
            assert_eq!(
                cursor
                    .pg_response_stream
                    .fields()
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["v", "v2", "op", "rw_timestamp"]
            );
            assert!(matches!(
                cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::Fetch {
                    from_snapshot: false,
                    rw_timestamp,
                    ..
                } if *rw_timestamp == changed_timestamp
            ));

            // The following `FETCH` is the first response allowed to advertise and encode the new
            // four-column row description.
            let (rows, fields) = cursor
                .fetch(1, args, &formats, None, &mut FetchCursorCancelHandle::new())
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(fields.len(), 4);
            assert_eq!(rows[0].values().len(), 4);
            assert_text_value(&rows[0], b"2");
            assert_eq!(rows[0].values()[1].as_ref().unwrap().as_ref(), b"20");
            assert_eq!(rows[0].values()[2].as_ref().unwrap().as_ref(), b"Insert");
            assert!(rows[0].values()[3].is_some());

            let lifecycle = lifecycle_for_test();
            let (chunk_stream, _) =
                SubscriptionCursorDataChunkStream::for_test(accumulated_rows_events);
            let mut cursor = SubscriptionCursor {
                lifecycle,
                cursor_name: "cur_with_accumulated_rows".to_owned(),
                subscription: Arc::new(SubscriptionCatalog {
                    name: "sub".to_owned(),
                    retention_seconds: 60,
                    ..Default::default()
                }),
                dependent_table_id: 0.into(),
                pg_response_stream: SubscriptionCursorPgResponseStream::new(
                    chunk_stream,
                    initial_fields.get_output_fields(),
                    SubscriptionCursorState::Fetch {
                        from_snapshot: true,
                        rw_timestamp: initial_timestamp,
                        expected_timestamp: None,
                        init_query_timer: Instant::now(),
                    },
                    Instant::now() + Duration::from_secs(60),
                ),
                cursor_metrics: Arc::new(CursorMetrics::for_test()),
                last_fetch: Instant::now(),
            };
            let session = Arc::new(SessionImpl::mock());
            let args = handler_args_for_test(session);
            // If rows accumulated before the schema barrier, that `FETCH` returns those rows with
            // the old description but still commits the new description for its successor.
            let (rows, fields) = cursor
                .fetch(
                    10,
                    args.clone(),
                    &vec![],
                    None,
                    &mut FetchCursorCancelHandle::new(),
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(fields.len(), 3);
            assert_text_value(&rows[0], b"1");
            assert_text_value(&rows[1], b"2");
            assert_eq!(
                cursor
                    .pg_response_stream
                    .fields()
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["v", "v2", "op", "rw_timestamp"]
            );

            // The next response then uses the already-committed schema and exposes the new column.
            let (rows, fields) = cursor
                .fetch(1, args, &vec![], None, &mut FetchCursorCancelHandle::new())
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(fields.len(), 4);
            assert_text_value(&rows[0], b"2");
            assert_eq!(rows[0].values()[1].as_ref().unwrap().as_ref(), b"20");
        }

        /// Verifies that an already-cancelled subscription `FETCH` fails before polling its raw
        /// stream and leaves metrics, seek-key progress, and subscription state untouched.
        #[tokio::test]
        async fn test_pre_cancelled_subscription_fetch_does_not_poll_or_advance() {
            let session = Arc::new(SessionImpl::mock());
            let (lifecycle, stream, polled) =
                SubscriptionCursorDataChunkStream::subscription_data_stream_for_test(false);
            let mut cursor =
                SubscriptionCursor::subscription_cursor_for_test(lifecycle, stream, false);
            let metrics = Arc::new(CursorMetrics::new(&prometheus::Registry::new()));
            cursor.cursor_metrics = metrics.clone();
            let fetch_duration = metrics
                .subscription_cursor_fetch_duration
                .with_label_values(&[&cursor.subscription.name]);
            let mut cancel_handle = FetchCursorCancelHandle::new();
            assert!(cancel_handle.cancel_tx.cancel());

            let error = cursor
                .fetch(
                    6,
                    handler_args_for_test(session),
                    &vec![],
                    None,
                    &mut cancel_handle,
                )
                .await
                .expect_err("a pre-cancelled FETCH must fail before polling data");
            assert!(error.to_string().contains("Cancelled by user"));
            // The biased cancellation branch must win before any stream or foreground-state work.
            assert!(!polled.load(Ordering::Relaxed));
            assert_eq!(fetch_duration.get_sample_count(), 0);
            assert!(cursor.pg_response_stream.seek_pk_row.is_none());
            assert!(matches!(
                cursor.pg_response_stream.subscription_state(),
                SubscriptionCursorState::InitLogStoreQuery { .. }
            ));
        }
    }
}
