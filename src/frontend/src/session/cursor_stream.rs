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

//! Dormant cursor-stream types.
//!
//! These types are intentionally kept off the active cursor execution path in this PR. A
//! follow-up will wire them into query and subscription cursors after their representation can be
//! reviewed independently.

use std::sync::Weak;

use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::row::{OwnedRow, Row as _, RowExt as _};

use super::*;
use crate::handler::util::to_pg_rows;
use crate::utils::WithOptions;

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
    /// Typed primary-key row used to resume after the committed subscription position.
    subscription_seek_pk_owned_row: Option<OwnedRow>,
}

impl CursorDataChunk {
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
                            subscription_seek_pk_owned_row: None,
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
                    fields.get_row_stream_fields_and_formats(&formats.to_vec(), from_snapshot);
                let column_types = row_fields
                    .iter()
                    .map(|field| field.data_type())
                    .collect_vec();
                let raw_formats = if row_formats.is_empty() {
                    &[][..]
                } else {
                    &row_formats[..column_types.len()]
                };
                let subscription_seek_pk_owned_rows = self
                    .chunk
                    .rows()
                    .map(|row| row.project(&fields.row_pk_indices).to_owned_row())
                    .collect_vec();
                to_pg_rows(&column_types, self.chunk, raw_formats, session_data)?
                    .into_iter()
                    .zip_eq_fast(subscription_seek_pk_owned_rows)
                    .map(|(row, subscription_seek_pk_owned_row)| {
                        let mut row = SubscriptionCursor::build_row(
                            row.take(),
                            (!from_snapshot).then_some(rw_timestamp),
                            &row_formats,
                            session_data,
                        )?;
                        Ok(FormattedCursorRow {
                            row: row.project(&fields.row_output_col_indices),
                            subscription_seek_pk_owned_row: Some(subscription_seek_pk_owned_row),
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
    /// Starts one subscription batch for the dormant persistent-stream implementation.
    ///
    /// This is deliberately separate from the active cursor path until the follow-up PR wires the
    /// new stream types into `SubscriptionCursor`.
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
        let plan_result = SubscriptionCursor::init_batch_plan_for_subscription_cursor(
            rw_timestamp,
            dependent_table_id,
            handler_args.clone(),
            None,
        )?;
        let plan_fragmenter_result = gen_batch_plan_fragmenter(&handler_args.session, plan_result)?;
        let (query_stream, _) = create_cursor_query_stream(
            handler_args.session,
            plan_fragmenter_result,
            query_shutdown_tx,
            query_shutdown_rx,
            snapshot,
        )
        .await?;
        Ok((query_stream, init_query_timer, table_catalog))
    }

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
                    let (query_stream, init_query_timer, catalog) = Self::initiate_query(
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
        seek_pk_owned_row_to_commit: Option<OwnedRow>,
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
    fn update_when_subscription_row_yielded(&mut self, seek_pk_owned_row: OwnedRow) {
        match self {
            Self::Query { .. } => unreachable!("query fetch cannot update subscription state"),
            Self::Subscription {
                seek_pk_owned_row_to_commit,
                ..
            } => *seek_pk_owned_row_to_commit = Some(seek_pk_owned_row),
        }
    }
}

/// State shared by concrete query and subscription PostgreSQL response streams.
struct CursorPgResponseStreamInner<S> {
    /// Irreversible source of raw chunks and ordered cursor control barriers.
    data_stream: S,
    /// Rows formatted from the current raw chunk and not yet returned.
    current_formatted_rows: VecDeque<FormattedCursorRow>,
    /// PostgreSQL result formats selected by the active `FETCH`.
    formats: Vec<Format>,
    /// Session settings captured when the active `FETCH` began.
    session_data: Option<StaticSessionData>,
    /// Output fields visible to the cursor foreground.
    output_fields: Vec<Field>,
    /// Whether the raw data stream reported an unrecoverable failure.
    failed: bool,
    /// Whether the active `FETCH` reached one of its normal stream boundaries.
    fetch_stream_terminated: bool,
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
    fn new(data_stream: S, output_fields: Vec<Field>) -> Self {
        Self {
            data_stream,
            current_formatted_rows: VecDeque::new(),
            formats: vec![],
            session_data: None,
            output_fields,
            failed: false,
            fetch_stream_terminated: false,
        }
    }

    fn fields(&self) -> Vec<Field> {
        self.output_fields.clone()
    }

    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl) {
        self.formats = formats.to_vec();
        self.session_data = Some(StaticSessionData {
            timezone: session.config().timezone(),
        });
        self.fetch_stream_terminated = false;
    }
}

impl<S> CursorPgResponseStreamInner<S>
where
    S: Stream<Item = std::result::Result<CursorDataChunkEvent, BoxedError>> + Unpin,
{
    fn poll_next_item(&mut self, cx: &mut Context<'_>) -> Poll<Result<CursorPgResponsePollItem>> {
        loop {
            if let Some(row) = self.current_formatted_rows.pop_front() {
                return Poll::Ready(Ok(CursorPgResponsePollItem::Row(row)));
            }
            match self.data_stream.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(CursorDataChunkEvent::Chunk(chunk)))) => {
                    let session_data = self
                        .session_data
                        .as_ref()
                        .expect("cursor response stream must be inside a FETCH");
                    self.current_formatted_rows =
                        chunk.into_pg_rows(&self.formats, session_data)?.into();
                }
                Poll::Ready(Some(Ok(CursorDataChunkEvent::Barrier(barrier)))) => {
                    return Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(barrier)));
                }
                Poll::Ready(Some(Err(error))) => {
                    self.failed = true;
                    self.fetch_stream_terminated = true;
                    return Poll::Ready(Err(error.into()));
                }
                Poll::Ready(None) => {
                    self.fetch_stream_terminated = true;
                    return Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd));
                }
            }
        }
    }
}

/// A regular query cursor's persistent PostgreSQL response stream.
struct QueryCursorPgResponseStream {
    inner: CursorPgResponseStreamInner<QueryCursorDataChunkStream>,
}

impl QueryCursorPgResponseStream {
    fn new(data_stream: QueryCursorDataChunkStream, output_fields: Vec<Field>) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
        }
    }

    fn fields(&self) -> Vec<Field> {
        self.inner.fields()
    }

    fn begin_fetch(&mut self, formats: &[Format], session: &SessionImpl) {
        self.inner.begin_fetch(formats, session);
    }
}

impl Stream for QueryCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.fetch_stream_terminated {
            return Poll::Ready(None);
        }
        match this.inner.poll_next_item(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Row(row))) => Poll::Ready(Some(Ok(row.row))),
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(
                CursorDataChunkBarrier::QueryEnd,
            ))) => {
                this.inner.fetch_stream_terminated = true;
                Poll::Ready(None)
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::Barrier(_))) => {
                this.inner.fetch_stream_terminated = true;
                Poll::Ready(Some(Err(ErrorCode::InternalError(
                    "query cursor received a non-query barrier".to_owned(),
                )
                .into())))
            }
            Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => Poll::Ready(None),
        }
    }
}

/// A subscription cursor's persistent PostgreSQL response stream and committed metadata.
struct SubscriptionCursorPgResponseStream {
    inner: CursorPgResponseStreamInner<SubscriptionCursorDataChunkStream>,
    subscription_state: SubscriptionCursorState,
    seek_pk_owned_row: Option<OwnedRow>,
    expires_at: Instant,
    subscription_idle: bool,
    wait_for_data_when_idle: bool,
    yielded_rows: usize,
}

impl SubscriptionCursorPgResponseStream {
    fn new(
        data_stream: SubscriptionCursorDataChunkStream,
        output_fields: Vec<Field>,
        subscription_state: SubscriptionCursorState,
        expires_at: Instant,
    ) -> Self {
        Self {
            inner: CursorPgResponseStreamInner::new(data_stream, output_fields),
            subscription_state,
            seek_pk_owned_row: None,
            expires_at,
            subscription_idle: false,
            wait_for_data_when_idle: false,
            yielded_rows: 0,
        }
    }

    fn fields(&self) -> Vec<Field> {
        self.inner.fields()
    }

    fn subscription_state(&self) -> &SubscriptionCursorState {
        &self.subscription_state
    }

    fn is_expired(&self, now: Instant) -> bool {
        now > self.expires_at
    }

    fn is_failed(&self) -> bool {
        self.inner.failed
    }

    fn begin_fetch(
        &mut self,
        formats: &[Format],
        session: &SessionImpl,
        wait_for_data_when_idle: bool,
    ) {
        self.inner.begin_fetch(formats, session);
        self.wait_for_data_when_idle = wait_for_data_when_idle;
        self.yielded_rows = 0;
    }
}

impl Stream for SubscriptionCursorPgResponseStream {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.inner.fetch_stream_terminated {
                return Poll::Ready(None);
            }
            match this.inner.poll_next_item(cx) {
                Poll::Pending if this.subscription_idle => {
                    if this.yielded_rows > 0 || !this.wait_for_data_when_idle {
                        this.inner.fetch_stream_terminated = true;
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
                    subscription_seek_pk_owned_row,
                }))) => {
                    this.subscription_idle = false;
                    this.yielded_rows += 1;
                    if let Some(seek_pk_owned_row) = subscription_seek_pk_owned_row {
                        this.seek_pk_owned_row = Some(seek_pk_owned_row);
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
                            this.subscription_state = SubscriptionCursorState::Fetch {
                                from_snapshot,
                                rw_timestamp,
                                expected_timestamp,
                                init_query_timer,
                            };
                            this.inner.output_fields = output_fields;
                            this.expires_at = expires_at;
                            false
                        }
                        CursorDataChunkBarrier::SubscriptionNewEpoch {
                            seek_timestamp,
                            expected_timestamp,
                        } => {
                            this.subscription_state = SubscriptionCursorState::InitLogStoreQuery {
                                seek_timestamp,
                                expected_timestamp,
                            };
                            false
                        }
                        CursorDataChunkBarrier::SubscriptionIdle => {
                            this.subscription_idle = true;
                            this.yielded_rows > 0 || !this.wait_for_data_when_idle
                        }
                        CursorDataChunkBarrier::SubscriptionIdleEnded => false,
                    };
                    if should_finish {
                        this.inner.fetch_stream_terminated = true;
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Ok(CursorPgResponsePollItem::DataChunkStreamEnd)) => {
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

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunkTestExt;
    use tokio::sync::mpsc;

    use super::*;

    /// Verifies that subscription seek metadata preserves scalar types and declared primary-key
    /// order in an `OwnedRow` instead of deriving the position from PostgreSQL-formatted bytes.
    #[test]
    fn test_subscription_seek_pk_owned_row_preserves_typed_values() {
        let fields = Arc::new(FieldsManager {
            columns_catalog: vec![],
            row_fields: vec![
                Field::with_name(DataType::Int32, "value"),
                Field::with_name(DataType::Int64, "hidden_pk_a"),
                Field::with_name(DataType::Varchar, "hidden_pk_b"),
                Field::with_name(DataType::Varchar, "op"),
                Field::with_name(DataType::Int64, "rw_timestamp"),
            ],
            row_output_col_indices: vec![0, 3, 4],
            // Deliberately differs from table-column order to model `PRIMARY KEY (b, a)`.
            row_pk_indices: vec![2, 1],
            stream_chunk_row_indices: vec![0, 1, 2],
            op_index: 3,
        });
        let session_data = StaticSessionData {
            timezone: "UTC".to_owned(),
        };

        // A snapshot chunk contains the visible value followed by the two hidden PK columns; the
        // synthetic `op` and `rw_timestamp` output columns are not present in the raw chunk.
        let rows = CursorDataChunk {
            chunk: DataChunk::from_pretty("i I T\n42 7 key"),
            metadata: CursorDataChunkMetadata::Subscription {
                fields,
                from_snapshot: true,
                rw_timestamp: 0,
            },
        }
        .into_pg_rows(&[], &session_data)
        .unwrap();

        // `row_pk_indices = [2, 1]` models `PRIMARY KEY (hidden_pk_b, hidden_pk_a)`, so the seek
        // row must preserve both that declared order and the original Varchar/Int64 scalar types.
        assert_eq!(
            rows[0].subscription_seek_pk_owned_row,
            Some(OwnedRow::new(vec![Some("key".into()), Some(7_i64.into())]))
        );
    }

    /// Verifies that the dormant response pipeline can persist one raw query stream across
    /// multiple fetch-format initializations without losing buffered rows.
    #[tokio::test]
    async fn test_query_cursor_stream_persists_across_fetch_boundaries() {
        let session = SessionImpl::mock();
        let (query_shutdown_tx, _) = ShutdownToken::new();
        let (query_chunk_tx, query_chunk_rx) = mpsc::channel(2);
        query_chunk_tx
            .send(Ok(DataChunk::from_pretty("i\n1\n2")))
            .await
            .unwrap();
        query_chunk_tx
            .send(Ok(DataChunk::from_pretty("i\n3")))
            .await
            .unwrap();
        drop(query_chunk_tx);

        let fields = vec![Field::with_name(DataType::Int32, "v")];
        let query_stream = CursorQueryStream::local(
            tokio_stream::wrappers::ReceiverStream::new(query_chunk_rx),
            query_shutdown_tx,
        );
        let chunk_stream = QueryCursorDataChunkStream::new(query_stream, fields.clone());
        let mut response_stream = QueryCursorPgResponseStream::new(chunk_stream, fields);

        response_stream.begin_fetch(&[], &session);
        let row = response_stream.next().await.unwrap().unwrap();
        assert_eq!(row.values()[0].as_ref().unwrap().as_ref(), b"1");

        response_stream.begin_fetch(&[], &session);
        for expected in [b"2".as_slice(), b"3".as_slice()] {
            let row = response_stream.next().await.unwrap().unwrap();
            assert_eq!(row.values()[0].as_ref().unwrap().as_ref(), expected);
        }
        assert!(response_stream.next().await.is_none());
    }
}
