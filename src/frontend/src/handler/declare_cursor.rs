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

use std::sync::Arc;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_batch::task::{ShutdownSender, ShutdownToken};
use risingwave_common::catalog::Field;
use risingwave_common::session_config::QueryMode;
use risingwave_common::util::epoch::Epoch;
use risingwave_sqlparser::ast::{
    DeclareCursorStatement, Ident, ObjectName, Query, Since, Statement,
};

use super::RwPgResponse;
use super::query::{
    BatchPlanFragmenterResult, gen_batch_plan_by_statement, gen_batch_plan_fragmenter,
};
use super::util::convert_unix_millis_to_logstore_u64;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::handler::query::{distribute_execute_for_cursor, local_execute_for_cursor};
use crate::scheduler::ReadSnapshot;
use crate::session::SessionImpl;
use crate::session::cursor_manager::{CursorDataChunkStream, CursorQueryStream};
use crate::{Binder, OptimizerContext};

pub async fn handle_declare_cursor(
    handler_args: HandlerArgs,
    stmt: DeclareCursorStatement,
) -> Result<RwPgResponse> {
    match stmt.declare_cursor {
        risingwave_sqlparser::ast::DeclareCursor::Query(query) => {
            handle_declare_query_cursor(handler_args, stmt.cursor_name, query).await
        }
        risingwave_sqlparser::ast::DeclareCursor::Subscription(sub_name, rw_timestamp) => {
            handle_declare_subscription_cursor(
                handler_args,
                sub_name,
                stmt.cursor_name,
                rw_timestamp,
            )
            .await
        }
    }
}
pub async fn handle_declare_subscription_cursor(
    handler_args: HandlerArgs,
    sub_name: ObjectName,
    cursor_name: Ident,
    rw_timestamp: Since,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let subscription = {
        let db_name = &session.database();
        let (sub_schema_name, sub_name) =
            Binder::resolve_schema_qualified_name(db_name, &sub_name)?;
        session.get_subscription_by_name(sub_schema_name, &sub_name)?
    };
    // Start the first query of cursor, which includes querying the table and querying the subscription's logstore
    let start_rw_timestamp = match rw_timestamp {
        risingwave_sqlparser::ast::Since::TimestampMsNum(start_rw_timestamp) => {
            check_cursor_unix_millis(start_rw_timestamp, subscription.retention_seconds)?;
            Some(convert_unix_millis_to_logstore_u64(start_rw_timestamp))
        }
        risingwave_sqlparser::ast::Since::ProcessTime => Some(Epoch::now().0),
        risingwave_sqlparser::ast::Since::Begin => {
            let min_unix_millis =
                Epoch::now().as_unix_millis() - subscription.retention_seconds * 1000;
            let subscription_build_millis = subscription.created_at_epoch.unwrap().as_unix_millis();
            let min_unix_millis = std::cmp::max(min_unix_millis, subscription_build_millis);
            Some(convert_unix_millis_to_logstore_u64(min_unix_millis))
        }
        risingwave_sqlparser::ast::Since::Full => None,
    };
    // Create cursor based on the response
    if let Err(e) = session
        .get_cursor_manager()
        .add_subscription_cursor(
            cursor_name.real_value(),
            start_rw_timestamp,
            subscription.dependent_table_id,
            subscription,
            &handler_args,
        )
        .await
    {
        session
            .env()
            .cursor_metrics
            .subscription_cursor_error_count
            .inc();
        return Err(e);
    }

    Ok(PgResponse::empty_result(StatementType::DECLARE_CURSOR))
}

fn check_cursor_unix_millis(unix_millis: u64, retention_seconds: u64) -> Result<()> {
    let now = Epoch::now().as_unix_millis();
    let min_unix_millis = now - retention_seconds * 1000;
    if unix_millis > now {
        return Err(ErrorCode::CatalogError(
            "rw_timestamp is too large, need to be less than the current unix_millis"
                .to_owned()
                .into(),
        )
        .into());
    }
    if unix_millis < min_unix_millis {
        return Err(ErrorCode::CatalogError("rw_timestamp is too small, need to be large than the current unix_millis - subscription's retention time".to_owned().into()).into());
    }
    Ok(())
}

async fn handle_declare_query_cursor(
    handler_args: HandlerArgs,
    cursor_name: Ident,
    query: Box<Query>,
) -> Result<RwPgResponse> {
    let (chunk_stream, fields) =
        create_stream_for_cursor_stmt(handler_args.clone(), Statement::Query(query)).await?;
    handler_args
        .session
        .get_cursor_manager()
        .add_query_cursor(cursor_name.real_value(), chunk_stream, fields)
        .await?;
    Ok(PgResponse::empty_result(StatementType::DECLARE_CURSOR))
}

pub async fn handle_bound_declare_query_cursor(
    handler_args: HandlerArgs,
    cursor_name: Ident,
    plan_fragmenter_result: BatchPlanFragmenterResult,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let (chunk_stream, fields) =
        create_chunk_stream_for_cursor(session, plan_fragmenter_result).await?;

    handler_args
        .session
        .get_cursor_manager()
        .add_query_cursor(cursor_name.real_value(), chunk_stream, fields)
        .await?;
    Ok(PgResponse::empty_result(StatementType::DECLARE_CURSOR))
}

pub async fn create_stream_for_cursor_stmt(
    handler_args: HandlerArgs,
    stmt: Statement,
) -> Result<(CursorDataChunkStream, Vec<Field>)> {
    let session = handler_args.session.clone();
    let plan_fragmenter_result = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan_result =
            gen_batch_plan_by_statement(&session, context.into(), stmt)?.unwrap_rw()?;
        gen_batch_plan_fragmenter(&session, plan_result)?
    };
    create_chunk_stream_for_cursor(session, plan_fragmenter_result).await
}

pub async fn create_chunk_stream_for_cursor(
    session: Arc<SessionImpl>,
    plan_fragmenter_result: BatchPlanFragmenterResult,
) -> Result<(CursorDataChunkStream, Vec<Field>)> {
    let query_mode = plan_fragmenter_result.query_mode;
    let snapshot = session.pinned_snapshot();
    let (query_shutdown_tx, query_shutdown_rx) = ShutdownToken::new();
    let (query_stream, fields) = create_cursor_query_stream(
        session,
        plan_fragmenter_result,
        query_shutdown_tx,
        query_shutdown_rx,
        snapshot,
    )
    .await?;

    Ok((
        match query_mode {
            QueryMode::Auto => unreachable!(),
            QueryMode::Local => CursorDataChunkStream::LocalDataChunk(Some(query_stream)),
            QueryMode::Distributed => {
                CursorDataChunkStream::DistributedDataChunk(Some(query_stream))
            }
        },
        fields,
    ))
}

/// Creates a cursor-owned local or distributed query stream using an explicit snapshot and
/// cursor-scoped shutdown handle.
pub async fn create_cursor_query_stream(
    session: Arc<SessionImpl>,
    plan_fragmenter_result: BatchPlanFragmenterResult,
    query_shutdown_tx: ShutdownSender,
    query_shutdown_rx: ShutdownToken,
    snapshot: ReadSnapshot,
) -> Result<(CursorQueryStream, Vec<Field>)> {
    let BatchPlanFragmenterResult {
        plan_fragmenter,
        query_mode,
        schema,
        ..
    } = plan_fragmenter_result;

    let query = plan_fragmenter.generate_complete_query().await?;
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    Ok((
        match query_mode {
            QueryMode::Auto => unreachable!(),
            QueryMode::Local => CursorQueryStream::local(
                local_execute_for_cursor(session.clone(), query, query_shutdown_rx, snapshot)?,
                query_shutdown_tx,
            ),
            QueryMode::Distributed => CursorQueryStream::distributed(
                distribute_execute_for_cursor(session.clone(), query, snapshot).await?,
                session.env().query_manager().clone(),
            ),
        },
        schema.fields.clone(),
    ))
}
