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

use std::fmt::Formatter;
use std::pin::Pin;

use futures::{Future, FutureExt, Stream, StreamExt};
use risingwave_sqlparser::ast::Statement;

use crate::error::PsqlError;
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_protocol::ParameterStatus;
use crate::pg_server::BoxedError;
use crate::types::{Format, Row};

pub type RowSet = Vec<Row>;
pub type RowSetResult = Result<RowSet, BoxedError>;

pub trait ValuesStream = Stream<Item = RowSetResult> + Unpin + Send;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[expect(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum StatementType {
    INSERT,
    INSERT_RETURNING,
    DELETE,
    DELETE_RETURNING,
    UPDATE,
    UPDATE_RETURNING,
    SELECT,
    MOVE,
    FETCH,
    COPY,
    EXPLAIN,
    CLOSE_CURSOR,
    CREATE_TABLE,
    CREATE_MATERIALIZED_VIEW,
    CREATE_VIEW,
    CREATE_SOURCE,
    CREATE_SINK,
    CREATE_SUBSCRIPTION,
    CREATE_DATABASE,
    CREATE_SCHEMA,
    CREATE_USER,
    CREATE_INDEX,
    CREATE_AGGREGATE,
    CREATE_FUNCTION,
    CREATE_CONNECTION,
    CREATE_SECRET,
    COMMENT,
    DECLARE_CURSOR,
    DESCRIBE,
    GRANT_PRIVILEGE,
    DISCARD,
    DROP_TABLE,
    DROP_MATERIALIZED_VIEW,
    DROP_VIEW,
    DROP_INDEX,
    DROP_FUNCTION,
    DROP_AGGREGATE,
    DROP_SOURCE,
    DROP_SINK,
    DROP_SUBSCRIPTION,
    DROP_SCHEMA,
    DROP_DATABASE,
    DROP_USER,
    DROP_CONNECTION,
    DROP_SECRET,
    ALTER_DATABASE,
    ALTER_DEFAULT_PRIVILEGES,
    ALTER_SCHEMA,
    ALTER_INDEX,
    ALTER_VIEW,
    ALTER_TABLE,
    ALTER_MATERIALIZED_VIEW,
    ALTER_SINK,
    ALTER_SUBSCRIPTION,
    ALTER_SOURCE,
    ALTER_FUNCTION,
    ALTER_CONNECTION,
    ALTER_SYSTEM,
    ALTER_SECRET,
    ALTER_FRAGMENT,
    ALTER_COMPACTION_GROUP,
    REVOKE_PRIVILEGE,
    REASSIGN_OWNED,
    DROP_OWNED,
    // Introduce ORDER_BY statement type cuz Calcite unvalidated AST has SqlKind.ORDER_BY. Note
    // that Statement Type is not designed to be one to one mapping with SqlKind.
    ORDER_BY,
    SET_VARIABLE,
    SHOW_VARIABLE,
    SHOW_COMMAND,
    START_TRANSACTION,
    UPDATE_USER,
    ABORT,
    FLUSH,
    REFRESH_TABLE,
    OTHER,
    // EMPTY is used when query statement is empty (e.g. ";").
    EMPTY,
    BEGIN,
    COMMIT,
    ROLLBACK,
    SET_TRANSACTION,
    CANCEL_COMMAND,
    FETCH_CURSOR,
    WAIT,
    KILL,
    BACKUP,
    DELETE_META_SNAPSHOTS,
    RECOVER,
    USE,
    PREPARE,
    DEALLOCATE,
    VACUUM,
}

impl std::fmt::Display for StatementType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub trait Callback = Future<Output = Result<(), BoxedError>> + Send;

pub type BoxedCallback = Pin<Box<dyn Callback>>;

pub struct PgResponse<VS> {
    stmt_type: StatementType,
    is_copy_query_to_stdout: bool,

    // row count of affected row. Used for INSERT, UPDATE, DELETE, COPY, and other statements that
    // don't return rows.
    row_cnt: Option<i32>,
    // Used for INSERT, UPDATE, DELETE to specify the format of the affected row count.
    row_cnt_format: Option<Format>,
    notices: Vec<String>,
    values_stream: Option<VS>,
    callback: Option<BoxedCallback>,
    row_desc: Vec<PgFieldDescriptor>,
    status: ParameterStatus,
}

pub struct PgResponseBuilder<VS> {
    stmt_type: StatementType,
    // row count of affected row. Used for INSERT, UPDATE, DELETE, COPY, and other statements that
    // don't return rows.
    row_cnt: Option<i32>,
    // Used for INSERT, UPDATE, DELETE to specify the format of the affected row count.
    row_cnt_format: Option<Format>,
    notices: Vec<String>,
    values_stream: Option<VS>,
    callback: Option<BoxedCallback>,
    row_desc: Vec<PgFieldDescriptor>,
    status: ParameterStatus,
}

impl<VS> From<PgResponseBuilder<VS>> for PgResponse<VS> {
    fn from(builder: PgResponseBuilder<VS>) -> Self {
        Self {
            stmt_type: builder.stmt_type,
            is_copy_query_to_stdout: false, // set a false from builder, alter later
            row_cnt: builder.row_cnt,
            row_cnt_format: builder.row_cnt_format,
            notices: builder.notices,
            values_stream: builder.values_stream,
            callback: builder.callback,
            row_desc: builder.row_desc,
            status: builder.status,
        }
    }
}

impl<VS> PgResponseBuilder<VS> {
    pub fn empty(stmt_type: StatementType) -> Self {
        let row_cnt = if stmt_type.is_query() { None } else { Some(0) };
        Self {
            stmt_type,
            row_cnt,
            row_cnt_format: None,
            notices: vec![],
            values_stream: None,
            callback: None,
            row_desc: vec![],
            status: Default::default(),
        }
    }

    pub fn row_cnt(self, row_cnt: i32) -> Self {
        Self {
            row_cnt: Some(row_cnt),
            ..self
        }
    }

    pub fn row_cnt_opt(self, row_cnt: Option<i32>) -> Self {
        Self { row_cnt, ..self }
    }

    pub fn row_cnt_format_opt(self, row_cnt_format: Option<Format>) -> Self {
        Self {
            row_cnt_format,
            ..self
        }
    }

    pub fn values(self, values_stream: VS, row_desc: Vec<PgFieldDescriptor>) -> Self {
        Self {
            values_stream: Some(values_stream),
            row_desc,
            ..self
        }
    }

    pub fn callback(self, callback: impl Callback + 'static) -> Self {
        Self {
            callback: Some(callback.boxed()),
            ..self
        }
    }

    pub fn notice(self, notice: impl ToString) -> Self {
        let mut notices = self.notices;
        notices.push(notice.to_string());
        Self { notices, ..self }
    }

    pub fn status(self, status: ParameterStatus) -> Self {
        Self { status, ..self }
    }
}

impl<VS> std::fmt::Debug for PgResponse<VS> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgResponse")
            .field("stmt_type", &self.stmt_type)
            .field("row_cnt", &self.row_cnt)
            .field("notices", &self.notices)
            .field("row_desc", &self.row_desc)
            .finish()
    }
}

impl StatementType {
    pub fn infer_from_statement(stmt: &Statement) -> Result<Self, String> {
        match stmt {
            Statement::Query(_) => Ok(StatementType::SELECT),
            Statement::Insert { returning, .. } => {
                if returning.is_empty() {
                    Ok(StatementType::INSERT)
                } else {
                    Ok(StatementType::INSERT_RETURNING)
                }
            }
            Statement::Delete { returning, .. } => {
                if returning.is_empty() {
                    Ok(StatementType::DELETE)
                } else {
                    Ok(StatementType::DELETE_RETURNING)
                }
            }
            Statement::Update { returning, .. } => {
                if returning.is_empty() {
                    Ok(StatementType::UPDATE)
                } else {
                    Ok(StatementType::UPDATE_RETURNING)
                }
            }
            Statement::Copy { .. } => Ok(StatementType::COPY),
            Statement::CreateTable { .. } => Ok(StatementType::CREATE_TABLE),
            Statement::CreateIndex { .. } => Ok(StatementType::CREATE_INDEX),
            Statement::CreateSchema { .. } => Ok(StatementType::CREATE_SCHEMA),
            Statement::CreateSource { .. } => Ok(StatementType::CREATE_SOURCE),
            Statement::CreateSink { .. } => Ok(StatementType::CREATE_SINK),
            Statement::CreateFunction { .. } => Ok(StatementType::CREATE_FUNCTION),
            Statement::CreateDatabase { .. } => Ok(StatementType::CREATE_DATABASE),
            Statement::CreateUser { .. } => Ok(StatementType::CREATE_USER),
            Statement::CreateView { materialized, .. } => {
                if *materialized {
                    Ok(StatementType::CREATE_MATERIALIZED_VIEW)
                } else {
                    Ok(StatementType::CREATE_VIEW)
                }
            }
            Statement::AlterTable { .. } => Ok(StatementType::ALTER_TABLE),
            Statement::AlterSystem { .. } => Ok(StatementType::ALTER_SYSTEM),
            Statement::AlterFragment { .. } => Ok(StatementType::ALTER_FRAGMENT),
            Statement::AlterCompactionGroup { .. } => Ok(StatementType::ALTER_COMPACTION_GROUP),
            Statement::DropFunction { .. } => Ok(StatementType::DROP_FUNCTION),
            Statement::Discard(..) => Ok(StatementType::DISCARD),
            Statement::SetVariable { .. } => Ok(StatementType::SET_VARIABLE),
            Statement::ShowVariable { .. } => Ok(StatementType::SHOW_VARIABLE),
            Statement::StartTransaction { .. } => Ok(StatementType::START_TRANSACTION),
            Statement::Begin { .. } => Ok(StatementType::BEGIN),
            Statement::Abort => Ok(StatementType::ABORT),
            Statement::Commit { .. } => Ok(StatementType::COMMIT),
            Statement::Rollback { .. } => Ok(StatementType::ROLLBACK),
            Statement::Grant { .. } => Ok(StatementType::GRANT_PRIVILEGE),
            Statement::Revoke { .. } => Ok(StatementType::REVOKE_PRIVILEGE),
            Statement::ReassignOwned { .. } => Ok(StatementType::REASSIGN_OWNED),
            Statement::DropOwned { .. } => Ok(StatementType::DROP_OWNED),
            Statement::Describe { .. } => Ok(StatementType::DESCRIBE),
            Statement::ShowCreateObject { .. } | Statement::ShowObjects { .. } => {
                Ok(StatementType::SHOW_COMMAND)
            }
            Statement::Drop(stmt) => match stmt.object_type {
                risingwave_sqlparser::ast::ObjectType::Table => Ok(StatementType::DROP_TABLE),
                risingwave_sqlparser::ast::ObjectType::View => Ok(StatementType::DROP_VIEW),
                risingwave_sqlparser::ast::ObjectType::MaterializedView => {
                    Ok(StatementType::DROP_MATERIALIZED_VIEW)
                }
                risingwave_sqlparser::ast::ObjectType::Index => Ok(StatementType::DROP_INDEX),
                risingwave_sqlparser::ast::ObjectType::Schema => Ok(StatementType::DROP_SCHEMA),
                risingwave_sqlparser::ast::ObjectType::Source => Ok(StatementType::DROP_SOURCE),
                risingwave_sqlparser::ast::ObjectType::Sink => Ok(StatementType::DROP_SINK),
                risingwave_sqlparser::ast::ObjectType::Database => Ok(StatementType::DROP_DATABASE),
                risingwave_sqlparser::ast::ObjectType::User => Ok(StatementType::DROP_USER),
                risingwave_sqlparser::ast::ObjectType::Connection => {
                    Ok(StatementType::DROP_CONNECTION)
                }
                risingwave_sqlparser::ast::ObjectType::Secret => Ok(StatementType::DROP_SECRET),
                risingwave_sqlparser::ast::ObjectType::Subscription => {
                    Ok(StatementType::DROP_SUBSCRIPTION)
                }
            },
            Statement::Explain { .. } => Ok(StatementType::EXPLAIN),
            Statement::DeclareCursor { .. } => Ok(StatementType::DECLARE_CURSOR),
            Statement::FetchCursor { .. } => Ok(StatementType::FETCH_CURSOR),
            Statement::CloseCursor { .. } => Ok(StatementType::CLOSE_CURSOR),
            Statement::Flush => Ok(StatementType::FLUSH),
            Statement::Wait(_) => Ok(StatementType::WAIT),
            Statement::Backup => Ok(StatementType::BACKUP),
            Statement::DeleteMetaSnapshots { .. } => Ok(StatementType::DELETE_META_SNAPSHOTS),
            Statement::Recover => Ok(StatementType::RECOVER),
            Statement::Use { .. } => Ok(StatementType::USE),
            Statement::Vacuum { .. } => Ok(StatementType::VACUUM),
            _ => Err("unsupported statement type".to_owned()),
        }
    }

    pub fn is_command(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT
                | StatementType::DELETE
                | StatementType::UPDATE
                | StatementType::MOVE
                | StatementType::COPY
                | StatementType::FETCH
                | StatementType::SELECT
                | StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }

    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT
                | StatementType::DELETE
                | StatementType::UPDATE
                | StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }

    pub fn is_query(&self) -> bool {
        matches!(
            self,
            StatementType::SELECT
                | StatementType::EXPLAIN
                | StatementType::SHOW_COMMAND
                | StatementType::SHOW_VARIABLE
                | StatementType::DESCRIBE
                | StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
                | StatementType::CANCEL_COMMAND
                | StatementType::BACKUP
                | StatementType::FETCH_CURSOR
        )
    }

    pub fn is_returning(&self) -> bool {
        matches!(
            self,
            StatementType::INSERT_RETURNING
                | StatementType::DELETE_RETURNING
                | StatementType::UPDATE_RETURNING
        )
    }
}

impl<VS> PgResponse<VS>
where
    VS: ValuesStream,
{
    pub fn builder(stmt_type: StatementType) -> PgResponseBuilder<VS> {
        PgResponseBuilder::empty(stmt_type)
    }

    pub fn empty_result(stmt_type: StatementType) -> Self {
        PgResponseBuilder::empty(stmt_type).into()
    }

    pub fn into_copy_query_to_stdout(mut self) -> Self {
        self.is_copy_query_to_stdout = true;
        self.stmt_type = StatementType::COPY;
        self
    }

    pub fn stmt_type(&self) -> StatementType {
        self.stmt_type
    }

    pub fn notices(&self) -> &[String] {
        &self.notices
    }

    pub fn status(&self) -> &ParameterStatus {
        &self.status
    }

    pub fn affected_rows_cnt(&self) -> Option<i32> {
        self.row_cnt
    }

    pub fn row_cnt_format(&self) -> Option<Format> {
        self.row_cnt_format
    }

    pub fn is_query(&self) -> bool {
        self.stmt_type.is_query()
    }

    pub fn is_empty(&self) -> bool {
        self.stmt_type == StatementType::EMPTY
    }

    pub fn is_copy_query_to_stdout(&self) -> bool {
        self.is_copy_query_to_stdout
    }

    pub fn row_desc(&self) -> &[PgFieldDescriptor] {
        &self.row_desc
    }

    pub fn values_stream(&mut self) -> &mut VS {
        self.values_stream.as_mut().expect("no values stream")
    }

    /// Run the callback if there is one.
    ///
    /// This should only be called after the values stream has been exhausted. Multiple calls to
    /// this function will be no-ops.
    pub async fn run_callback(&mut self) -> Result<(), PsqlError> {
        // Check if the stream is exhausted.
        if let Some(values_stream) = &mut self.values_stream {
            assert!(values_stream.next().await.is_none());
        }

        if let Some(callback) = self.callback.take() {
            callback.await.map_err(PsqlError::SimpleQueryError)?;
        }
        Ok(())
    }
}
