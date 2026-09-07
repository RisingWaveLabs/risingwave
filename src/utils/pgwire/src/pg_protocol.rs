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

use std::any::Any;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::str::Utf8Error;
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};
use std::{io, str};

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::stream::StreamExt;
use itertools::Itertools;
use openssl::ssl::{SslAcceptor, SslContext, SslContextRef, SslMethod};
use risingwave_common::types::DataType;
use risingwave_common::util::deployment::Deployment;
use risingwave_common::util::env_var::env_var_is_true;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_common::util::query_log::*;
use risingwave_common::{PG_VERSION, SERVER_ENCODING, STANDARD_CONFORMING_STRINGS};
use risingwave_sqlparser::ast::{RedactSqlOptionKeywordsRef, Statement};
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_openssl::SslStream;
use tracing::Instrument;

use crate::error::{PsqlError, PsqlResult};
use crate::error_or_notice::Severity;
use crate::memory_manager::{MessageMemoryGuard, MessageMemoryManagerRef};
use crate::net::AddressRef;
use crate::pg_extended::ResultCache;
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeBindMessage, FeCancelMessage,
    FeCloseMessage, FeDescribeMessage, FeExecuteMessage, FeMessage, FeMessageHeader,
    FeParseMessage, FePasswordMessage, FeStartupMessage, ServerThrottleReason, TransactionStatus,
};
use crate::pg_server::{Session, SessionManager, UserAuthenticator};
use crate::types::Format;

/// Truncates query log if it's longer than `RW_QUERY_LOG_TRUNCATE_LEN`, to avoid log file being too
/// large.
static RW_QUERY_LOG_TRUNCATE_LEN: LazyLock<usize> =
    LazyLock::new(|| match std::env::var("RW_QUERY_LOG_TRUNCATE_LEN") {
        Ok(len) if len.parse::<usize>().is_ok() => len.parse::<usize>().unwrap(),
        _ => 65536,
    });

tokio::task_local! {
    /// The current session. Concrete type is erased for different session implementations.
    pub static CURRENT_SESSION: Weak<dyn Any + Send + Sync>
}

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol<S, SM>
where
    SM: SessionManager,
{
    /// Used for write/read pg messages.
    stream: PgStream<S>,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<SM>,
    session: Option<Arc<SM::Session>>,

    result_cache: HashMap<String, ResultCache<<SM::Session as Session>::ValuesStream>>,
    unnamed_prepare_statement:
        Option<PreparedStatementData<<SM::Session as Session>::PreparedStatement>>,
    prepare_statement_store:
        HashMap<String, PreparedStatementData<<SM::Session as Session>::PreparedStatement>>,
    unnamed_portal: Option<PortalData<<SM::Session as Session>::Portal>>,
    portal_store: HashMap<String, PortalData<<SM::Session as Session>::Portal>>,
    // Used to store the dependency of portal and prepare statement.
    // When we close a prepare statement, we need to close all the portals that depend on it.
    statement_portal_dependency: HashMap<String, Vec<String>>,

    // Used for ssl connection.
    // If None, not expected to build ssl connection (panic).
    tls_context: Option<SslContext>,

    // TLS configuration including SSL enforcement setting
    tls_config: Option<TlsConfig>,

    // Used in extended query protocol. When encounter error in extended query, we need to ignore
    // the following message util sync message.
    ignore_util_sync: bool,

    // Client Address
    peer_addr: AddressRef,

    redact_sql_option_keywords: Option<RedactSqlOptionKeywordsRef>,
    message_memory_manager: MessageMemoryManagerRef,
}

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// The path to the TLS certificate.
    pub cert: String,
    /// The path to the TLS key.
    pub key: String,
    /// Whether to enforce SSL connections (reject non-SSL clients).
    pub enforce_ssl: bool,
}

impl TlsConfig {
    pub fn new_default() -> anyhow::Result<Option<Self>> {
        let cert = std::env::var("RW_SSL_CERT").ok();
        let key = std::env::var("RW_SSL_KEY").ok();
        let enforce_ssl = env_var_is_true("RW_SSL_ENFORCE");

        if cert.is_some() ^ key.is_some() {
            return Err(anyhow::anyhow!(
                "RW_SSL_CERT and RW_SSL_KEY must be set together"
            ));
        }

        if enforce_ssl && cert.is_none() {
            return Err(anyhow::anyhow!(
                "RW_SSL_ENFORCE requires RW_SSL_CERT and RW_SSL_KEY to be set"
            ));
        }

        let (Some(cert), Some(key)) = (cert, key) else {
            return Ok(None);
        };

        tracing::info!(
            "RW_SSL_CERT={}, RW_SSL_KEY={}, RW_SSL_ENFORCE={}",
            cert,
            key,
            enforce_ssl
        );
        Ok(Some(Self {
            cert,
            key,
            enforce_ssl,
        }))
    }
}

impl<S, SM> Drop for PgProtocol<S, SM>
where
    SM: SessionManager,
{
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            // Clear the session in session manager.
            self.session_mgr.end_session(session);
        }
    }
}

/// States flow happened from top to down.
#[derive(PartialEq, Eq)]
enum PgProtocolState {
    Startup,
    Authentication,
    Regular,
}

#[derive(Clone)]
struct PreparedStatementData<S> {
    statement: S,
    sql: Arc<str>,
}

#[derive(Clone)]
struct PortalData<P> {
    portal: P,
    sql: Arc<str>,
}

/// Truncate 0 from C string in Bytes and stringify it (returns slice, no allocations).
///
/// PG protocol strings are always C strings.
pub fn cstr_to_str(b: &Bytes) -> Result<&str, Utf8Error> {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null)
}

fn get_redacted_and_truncated_sql(
    sql: &str,
    redact_sql_option_keywords: Option<RedactSqlOptionKeywordsRef>,
) -> String {
    let redacted_sql = if let Some(keywords) = redact_sql_option_keywords
        && !keywords.is_empty()
    {
        redact_sql(sql, keywords)
    } else {
        sql.to_owned()
    };
    let truncated = truncated_fmt::TruncatedFmt(&redacted_sql, *RW_QUERY_LOG_TRUNCATE_LEN);
    truncated.to_string()
}

/// Record `sql` in the current tracing span.
fn record_sql_in_span(
    sql: &str,
    redact_sql_option_keywords: Option<RedactSqlOptionKeywordsRef>,
    span: &mut tracing::Span,
) {
    let redacted_and_truncated_sql =
        get_redacted_and_truncated_sql(sql, redact_sql_option_keywords);
    span.record("sql", tracing::field::display(&redacted_and_truncated_sql));
}

fn record_user_in_span(user: &str, span: &mut tracing::Span) {
    span.record("user", tracing::field::display(user));
}

/// Redacts sensitive SQL fields. Data in DML is not redacted.
fn redact_sql(sql: &str, keywords: RedactSqlOptionKeywordsRef) -> String {
    match Parser::parse_sql(sql) {
        Ok(sqls) => sqls
            .into_iter()
            .map(|sql| sql.to_redacted_string(keywords.clone()))
            .join(";"),
        Err(_) => sql.to_owned(),
    }
}

#[derive(Clone)]
pub struct ConnectionContext {
    pub tls_config: Option<TlsConfig>,
    pub redact_sql_option_keywords: Option<RedactSqlOptionKeywordsRef>,
    pub message_memory_manager: MessageMemoryManagerRef,
    pub stream_flush_threshold_bytes: usize,
}

impl<S, SM> PgProtocol<S, SM>
where
    S: PgByteStream,
    SM: SessionManager,
{
    pub fn new(
        stream: S,
        session_mgr: Arc<SM>,
        peer_addr: AddressRef,
        context: ConnectionContext,
    ) -> Self {
        let ConnectionContext {
            tls_config,
            redact_sql_option_keywords,
            message_memory_manager,
            stream_flush_threshold_bytes,
        } = context;
        Self {
            stream: PgStream::new(stream, stream_flush_threshold_bytes),
            is_terminate: false,
            state: PgProtocolState::Startup,
            session_mgr,
            session: None,
            tls_context: tls_config
                .as_ref()
                .and_then(|e| build_ssl_ctx_from_config(e).ok()),
            tls_config,
            result_cache: Default::default(),
            unnamed_prepare_statement: Default::default(),
            prepare_statement_store: Default::default(),
            unnamed_portal: Default::default(),
            portal_store: Default::default(),
            statement_portal_dependency: Default::default(),
            ignore_util_sync: false,
            peer_addr,
            redact_sql_option_keywords,
            message_memory_manager,
        }
    }

    /// Run the protocol to serve the connection.
    pub async fn run(&mut self) {
        let mut notice_fut = None;

        loop {
            // Once a session is present, create a future to subscribe and send notices asynchronously.
            if notice_fut.is_none()
                && let Some(session) = self.session.clone()
            {
                let mut stream = self.stream.clone();
                notice_fut = Some(Box::pin(async move {
                    loop {
                        let notice = session.next_notice().await;
                        if let Err(e) = stream.write(BeMessage::NoticeResponse(&notice)).await {
                            tracing::error!(error = %e.as_report(), notice, "failed to send notice");
                        }
                    }
                }));
            }

            // Read and process messages.
            let process = std::pin::pin!(async {
                let (msg, _memory_guard) = match self.read_message().await {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::error!(error = %e.as_report(), "error when reading message");
                        return true; // terminate the connection
                    }
                };
                tracing::trace!(?msg, "received message");
                self.process(msg).await
            });

            let terminated = if let Some(notice_fut) = notice_fut.as_mut() {
                tokio::select! {
                    _ = notice_fut => unreachable!(),
                    terminated = process => terminated,
                }
            } else {
                process.await
            };

            if terminated {
                break;
            }
        }
    }

    /// Processes one message. Returns true if the connection is terminated.
    pub async fn process(&mut self, msg: FeMessage) -> bool {
        self.do_process(msg).await.is_none() || self.is_terminate
    }

    /// The root tracing span for processing a message. The target of the span is
    /// [`PGWIRE_ROOT_SPAN_TARGET`].
    ///
    /// This is used to provide context for the (slow) query logs and traces.
    ///
    /// The span is only effective if there's a current session and the message is
    /// query-related. Otherwise, `Span::none()` is returned.
    fn root_span_for_msg(&self, msg: &FeMessage) -> tracing::Span {
        let Some(session_id) = self.session.as_ref().map(|s| s.id().0) else {
            return tracing::Span::none();
        };

        let mode = match msg {
            FeMessage::Query(_) => "simple query",
            FeMessage::Parse(_) => "extended query parse",
            FeMessage::Execute(_) => "extended query execute",
            _ => return tracing::Span::none(),
        };

        let mut span = tracing::info_span!(
            target: PGWIRE_ROOT_SPAN_TARGET,
            "handle_query",
            mode,
            session_id,
            sql = tracing::field::Empty,
            user = tracing::field::Empty,
        );
        match msg {
            FeMessage::Execute(execute_msg) => {
                if let Ok(portal_name) = cstr_to_str(&execute_msg.portal_name)
                    && let Ok(sql) = self.get_portal_sql(portal_name)
                {
                    record_sql_in_span(&sql, self.redact_sql_option_keywords.clone(), &mut span);
                }
            }
            _ => {
                if let Ok(sql) = msg.get_sql()
                    && let Some(sql) = sql
                {
                    record_sql_in_span(sql, self.redact_sql_option_keywords.clone(), &mut span);
                }
            }
        }
        if let Some(current_session) = self.session.as_ref() {
            record_user_in_span(&current_session.user(), &mut span);
        }
        span
    }

    /// Return type `Option<()>` is essentially a bool, but allows `?` for early return.
    /// - `None` means to terminate the current connection
    /// - `Some(())` means to continue processing the next message
    async fn do_process(&mut self, msg: FeMessage) -> Option<()> {
        let span = self.root_span_for_msg(&msg);
        let weak_session = self
            .session
            .as_ref()
            .map(|s| Arc::downgrade(s) as Weak<dyn Any + Send + Sync>);

        // Processing the message itself.
        //
        // Note: pin the future to avoid stack overflow as we'll wrap it multiple times
        // in the following code.
        let fut = Box::pin(self.do_process_inner(msg));

        // Set the current session as the context when processing the message, if exists.
        let fut = async move {
            if let Some(session) = weak_session {
                CURRENT_SESSION.scope(session, fut).await
            } else {
                fut.await
            }
        };

        // Catch unwind.
        let fut = async move {
            AssertUnwindSafe(fut)
                .rw_catch_unwind()
                .await
                .unwrap_or_else(|payload| {
                    Err(PsqlError::Panic(
                        panic_message::panic_message(&payload).to_owned(),
                    ))
                })
        };

        // Slow query log.
        let fut = async move {
            let period = *SLOW_QUERY_LOG_PERIOD;
            let mut fut = std::pin::pin!(fut);
            let mut elapsed = Duration::ZERO;

            // Report the SQL in the log periodically if the query is slow.
            loop {
                match tokio::time::timeout(period, &mut fut).await {
                    Ok(result) => break result,
                    Err(_) => {
                        elapsed += period;
                        tracing::info!(
                            target: PGWIRE_SLOW_QUERY_LOG,
                            elapsed = %format_args!("{}ms", elapsed.as_millis()),
                            "slow query"
                        );
                    }
                }
            }
        };

        // Query log.
        let fut = async move {
            if !tracing::Span::current().is_none() {
                tracing::info!(
                    target: PGWIRE_QUERY_LOG,
                    status = "started",
                );
            }

            let start = Instant::now();
            let result = fut.await;
            let elapsed = start.elapsed();

            // Always log if an error occurs.
            // Note: all messages will be processed through this code path, making it the
            //       only necessary place to log errors.
            if let Err(error) = &result {
                if cfg!(debug_assertions) && !Deployment::current().is_ci() {
                    // For local debugging, we print the error with backtrace.
                    // It's useful only when:
                    // - no additional context is added to the error
                    // - backtrace is captured in the error
                    // - backtrace is not printed in the middle
                    tracing::error!(error = ?error.as_report(), "error when process message");
                } else {
                    tracing::error!(error = %error.as_report(), "error when process message");
                }
            }

            // Log to optionally-enabled target `PGWIRE_QUERY_LOG`.
            // Only log if we're currently in a tracing span set in `span_for_msg`.
            if !tracing::Span::current().is_none() {
                tracing::info!(
                    target: PGWIRE_QUERY_LOG,
                    status = if result.is_ok() { "ok" } else { "err" },
                    time = %format_args!("{}ms", elapsed.as_millis()),
                );
            }

            result
        };

        // Tracing span.
        let fut = fut.instrument(span);

        // Execute the future and handle the error.
        match fut.await {
            Ok(()) => Some(()),
            Err(e) => {
                match e {
                    PsqlError::IoError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return None;
                        }
                    }

                    PsqlError::SslError(_) => {
                        // For ssl error, because the stream has already been consumed, so there is
                        // no way to write more message.
                        return None;
                    }

                    PsqlError::StartupError(_)
                    | PsqlError::PasswordError
                    | PsqlError::ProtocolError(_) => {
                        self.stream
                            .write_no_flush(BeMessage::ErrorResponse {
                                error: &e,
                                // At this time we're not in a session, use compact error message for
                                // better alignment with Postgres' UI.
                                pretty: false,
                                severity: Some(Severity::Fatal),
                            })
                            .ok()?;
                        let _ = self.stream.flush().await;
                        return None;
                    }

                    PsqlError::SimpleQueryError(_) | PsqlError::ServerThrottle(_) => {
                        self.stream
                            .write_no_flush(BeMessage::ErrorResponse {
                                error: &e,
                                pretty: true,
                                severity: None,
                            })
                            .ok()?;
                        self.ready_for_query().ok()?;
                    }

                    PsqlError::IdleInTxnTimeout | PsqlError::Panic(_) => {
                        self.stream
                            .write_no_flush(BeMessage::ErrorResponse {
                                error: &e,
                                pretty: true,
                                severity: None,
                            })
                            .ok()?;
                        let _ = self.stream.flush().await;

                        // 1. Catching the panic during message processing may leave the session in an
                        // inconsistent state. We forcefully close the connection (then end the
                        // session) here for safety.
                        // 2. Idle in transaction timeout should also close the connection.
                        return None;
                    }

                    PsqlError::Uncategorized(_)
                    | PsqlError::ExtendedPrepareError(_)
                    | PsqlError::ExtendedExecuteError(_) => {
                        self.stream
                            .write_no_flush(BeMessage::ErrorResponse {
                                error: &e,
                                pretty: true,
                                severity: None,
                            })
                            .ok()?;
                    }
                }
                let _ = self.stream.flush().await;
                Some(())
            }
        }
    }

    async fn do_process_inner(&mut self, msg: FeMessage) -> PsqlResult<()> {
        if self.state == PgProtocolState::Authentication && !matches!(&msg, FeMessage::Password(_))
        {
            return Err(PsqlError::protocol_error(
                "expected PasswordMessage during authentication",
            ));
        }

        // Ignore util sync message.
        if self.ignore_util_sync {
            if let FeMessage::Sync = msg {
            } else {
                tracing::trace!("ignore message {:?} until sync.", msg);
                return Ok(());
            }
        }

        match msg {
            FeMessage::Gss => self.process_gss_msg().await?,
            FeMessage::Ssl => self.process_ssl_msg().await?,
            FeMessage::Startup(msg) => self.process_startup_msg(msg).await?,
            FeMessage::Password(msg) => self.process_password_msg(msg).await?,
            FeMessage::Query(query_msg) => {
                let sql = Arc::from(query_msg.get_sql()?);
                // The process_query_msg can be slow. Release potential large FeQueryMessage early.
                drop(query_msg);
                self.process_query_msg(sql).await?
            }
            FeMessage::CancelQuery(m) => self.process_cancel_msg(m)?,
            FeMessage::Terminate => self.process_terminate(),
            FeMessage::Parse(m) => {
                if let Err(err) = self.process_parse_msg(m).await {
                    self.ignore_util_sync = true;
                    return Err(err);
                }
            }
            FeMessage::Bind(m) => {
                if let Err(err) = self.process_bind_msg(m) {
                    self.ignore_util_sync = true;
                    return Err(err);
                }
            }
            FeMessage::Execute(m) => {
                if let Err(err) = self.process_execute_msg(m).await {
                    self.ignore_util_sync = true;
                    return Err(err);
                }
            }
            FeMessage::Describe(m) => {
                if let Err(err) = self.process_describe_msg(m) {
                    self.ignore_util_sync = true;
                    return Err(err);
                }
            }
            FeMessage::Sync => {
                self.ignore_util_sync = false;
                self.ready_for_query()?
            }
            FeMessage::Close(m) => {
                if let Err(err) = self.process_close_msg(m) {
                    self.ignore_util_sync = true;
                    return Err(err);
                }
            }
            FeMessage::Flush => {
                if let Err(err) = self.stream.flush().await {
                    self.ignore_util_sync = true;
                    return Err(err.into());
                }
            }
            FeMessage::HealthCheck => self.process_health_check(),
            FeMessage::ServerThrottle(reason) => match reason {
                ServerThrottleReason::TooLargeMessage => {
                    return Err(PsqlError::ServerThrottle(format!(
                        "max_single_query_size_bytes {} has been exceeded, please either reduce the query size or increase the limit",
                        self.message_memory_manager.max_filter_bytes
                    )));
                }
                ServerThrottleReason::TooManyMemoryUsage => {
                    return Err(PsqlError::ServerThrottle(format!(
                        "max_total_query_size_bytes {} has been exceeded, please either retry or increase the limit",
                        self.message_memory_manager.max_running_bytes
                    )));
                }
            },
        }
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_message(&mut self) -> io::Result<(FeMessage, Option<MessageMemoryGuard>)> {
        match self.state {
            PgProtocolState::Startup => self
                .stream
                .read_startup()
                .await
                .map(|message: FeMessage| (message, None)),
            PgProtocolState::Authentication | PgProtocolState::Regular => {
                self.stream.read_header().await?;
                let guard = if let Some(ref header) = self.stream.read_header {
                    let payload_len = std::cmp::max(header.payload_len, 0) as u64;
                    let (reason, guard) = self.message_memory_manager.add(payload_len);
                    if let Some(reason) = reason {
                        // Release the memory ASAP.
                        drop(guard);
                        self.stream.skip_body().await?;
                        return Ok((FeMessage::ServerThrottle(reason), None));
                    }
                    guard
                } else {
                    None
                };
                let message = self.stream.read_body().await?;
                Ok((message, guard))
            }
        }
    }

    /// Writes a `ReadyForQuery` message to the client without flushing.
    fn ready_for_query(&mut self) -> io::Result<()> {
        self.stream.write_no_flush(BeMessage::ReadyForQuery(
            self.session
                .as_ref()
                .map(|s| s.transaction_status())
                .unwrap_or(TransactionStatus::Idle),
        ))
    }

    async fn process_gss_msg(&mut self) -> PsqlResult<()> {
        // We don't support GSSAPI, so we just say no gracefully.
        self.stream.write(BeMessage::EncryptionResponseNo).await?;
        Ok(())
    }

    async fn process_ssl_msg(&mut self) -> PsqlResult<()> {
        if let Some(context) = self.tls_context.as_ref() {
            // If got and ssl context, say yes for ssl connection.
            // Construct ssl stream and replace with current one.
            self.stream.write(BeMessage::EncryptionResponseSsl).await?;
            self.stream.upgrade_to_ssl(context).await?;
        } else {
            // If no, say no for encryption.
            self.stream.write(BeMessage::EncryptionResponseNo).await?;
        }

        Ok(())
    }

    async fn process_startup_msg(&mut self, msg: FeStartupMessage) -> PsqlResult<()> {
        // Check SSL enforcement: if SSL is enforced but connection is not using SSL, reject
        if let Some(ref tls_config) = self.tls_config
            && tls_config.enforce_ssl
            && !self.stream.is_ssl_connection().await
        {
            return Err(PsqlError::StartupError(
                "SSL connection is required but not established".into(),
            ));
        }

        let db_name = msg
            .config
            .get("database")
            .cloned()
            .unwrap_or_else(|| "dev".to_owned());
        let user_name = msg
            .config
            .get("user")
            .cloned()
            .unwrap_or_else(|| "root".to_owned());

        let session = self
            .session_mgr
            .connect(&db_name, &user_name, self.peer_addr.clone())
            .map_err(|e| PsqlError::StartupError(e.into()))?;

        if let Some(options) = msg.config.get("options") {
            for (key, value) in parse_options(options)? {
                session
                    .set_config(&key, value)
                    .map_err(|e| PsqlError::StartupError(e.into()))?;
            }
        }
        // dedicated `application_name` has higher priority than `options`
        let application_name = msg.config.get("application_name");
        if let Some(application_name) = application_name {
            session
                .set_config("application_name", application_name.clone())
                .map_err(|e| PsqlError::StartupError(e.into()))?;
        }

        self.state = match session.user_authenticator() {
            UserAuthenticator::None => {
                self.stream.write_no_flush(BeMessage::AuthenticationOk)?;

                // Cancel request need this for identify and verification. According to postgres
                // doc, it should be written to buffer after receive AuthenticationOk.
                self.stream
                    .write_no_flush(BeMessage::BackendKeyData(session.id()))?;

                self.stream.write_no_flush(BeMessage::ParameterStatus(
                    BeParameterStatusMessage::TimeZone(
                        &session
                            .get_config("timezone")
                            .map_err(|e| PsqlError::StartupError(e.into()))?,
                    ),
                ))?;
                self.stream
                    .write_parameter_status_msg_no_flush(&ParameterStatus {
                        application_name: application_name.cloned(),
                    })?;
                self.ready_for_query()?;
                PgProtocolState::Regular
            }
            UserAuthenticator::ClearText(_)
            | UserAuthenticator::OAuth { .. }
            | UserAuthenticator::Ldap(..) => {
                self.stream
                    .write_no_flush(BeMessage::AuthenticationCleartextPassword)?;
                PgProtocolState::Authentication
            }
            UserAuthenticator::Md5WithSalt { salt, .. } => {
                self.stream
                    .write_no_flush(BeMessage::AuthenticationMd5Password(salt))?;
                PgProtocolState::Authentication
            }
        };

        self.session = Some(session);
        Ok(())
    }

    async fn process_password_msg(&mut self, msg: FePasswordMessage) -> PsqlResult<()> {
        let session = self.session.as_ref().unwrap();
        let authenticator = session.user_authenticator();
        authenticator.authenticate(&msg.password).await?;
        self.stream.write_no_flush(BeMessage::AuthenticationOk)?;
        let timezone = session
            .get_config("timezone")
            .map_err(|e| PsqlError::StartupError(e.into()))?;
        self.stream.write_no_flush(BeMessage::ParameterStatus(
            BeParameterStatusMessage::TimeZone(&timezone),
        ))?;
        self.stream
            .write_parameter_status_msg_no_flush(&ParameterStatus::default())?;
        self.ready_for_query()?;
        self.state = PgProtocolState::Regular;
        Ok(())
    }

    fn process_cancel_msg(&mut self, m: FeCancelMessage) -> PsqlResult<()> {
        let session_id = (m.target_process_id, m.target_secret_key);
        tracing::trace!("cancel query in session: {:?}", session_id);
        self.session_mgr.cancel_queries_in_session(session_id);
        self.session_mgr.cancel_creating_jobs_in_session(session_id);
        self.is_terminate = true;
        Ok(())
    }

    async fn process_query_msg(&mut self, sql: Arc<str>) -> PsqlResult<()> {
        let truncated_sql =
            get_redacted_and_truncated_sql(&sql, self.redact_sql_option_keywords.clone());
        let session = self.session.clone().unwrap();

        session.check_idle_in_transaction_timeout()?;
        // Store only truncated SQL in context to prevent excessive memory usage from large SQL.
        let _exec_context_guard = session.init_exec_context(truncated_sql.into());
        self.inner_process_query_msg(sql, session.clone()).await
    }

    async fn inner_process_query_msg(
        &mut self,
        sql: Arc<str>,
        session: Arc<SM::Session>,
    ) -> PsqlResult<()> {
        // Parse sql.
        let stmts =
            Parser::parse_sql(&sql).map_err(|err| PsqlError::SimpleQueryError(err.into()))?;
        // The following inner_process_query_msg_one_stmt can be slow. Release potential large String early.
        drop(sql);
        if stmts.is_empty() {
            self.stream.write_no_flush(BeMessage::EmptyQueryResponse)?;
        }

        // Execute multiple statements in simple query. KISS later.
        for stmt in stmts {
            self.inner_process_query_msg_one_stmt(stmt, session.clone())
                .await?;
        }
        // Put this line inside the for loop above will lead to unfinished/stuck regress test...Not
        // sure the reason.
        self.ready_for_query()?;
        Ok(())
    }

    async fn inner_process_query_msg_one_stmt(
        &mut self,
        stmt: Statement,
        session: Arc<SM::Session>,
    ) -> PsqlResult<()> {
        let session = session.clone();

        // execute query
        let res = session.clone().run_one_query(stmt, Format::Text).await;

        // Take all remaining notices (if any) and send them before `CommandComplete`.
        while let Some(notice) = session.next_notice().now_or_never() {
            self.stream
                .write_no_flush(BeMessage::NoticeResponse(&notice))?;
        }

        let mut res = res.map_err(|e| PsqlError::SimpleQueryError(e.into()))?;

        for notice in res.notices() {
            self.stream
                .write_no_flush(BeMessage::NoticeResponse(notice))?;
        }

        let status = res.status();
        if let Some(ref application_name) = status.application_name {
            self.stream.write_no_flush(BeMessage::ParameterStatus(
                BeParameterStatusMessage::ApplicationName(application_name),
            ))?;
        }

        if res.is_copy_query_to_stdout() {
            self.stream
                .write_no_flush(BeMessage::CopyOutResponse(res.row_desc().len()))?;
            let mut count = 0;
            while let Some(row_set) = res.values_stream().next().await {
                let row_set = row_set.map_err(PsqlError::SimpleQueryError)?;
                for row in row_set {
                    self.stream
                        .write_streaming(BeMessage::CopyData(&row))
                        .await?;
                    count += 1;
                }
            }

            self.stream.write_no_flush(BeMessage::CopyDone)?;

            // Run the callback before sending the `CommandComplete` message.
            res.run_callback().await?;

            self.stream
                .write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                    stmt_type: res.stmt_type(),
                    rows_cnt: count,
                }))?;
        } else if res.is_query() {
            self.stream
                .write_no_flush(BeMessage::RowDescription(res.row_desc()))?;

            let mut rows_cnt = 0;

            while let Some(row_set) = res.values_stream().next().await {
                let row_set = row_set.map_err(PsqlError::SimpleQueryError)?;
                for row in row_set {
                    self.stream
                        .write_streaming(BeMessage::DataRow(&row))
                        .await?;
                    rows_cnt += 1;
                }
            }

            // Run the callback before sending the `CommandComplete` message.
            res.run_callback().await?;

            self.stream
                .write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                    stmt_type: res.stmt_type(),
                    rows_cnt,
                }))?;
        } else if res.stmt_type().is_dml() && !res.stmt_type().is_returning() {
            let first_row_set = res.values_stream().next().await;
            let first_row_set = match first_row_set {
                None => {
                    return Err(PsqlError::Uncategorized(
                        anyhow::anyhow!("no affected rows in output").into(),
                    ));
                }
                Some(row) => row.map_err(PsqlError::SimpleQueryError)?,
            };
            let affected_rows_str = first_row_set[0].values()[0]
                .as_ref()
                .expect("compute node should return affected rows in output");

            assert!(matches!(res.row_cnt_format(), Some(Format::Text)));
            let affected_rows_cnt = String::from_utf8(affected_rows_str.to_vec())
                .unwrap()
                .parse()
                .unwrap_or_default();

            // Run the callback before sending the `CommandComplete` message.
            res.run_callback().await?;

            self.stream
                .write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                    stmt_type: res.stmt_type(),
                    rows_cnt: affected_rows_cnt,
                }))?;
        } else {
            // Run the callback before sending the `CommandComplete` message.
            res.run_callback().await?;

            self.stream
                .write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                    stmt_type: res.stmt_type(),
                    rows_cnt: 0,
                }))?;
        }

        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    fn process_health_check(&mut self) {
        tracing::debug!("health check");
        self.is_terminate = true;
    }

    async fn process_parse_msg(&mut self, mut msg: FeParseMessage) -> PsqlResult<()> {
        let sql = Arc::from(cstr_to_str(&msg.sql_bytes).unwrap());
        let session = self.session.clone().unwrap();
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_owned();
        let type_ids = std::mem::take(&mut msg.type_ids);
        // The inner_process_parse_msg can be slow. Release potential large FeParseMessage early.
        drop(msg);
        self.inner_process_parse_msg(session, sql, statement_name, type_ids)
            .await?;
        Ok(())
    }

    async fn inner_process_parse_msg(
        &mut self,
        session: Arc<SM::Session>,
        sql: Arc<str>,
        statement_name: String,
        type_ids: Vec<i32>,
    ) -> PsqlResult<()> {
        if statement_name.is_empty() {
            // Remove the unnamed prepare statement first, in case the unsupported sql binds a wrong
            // prepare statement.
            self.unnamed_prepare_statement.take();
        } else if self.prepare_statement_store.contains_key(&statement_name) {
            return Err(PsqlError::ExtendedPrepareError(
                "Duplicated statement name".into(),
            ));
        }

        let stmt = {
            let stmts = Parser::parse_sql(&sql)
                .map_err(|err| PsqlError::ExtendedPrepareError(err.into()))?;
            if stmts.len() > 1 {
                return Err(PsqlError::ExtendedPrepareError(
                    "Only one statement is allowed in extended query mode".into(),
                ));
            }

            stmts.into_iter().next()
        };

        let param_types: Vec<Option<DataType>> = type_ids
            .iter()
            .map(|&id| {
                // 0 means unspecified type
                // ref: https://www.postgresql.org/docs/15/protocol-message-formats.html#:~:text=Placing%20a%20zero%20here%20is%20equivalent%20to%20leaving%20the%20type%20unspecified.
                if id == 0 {
                    Ok(None)
                } else {
                    DataType::from_oid(id)
                        .map(Some)
                        .map_err(|e| PsqlError::ExtendedPrepareError(e.into()))
                }
            })
            .try_collect()?;

        let prepare_statement = session
            .parse(stmt, param_types)
            .await
            .map_err(|e| PsqlError::ExtendedPrepareError(e.into()))?;
        let prepare_statement = PreparedStatementData {
            statement: prepare_statement,
            sql,
        };

        if statement_name.is_empty() {
            self.unnamed_prepare_statement.replace(prepare_statement);
        } else {
            self.prepare_statement_store
                .insert(statement_name.clone(), prepare_statement);
        }

        self.statement_portal_dependency
            .entry(statement_name)
            .or_default()
            .clear();

        self.stream.write_no_flush(BeMessage::ParseComplete)?;
        Ok(())
    }

    fn process_bind_msg(&mut self, msg: FeBindMessage) -> PsqlResult<()> {
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_owned();
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_owned();
        let session = self.session.clone().unwrap();

        if self.portal_store.contains_key(&portal_name) {
            return Err(PsqlError::Uncategorized("Duplicated portal name".into()));
        }

        let prepare_statement = self.get_statement_data(&statement_name)?.clone();

        let result_formats = msg
            .result_format_codes
            .iter()
            .map(|&format_code| Format::from_i16(format_code))
            .try_collect()?;
        let param_formats = msg
            .param_format_codes
            .iter()
            .map(|&format_code| Format::from_i16(format_code))
            .try_collect()?;

        let portal = session
            .bind(
                prepare_statement.statement,
                msg.params,
                param_formats,
                result_formats,
            )
            .map_err(|e| PsqlError::Uncategorized(e.into()))?;
        let portal = PortalData {
            portal,
            sql: prepare_statement.sql,
        };

        if portal_name.is_empty() {
            self.result_cache.remove(&portal_name);
            self.unnamed_portal.replace(portal);
        } else {
            assert!(
                !self.result_cache.contains_key(&portal_name),
                "Named portal never can be overridden."
            );
            self.portal_store.insert(portal_name.clone(), portal);
        }

        self.statement_portal_dependency
            .get_mut(&statement_name)
            .unwrap()
            .push(portal_name);

        self.stream.write_no_flush(BeMessage::BindComplete)?;
        Ok(())
    }

    async fn process_execute_msg(&mut self, msg: FeExecuteMessage) -> PsqlResult<()> {
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_owned();
        let row_max = msg.max_rows as usize;
        drop(msg);
        let session = self.session.clone().unwrap();

        match self.result_cache.remove(&portal_name) {
            Some(mut result_cache) => {
                assert!(self.portal_store.contains_key(&portal_name));

                let is_consume_completed =
                    result_cache.consume::<S>(row_max, &mut self.stream).await?;

                if !is_consume_completed {
                    self.result_cache.insert(portal_name, result_cache);
                }
            }
            _ => {
                let portal = self.get_portal_data(&portal_name)?.clone();
                let sql = format!("{}", portal.portal);
                let truncated_sql =
                    get_redacted_and_truncated_sql(&sql, self.redact_sql_option_keywords.clone());
                drop(sql);

                session.check_idle_in_transaction_timeout()?;
                // Store only truncated SQL in context to prevent excessive memory usage from large SQL.
                let _exec_context_guard = session.init_exec_context(truncated_sql.into());
                let result = session.clone().execute(portal.portal).await;

                let pg_response = result.map_err(|e| PsqlError::ExtendedExecuteError(e.into()))?;
                let mut result_cache = ResultCache::new(pg_response);
                let is_consume_completed =
                    result_cache.consume::<S>(row_max, &mut self.stream).await?;
                if !is_consume_completed {
                    self.result_cache.insert(portal_name, result_cache);
                }
            }
        }

        Ok(())
    }

    fn process_describe_msg(&mut self, msg: FeDescribeMessage) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.name).unwrap().to_owned();
        let session = self.session.clone().unwrap();
        //  b'S' => Statement
        //  b'P' => Portal

        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            let prepare_statement = self.get_statement(&name)?;

            let (param_types, row_descriptions) = self
                .session
                .clone()
                .unwrap()
                .describe_statement(prepare_statement)
                .map_err(|e| PsqlError::Uncategorized(e.into()))?;
            self.stream.write_no_flush(BeMessage::ParameterDescription(
                &param_types.iter().map(|t| t.to_oid()).collect_vec(),
            ))?;

            if row_descriptions.is_empty() {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(BeMessage::NoData)?;
            } else {
                self.stream
                    .write_no_flush(BeMessage::RowDescription(&row_descriptions))?;
            }
        } else if msg.kind == b'P' {
            let portal = self.get_portal(&name)?;

            let row_descriptions = session
                .describe_portal(portal)
                .map_err(|e| PsqlError::Uncategorized(e.into()))?;

            if row_descriptions.is_empty() {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(BeMessage::NoData)?;
            } else {
                self.stream
                    .write_no_flush(BeMessage::RowDescription(&row_descriptions))?;
            }
        }
        Ok(())
    }

    fn process_close_msg(&mut self, msg: FeCloseMessage) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.name).unwrap().to_owned();
        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            if name.is_empty() {
                self.unnamed_prepare_statement = None;
            } else {
                self.prepare_statement_store.remove(&name);
            }
            for portal_name in self
                .statement_portal_dependency
                .remove(&name)
                .unwrap_or_default()
            {
                self.remove_portal(&portal_name);
            }
        } else if msg.kind == b'P' {
            self.remove_portal(&name);
        }
        self.stream.write_no_flush(BeMessage::CloseComplete)?;
        Ok(())
    }

    fn remove_portal(&mut self, portal_name: &str) {
        if portal_name.is_empty() {
            self.unnamed_portal = None;
        } else {
            self.portal_store.remove(portal_name);
        }
        self.result_cache.remove(portal_name);
    }

    fn get_portal(&self, portal_name: &str) -> PsqlResult<<SM::Session as Session>::Portal> {
        Ok(self.get_portal_data(portal_name)?.portal.clone())
    }

    fn get_portal_data(
        &self,
        portal_name: &str,
    ) -> PsqlResult<&PortalData<<SM::Session as Session>::Portal>> {
        if portal_name.is_empty() {
            self.unnamed_portal
                .as_ref()
                .ok_or_else(|| PsqlError::Uncategorized("unnamed portal not found".into()))
        } else {
            self.portal_store.get(portal_name).ok_or_else(|| {
                PsqlError::Uncategorized(format!("Portal {} not found", portal_name).into())
            })
        }
    }

    fn get_statement(
        &self,
        statement_name: &str,
    ) -> PsqlResult<<SM::Session as Session>::PreparedStatement> {
        Ok(self.get_statement_data(statement_name)?.statement.clone())
    }

    fn get_statement_data(
        &self,
        statement_name: &str,
    ) -> PsqlResult<&PreparedStatementData<<SM::Session as Session>::PreparedStatement>> {
        if statement_name.is_empty() {
            self.unnamed_prepare_statement.as_ref().ok_or_else(|| {
                PsqlError::Uncategorized("unnamed prepare statement not found".into())
            })
        } else {
            self.prepare_statement_store
                .get(statement_name)
                .ok_or_else(|| {
                    PsqlError::Uncategorized(
                        format!("Prepare statement {} not found", statement_name).into(),
                    )
                })
        }
    }

    fn get_portal_sql(&self, portal_name: &str) -> PsqlResult<Arc<str>> {
        Ok(self.get_portal_data(portal_name)?.sql.clone())
    }
}

enum PgStreamInner<S> {
    /// Used for the intermediate state when converting from unencrypted to ssl stream.
    Placeholder,
    /// An unencrypted stream.
    Unencrypted(S),
    /// An ssl stream.
    Ssl(SslStream<S>),
}

/// Trait for a byte stream that can be used for pg protocol.
pub trait PgByteStream: AsyncWrite + AsyncRead + Unpin + Send + 'static {}
impl<S> PgByteStream for S where S: AsyncWrite + AsyncRead + Unpin + Send + 'static {}

/// Wraps a byte stream and read/write pg messages.
///
/// Cloning a `PgStream` will share the same stream but a fresh & independent write buffer,
/// so that it can be used to write messages concurrently without interference.
pub struct PgStream<S> {
    /// The underlying stream.
    stream: Arc<Mutex<PgStreamInner<S>>>,
    /// Write into buffer before flush to stream.
    write_buf: BytesMut,
    stream_flush_threshold_bytes: usize,
    read_header: Option<FeMessageHeader>,
}

impl<S> PgStream<S> {
    /// Create a new `PgStream` with the given stream and streaming flush threshold.
    pub fn new(stream: S, stream_flush_threshold_bytes: usize) -> Self {
        const DEFAULT_WRITE_BUF_CAPACITY: usize = 10 * 1024;

        Self {
            stream: Arc::new(Mutex::new(PgStreamInner::Unencrypted(stream))),
            write_buf: BytesMut::with_capacity(DEFAULT_WRITE_BUF_CAPACITY),
            stream_flush_threshold_bytes,
            read_header: None,
        }
    }

    /// Check if the current connection is using SSL
    async fn is_ssl_connection(&self) -> bool {
        let stream = self.stream.lock().await;
        matches!(*stream, PgStreamInner::Ssl(_))
    }
}

impl<S> Clone for PgStream<S> {
    fn clone(&self) -> Self {
        Self {
            stream: Arc::clone(&self.stream),
            write_buf: BytesMut::with_capacity(self.write_buf.capacity()),
            stream_flush_threshold_bytes: self.stream_flush_threshold_bytes,
            read_header: self.read_header.clone(),
        }
    }
}

/// At present there is a hard-wired set of parameters for which
/// ParameterStatus will be generated: they are:
///
///  * `server_version`
///  * `server_encoding`
///  * `client_encoding`
///  * `application_name`
///  * `is_superuser`
///  * `session_authorization`
///  * `DateStyle`
///  * `IntervalStyle`
///  * `TimeZone`
///  * `integer_datetimes`
///  * `standard_conforming_string`
///
/// See: <https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC>.
#[derive(Debug, Default, Clone)]
pub struct ParameterStatus {
    pub application_name: Option<String>,
}

impl<S> PgStream<S>
where
    S: PgByteStream,
{
    async fn read_startup(&mut self) -> io::Result<FeMessage> {
        let mut stream = self.stream.lock().await;
        match &mut *stream {
            PgStreamInner::Placeholder => unreachable!(),
            PgStreamInner::Unencrypted(stream) => FeStartupMessage::read(stream).await,
            PgStreamInner::Ssl(ssl_stream) => FeStartupMessage::read(ssl_stream).await,
        }
    }

    async fn read_header(&mut self) -> io::Result<()> {
        let mut stream = self.stream.lock().await;
        match &mut *stream {
            PgStreamInner::Placeholder => unreachable!(),
            PgStreamInner::Unencrypted(stream) => {
                self.read_header = Some(FeMessage::read_header(stream).await?);
                Ok(())
            }
            PgStreamInner::Ssl(ssl_stream) => {
                self.read_header = Some(FeMessage::read_header(ssl_stream).await?);
                Ok(())
            }
        }
    }

    async fn read_body(&mut self) -> io::Result<FeMessage> {
        let mut stream = self.stream.lock().await;
        let header = self
            .read_header
            .take()
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "header not found"))?;
        match &mut *stream {
            PgStreamInner::Placeholder => unreachable!(),
            PgStreamInner::Unencrypted(stream) => FeMessage::read_body(stream, header).await,
            PgStreamInner::Ssl(ssl_stream) => FeMessage::read_body(ssl_stream, header).await,
        }
    }

    async fn skip_body(&mut self) -> io::Result<()> {
        let mut stream = self.stream.lock().await;
        let header = self
            .read_header
            .take()
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "header not found"))?;
        match &mut *stream {
            PgStreamInner::Placeholder => unreachable!(),
            PgStreamInner::Unencrypted(stream) => FeMessage::skip_body(stream, header).await,
            PgStreamInner::Ssl(ssl_stream) => FeMessage::skip_body(ssl_stream, header).await,
        }
    }

    fn write_parameter_status_msg_no_flush(&mut self, status: &ParameterStatus) -> io::Result<()> {
        self.write_no_flush(BeMessage::ParameterStatus(
            BeParameterStatusMessage::ClientEncoding(SERVER_ENCODING),
        ))?;
        self.write_no_flush(BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString(STANDARD_CONFORMING_STRINGS),
        ))?;
        self.write_no_flush(BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion(PG_VERSION),
        ))?;
        if let Some(application_name) = &status.application_name {
            self.write_no_flush(BeMessage::ParameterStatus(
                BeParameterStatusMessage::ApplicationName(application_name),
            ))?;
        }
        Ok(())
    }

    pub fn write_no_flush(&mut self, message: BeMessage<'_>) -> io::Result<()> {
        BeMessage::write(&mut self.write_buf, message)
    }

    /// Write a message that is part of a potentially large response, flushing periodically to
    /// bound the write buffer and propagate network backpressure to the result stream.
    pub(crate) async fn write_streaming(&mut self, message: BeMessage<'_>) -> io::Result<()> {
        self.write_no_flush(message)?;
        if self.write_buf.len() >= self.stream_flush_threshold_bytes {
            self.flush().await?;
        }
        Ok(())
    }

    async fn write(&mut self, message: BeMessage<'_>) -> io::Result<()> {
        self.write_no_flush(message)?;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        let mut stream = self.stream.lock().await;
        match &mut *stream {
            PgStreamInner::Placeholder => unreachable!(),
            PgStreamInner::Unencrypted(stream) => {
                stream.write_all(&self.write_buf).await?;
                stream.flush().await?;
            }
            PgStreamInner::Ssl(ssl_stream) => {
                ssl_stream.write_all(&self.write_buf).await?;
                ssl_stream.flush().await?;
            }
        }
        self.write_buf.clear();
        Ok(())
    }
}

impl<S> PgStream<S>
where
    S: PgByteStream,
{
    /// Convert the underlying stream to ssl stream based on the given context.
    async fn upgrade_to_ssl(&mut self, ssl_ctx: &SslContextRef) -> PsqlResult<()> {
        let mut stream = self.stream.lock().await;

        match std::mem::replace(&mut *stream, PgStreamInner::Placeholder) {
            PgStreamInner::Unencrypted(unencrypted_stream) => {
                let ssl = openssl::ssl::Ssl::new(ssl_ctx).unwrap();
                let mut ssl_stream =
                    tokio_openssl::SslStream::new(ssl, unencrypted_stream).unwrap();

                if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                    tracing::warn!(error = %e.as_report(), "Unable to set up an ssl connection");
                    let _ = ssl_stream.shutdown().await;
                    return Err(e.into());
                }

                *stream = PgStreamInner::Ssl(ssl_stream);
            }
            PgStreamInner::Ssl(_) => panic!("the stream is already ssl"),
            PgStreamInner::Placeholder => unreachable!(),
        }

        Ok(())
    }
}

fn build_ssl_ctx_from_config(tls_config: &TlsConfig) -> PsqlResult<SslContext> {
    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();

    let key_path = &tls_config.key;
    let cert_path = &tls_config.cert;

    // Build ssl acceptor according to the config.
    // Now we set every verify to true.
    acceptor
        .set_private_key_file(key_path, openssl::ssl::SslFiletype::PEM)
        .map_err(|e| PsqlError::Uncategorized(e.into()))?;
    acceptor
        .set_ca_file(cert_path)
        .map_err(|e| PsqlError::Uncategorized(e.into()))?;
    acceptor
        .set_certificate_chain_file(cert_path)
        .map_err(|e| PsqlError::Uncategorized(e.into()))?;
    let acceptor = acceptor.build();

    Ok(acceptor.into_context())
}

pub mod truncated_fmt {
    use std::fmt::*;

    struct TruncatedFormatter<'a, 'b> {
        remaining: usize,
        finished: bool,
        f: &'a mut Formatter<'b>,
    }
    impl Write for TruncatedFormatter<'_, '_> {
        fn write_str(&mut self, s: &str) -> Result {
            if self.finished {
                return Ok(());
            }

            if self.remaining < s.len() {
                let actual = s.floor_char_boundary(self.remaining);
                self.f.write_str(&s[0..actual])?;
                self.remaining -= actual;
                self.f.write_str(&format!("...(truncated,{})", s.len()))?;
                self.finished = true; // so that ...(truncated) is printed exactly once
            } else {
                self.f.write_str(s)?;
                self.remaining -= s.len();
            }
            Ok(())
        }
    }

    pub struct TruncatedFmt<'a, T>(pub &'a T, pub usize);

    impl<T> Debug for TruncatedFmt<'_, T>
    where
        T: Debug,
    {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            TruncatedFormatter {
                remaining: self.1,
                finished: false,
                f,
            }
            .write_fmt(format_args!("{:?}", self.0))
        }
    }

    impl<T> Display for TruncatedFmt<'_, T>
    where
        T: Display,
    {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            TruncatedFormatter {
                remaining: self.1,
                finished: false,
                f,
            }
            .write_fmt(format_args!("{}", self.0))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_trunc_utf8() {
            assert_eq!(
                format!("{}", TruncatedFmt(&"select '🌊';", 10)),
                "select '...(truncated,14)",
            );
        }
    }
}

/// Handle `options` in `StartupMessage` from client
///
/// It is like shell arguments but only respects backslash-escape and space;
/// quotes have no special meaning and are handled literally.
///
/// PostgreSQL allows both `-c key=value` and `--key=value`.
///
/// `key-name` is normalized as `key_name`.
///
/// * <https://github.com/postgres/postgres/blob/REL_18_1/src/backend/utils/init/postinit.c#L487>
/// * <https://github.com/postgres/postgres/blob/REL_18_1/src/backend/tcop/postgres.c#L3866>
/// * <https://github.com/postgres/postgres/blob/REL_18_1/src/backend/utils/misc/guc.c#L6361>
fn parse_options(options: &str) -> PsqlResult<Vec<(String, String)>> {
    let mut args = Vec::new();
    let mut current_arg = String::new();
    let mut chars = options.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next_c) = chars.next() {
                current_arg.push(next_c);
            }
        } else if c.is_ascii_whitespace() {
            if !current_arg.is_empty() {
                args.push(std::mem::take(&mut current_arg));
            }
        } else {
            current_arg.push(c);
        }
    }
    if !current_arg.is_empty() {
        args.push(current_arg);
    }

    let mut args_iter = args.into_iter();
    let mut config = Vec::new();

    while let Some(arg) = args_iter.next() {
        if arg == "-c" {
            if let Some(config_str) = args_iter.next() {
                if let Some((key, value)) = config_str.split_once('=') {
                    let key = key.replace("-", "_");
                    config.push((key, value.to_owned()));
                } else {
                    return Err(PsqlError::StartupError(
                        format!("invalid config format: {}", config_str).into(),
                    ));
                }
            } else {
                return Err(PsqlError::StartupError("missing argument for -c".into()));
            }
        } else if let Some(config_str) = arg.strip_prefix("--") {
            if let Some((key, value)) = config_str.split_once('=') {
                let key = key.replace("-", "_");
                config.push((key, value.to_owned()));
            } else {
                return Err(PsqlError::StartupError(
                    format!("invalid config format: {}", config_str).into(),
                ));
            }
        } else {
            tracing::warn!(
                arg,
                "ignoring unrecognized option for backward compatibility"
            );
        }
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::types::Row;

    #[tokio::test]
    async fn test_streaming_write_flushes_at_threshold() {
        const STREAM_FLUSH_THRESHOLD: usize = 64 * 1024;

        let (server, mut client) = tokio::io::duplex(STREAM_FLUSH_THRESHOLD * 2);
        let mut stream = PgStream::new(server, STREAM_FLUSH_THRESHOLD);

        let small_row = Row::new(vec![Some(Bytes::from_static(b"small"))]);
        stream
            .write_streaming(BeMessage::DataRow(&small_row))
            .await
            .unwrap();
        assert!(!stream.write_buf.is_empty());

        let large_row = Row::new(vec![Some(Bytes::from(vec![0; STREAM_FLUSH_THRESHOLD]))]);
        stream
            .write_streaming(BeMessage::DataRow(&large_row))
            .await
            .unwrap();
        assert!(stream.write_buf.is_empty());

        let mut message_tag = [0];
        client.read_exact(&mut message_tag).await.unwrap();
        assert_eq!(message_tag[0], b'D');
    }

    #[test]
    fn test_redact_parsable_sql() {
        let keywords = Arc::new(HashSet::from(["v2".into(), "v4".into(), "b".into()]));
        let sql = r"
        create source temp (k bigint, v varchar) with (
            connector = 'datagen',
            v1 = 123,
            v2 = 'with',
            v3 = false,
            v4 = '',
        ) FORMAT plain ENCODE json (a='1',b='2')
        ";
        assert_eq!(
            redact_sql(sql, keywords),
            "CREATE SOURCE temp (k BIGINT, v CHARACTER VARYING) WITH (connector = 'datagen', v1 = 123, v2 = [REDACTED], v3 = false, v4 = [REDACTED]) FORMAT PLAIN ENCODE JSON (a = '1', b = [REDACTED])"
        );
    }

    #[test]
    fn test_redact_user_password_sql() {
        let keywords = Arc::new(HashSet::from(["password".into()]));

        assert_eq!(
            redact_sql("ALTER USER WITH PASSWORD 'rw_password_2'", keywords.clone()),
            "ALTER USER WITH PASSWORD [REDACTED]"
        );
        assert_eq!(
            redact_sql(
                "ALTER USER foo WITH ENCRYPTED PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'",
                keywords.clone(),
            ),
            "ALTER USER foo WITH ENCRYPTED PASSWORD [REDACTED]"
        );
        assert_eq!(
            redact_sql("CREATE USER foo WITH PASSWORD 'rw_password_2'", keywords),
            "CREATE USER foo WITH PASSWORD [REDACTED]"
        );
    }

    #[test]
    fn test_parse_options() {
        assert_eq!(parse_options("").unwrap(), vec![]);
        assert_eq!(
            parse_options("-c a=1 -c b=2").unwrap(),
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
        assert_eq!(
            parse_options("-c   key=value").unwrap(),
            vec![("key".into(), "value".into())]
        );
        // Custom parser treats quotes as normal characters, so they are included in value
        assert_eq!(
            parse_options("-c key='value'").unwrap(),
            vec![("key".into(), "'value'".into())]
        );

        // Test backslash escaping for spaces (standard Postgres way)
        assert_eq!(
            parse_options(r#"-c key=value\ with\ spaces"#).unwrap(),
            vec![("key".into(), "value with spaces".into())]
        );
        assert_eq!(
            parse_options(r#"-c search_path=my\ schema"#).unwrap(),
            vec![("search_path".into(), "my schema".into())]
        );

        assert!(parse_options("-c").is_err());
        assert!(parse_options("-c foo").is_err()); // missing =
        assert!(parse_options("--foo").is_err()); // missing = in -- option

        assert_eq!(
            parse_options("--foo=bar").unwrap(),
            vec![("foo".into(), "bar".into())]
        );
        assert_eq!(
            parse_options(r#"--foo=bar\ baz"#).unwrap(),
            vec![("foo".into(), "bar baz".into())]
        );
        assert_eq!(
            parse_options("-c a=1 --b=2").unwrap(),
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
        // Unpaired trailing backslash is silently dropped, same as PostgreSQL
        assert_eq!(
            parse_options(r#"-c a=b\"#).unwrap(),
            vec![("a".into(), "b".into())]
        );
    }
}
