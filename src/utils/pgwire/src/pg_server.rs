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

use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use parking_lot::Mutex;
use risingwave_common::config::HbaEntry;
use risingwave_common::id::DatabaseId;
use risingwave_common::types::DataType;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_sqlparser::ast::Statement;
use serde::Deserialize;
use thiserror_ext::AsReport;

use crate::error::{PsqlError, PsqlResult};
use crate::ldap_auth::LdapAuthenticator;
use crate::net::{AddressRef, Listener, TcpKeepalive};
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_message::TransactionStatus;
use crate::pg_protocol::{ConnectionContext, PgByteStream, PgProtocol};
use crate::pg_response::{PgResponse, ValuesStream};
use crate::types::Format;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;
type ProcessId = i32;
type SecretKey = i32;
pub type SessionId = (ProcessId, SecretKey);

/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager: Send + Sync + 'static {
    type Error: Into<BoxedError>;
    type Session: Session<Error = Self::Error>;

    /// In the process of auto schema change, we need a dummy session to access
    /// catalog information in frontend and build a replace plan for the table.
    fn create_dummy_session(
        &self,
        database_id: DatabaseId,
    ) -> Result<Arc<Self::Session>, Self::Error>;

    fn connect(
        &self,
        database: &str,
        user_name: &str,
        peer_addr: AddressRef,
    ) -> Result<Arc<Self::Session>, Self::Error>;

    fn cancel_queries_in_session(&self, session_id: SessionId);

    fn cancel_creating_jobs_in_session(&self, session_id: SessionId);

    fn end_session(&self, session: &Self::Session);

    /// Run some cleanup tasks before the server shutdown.
    fn shutdown(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
pub trait Session: Send + Sync {
    type Error: Into<BoxedError>;
    type ValuesStream: ValuesStream;
    type PreparedStatement: Send + Clone + 'static;
    type Portal: Send + Clone + std::fmt::Display + 'static;

    /// The str sql can not use the unparse from AST: There is some problem when dealing with create
    /// view, see <https://github.com/risingwavelabs/risingwave/issues/6801>.
    fn run_one_query(
        self: Arc<Self>,
        stmt: Statement,
        format: Format,
    ) -> impl Future<Output = Result<PgResponse<Self::ValuesStream>, Self::Error>> + Send;

    fn parse(
        self: Arc<Self>,
        sql: Option<Statement>,
        params_types: Vec<Option<DataType>>,
    ) -> impl Future<Output = Result<Self::PreparedStatement, Self::Error>> + Send;

    /// Receive the next notice message to send to the client.
    ///
    /// This function should be cancellation-safe.
    fn next_notice(self: &Arc<Self>) -> impl Future<Output = String> + Send;

    fn bind(
        self: Arc<Self>,
        prepare_statement: Self::PreparedStatement,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
        result_formats: Vec<Format>,
    ) -> Result<Self::Portal, Self::Error>;

    fn execute(
        self: Arc<Self>,
        portal: Self::Portal,
    ) -> impl Future<Output = Result<PgResponse<Self::ValuesStream>, Self::Error>> + Send;

    fn describe_statement(
        self: Arc<Self>,
        prepare_statement: Self::PreparedStatement,
    ) -> Result<(Vec<DataType>, Vec<PgFieldDescriptor>), Self::Error>;

    fn describe_portal(
        self: Arc<Self>,
        portal: Self::Portal,
    ) -> Result<Vec<PgFieldDescriptor>, Self::Error>;

    fn user_authenticator(&self) -> &UserAuthenticator;

    fn id(&self) -> SessionId;

    fn get_config(&self, key: &str) -> Result<String, Self::Error>;

    fn set_config(&self, key: &str, value: String) -> Result<String, Self::Error>;

    fn transaction_status(&self) -> TransactionStatus;

    fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard;

    fn check_idle_in_transaction_timeout(&self) -> PsqlResult<()>;

    fn user(&self) -> String;
}

/// Each session could run different SQLs multiple times.
/// `ExecContext` represents the lifetime of a running SQL in the current session.
pub struct ExecContext {
    pub running_sql: Arc<str>,
    /// The instant of the running sql
    pub last_instant: Instant,
    /// A reference used to update when `ExecContext` is dropped
    pub last_idle_instant: Arc<Mutex<Option<Instant>>>,
}

/// `ExecContextGuard` holds a `Arc` pointer. Once `ExecContextGuard` is dropped,
/// the inner `Arc<ExecContext>` should not be referred anymore, so that its `Weak` reference (used in `SessionImpl`) will be the same lifecycle of the running sql execution context.
pub struct ExecContextGuard(#[expect(dead_code)] Arc<ExecContext>);

impl ExecContextGuard {
    pub fn new(exec_context: Arc<ExecContext>) -> Self {
        Self(exec_context)
    }
}

impl Drop for ExecContext {
    fn drop(&mut self) {
        *self.last_idle_instant.lock() = Some(Instant::now());
    }
}

#[derive(Debug, Clone)]
pub enum UserAuthenticator {
    // No need to authenticate.
    None,
    // raw password in clear-text form.
    ClearText(Vec<u8>),
    // password encrypted with random salt.
    Md5WithSalt {
        encrypted_password: Vec<u8>,
        salt: [u8; 4],
    },
    OAuth {
        metadata: HashMap<String, String>,
        cluster_id: String,
    },
    Ldap(String, HbaEntry),
}

/// A JWK Set is a JSON object that represents a set of JWKs.
/// The JSON object MUST have a "keys" member, with its value being an array of JWKs.
/// See <https://www.rfc-editor.org/rfc/rfc7517.html#section-5> for more details.
#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

/// A JSON Web Key (JWK) is a JSON object that represents a cryptographic key.
/// See <https://www.rfc-editor.org/rfc/rfc7517.html#section-4> for more details.
#[derive(Debug, Deserialize)]
struct Jwk {
    kty: Option<String>, // Key Type
    kid: Option<String>, // Key ID (OPTIONAL per RFC 7517 section 4.5)
    alg: Option<String>, // Algorithm (OPTIONAL per RFC 7517 section 4.4)
    n: Option<String>,   // RSA modulus
    e: Option<String>,   // RSA exponent
}

/// Algorithms we accept for JWT signature verification.
///
/// Restricted to RSA-family algorithms because the only `DecodingKey` we build
/// is from RSA components (`n`, `e`). Pinning the algorithm to a server-side
/// allow-list also prevents the classic alg-confusion attack: a token with
/// `alg: "none"` (no signature) or `alg: "HS256"` forged using the RSA public
/// key as the HMAC secret cannot select a verification algorithm outside this
/// set.
const ALLOWED_JWT_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
];
const RSA_JWK_KEY_TYPE: &str = "RSA";
/// Optional OAuth user option overriding the expected JWT `aud`.
const OAUTH_AUDIENCE_KEY: &str = "audience";

async fn validate_jwt(
    jwt: &str,
    jwks_url: &str,
    issuer: &str,
    audience: &str,
    metadata: &HashMap<String, String>,
) -> Result<bool, BoxedError> {
    let jwks: Jwks = reqwest::get(jwks_url).await?.json().await?;
    validate_jwt_with_jwks(jwt, &jwks, issuer, audience, metadata)
}

fn audience_from_cluster_id(cluster_id: &str) -> String {
    format!("urn:risingwave:cluster:{}", cluster_id)
}

fn resolve_oauth_audience(
    configured: Option<String>,
    cluster_id: &str,
) -> Result<String, BoxedError> {
    let Some(configured_audience) = configured else {
        return Ok(audience_from_cluster_id(cluster_id));
    };

    let audience = configured_audience.trim();
    if audience.is_empty() {
        return Err(format!("OAuth option `{OAUTH_AUDIENCE_KEY}` cannot be empty").into());
    }
    Ok(audience.to_owned())
}

fn validate_jwt_with_jwks(
    jwt: &str,
    jwks: &Jwks,
    issuer: &str,
    audience: &str,
    metadata: &HashMap<String, String>,
) -> Result<bool, BoxedError> {
    let header = decode_header(jwt)?;

    // 1. Retrieve the kid from the header to find compatible JWKs in the JWK Set.
    let kid = header.kid.ok_or("JWT header missing 'kid' field")?;

    // 2. Decide which algorithm to use.
    //
    // Per RFC 7517 §4.4 the JWK `alg` member is OPTIONAL. When the JWK pins an
    // `alg`, the JWT header MUST match it; when it doesn't, we fall back to
    // the header's `alg` but only after checking it against a server-side
    // allow-list. The allow-list is what ultimately blocks alg-confusion: an
    // attacker-chosen `alg` from the token header alone must never be trusted
    // to select the verification algorithm.
    if !ALLOWED_JWT_ALGORITHMS.contains(&header.alg) {
        return Err(format!("JWT alg {:?} is not allowed", header.alg).into());
    }

    // A JWK Set can contain unrelated key types, keys without an optional `kid`,
    // and equivalent keys of different types that share a `kid`. Scan all
    // candidates instead of letting the first same-`kid` key shadow a usable
    // RSA key later in the set.
    let jwk = jwks
        .keys
        .iter()
        .filter(|jwk| jwk.kid.as_deref() == Some(kid.as_str()))
        .find(|jwk| {
            jwk.kty.as_deref() == Some(RSA_JWK_KEY_TYPE)
                && jwk.n.is_some()
                && jwk.e.is_some()
                && match jwk.alg.as_deref() {
                    Some(alg) => Algorithm::from_str(alg).is_ok_and(|alg| alg == header.alg),
                    None => true,
                }
        })
        .ok_or_else(|| format!("No compatible RSA key found in JWKS for kid: '{}'", kid))?;

    // 3. Decode the JWT and validate the claims.
    let n = jwk.n.as_deref().expect("RSA candidate must have a modulus");
    let e = jwk
        .e
        .as_deref()
        .expect("RSA candidate must have an exponent");
    let decoding_key = DecodingKey::from_rsa_components(n, e)?;
    let mut validation = Validation::new(header.alg);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.set_required_spec_claims(&["exp", "iss", "aud"]);
    let token_data = decode::<HashMap<String, serde_json::Value>>(jwt, &decoding_key, &validation)?;

    // 4. Check if the metadata in the token matches.
    if !metadata.iter().all(
        |(k, v)| matches!(token_data.claims.get(k), Some(serde_json::Value::String(s)) if s == v),
    ) {
        return Err("metadata in jwt does not match with metadata declared with user".into());
    }
    Ok(true)
}

impl UserAuthenticator {
    pub async fn authenticate(&self, password: &[u8]) -> PsqlResult<()> {
        let success = match self {
            UserAuthenticator::None => true,
            UserAuthenticator::ClearText(text) => password == text,
            UserAuthenticator::Md5WithSalt {
                encrypted_password, ..
            } => encrypted_password == password,
            UserAuthenticator::OAuth {
                metadata,
                cluster_id,
            } => {
                let mut metadata = metadata.clone();
                let jwks_url = metadata.remove("jwks_url").unwrap();
                let issuer = metadata.remove("issuer").unwrap();
                let audience =
                    resolve_oauth_audience(metadata.remove(OAUTH_AUDIENCE_KEY), cluster_id)
                        .map_err(PsqlError::StartupError)?;
                validate_jwt(
                    &String::from_utf8_lossy(password),
                    &jwks_url,
                    &issuer,
                    &audience,
                    &metadata,
                )
                .await
                .map_err(PsqlError::StartupError)?
            }
            UserAuthenticator::Ldap(user_name, hba_entry) => {
                let ldap_auth = LdapAuthenticator::new(hba_entry)?;
                // Convert password to string, defaulting to empty if not valid UTF-8
                let password_str = String::from_utf8_lossy(password).into_owned();
                ldap_auth.authenticate(user_name, &password_str).await?
            }
        };
        if !success {
            return Err(PsqlError::PasswordError);
        }
        Ok(())
    }
}

/// Binds a Tcp or Unix listener at `addr`. Spawn a coroutine to serve every new connection.
///
/// Returns when the `shutdown` token is triggered.
pub async fn pg_serve(
    addr: &str,
    tcp_keepalive: TcpKeepalive,
    session_mgr: Arc<impl SessionManager>,
    context: ConnectionContext,
    shutdown: CancellationToken,
) -> Result<(), BoxedError> {
    let listener = Listener::bind(addr).await?;
    tracing::info!(addr, "server started");

    let acceptor_runtime = BackgroundShutdownRuntime::from({
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(1);
        builder
            .thread_name("rw-acceptor")
            .enable_all()
            .build()
            .unwrap()
    });

    #[cfg(not(madsim))]
    let worker_runtime = tokio::runtime::Handle::current();
    #[cfg(madsim)]
    let worker_runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let session_mgr_clone = session_mgr.clone();
    let f = async move {
        loop {
            let conn_ret = listener.accept(&tcp_keepalive).await;
            match conn_ret {
                Ok((stream, peer_addr)) => {
                    tracing::info!(%peer_addr, "accept connection");
                    worker_runtime.spawn(handle_connection(
                        stream,
                        session_mgr_clone.clone(),
                        Arc::new(peer_addr),
                        context.clone(),
                    ));
                }

                Err(e) => {
                    tracing::error!(error = %e.as_report(), "failed to accept connection",);
                }
            }
        }
    };
    acceptor_runtime.spawn(f);

    // Wait for the shutdown signal.
    shutdown.cancelled().await;

    // Stop accepting new connections.
    drop(acceptor_runtime);
    // Shutdown session manager, typically close all existing sessions.
    session_mgr.shutdown().await;

    Ok(())
}

pub async fn handle_connection<S, SM>(
    stream: S,
    session_mgr: Arc<SM>,
    peer_addr: AddressRef,
    context: ConnectionContext,
) where
    S: PgByteStream,
    SM: SessionManager,
{
    PgProtocol::new(stream, session_mgr, peer_addr, context)
        .run()
        .await;
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use risingwave_common::id::DatabaseId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::tokio_util::sync::CancellationToken;
    use risingwave_sqlparser::ast::Statement;
    use tokio_postgres::NoTls;

    use crate::error::PsqlResult;
    use crate::memory_manager::MessageMemoryManager;
    use crate::pg_field_descriptor::PgFieldDescriptor;
    use crate::pg_message::TransactionStatus;
    use crate::pg_protocol::ConnectionContext;
    use crate::pg_response::{PgResponse, RowSetResult, StatementType};
    use crate::pg_server::{
        BoxedError, ExecContext, ExecContextGuard, Session, SessionId, SessionManager,
        UserAuthenticator, pg_serve,
    };
    use crate::types;
    use crate::types::Row;

    struct MockSessionManager {}
    struct MockSession {}

    const STREAMING_TEST_QUERY: &str = "SELECT 'pgwire_streaming_test'";

    impl SessionManager for MockSessionManager {
        type Error = BoxedError;
        type Session = MockSession;

        fn create_dummy_session(
            &self,
            _database_id: DatabaseId,
        ) -> Result<Arc<Self::Session>, Self::Error> {
            unimplemented!()
        }

        fn connect(
            &self,
            _database: &str,
            _user_name: &str,
            _peer_addr: crate::net::AddressRef,
        ) -> Result<Arc<Self::Session>, Self::Error> {
            Ok(Arc::new(MockSession {}))
        }

        fn cancel_queries_in_session(&self, _session_id: SessionId) {
            todo!()
        }

        fn cancel_creating_jobs_in_session(&self, _session_id: SessionId) {
            todo!()
        }

        fn end_session(&self, _session: &Self::Session) {}
    }

    impl Session for MockSession {
        type Error = BoxedError;
        type Portal = String;
        type PreparedStatement = String;
        type ValuesStream = BoxStream<'static, RowSetResult>;

        async fn run_one_query(
            self: Arc<Self>,
            _stmt: Statement,
            _format: types::Format,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, Self::Error> {
            Ok(PgResponse::builder(StatementType::SELECT)
                .values(
                    futures::stream::iter(vec![Ok(vec![Row::new(vec![Some(Bytes::new())])])])
                        .boxed(),
                    vec![
                        // 1043 is the oid of varchar type.
                        // -1 is the type len of varchar type.
                        PgFieldDescriptor::new("".to_owned(), 1043, -1);
                        1
                    ],
                )
                .into())
        }

        async fn parse(
            self: Arc<Self>,
            sql: Option<Statement>,
            _params_types: Vec<Option<DataType>>,
        ) -> Result<String, Self::Error> {
            Ok(sql.map(|stmt| stmt.to_string()).unwrap_or_default())
        }

        fn bind(
            self: Arc<Self>,
            prepare_statement: String,
            _params: Vec<Option<Bytes>>,
            _param_formats: Vec<types::Format>,
            _result_formats: Vec<types::Format>,
        ) -> Result<String, Self::Error> {
            Ok(prepare_statement)
        }

        async fn execute(
            self: Arc<Self>,
            portal: String,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, Self::Error> {
            if portal == STREAMING_TEST_QUERY {
                let first_row = futures::stream::once(async {
                    Ok(vec![Row::new(vec![Some(Bytes::from(vec![
                        b'x';
                        128 * 1024
                    ]))])])
                });
                let remaining_rows = futures::stream::pending();
                return Ok(PgResponse::builder(StatementType::SELECT)
                    .values(
                        first_row.chain(remaining_rows).boxed(),
                        vec![PgFieldDescriptor::new("".to_owned(), 1043, -1)],
                    )
                    .into());
            }

            Ok(PgResponse::builder(StatementType::SELECT)
                .values(
                    futures::stream::iter(vec![Ok(vec![Row::new(vec![Some(Bytes::new())])])])
                        .boxed(),
                    vec![
                    // 1043 is the oid of varchar type.
                    // -1 is the type len of varchar type.
                    PgFieldDescriptor::new("".to_owned(), 1043, -1);
                    1
                ],
                )
                .into())
        }

        fn describe_statement(
            self: Arc<Self>,
            _statement: String,
        ) -> Result<(Vec<DataType>, Vec<PgFieldDescriptor>), Self::Error> {
            Ok((
                vec![],
                vec![PgFieldDescriptor::new("".to_owned(), 1043, -1)],
            ))
        }

        fn describe_portal(
            self: Arc<Self>,
            _portal: String,
        ) -> Result<Vec<PgFieldDescriptor>, Self::Error> {
            Ok(vec![PgFieldDescriptor::new("".to_owned(), 1043, -1)])
        }

        fn user_authenticator(&self) -> &UserAuthenticator {
            &UserAuthenticator::None
        }

        fn id(&self) -> SessionId {
            (0, 0)
        }

        fn get_config(&self, key: &str) -> Result<String, Self::Error> {
            match key {
                "timezone" => Ok("UTC".to_owned()),
                _ => Err(format!("Unknown config key: {key}").into()),
            }
        }

        fn set_config(&self, _key: &str, _value: String) -> Result<String, Self::Error> {
            Ok("".to_owned())
        }

        async fn next_notice(self: &Arc<Self>) -> String {
            std::future::pending().await
        }

        fn transaction_status(&self) -> TransactionStatus {
            TransactionStatus::Idle
        }

        fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard {
            let exec_context = Arc::new(ExecContext {
                running_sql: sql,
                last_instant: Instant::now(),
                last_idle_instant: Default::default(),
            });
            ExecContextGuard::new(exec_context)
        }

        fn check_idle_in_transaction_timeout(&self) -> PsqlResult<()> {
            Ok(())
        }

        fn user(&self) -> String {
            "mock".to_owned()
        }
    }

    async fn do_test_query(bind_addr: impl Into<String>, pg_config: impl Into<String>) {
        let bind_addr = bind_addr.into();
        let pg_config = pg_config.into();

        let session_mgr = MockSessionManager {};
        tokio::spawn(async move {
            pg_serve(
                &bind_addr,
                socket2::TcpKeepalive::new(),
                Arc::new(session_mgr),
                ConnectionContext {
                    tls_config: None,
                    redact_sql_option_keywords: None,
                    message_memory_manager: MessageMemoryManager::new(u64::MAX, u64::MAX, u64::MAX)
                        .into(),
                    stream_flush_threshold_bytes: 64 * 1024,
                },
                CancellationToken::new(), // dummy
            )
            .await
        });
        // wait for server to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect to the database.
        let (client, connection) = tokio_postgres::connect(&pg_config, NoTls).await.unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rows = client
            .simple_query("SELECT ''")
            .await
            .expect("Error executing query");
        // Row + CommandComplete
        assert_eq!(rows.len(), 2);

        let rows = client
            .query("SELECT ''", &[])
            .await
            .expect("Error executing query");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_tcp() {
        do_test_query("127.0.0.1:10000", "host=localhost port=10000").await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_query_unix() {
        let port: i16 = 10000;
        let dir = tempfile::TempDir::new().unwrap();
        let sock = dir.path().join(format!(".s.PGSQL.{port}"));

        do_test_query(
            format!("unix:{}", sock.to_str().unwrap()),
            format!("host={} port={}", dir.path().to_str().unwrap(), port),
        )
        .await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_large_query_row_is_streamed_before_query_finishes() {
        let port: i16 = 10001;
        let dir = tempfile::TempDir::new().unwrap();
        let sock = dir.path().join(format!(".s.PGSQL.{port}"));
        let bind_addr = format!("unix:{}", sock.to_str().unwrap());
        let pg_config = format!("host={} port={port}", dir.path().to_str().unwrap());
        let cancellation = CancellationToken::new();

        let server_cancellation = cancellation.clone();
        tokio::spawn(async move {
            pg_serve(
                &bind_addr,
                socket2::TcpKeepalive::new(),
                Arc::new(MockSessionManager {}),
                ConnectionContext {
                    tls_config: None,
                    redact_sql_option_keywords: None,
                    message_memory_manager: MessageMemoryManager::new(u64::MAX, u64::MAX, u64::MAX)
                        .into(),
                    stream_flush_threshold_bytes: 64 * 1024,
                },
                server_cancellation,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (client, connection) = tokio_postgres::connect(&pg_config, NoTls).await.unwrap();
        let connection_handle = tokio::spawn(connection);
        let params: &[&str] = &[];
        let rows = client
            .query_raw(STREAMING_TEST_QUERY, params)
            .await
            .unwrap();
        futures::pin_mut!(rows);

        let first_row = tokio::time::timeout(Duration::from_secs(5), rows.next())
            .await
            .expect("first row should be sent before the result stream finishes")
            .unwrap()
            .unwrap();
        assert_eq!(first_row.get::<_, &str>(0).len(), 128 * 1024);

        drop(client);
        cancellation.cancel();
        connection_handle.abort();
    }

    mod jwt_validation_tests {
        use std::collections::HashMap;
        use std::time::{SystemTime, UNIX_EPOCH};

        use base64::Engine;
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        use rsa::pkcs1::EncodeRsaPrivateKey;
        use rsa::traits::PublicKeyParts;
        use rsa::{RsaPrivateKey, RsaPublicKey};
        use serde_json::json;

        use crate::pg_server::{Jwk, Jwks, resolve_oauth_audience, validate_jwt_with_jwks};

        fn create_test_rsa_keys() -> (RsaPrivateKey, RsaPublicKey) {
            let mut rng = rand::thread_rng();
            let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("failed to generate a key");
            let public_key = RsaPublicKey::from(&private_key);
            (private_key, public_key)
        }

        fn create_test_jwks(public_key: &RsaPublicKey, kid: &str, alg: Option<&str>) -> Jwks {
            let n = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(public_key.n().to_bytes_be());
            let e = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(public_key.e().to_bytes_be());

            Jwks {
                keys: vec![Jwk {
                    kty: Some("RSA".to_owned()),
                    kid: Some(kid.to_owned()),
                    alg: alg.map(ToOwned::to_owned),
                    n: Some(n),
                    e: Some(e),
                }],
            }
        }

        fn create_jwt_token(
            private_key: &RsaPrivateKey,
            kid: &str,
            algorithm: Algorithm,
            issuer: &str,
            audience: Option<&str>,
            exp: u64,
            additional_claims: HashMap<String, serde_json::Value>,
        ) -> String {
            let mut header = Header::new(algorithm);
            header.kid = Some(kid.to_owned());

            let mut claims = json!({
                "iss": issuer,
                "exp": exp,
            });

            if let Some(aud) = audience {
                claims["aud"] = json!(aud);
            }

            for (key, value) in additional_claims {
                claims[key] = value;
            }

            let encoding_key = EncodingKey::from_rsa_pem(
                private_key
                    .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();

            jsonwebtoken::encode(&header, &claims, &encoding_key).unwrap()
        }

        fn get_future_timestamp() -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600 // 1 hour from now
        }

        fn get_past_timestamp() -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - 3600 // 1 hour ago
        }

        #[test]
        fn test_jwt_with_invalid_audience() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let metadata = HashMap::new();

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:wrong-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert!(error.to_string().contains("InvalidAudience"));
        }

        #[test]
        fn test_jwt_with_configured_audience() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let mut additional_claims = HashMap::new();
            additional_claims.insert("aud".to_owned(), json!(["urn:sn:cloud:o-for6u"]));
            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                None,
                get_future_timestamp(),
                additional_claims,
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:sn:cloud:o-for6u",
                &HashMap::new(),
            );

            assert!(result.unwrap());
        }

        #[test]
        fn test_empty_configured_audience() {
            let error =
                resolve_oauth_audience(Some("  ".to_owned()), "test-cluster-id").unwrap_err();
            assert!(error.to_string().contains("cannot be empty"));
        }

        #[test]
        fn test_default_audience_from_cluster_id() {
            let audience = resolve_oauth_audience(None, "test-cluster-id").unwrap();
            assert_eq!(audience, "urn:risingwave:cluster:test-cluster-id");
        }

        #[test]
        fn test_jwt_with_missing_audience() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let metadata = HashMap::new();

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                None, // No audience claim
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert!(error.to_string().contains("Missing required claim: aud"));
        }

        #[test]
        fn test_jwt_with_invalid_issuer() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let metadata = HashMap::new();

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://wrong-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert!(error.to_string().contains("InvalidIssuer"));
        }

        #[test]
        fn test_jwt_with_kid_not_found_in_jwks() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "different-kid", Some("RS256"));

            let metadata = HashMap::new();

            let jwt = create_jwt_token(
                &private_key,
                "missing-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("No compatible RSA key found in JWKS for kid: 'missing-kid'")
            );
        }

        #[test]
        fn test_jwt_with_empty_jwks_reports_no_matching_key() {
            let (private_key, _) = create_test_rsa_keys();
            let jwks = Jwks { keys: vec![] };

            let jwt = create_jwt_token(
                &private_key,
                "missing-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &HashMap::new(),
            );

            let error = result.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("No compatible RSA key found in JWKS for kid: 'missing-kid'")
            );
        }

        #[test]
        fn test_jwt_with_expired_token() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let metadata = HashMap::new();

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_past_timestamp(), // Expired token
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert!(error.to_string().contains("ExpiredSignature"));
        }

        #[test]
        fn test_jwt_with_invalid_signature() {
            let (_, public_key) = create_test_rsa_keys();
            let (wrong_private_key, _) = create_test_rsa_keys(); // Different key pair
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let metadata = HashMap::new();

            // Sign with wrong private key
            let jwt = create_jwt_token(
                &wrong_private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert!(error.to_string().contains("InvalidSignature"));
        }

        #[test]
        fn test_metadata_validation_success() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let mut metadata = HashMap::new();
            metadata.insert("role".to_owned(), "admin".to_owned());
            metadata.insert("department".to_owned(), "security".to_owned());

            let mut claims = HashMap::new();
            claims.insert("role".to_owned(), json!("admin"));
            claims.insert("department".to_owned(), json!("security"));
            claims.insert("extra_claim".to_owned(), json!("ignored")); // Extra claims are fine

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                claims,
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            assert!(result.unwrap());
        }

        #[test]
        fn test_metadata_validation_failure() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS256"));

            let mut metadata = HashMap::new();
            metadata.insert("role".to_owned(), "admin".to_owned());
            metadata.insert("department".to_owned(), "security".to_owned());

            let mut claims = HashMap::new();
            claims.insert("role".to_owned(), json!("user")); // Wrong role
            claims.insert("department".to_owned(), json!("security"));

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                claims,
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &metadata,
            );

            let error = result.unwrap_err();
            assert_eq!(
                error.to_string(),
                "metadata in jwt does not match with metadata declared with user"
            );
        }

        #[test]
        fn test_jwt_with_jwk_missing_alg_succeeds() {
            let (private_key, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", None);

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &HashMap::new(),
            );

            assert!(result.unwrap());
        }

        #[test]
        fn test_jwt_with_mixed_jwks_ignores_non_matching_ec_key() {
            let (private_key, public_key) = create_test_rsa_keys();
            let mut jwks = create_test_jwks(&public_key, "rsa-kid", Some("RS256"));
            jwks.keys.push(Jwk {
                kty: Some("EC".to_owned()),
                kid: Some("ec-kid".to_owned()),
                alg: Some("ES256".to_owned()),
                n: None,
                e: None,
            });

            let jwt = create_jwt_token(
                &private_key,
                "rsa-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &HashMap::new(),
            );

            assert!(result.unwrap());
        }

        #[test]
        fn test_jwks_deserializes_ec_key_without_kid() {
            let jwks: Jwks = serde_json::from_value(json!({
                "keys": [
                    {
                        "alg": "ES256",
                        "kty": "EC",
                        "crv": "P-256",
                        "x": "x-coordinate",
                        "y": "y-coordinate"
                    },
                    {
                        "kid": "rsa-kid",
                        "alg": "RS256",
                        "kty": "RSA",
                        "n": "modulus",
                        "e": "AQAB"
                    }
                ]
            }))
            .unwrap();

            assert_eq!(jwks.keys.len(), 2);
            assert!(jwks.keys[0].kid.is_none());
            assert_eq!(jwks.keys[1].kid.as_deref(), Some("rsa-kid"));
            assert_eq!(jwks.keys[1].n.as_deref(), Some("modulus"));
        }

        #[test]
        fn test_jwt_uses_compatible_rsa_key_when_ec_key_has_same_kid() {
            let (private_key, public_key) = create_test_rsa_keys();
            let mut jwks = create_test_jwks(&public_key, "shared-kid", Some("RS256"));
            jwks.keys.insert(
                0,
                Jwk {
                    kty: Some("EC".to_owned()),
                    kid: Some("shared-kid".to_owned()),
                    alg: Some("ES256".to_owned()),
                    n: None,
                    e: None,
                },
            );

            let jwt = create_jwt_token(
                &private_key,
                "shared-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &HashMap::new(),
            );

            assert!(result.unwrap());
        }

        #[test]
        fn test_jwt_with_jwk_missing_alg_rejects_disallowed_header_alg() {
            let (_, public_key) = create_test_rsa_keys();
            let jwks = create_test_jwks(&public_key, "test-kid", None);

            // Craft a token whose header claims HS256. The allow-list check
            // must reject it before the signature is ever verified — this is
            // the defence against the classic "alg=HS256 forged with the RSA
            // public key as the HMAC secret" confusion attack.
            let mut header = Header::new(Algorithm::HS256);
            header.kid = Some("test-kid".to_owned());
            let claims = json!({
                "iss": "https://test-issuer.com",
                "aud": "urn:risingwave:cluster:test-cluster-id",
                "exp": get_future_timestamp(),
            });
            let jwt = jsonwebtoken::encode(
                &header,
                &claims,
                &EncodingKey::from_secret(b"attacker-chosen"),
            )
            .unwrap();

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &HashMap::new(),
            );

            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("is not allowed"),
                "unexpected error: {}",
                error
            );
        }

        #[test]
        fn test_jwt_alg_mismatch_between_header_and_jwk() {
            let (private_key, public_key) = create_test_rsa_keys();
            // JWK pins RS384; token header declares RS256 — must be rejected.
            let jwks = create_test_jwks(&public_key, "test-kid", Some("RS384"));

            let jwt = create_jwt_token(
                &private_key,
                "test-kid",
                Algorithm::RS256,
                "https://test-issuer.com",
                Some("urn:risingwave:cluster:test-cluster-id"),
                get_future_timestamp(),
                HashMap::new(),
            );

            let result = validate_jwt_with_jwks(
                &jwt,
                &jwks,
                "https://test-issuer.com",
                "urn:risingwave:cluster:test-cluster-id",
                &HashMap::new(),
            );

            let error = result.unwrap_err();
            assert_eq!(
                error.to_string(),
                "No compatible RSA key found in JWKS for kid: 'test-kid'"
            );
        }
    }
}
