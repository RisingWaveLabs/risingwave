// Copyright 2026 RisingWave Labs
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

use std::fmt;
use std::time::Duration;

use anyhow::Context;
use reqwest::header::LOCATION;
use reqwest::redirect::Policy;
use reqwest::{Client as HttpClient, Response, StatusCode, Url};
use risingwave_common::bail;
use risingwave_common::util::retry::exponential_backoff;
use tokio_retry::RetryIf;
use tokio_retry::strategy::jitter;

use super::PulsarSchemaInfo;
use crate::error::ConnectorResult;
use crate::schema::{AWS_GLUE_SCHEMA_ARN_KEY, SCHEMA_LOCATION_KEY, SCHEMA_REGISTRY_KEY};
use crate::source::pulsar::topic::parse_topic;
use crate::with_options::{Get, GetKeyIter};

pub const PULSAR_SCHEMA_PREFIX: &str = "schema.pulsar.";
pub const PULSAR_SCHEMA_URL_KEY: &str = "schema.pulsar.url";
pub const PULSAR_SCHEMA_AUTH_TOKEN_KEY: &str = "schema.pulsar.auth.token";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_DELAY_MS: u64 = 100;
const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(3);
const DEFAULT_MAX_RETRIES: usize = 3;
const MAX_REDIRECTS: usize = 5;

#[derive(Debug, thiserror::Error)]
enum RequestError {
    #[error("failed to send request: {0}")]
    Send(#[source] reqwest::Error),
    #[error("request returned HTTP status {0}")]
    Status(StatusCode),
    #[error("invalid redirect: {0}")]
    InvalidRedirect(String),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("failed to parse response: {0}")]
    Json(#[source] reqwest::Error),
}

impl RequestError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Send(_) => true,
            Self::Status(status) => {
                *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
            }
            Self::InvalidRedirect(_) | Self::TooManyRedirects | Self::Json(_) => false,
        }
    }
}

#[derive(Clone)]
pub struct PulsarSchemaConfig {
    url: String,
    auth_token: Option<String>,
}

impl fmt::Debug for PulsarSchemaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PulsarSchemaConfig")
            .field("url", &self.url)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl PulsarSchemaConfig {
    pub fn from_options<T: Get + GetKeyIter>(options: &T) -> ConnectorResult<Option<Self>> {
        if let Some(option) = options.key_iter().find(|option| {
            option.starts_with(PULSAR_SCHEMA_PREFIX)
                && !matches!(
                    *option,
                    PULSAR_SCHEMA_URL_KEY | PULSAR_SCHEMA_AUTH_TOKEN_KEY
                )
        }) {
            bail!("unsupported Pulsar schema option `{option}`");
        }

        let url = options.get(PULSAR_SCHEMA_URL_KEY).cloned();
        let auth_token = options.get(PULSAR_SCHEMA_AUTH_TOKEN_KEY).cloned();

        let Some(url) = url else {
            if auth_token.is_some() {
                bail!("`{PULSAR_SCHEMA_AUTH_TOKEN_KEY}` requires `{PULSAR_SCHEMA_URL_KEY}`");
            }
            return Ok(None);
        };
        if url.is_empty() {
            bail!("`{PULSAR_SCHEMA_URL_KEY}` must not be empty");
        }
        if auth_token.as_ref().is_some_and(String::is_empty) {
            bail!("`{PULSAR_SCHEMA_AUTH_TOKEN_KEY}` must not be empty");
        }
        if let Some(option) = options.key_iter().find(|option| {
            matches!(*option, SCHEMA_LOCATION_KEY | AWS_GLUE_SCHEMA_ARN_KEY)
                || *option == SCHEMA_REGISTRY_KEY
                || option.starts_with("schema.registry.")
        }) {
            bail!("`{option}` cannot be combined with `{PULSAR_SCHEMA_URL_KEY}`");
        }

        Ok(Some(Self { url, auth_token }))
    }
}

pub struct Client {
    inner: HttpClient,
    url: Url,
    auth_token: Option<String>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("url", &self.url)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish_non_exhaustive()
    }
}

impl Client {
    pub fn new(config: &PulsarSchemaConfig) -> ConnectorResult<Self> {
        let url = Url::parse(&config.url).context("invalid Pulsar schema URL")?;
        if url.cannot_be_a_base() {
            bail!("Pulsar schema URL must be a base URL");
        }
        if !matches!(url.scheme(), "http" | "https") {
            bail!("Pulsar schema URL must use HTTP or HTTPS");
        }
        if !url.username().is_empty() || url.password().is_some() {
            bail!("Pulsar schema URL must not contain credentials");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!("Pulsar schema URL must not contain a query or fragment");
        }

        // Pulsar Admin APIs may redirect to the topic-owning broker. Handle redirects below so
        // bearer authentication can be applied again when the host changes.
        let inner = HttpClient::builder()
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .build()
            .context("failed to build Pulsar schema client")?;
        Ok(Self {
            inner,
            url,
            auth_token: config.auth_token.clone(),
        })
    }

    fn build_schema_url(&self, topic: &str, version: Option<i64>) -> ConnectorResult<Url> {
        let topic = parse_topic(topic)?;
        let topic_name = topic.topic_str_without_partition()?;
        let mut url = self.url.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Pulsar schema URL must be a base URL"))?;
        path.extend([
            "admin",
            "v2",
            "schemas",
            topic.tenant.as_str(),
            topic.namespace.as_str(),
            topic_name.as_str(),
            "schema",
        ]);
        if let Some(version) = version {
            path.push(version.to_string().as_str());
        }
        drop(path);
        Ok(url)
    }

    fn redirect_url(current_url: &Url, response: &Response) -> Result<Url, RequestError> {
        let location = response
            .headers()
            .get(LOCATION)
            .ok_or_else(|| RequestError::InvalidRedirect("missing Location header".to_owned()))?
            .to_str()
            .map_err(|error| RequestError::InvalidRedirect(error.to_string()))?;
        let redirect_url = current_url
            .join(location)
            .map_err(|error| RequestError::InvalidRedirect(error.to_string()))?;
        if !matches!(redirect_url.scheme(), "http" | "https") {
            return Err(RequestError::InvalidRedirect(format!(
                "unsupported URL scheme `{}`",
                redirect_url.scheme()
            )));
        }
        if current_url.scheme() == "https" && redirect_url.scheme() == "http" {
            return Err(RequestError::InvalidRedirect(
                "refusing to redirect from HTTPS to HTTP".to_owned(),
            ));
        }
        Ok(redirect_url)
    }

    async fn request_schema(&self, url: &Url) -> Result<PulsarSchemaInfo, RequestError> {
        let mut request_url = url.clone();
        let mut redirects = 0;
        loop {
            let mut request = self.inner.get(request_url.clone());
            if let Some(token) = self.auth_token.as_ref() {
                request = request.bearer_auth(token);
            }

            let response = request.send().await.map_err(RequestError::Send)?;
            if matches!(
                response.status(),
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            ) {
                if redirects >= MAX_REDIRECTS {
                    return Err(RequestError::TooManyRedirects);
                }
                redirects += 1;
                request_url = Self::redirect_url(&request_url, &response)?;
                continue;
            }
            if !response.status().is_success() {
                return Err(RequestError::Status(response.status()));
            }
            return response.json().await.map_err(RequestError::Json);
        }
    }

    pub async fn get_schema(
        &self,
        topic: &str,
        version: Option<i64>,
    ) -> ConnectorResult<PulsarSchemaInfo> {
        let url = self.build_schema_url(topic, version)?;
        let retry_strategy = exponential_backoff(
            Duration::from_millis(DEFAULT_RETRY_DELAY_MS),
            2,
            DEFAULT_MAX_RETRY_DELAY,
        )
        .take(DEFAULT_MAX_RETRIES)
        .map(jitter);
        RetryIf::spawn(
            retry_strategy,
            || self.request_schema(&url),
            |error: &RequestError| {
                let retryable = error.is_retryable();
                if retryable {
                    tracing::debug!(%error, "retrying Pulsar schema request");
                }
                retryable
            },
        )
        .await
        .map_err(|error| {
            anyhow::Error::new(error)
                .context(format!("failed to fetch Pulsar schema from {url}"))
                .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(not(madsim))]
    use std::io::{Read, Write};
    #[cfg(not(madsim))]
    use std::net::TcpListener;
    #[cfg(not(madsim))]
    use std::sync::mpsc;
    #[cfg(not(madsim))]
    use std::thread;

    use super::*;

    fn config(url: String, token: Option<&str>) -> PulsarSchemaConfig {
        let mut options = BTreeMap::from([(PULSAR_SCHEMA_URL_KEY.to_owned(), url)]);
        if let Some(token) = token {
            options.insert(PULSAR_SCHEMA_AUTH_TOKEN_KEY.to_owned(), token.to_owned());
        }
        PulsarSchemaConfig::from_options(&options).unwrap().unwrap()
    }

    fn client() -> Client {
        Client::new(&config(
            "http://localhost:8080".to_owned(),
            Some("test-token"),
        ))
        .unwrap()
    }

    #[test]
    fn schema_url_from_full_topic() {
        let client = client();
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events", Some(42))
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema/42"
        );
    }

    #[test]
    fn schema_url_from_short_partitioned_and_escaped_topics() {
        let client = client();
        assert_eq!(
            client.build_schema_url("events", None).unwrap().as_str(),
            "http://localhost:8080/admin/v2/schemas/public/default/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events-partition-1", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events?region=us", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events%3Fregion=us/schema"
        );
    }

    #[test]
    fn config_is_optional_and_requires_url_for_token() {
        assert!(
            PulsarSchemaConfig::from_options(&BTreeMap::<String, String>::new())
                .unwrap()
                .is_none()
        );
        let options =
            BTreeMap::from([(PULSAR_SCHEMA_AUTH_TOKEN_KEY.to_owned(), "token".to_owned())]);
        assert!(PulsarSchemaConfig::from_options(&options).is_err());
    }

    #[test]
    fn config_rejects_unknown_and_overlapping_options() {
        for option in [
            "schema.pulsar.ca",
            SCHEMA_REGISTRY_KEY,
            "schema.registry.username",
            SCHEMA_LOCATION_KEY,
            AWS_GLUE_SCHEMA_ARN_KEY,
        ] {
            let options = BTreeMap::from([
                (
                    PULSAR_SCHEMA_URL_KEY.to_owned(),
                    "http://localhost:8080".to_owned(),
                ),
                (option.to_owned(), "value".to_owned()),
            ]);
            assert!(
                PulsarSchemaConfig::from_options(&options).is_err(),
                "expected `{option}` to be rejected"
            );
        }
    }

    #[test]
    fn schema_auth_is_separate_from_broker_auth() {
        let options = BTreeMap::from([
            (
                PULSAR_SCHEMA_URL_KEY.to_owned(),
                "http://localhost:8080".to_owned(),
            ),
            (
                PULSAR_SCHEMA_AUTH_TOKEN_KEY.to_owned(),
                "schema-token".to_owned(),
            ),
            ("auth.token".to_owned(), "broker-token".to_owned()),
        ]);
        let config = PulsarSchemaConfig::from_options(&options).unwrap().unwrap();
        assert_eq!(config.auth_token.as_deref(), Some("schema-token"));
    }

    #[test]
    fn config_debug_redacts_token() {
        let config = config("http://localhost:8080".to_owned(), Some("secret-token"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[cfg(not(madsim))]
    fn spawn_http_server(
        responses: Vec<String>,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let read = stream.read(&mut buffer).unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                request_tx
                    .send(String::from_utf8(request).unwrap())
                    .unwrap();
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (format!("http://{addr}"), request_rx, handle)
    }

    #[cfg(not(madsim))]
    fn response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn cross_host_redirect_is_followed_and_token_is_sent() {
        let body = r#"{"version":1,"type":"AVRO","data":"{}"}"#;
        let (target_url, target_requests, target_handle) =
            spawn_http_server(vec![response("200 OK", body)]);
        let redirect = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {target_url}/schema\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let (admin_url, admin_requests, admin_handle) = spawn_http_server(vec![redirect]);
        let client = Client::new(&config(admin_url, Some("test-token"))).unwrap();

        let schema = client.get_schema("tenant/ns/events", None).await.unwrap();
        assert_eq!(schema.version, 1);
        admin_requests.recv().unwrap();
        let redirected_request = target_requests.recv().unwrap().to_ascii_lowercase();
        assert!(redirected_request.starts_with("get /schema "));
        assert!(redirected_request.contains("authorization: bearer test-token"));
        admin_handle.join().unwrap();
        target_handle.join().unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn server_error_is_retried() {
        let body = r#"{"version":2,"type":"AVRO","data":"{}"}"#;
        let (admin_url, requests, handle) = spawn_http_server(vec![
            response("503 Service Unavailable", ""),
            response("200 OK", body),
        ]);
        let client = Client::new(&config(admin_url, None)).unwrap();

        let schema = client.get_schema("tenant/ns/events", None).await.unwrap();
        assert_eq!(schema.version, 2);
        requests.recv().unwrap();
        requests.recv().unwrap();
        assert!(requests.try_recv().is_err());
        handle.join().unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn client_error_and_malformed_body_are_not_retried() {
        for response in [response("404 Not Found", ""), response("200 OK", "{")] {
            let (admin_url, requests, handle) = spawn_http_server(vec![response]);
            let client = Client::new(&config(admin_url, None)).unwrap();

            client
                .get_schema("tenant/ns/events", None)
                .await
                .unwrap_err();
            requests.recv().unwrap();
            assert!(requests.try_recv().is_err());
            handle.join().unwrap();
        }
    }
}
