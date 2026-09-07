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

use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, JsonbRef, ScalarRefImpl};
use serde::Deserialize;
use thiserror_ext::AsReport;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam, SinkWriterParam};

pub const HTTP_SINK: &str = "http";
const HTTP_SINK_PAYLOAD_COLUMN: &str = "payload";
const HTTP_SINK_URL_COLUMN: &str = "url";
const HTTP_SINK_METHOD_COLUMN: &str = "method";

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct HttpConfig {
    /// The endpoint URL to send data to.
    pub url: Option<String>,

    /// Static HTTP request method. Any syntactically valid method is accepted. Defaults to `POST`.
    /// Cannot be used together with a dynamic `method` column.
    pub method: Option<String>,

    /// Content-Type header value. Defaults to `text/plain` for `varchar` and `application/json`
    /// for `jsonb`.
    pub content_type: Option<String>,

    /// Sink type, must be "append-only".
    pub r#type: String,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(HttpConfig);

impl EnforceSecret for HttpConfig {}

impl HttpConfig {
    pub fn from_btreemap(
        values: BTreeMap<String, String>,
    ) -> Result<(Self, BTreeMap<String, String>)> {
        // Extract header.* keys before serde parsing
        let mut headers = BTreeMap::new();
        let mut rest = BTreeMap::new();
        for (k, v) in &values {
            if let Some(header_name) = k.strip_prefix("header.") {
                headers.insert(header_name.to_owned(), v.clone());
            } else {
                rest.insert(k.clone(), v.clone());
            }
        }

        let config = serde_json::from_value::<HttpConfig>(serde_json::to_value(rest).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY {
            return Err(SinkError::Config(anyhow!(
                "HTTP sink only supports append-only mode"
            )));
        }

        Ok((config, headers))
    }
}

#[derive(Clone, Debug)]
enum HttpUrl {
    Static(reqwest::Url),
    Dynamic { url_index: usize },
}

#[derive(Clone, Debug)]
enum HttpMethod {
    Static(reqwest::Method),
    Dynamic { method_index: usize },
}

fn parse_http_method(method: &str) -> anyhow::Result<reqwest::Method> {
    // Preserve the existing case-insensitive behavior for standard methods. Extension methods are
    // case-sensitive and should be sent exactly as supplied.
    for standard_method in [
        reqwest::Method::GET,
        reqwest::Method::HEAD,
        reqwest::Method::POST,
        reqwest::Method::PUT,
        reqwest::Method::DELETE,
        reqwest::Method::CONNECT,
        reqwest::Method::OPTIONS,
        reqwest::Method::TRACE,
        reqwest::Method::PATCH,
    ] {
        if method.eq_ignore_ascii_case(standard_method.as_str()) {
            return Ok(standard_method);
        }
    }

    method.parse().context("invalid HTTP method")
}

fn parse_static_http_method(method: Option<&str>) -> Result<HttpMethod> {
    let method = match method {
        Some(method) => parse_http_method(method)
            .with_context(|| format!("invalid HTTP sink method '{method}'"))
            .map_err(SinkError::Config)?,
        None => reqwest::Method::POST,
    };
    Ok(HttpMethod::Static(method))
}

/// Validates the HTTP sink parameters and returns the sink so callers can use it directly without
/// re-parsing.
fn validate_http_sink(
    is_append_only: bool,
    ignore_delete: bool,
    schema: &Schema,
    url: Option<&str>,
    method: Option<&str>,
    content_type: Option<&str>,
    headers: &BTreeMap<String, String>,
    unknown_fields: std::collections::HashMap<String, String>,
) -> Result<HttpSink> {
    if !is_append_only && !ignore_delete {
        return Err(SinkError::Config(anyhow!(
            "HTTP sink only supports append-only mode"
        )));
    }

    let fields = schema.fields();
    let (payload_index, url, method, payload_type) = if fields.len() == 1 {
        let Some(url) = url else {
            return Err(SinkError::Config(anyhow!(
                "HTTP sink requires url option when schema has exactly 1 column"
            )));
        };
        let url = url
            .parse()
            .context("invalid URL")
            .map_err(SinkError::Config)?;
        let method = parse_static_http_method(method)?;
        (0, HttpUrl::Static(url), method, fields[0].data_type.clone())
    } else {
        for field in fields {
            match field.name.as_str() {
                HTTP_SINK_PAYLOAD_COLUMN | HTTP_SINK_URL_COLUMN | HTTP_SINK_METHOD_COLUMN => {}
                _ => {
                    return Err(SinkError::Config(anyhow!(
                        "HTTP sink with multiple columns only supports payload, url, and method columns, got {}",
                        field.name
                    )));
                }
            }
        }

        let payload_index = fields
            .iter()
            .position(|field| field.name == HTTP_SINK_PAYLOAD_COLUMN)
            .ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "HTTP sink with multiple columns requires a payload column"
                ))
            })?;
        let url_index = fields
            .iter()
            .position(|field| field.name == HTTP_SINK_URL_COLUMN);
        let url = match (url, url_index) {
            (Some(_), Some(_)) => {
                return Err(SinkError::Config(anyhow!(
                    "HTTP sink url option cannot coexist with url column"
                )));
            }
            (Some(url), None) => {
                let url = url
                    .parse()
                    .context("invalid URL")
                    .map_err(SinkError::Config)?;
                HttpUrl::Static(url)
            }
            (None, Some(url_index)) => {
                if fields[url_index].data_type != DataType::Varchar {
                    return Err(SinkError::Config(anyhow!(
                        "HTTP sink url column must be varchar, got {:?}",
                        fields[url_index].data_type
                    )));
                }
                HttpUrl::Dynamic { url_index }
            }
            (None, None) => {
                return Err(SinkError::Config(anyhow!(
                    "HTTP sink requires either url option or url column"
                )));
            }
        };

        let method_index = fields
            .iter()
            .position(|field| field.name == HTTP_SINK_METHOD_COLUMN);
        let method = match (method, method_index) {
            (Some(_), Some(_)) => {
                return Err(SinkError::Config(anyhow!(
                    "HTTP sink method option cannot coexist with method column"
                )));
            }
            (Some(method), None) => parse_static_http_method(Some(method))?,
            (None, Some(method_index)) => {
                if fields[method_index].data_type != DataType::Varchar {
                    return Err(SinkError::Config(anyhow!(
                        "HTTP sink method column must be varchar, got {:?}",
                        fields[method_index].data_type
                    )));
                }
                HttpMethod::Dynamic { method_index }
            }
            (None, None) => parse_static_http_method(None)?,
        };

        (
            payload_index,
            url,
            method,
            fields[payload_index].data_type.clone(),
        )
    };

    if payload_type != DataType::Varchar && payload_type != DataType::Jsonb {
        return Err(SinkError::Config(anyhow!(
            "HTTP sink payload column must be varchar or jsonb, got {:?}",
            payload_type
        )));
    }

    let mut header_map = HeaderMap::new();
    header_map.insert(
        CONTENT_TYPE,
        content_type
            .unwrap_or(match payload_type {
                DataType::Varchar => "text/plain",
                DataType::Jsonb => "application/json",
                _ => unreachable!("validated HTTP sink column type"),
            })
            .parse()
            .context("invalid content_type")
            .map_err(SinkError::Config)?,
    );
    for (k, v) in headers {
        let name: HeaderName = k
            .parse()
            .with_context(|| format!("invalid header name '{k}'"))
            .map_err(SinkError::Config)?;
        let value: HeaderValue = v
            .parse()
            .with_context(|| format!("invalid header value for '{k}'"))
            .map_err(SinkError::Config)?;
        header_map.insert(name, value);
    }

    Ok(HttpSink {
        url,
        method,
        payload_index,
        header_map,
        unknown_fields,
    })
}

#[derive(Clone, Debug)]
pub struct HttpSink {
    url: HttpUrl,
    method: HttpMethod,
    payload_index: usize,
    header_map: HeaderMap,
    unknown_fields: std::collections::HashMap<String, String>,
}

impl EnforceSecret for HttpSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            HttpConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for HttpSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let (config, headers) = HttpConfig::from_btreemap(param.properties)?;
        validate_http_sink(
            param.sink_type.is_append_only(),
            param.ignore_delete,
            &schema,
            config.url.as_deref(),
            config.method.as_deref(),
            config.content_type.as_deref(),
            &headers,
            config.unknown_fields,
        )
    }
}

impl Sink for HttpSink {
    type LogSinker = AsyncTruncateLogSinkerOf<HttpSinkWriter>;

    const SINK_NAME: &'static str = HTTP_SINK;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(&self.unknown_fields)
    }

    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(HttpSinkWriter::new(
            self.url.clone(),
            self.method.clone(),
            self.payload_index,
            self.header_map.clone(),
        )?
        .into_log_sinker(usize::MAX))
    }
}

pub struct HttpSinkWriter {
    client: reqwest::Client,
    url: HttpUrl,
    method: HttpMethod,
    payload_index: usize,
}

impl HttpSinkWriter {
    fn new(
        url: HttpUrl,
        method: HttpMethod,
        payload_index: usize,
        header_map: HeaderMap,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .context("failed to build HTTP client")
            .map_err(SinkError::Http)?;

        Ok(Self {
            client,
            url,
            method,
            payload_index,
        })
    }

    fn extract_url(&self, row: &impl Row) -> Result<Option<reqwest::Url>> {
        match &self.url {
            HttpUrl::Static(url) => Ok(Some(url.clone())),
            HttpUrl::Dynamic { url_index } => match row.datum_at(*url_index) {
                Some(ScalarRefImpl::Utf8(url)) if !url.is_empty() => {
                    match url.parse::<reqwest::Url>() {
                        Ok(url) => Ok(Some(url)),
                        Err(err) => {
                            tracing::warn!(
                                error = %err.as_report(),
                                payload = %self.strip_payload_for_log(row),
                                "skip HTTP sink row due to invalid URL in url column"
                            );
                            Ok(None)
                        }
                    }
                }
                Some(ScalarRefImpl::Utf8(_)) | None => {
                    tracing::warn!(
                        payload = %self.strip_payload_for_log(row),
                        "skip HTTP sink row due to null or empty url column"
                    );
                    Ok(None)
                }
                Some(_) => Err(SinkError::Http(anyhow!(
                    "unexpected url column type, expected varchar"
                ))),
            },
        }
    }

    fn extract_method(&self, row: &impl Row) -> Result<Option<reqwest::Method>> {
        match &self.method {
            HttpMethod::Static(method) => Ok(Some(method.clone())),
            HttpMethod::Dynamic { method_index } => match row.datum_at(*method_index) {
                Some(ScalarRefImpl::Utf8(method)) => match parse_http_method(method) {
                    Ok(method) => Ok(Some(method)),
                    Err(err) => {
                        tracing::warn!(
                            error = %err.as_report(),
                            payload = %self.strip_payload_for_log(row),
                            "skip HTTP sink row due to invalid HTTP method in method column"
                        );
                        Ok(None)
                    }
                },
                None => {
                    tracing::warn!(
                        payload = %self.strip_payload_for_log(row),
                        "skip HTTP sink row due to null method column"
                    );
                    Ok(None)
                }
                Some(_) => Err(SinkError::Http(anyhow!(
                    "unexpected method column type, expected varchar"
                ))),
            },
        }
    }

    fn strip_payload_for_log(&self, row: &impl Row) -> String {
        match row.datum_at(self.payload_index) {
            Some(ScalarRefImpl::Utf8(s)) => strip_text_payload(s),
            Some(ScalarRefImpl::Jsonb(j)) => strip_jsonb_payload(j),
            Some(_) => "<unexpected payload type>".to_owned(),
            None => "NULL".to_owned(),
        }
    }

    fn extract_payload(&self, row: &impl Row) -> Result<Option<String>> {
        Ok(match row.datum_at(self.payload_index) {
            Some(ScalarRefImpl::Utf8(s)) => Some(s.to_owned()),
            Some(ScalarRefImpl::Jsonb(j)) => Some(j.to_string()),
            Some(_) => {
                return Err(SinkError::Http(anyhow!(
                    "unexpected payload column type, expected varchar or jsonb"
                )));
            }
            None => None,
        })
    }
}

fn strip_text_payload(payload: &str) -> String {
    const EDGE_CHAR_COUNT: usize = 100;
    let char_count = payload.chars().count();
    if char_count <= EDGE_CHAR_COUNT * 2 {
        return payload.to_owned();
    }

    let prefix: String = payload.chars().take(EDGE_CHAR_COUNT).collect();
    let suffix: String = payload.chars().skip(char_count - EDGE_CHAR_COUNT).collect();
    format!("{prefix}...{suffix}")
}

fn strip_jsonb_payload(payload: JsonbRef<'_>) -> String {
    if payload.is_array() {
        let len = payload.array_len().expect("checked JSON array type");
        return match len {
            0 => "[]".to_owned(),
            1 => {
                let first = payload.access_array_element(0).expect("JSON array element");
                format!("[{}]", strip_jsonb_payload(first))
            }
            2 => {
                let first = payload.access_array_element(0).expect("JSON array element");
                let second = payload.access_array_element(1).expect("JSON array element");
                format!(
                    "[{},{}]",
                    strip_jsonb_payload(first),
                    strip_jsonb_payload(second)
                )
            }
            _ => {
                let first = payload.access_array_element(0).expect("JSON array element");
                let last = payload
                    .access_array_element(len - 1)
                    .expect("JSON array element");
                format!(
                    "[{},...,{}]",
                    strip_jsonb_payload(first),
                    strip_jsonb_payload(last)
                )
            }
        };
    }

    if let Ok(fields) = payload.object_key_values() {
        let fields = fields
            .map(|(key, value)| {
                let key = serde_json::to_string(key).expect("serialize JSON object key");
                format!("{key}:{}", strip_jsonb_payload(value))
            })
            .collect::<Vec<_>>();
        return format!("{{{}}}", fields.join(","));
    }

    if let Ok(value) = payload.as_str() {
        return serde_json::to_string(&strip_text_payload(value)).expect("serialize JSON string");
    }

    payload.to_string()
}

impl AsyncTruncateSinkWriter for HttpSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let Some(method) = self.extract_method(&row)? else {
                continue;
            };
            let payload = self.extract_payload(&row)?;
            if payload.is_none() && method != reqwest::Method::DELETE {
                continue;
            }
            let Some(url) = self.extract_url(&row)? else {
                continue;
            };

            let mut request = self.client.request(method, url);
            if let Some(payload) = payload {
                request = request.body(payload);
            }

            let resp = request
                .send()
                .await
                .context("HTTP request failed")
                .map_err(SinkError::Http)?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(SinkError::Http(anyhow!(
                    "HTTP sink received non-success response: {} {}",
                    status,
                    body
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{JsonbVal, Scalar};

    use super::*;

    #[test]
    fn test_parse_http_method() {
        assert_eq!(parse_http_method("patch").unwrap(), reqwest::Method::PATCH);
        assert_eq!(
            parse_http_method("DeLeTe").unwrap(),
            reqwest::Method::DELETE
        );
        assert_eq!(parse_http_method("PROPFIND").unwrap().as_str(), "PROPFIND");
        assert!(parse_http_method("invalid method").is_err());
    }

    #[test]
    fn test_strip_text_payload() {
        assert_eq!(strip_text_payload("short payload"), "short payload");

        let payload = format!("{}{}", "a".repeat(100), "z".repeat(100));
        assert_eq!(strip_text_payload(&payload), payload);

        let payload = format!("{}middle{}", "a".repeat(101), "z".repeat(101));
        assert_eq!(
            strip_text_payload(&payload),
            format!("{}...{}", "a".repeat(100), "z".repeat(100))
        );
    }

    #[test]
    fn test_strip_jsonb_payload() {
        let payload: JsonbVal = r#"{
            "id": 1,
            "items": [1, {"nested": ["first", "middle", "last"]}, 3],
            "message": "short"
        }"#
        .parse()
        .unwrap();

        assert_eq!(
            strip_jsonb_payload(payload.as_scalar_ref()),
            r#"{"id":1,"items":[1,...,3],"message":"short"}"#
        );

        let payload: JsonbVal = r#"["only"]"#.parse().unwrap();
        assert_eq!(strip_jsonb_payload(payload.as_scalar_ref()), r#"["only"]"#);

        let payload: JsonbVal = r#"["first","last"]"#.parse().unwrap();
        assert_eq!(
            strip_jsonb_payload(payload.as_scalar_ref()),
            r#"["first","last"]"#
        );
    }
}
