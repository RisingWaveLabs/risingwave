// Copyright 2023 RisingWave Labs
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

use std::fmt::Debug;
use std::sync::Arc;

use reqwest::Method;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::{ParseError, Url};

use crate::schema::{InvalidOptionError, bail_invalid_option_error};

pub fn handle_sr_list(addr: &str) -> Result<Vec<Url>, InvalidOptionError> {
    let segment = addr.split(',').collect::<Vec<&str>>();
    let mut errs: Vec<ParseError> = Vec::with_capacity(segment.len());
    let mut urls = Vec::with_capacity(segment.len());
    for ele in segment {
        match ele.parse::<Url>() {
            Ok(url) => urls.push(url),
            Err(e) => errs.push(e),
        }
    }
    if urls.is_empty() {
        bail_invalid_option_error!("no valid url provided, errs: {errs:?}");
    }
    tracing::debug!(
        "schema registry client will use url {:?} to connect, the rest failed because: {:?}",
        urls,
        errs
    );
    Ok(urls)
}

#[derive(Debug, thiserror::Error)]
pub enum WireFormatError {
    #[error("failed to match the magic byte 0")]
    NoMagic,
    #[error("failed to read the 4-byte schema ID")]
    NoSchemaId,
    #[error("failed to parse message indexes")]
    ParseMessageIndexes,
}

/// Returns `(schema_id, payload)`
///
/// Refer to [Confluent schema registry wire format](https://docs.confluent.io/platform/7.6/schema-registry/fundamentals/serdes-develop/index.html#wire-format)
///
/// | Bytes | Area        | Description                                                                                        |
/// |-------|-------------|----------------------------------------------------------------------------------------------------|
/// | 0     | Magic Byte  | Confluent serialization format version number; currently always `0`.                               |
/// | 1-4   | Schema ID   | 4-byte schema ID as returned by Schema Registry.                                                   |
/// | 5-... | Data        | Serialized data for the specified schema format (for example, binary encoding for Avro or Protobuf.|
pub(crate) fn extract_schema_id(payload: &[u8]) -> Result<(i32, &[u8]), WireFormatError> {
    use byteorder::{BigEndian, ReadBytesExt as _};

    let mut cursor = payload;
    if !cursor.read_u8().is_ok_and(|magic| magic == 0) {
        return Err(WireFormatError::NoMagic);
    }

    let schema_id = cursor
        .read_i32::<BigEndian>()
        .map_err(|_| WireFormatError::NoSchemaId)?;

    Ok((schema_id, cursor))
}

pub(crate) struct SchemaRegistryCtx {
    pub username: Option<String>,
    pub password: Option<String>,
    pub client: reqwest::Client,
    pub path: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("confluent registry send req error: {0}")]
    Send(#[source] reqwest::Error),
    #[error("confluent registry parse resp error: {0}")]
    Json(#[source] reqwest::Error),
    #[error(transparent)]
    Unsuccessful(ErrorResp),
}

pub(crate) async fn req_inner<T>(
    ctx: Arc<SchemaRegistryCtx>,
    mut url: Url,
    method: Method,
) -> Result<T, RequestError>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    url.path_segments_mut()
        .expect("constructor validated URL can be a base")
        .extend(&ctx.path);
    tracing::debug!("request to url: {}, method {}", &url, &method);
    let mut request_builder = ctx.client.request(method, url);

    if let Some(ref username) = ctx.username {
        request_builder = request_builder.basic_auth(username, ctx.password.as_ref());
    }
    request(request_builder).await
}

async fn request<T>(req: reqwest::RequestBuilder) -> Result<T, RequestError>
where
    T: DeserializeOwned,
{
    let res = req.send().await.map_err(RequestError::Send)?;
    let status = res.status();
    if status.is_success() {
        res.json().await.map_err(RequestError::Json)
    } else {
        let res = res.json().await.map_err(RequestError::Json)?;
        Err(RequestError::Unsuccessful(res))
    }
}

/// `Schema` format of confluent schema registry
#[derive(Debug, Eq, PartialEq)]
pub struct ConfluentSchema {
    /// The id of the schema
    pub id: i32,
    /// The raw text of the schema def
    pub content: String,
}

/// `Subject` stored in confluent schema registry
#[derive(Debug, Eq, PartialEq)]
pub struct Subject {
    /// The version of the current schema
    pub version: i32,
    /// The name of the schema
    pub name: String,
    /// The schema corresponding to that `version`
    pub schema: ConfluentSchema,
}

/// One schema can reference another schema
/// (e.g., import "other.proto" in protobuf)
#[derive(Debug, Deserialize)]
pub struct SchemaReference {
    /// The name of the reference.
    #[expect(dead_code)]
    pub name: String,
    /// The subject that the referenced schema belongs to
    pub subject: String,
    /// The version of the referenced schema
    pub version: i32,
}

#[derive(Debug, Deserialize)]
pub struct GetByIdResp {
    pub schema: String,
}

#[derive(Debug, Deserialize)]
pub struct GetBySubjectResp {
    pub id: i32,
    pub schema: String,
    pub version: i32,
    pub subject: String,
    // default to empty/non-reference
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// <https://docs.confluent.io/platform/7.5/schema-registry/develop/api.html#errors>
#[derive(Debug, Deserialize, thiserror::Error)]
#[error("confluent schema registry error {error_code}: {message}")]
pub struct ErrorResp {
    error_code: i32,
    message: String,
}

#[cfg(test)]
mod test {
    use super::super::handle_sr_list;

    #[test]
    fn test_handle_sr_list() {
        let addr1 = "http://localhost:8081".to_owned();
        assert_eq!(
            handle_sr_list(&addr1).unwrap(),
            vec!["http://localhost:8081".parse().unwrap()]
        );

        let addr2 = "http://localhost:8081,http://localhost:8082".to_owned();
        assert_eq!(
            handle_sr_list(&addr2).unwrap(),
            vec![
                "http://localhost:8081".parse().unwrap(),
                "http://localhost:8082".parse().unwrap()
            ]
        );

        let fail_addr = "http://localhost:8081,12345".to_owned();
        assert_eq!(
            handle_sr_list(&fail_addr).unwrap(),
            vec!["http://localhost:8081".parse().unwrap(),]
        );

        let all_fail_addr = "54321,12345".to_owned();
        assert!(handle_sr_list(&all_fail_addr).is_err());
    }
}
