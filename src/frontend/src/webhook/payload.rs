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

use anyhow::anyhow;
use axum::http::{HeaderMap, StatusCode};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::ToOwnedDatum;
use risingwave_connector::parser::{
    Access, AccessResult, BigintUnsignedHandlingMode, JsonAccessBuilder, JsonProperties,
    TimeHandling, TimestampHandling, TimestamptzHandling,
};

use super::WebhookTableColumnDesc;
use super::utils::{Result, err};

const WEBHOOK_FORMAT_HEADER: &str = "x-rw-webhook-format";
const WEBHOOK_ENCODE_HEADER: &str = "x-rw-webhook-encode";
const WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER: &str = "x-rw-webhook-json-timestamp-handling-mode";
const WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER: &str =
    "x-rw-webhook-json-timestamptz-handling-mode";
const WEBHOOK_JSON_TIME_HANDLING_HEADER: &str = "x-rw-webhook-json-time-handling-mode";
const WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER: &str =
    "x-rw-webhook-json-bigint-unsigned-handling-mode";
const WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER: &str = "x-rw-webhook-json-handle-toast-columns";

pub(crate) fn build_json_access_builder(headers: &HeaderMap) -> Result<JsonAccessBuilder> {
    JsonAccessBuilder::new(json_properties_from_headers(headers)?).map_err(|e| {
        err(
            anyhow!(e).context("failed to build webhook JSON decoder"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })
}

pub(crate) fn owned_row_from_payload_row(
    access_builder: &mut JsonAccessBuilder,
    columns: &[WebhookTableColumnDesc],
    payload_row: &[u8],
) -> Result<OwnedRow> {
    let access = access_builder
        .generate_json_access(payload_row.to_vec())
        .map_err(|e| {
            err(
                anyhow!(e).context("failed to decode webhook JSON payload"),
                StatusCode::UNPROCESSABLE_ENTITY,
            )
        })?;
    let row = columns
        .iter()
        .map(
            |column| match access.access(&[column.name.as_str()], &column.data_type) {
                Ok(datum) => Ok(datum.to_owned_datum()),
                Err(error) if column.is_pk => Err(error),
                Err(_) => Ok(None),
            },
        )
        .collect::<AccessResult<Vec<_>>>()
        .map(OwnedRow::new);
    row.map_err(|e| {
        err(
            anyhow!(e).context("failed to decode webhook JSON payload"),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
    })
}

fn json_properties_from_headers(headers: &HeaderMap) -> Result<JsonProperties> {
    let format = header_value(headers, WEBHOOK_FORMAT_HEADER)?.unwrap_or_else(|| "plain".into());
    if !format.eq_ignore_ascii_case("plain") {
        return Err(err(
            anyhow!("unsupported webhook payload format `{format}`"),
            StatusCode::BAD_REQUEST,
        ));
    }

    let encode = header_value(headers, WEBHOOK_ENCODE_HEADER)?.unwrap_or_else(|| "json".into());
    if !encode.eq_ignore_ascii_case("json") {
        return Err(err(
            anyhow!("unsupported webhook payload encoding `{encode}`"),
            StatusCode::BAD_REQUEST,
        ));
    }

    let timestamp_handling =
        parse_timestamp_handling(headers, WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER)?;
    let timestamptz_handling =
        parse_timestamptz_handling(headers, WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER)?;
    let time_handling = parse_time_handling(headers, WEBHOOK_JSON_TIME_HANDLING_HEADER)?;
    let bigint_unsigned_handling =
        parse_bigint_unsigned_handling(headers, WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER)?;
    let handle_toast_columns =
        parse_bool_header(headers, WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER)?.unwrap_or(false);

    Ok(JsonProperties {
        use_schema_registry: false,
        timestamp_handling,
        timestamptz_handling,
        time_handling,
        bigint_unsigned_handling,
        handle_toast_columns,
    })
}

fn parse_timestamp_handling(
    headers: &HeaderMap,
    key: &'static str,
) -> Result<Option<TimestampHandling>> {
    match header_value(headers, key)?.as_deref() {
        Some("milli") => Ok(Some(TimestampHandling::Milli)),
        Some("guess_number_unit") => Ok(Some(TimestampHandling::GuessNumberUnit)),
        Some(value) => Err(err(
            anyhow!("unrecognized value `{value}` for `{key}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn parse_timestamptz_handling(
    headers: &HeaderMap,
    key: &'static str,
) -> Result<Option<TimestamptzHandling>> {
    header_value(headers, key)?
        .as_deref()
        .map(TimestamptzHandling::from_options)
        .transpose()
        .map_err(|e| {
            err(
                anyhow!(e).context("invalid webhook JSON decoder option"),
                StatusCode::BAD_REQUEST,
            )
        })
}

fn parse_time_handling(headers: &HeaderMap, key: &'static str) -> Result<Option<TimeHandling>> {
    match header_value(headers, key)?.as_deref() {
        Some("milli") => Ok(Some(TimeHandling::Milli)),
        Some("micro") => Ok(Some(TimeHandling::Micro)),
        Some(value) => Err(err(
            anyhow!("unrecognized value `{value}` for `{key}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn parse_bigint_unsigned_handling(
    headers: &HeaderMap,
    key: &'static str,
) -> Result<Option<BigintUnsignedHandlingMode>> {
    match header_value(headers, key)?.as_deref() {
        Some("long") => Ok(Some(BigintUnsignedHandlingMode::Long)),
        Some("precise") => Ok(Some(BigintUnsignedHandlingMode::Precise)),
        Some(value) => Err(err(
            anyhow!("unrecognized value `{value}` for `{key}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn parse_bool_header(headers: &HeaderMap, key: &'static str) -> Result<Option<bool>> {
    match header_value(headers, key)?.as_deref() {
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(value) => Err(err(
            anyhow!("unrecognized value `{value}` for `{key}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn header_value(headers: &HeaderMap, key: &'static str) -> Result<Option<String>> {
    headers
        .get(key)
        .map(|value| {
            value.to_str().map(|value| value.to_owned()).map_err(|e| {
                err(
                    anyhow!(e).context(format!("invalid UTF-8 in `{key}` header")),
                    StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use super::*;

    fn decode_payload_row(columns: &[WebhookTableColumnDesc], payload: &[u8]) -> Result<OwnedRow> {
        let mut access_builder = build_json_access_builder(&HeaderMap::new())?;
        owned_row_from_payload_row(&mut access_builder, columns, payload)
    }

    fn test_columns(columns: &[(&str, DataType, bool)]) -> Vec<WebhookTableColumnDesc> {
        columns
            .iter()
            .map(|(name, data_type, is_pk)| WebhookTableColumnDesc {
                name: (*name).to_owned(),
                data_type: data_type.clone(),
                is_pk: *is_pk,
            })
            .collect()
    }

    #[test]
    fn test_parse_plain_json_config_defaults() {
        let config = json_properties_from_headers(&HeaderMap::new()).unwrap();
        assert!(config.timestamptz_handling.is_none());
    }

    #[test]
    fn test_parse_plain_json_config_rejects_unsupported_encode() {
        let mut headers = HeaderMap::new();
        headers.insert(WEBHOOK_ENCODE_HEADER, HeaderValue::from_static("csv"));
        let err = json_properties_from_headers(&headers).unwrap_err();
        assert_eq!(err.code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_parse_plain_json_config_accepts_timestamptz_mode() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER,
            HeaderValue::from_static("milli"),
        );
        let config = json_properties_from_headers(&headers).unwrap();
        assert!(matches!(
            config.timestamptz_handling,
            Some(TimestamptzHandling::Milli)
        ));
    }

    #[test]
    fn test_parse_plain_json_config_accepts_all_supported_modes() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER,
            HeaderValue::from_static("milli"),
        );
        headers.insert(
            WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER,
            HeaderValue::from_static("micro"),
        );
        headers.insert(
            WEBHOOK_JSON_TIME_HANDLING_HEADER,
            HeaderValue::from_static("milli"),
        );
        headers.insert(
            WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER,
            HeaderValue::from_static("precise"),
        );
        headers.insert(
            WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER,
            HeaderValue::from_static("true"),
        );

        let config = json_properties_from_headers(&headers).unwrap();
        assert!(matches!(
            config.timestamp_handling,
            Some(TimestampHandling::Milli)
        ));
        assert!(matches!(
            config.timestamptz_handling,
            Some(TimestamptzHandling::Micro)
        ));
        assert!(matches!(config.time_handling, Some(TimeHandling::Milli)));
        assert!(matches!(
            config.bigint_unsigned_handling,
            Some(BigintUnsignedHandlingMode::Precise)
        ));
        assert!(config.handle_toast_columns);
    }

    #[test]
    fn test_parse_plain_json_config_rejects_invalid_supported_modes() {
        for header in [
            WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER,
            WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER,
            WEBHOOK_JSON_TIME_HANDLING_HEADER,
            WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER,
            WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER,
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header, HeaderValue::from_static("invalid"));
            let err = json_properties_from_headers(&headers).unwrap_err();
            assert_eq!(err.code(), StatusCode::BAD_REQUEST, "{header}");
        }
    }

    #[test]
    fn test_decode_plain_json_payload() {
        let columns = test_columns(&[
            ("id", DataType::Int32, true),
            ("name", DataType::Varchar, false),
        ]);
        let row = decode_payload_row(&columns, br#"{"id":1,"name":"alice"}"#).unwrap();

        assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
        assert_eq!(
            row.datum_at(1).to_owned_datum(),
            Some(ScalarImpl::Utf8("alice".into()))
        );
    }

    #[test]
    fn test_decode_plain_json_payload_rejects_missing_pk() {
        let columns = test_columns(&[
            ("id", DataType::Int32, true),
            ("name", DataType::Varchar, false),
        ]);
        let err = decode_payload_row(&columns, br#"{"name":"alice"}"#).unwrap_err();
        assert_eq!(err.code(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn test_decode_single_json_object_payload() {
        let columns = test_columns(&[
            ("id", DataType::Int32, true),
            ("price", DataType::Decimal, false),
            ("created_at", DataType::Timestamp, false),
            ("name", DataType::Varchar, false),
        ]);
        let row = decode_payload_row(
            &columns,
            br#"{"id":1,"price":"19.99","created_at":"2026-04-15 10:00:00","name":"alice"}"#,
        )
        .unwrap();

        assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
        assert!(matches!(
            row.datum_at(1).to_owned_datum(),
            Some(ScalarImpl::Decimal(_))
        ));
        assert!(matches!(
            row.datum_at(2).to_owned_datum(),
            Some(ScalarImpl::Timestamp(_))
        ));
        assert_eq!(
            row.datum_at(3).to_owned_datum(),
            Some(ScalarImpl::Utf8("alice".into()))
        );
    }

    #[test]
    fn test_decode_single_json_object_payload_rejects_missing_pk() {
        let columns = test_columns(&[
            ("id", DataType::Int32, true),
            ("name", DataType::Varchar, false),
        ]);
        let err = decode_payload_row(&columns, br#"{"name":"alice"}"#).unwrap_err();
        assert_eq!(err.code(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
