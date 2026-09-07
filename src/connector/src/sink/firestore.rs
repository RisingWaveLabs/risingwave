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

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use firestore::{FirestoreDb, FirestoreDbOptions};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row as _;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::enforce_secret::EnforceSecret;

pub const FIRESTORE_SINK: &str = "firestore";

const DEFAULT_MAX_BATCH_SIZE: usize = 500; // Firestore batch limit
const DEFAULT_MAX_FUTURE_SEND_NUMS: usize = 256;

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct FirestoreConfig {
    /// Google Cloud project ID
    #[serde(rename = "firestore.project_id")]
    pub project_id: String,

    /// Firestore collection name
    #[serde(rename = "firestore.collection")]
    pub collection: String,

    /// Optional: Firestore database ID (for multi-database projects)
    #[serde(rename = "firestore.database_id")]
    pub database_id: Option<String>,

    /// A JSON string containing the service account credentials for authorization
    /// see the [service-account](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) credentials guide.
    #[serde(rename = "firestore.credentials")]
    pub credentials: Option<String>,

    /// Column whose value is used as the Firestore document ID. Values must be unique and valid
    /// Firestore document IDs. Duplicate values intentionally map multiple rows to the same
    /// Firestore document.
    #[serde(rename = "firestore.document_id_column")]
    pub document_id_column: Option<String>,

    /// Maximum batch size for batch writes (default: 500)
    #[serde(
        rename = "firestore.max_batch_size",
        default = "default_max_batch_size"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_size: usize,

    /// Maximum number of in-flight futures (default: 256)
    #[serde(
        rename = "firestore.max_future_send_nums",
        default = "default_max_future_send_nums"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_future_send_nums: usize,
}

fn default_max_batch_size() -> usize {
    DEFAULT_MAX_BATCH_SIZE
}

fn default_max_future_send_nums() -> usize {
    DEFAULT_MAX_FUTURE_SEND_NUMS
}

impl EnforceSecret for FirestoreConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "firestore.credentials",
    };
}

impl FirestoreConfig {
    pub async fn build_client(&self) -> Result<FirestoreDb> {
        let options = if let Some(ref database_id) = self.database_id {
            FirestoreDbOptions::new(self.project_id.clone()).with_database_id(database_id.clone())
        } else {
            FirestoreDbOptions::new(self.project_id.clone())
        };

        let db = if let Some(ref cred) = self.credentials {
            // Use JSON credentials string with gcloud-sdk's TokenSourceType
            // firestore internally uses gcloud-sdk, so we use TokenSourceType::Json
            use gcloud_sdk::{GCP_DEFAULT_SCOPES, TokenSourceType};

            FirestoreDb::with_options_token_source(
                options,
                GCP_DEFAULT_SCOPES.clone(),
                TokenSourceType::Json(cred.clone()),
            )
            .await
        } else {
            // Use default credentials (from environment, metadata server, etc.)
            FirestoreDb::with_options(options).await
        };

        db.map_err(|e| {
            SinkError::Firestore(anyhow!(e).context("failed to create Firestore client"))
        })
    }

    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        serde_json::from_value::<FirestoreConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSink {
    pub config: FirestoreConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
}

impl EnforceSecret for FirestoreSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            FirestoreConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl Sink for FirestoreSink {
    type LogSinker = AsyncTruncateLogSinkerOf<FirestoreSinkWriter>;

    const SINK_NAME: &'static str = FIRESTORE_SINK;

    async fn validate(&self) -> Result<()> {
        if self.config.max_batch_size == 0 {
            return Err(SinkError::Config(anyhow!(
                "firestore.max_batch_size must be greater than 0"
            )));
        }
        if self.config.max_batch_size > DEFAULT_MAX_BATCH_SIZE {
            return Err(SinkError::Config(anyhow!(
                "firestore.max_batch_size must not exceed {}",
                DEFAULT_MAX_BATCH_SIZE
            )));
        }
        if self.config.max_future_send_nums == 0 {
            return Err(SinkError::Config(anyhow!(
                "firestore.max_future_send_nums must be greater than 0"
            )));
        }

        // Validate connection by creating a client
        let _client: FirestoreDb = self
            .config
            .build_client()
            .await
            .context("Failed to validate Firestore connection")?;

        // Validate document_id_column if specified
        if let Some(ref doc_id_col) = self.config.document_id_column {
            let col_exists = self.schema.fields().iter().any(|f| &f.name == doc_id_col);
            if !col_exists {
                return Err(SinkError::Firestore(anyhow!(
                    "document_id_column '{}' not found in schema",
                    doc_id_col
                )));
            }
        }

        // Validate that we have at least one PK if no document_id_column is specified
        if self.config.document_id_column.is_none() && self.pk_indices.is_empty() {
            return Err(SinkError::Firestore(anyhow!(
                "Either document_id_column or primary key must be specified"
            )));
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(FirestoreSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
        )
        .await?
        .into_log_sinker(self.config.max_future_send_nums))
    }
}

impl TryFrom<SinkParam> for FirestoreSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = FirestoreConfig::from_btreemap(param.properties)?;

        Ok(Self {
            config,
            schema,
            pk_indices,
        })
    }
}

pub struct FirestoreSinkWriter {
    client: FirestoreDb,
    config: FirestoreConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    doc_id_col_idx: Option<usize>,
}

impl FirestoreSinkWriter {
    pub async fn new(
        config: FirestoreConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let client = config.build_client().await?;

        // Find document ID column index
        let doc_id_col_idx = if let Some(ref col_name) = config.document_id_column {
            schema.fields().iter().position(|f| &f.name == col_name)
        } else {
            None
        };

        Ok(Self {
            client,
            config,
            schema,
            pk_indices,
            doc_id_col_idx,
        })
    }

    fn get_document_id(&self, row: RowRef<'_>) -> Result<String> {
        if let Some(idx) = self.doc_id_col_idx {
            // Use specified document ID column
            let datum = row.datum_at(idx);
            match datum {
                Some(scalar) => {
                    let doc_id = scalar.to_text();
                    validate_firestore_document_id(&doc_id)?;
                    Ok(doc_id)
                }
                None => Err(SinkError::Firestore(anyhow!(
                    "document_id_column value is null"
                ))),
            }
        } else {
            // Encode the PK tuple unambiguously. Joining text values is unsafe for
            // composite keys because values may contain the separator.
            let pk_values: Vec<JsonValue> = self
                .pk_indices
                .iter()
                .map(|&idx| {
                    let field = &self.schema.fields()[idx];
                    row.datum_at(idx)
                        .map(|s| JsonValue::String(s.to_text_with_type(&field.data_type)))
                        .unwrap_or(JsonValue::Null)
                })
                .collect();
            encode_pk_document_id(pk_values)
        }
    }

    fn format_row(&self, row: RowRef<'_>) -> Result<HashMap<String, JsonValue>> {
        let mut map = HashMap::new();
        for (datum, field) in row.iter().zip_eq_debug(self.schema.fields()) {
            let value = format_datum(datum, &field.data_type)?;
            map.insert(field.name.clone(), value);
        }
        Ok(map)
    }

    fn write_chunk_inner(&mut self, chunk: StreamChunk) -> Result<WriteChunkFuture> {
        let mut operations = BTreeMap::new();

        for (op, row) in chunk.rows() {
            let doc_id = self.get_document_id(row)?;

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let data = self.format_row(row)?;
                    operations.insert(doc_id.clone(), FirestoreOperation::Upsert { doc_id, data });
                }
                Op::Delete => {
                    operations.insert(doc_id.clone(), FirestoreOperation::Delete { doc_id });
                }
                Op::UpdateDelete => {
                    operations.insert(doc_id.clone(), FirestoreOperation::Delete { doc_id });
                }
            }
        }

        Ok(self.write_operations(operations.into_values().collect()))
    }

    fn write_operations(&self, operations: Vec<FirestoreOperation>) -> WriteChunkFuture {
        let client = self.client.clone();
        let collection = self.config.collection.clone();
        let futures = operations
            .chunks(self.config.max_batch_size)
            .map(|chunk| chunk.to_vec())
            .map(move |chunk| {
                let client = client.clone();
                let collection = collection.clone();

                async move {
                    let batch_writer = client.create_simple_batch_writer().await.map_err(|e| {
                        SinkError::Firestore(
                            anyhow!(e).context("failed to create Firestore batch writer"),
                        )
                    })?;
                    let mut batch = batch_writer.new_batch();
                    let mut write_contexts = Vec::with_capacity(chunk.len());

                    for op in &chunk {
                        match op {
                            FirestoreOperation::Upsert { doc_id, data } => {
                                let data = FirestoreJsonObject(data);
                                batch
                                    .update_object(&collection, doc_id, &data, None, None, vec![])
                                    .map_err(|e| {
                                        SinkError::Firestore(
                                            anyhow!(e)
                                                .context("failed to add upsert document to batch"),
                                        )
                                    })?;
                                write_contexts.push(format!("upsert document '{}'", doc_id));
                            }
                            FirestoreOperation::Delete { doc_id } => {
                                batch.delete_by_id(&collection, doc_id, None).map_err(|e| {
                                    SinkError::Firestore(
                                        anyhow!(e)
                                            .context("failed to add delete document to batch"),
                                    )
                                })?;
                                write_contexts.push(format!("delete document '{}'", doc_id));
                            }
                        }
                    }

                    let response = batch.write().await.map_err(|e| {
                        SinkError::Firestore(
                            anyhow!(e).context("failed to execute Firestore batch write"),
                        )
                    })?;

                    if response.statuses.len() != write_contexts.len() {
                        return Err(SinkError::Firestore(anyhow!(
                            "Firestore batch write returned {} statuses for {} writes",
                            response.statuses.len(),
                            write_contexts.len()
                        )));
                    }

                    for (status, context) in response.statuses.iter().zip_eq_debug(&write_contexts)
                    {
                        if status.code != 0 {
                            return Err(SinkError::Firestore(anyhow!(
                                "Firestore batch write failed to {}: code={}, message={}",
                                context,
                                status.code,
                                status.message
                            )));
                        }
                    }

                    Ok::<(), SinkError>(())
                }
            });

        Box::pin(futures::future::try_join_all(futures))
    }
}

#[derive(Clone)]
enum FirestoreOperation {
    Upsert {
        doc_id: String,
        data: HashMap<String, JsonValue>,
    },
    Delete {
        doc_id: String,
    },
}

struct FirestoreJsonObject<'a>(&'a HashMap<String, JsonValue>);

impl Serialize for FirestoreJsonObject<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key, &FirestoreJsonValue(value))?;
        }
        map.end()
    }
}

struct FirestoreJsonValue<'a>(&'a JsonValue);

impl Serialize for FirestoreJsonValue<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            JsonValue::Null => {
                firestore::serialize_as_null::serialize(&Option::<()>::None, serializer)
            }
            JsonValue::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(&FirestoreJsonValue(value))?;
                }
                seq.end()
            }
            JsonValue::Object(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, &FirestoreJsonValue(value))?;
                }
                map.end()
            }
            value => value.serialize(serializer),
        }
    }
}

pub type WriteChunkFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = std::result::Result<Vec<()>, SinkError>> + Send>,
>;

fn format_datum(datum: Option<ScalarRefImpl<'_>>, data_type: &DataType) -> Result<JsonValue> {
    let Some(scalar) = datum else {
        return Ok(JsonValue::Null);
    };

    let value = match data_type {
        DataType::Boolean => JsonValue::Bool(scalar.into_bool()),
        DataType::Int16 => JsonValue::Number(scalar.into_int16().into()),
        DataType::Int32 => JsonValue::Number(scalar.into_int32().into()),
        DataType::Int64 => JsonValue::Number(scalar.into_int64().into()),
        DataType::Float32 => {
            let f = scalar.into_float32().0;
            serde_json::Number::from_f64(f as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::String(f.to_string()))
        }
        DataType::Float64 => {
            let f = scalar.into_float64().0;
            serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::String(f.to_string()))
        }
        DataType::Decimal
        | DataType::Int256
        | DataType::Serial
        | DataType::Interval
        | DataType::Date
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz => JsonValue::String(scalar.to_text_with_type(data_type)),
        DataType::Varchar => JsonValue::String(scalar.into_utf8().to_owned()),
        DataType::Bytea => {
            let bytes = scalar.into_bytea();
            JsonValue::String(STANDARD.encode(bytes))
        }
        DataType::Jsonb => {
            let jsonb = scalar.into_jsonb();
            serde_json::from_str(&jsonb.to_string())
                .unwrap_or_else(|_| JsonValue::String(jsonb.to_string()))
        }
        DataType::List(inner) => {
            let list = scalar.into_list();
            let values: Result<Vec<JsonValue>> = list
                .iter()
                .map(|item| format_datum(item, inner.elem()))
                .collect();
            JsonValue::Array(values?)
        }
        DataType::Struct(st) => {
            let struct_ref = scalar.into_struct();
            let mut map = serde_json::Map::new();
            for (datum, (name, dt)) in struct_ref.iter_fields_ref().zip_eq_debug(st.iter()) {
                map.insert(name.to_owned(), format_datum(datum, dt)?);
            }
            JsonValue::Object(map)
        }
        DataType::Map(_) => {
            return Err(SinkError::Firestore(anyhow!(
                "Map type is not supported yet"
            )));
        }
        DataType::Vector(_) => {
            return Err(SinkError::Firestore(anyhow!(
                "Vector type is not supported yet"
            )));
        }
    };
    Ok(value)
}

fn encode_pk_document_id(pk_values: Vec<JsonValue>) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&pk_values).map_err(|e| {
            SinkError::Firestore(anyhow!(e).context("failed to encode primary key"))
        })?,
    ))
}

fn validate_firestore_document_id(doc_id: &str) -> Result<()> {
    if doc_id.is_empty() {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID must not be empty"
        )));
    }
    if doc_id.contains('/') {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID must not contain '/'"
        )));
    }
    if doc_id == "." || doc_id == ".." {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID must not be '.' or '..'"
        )));
    }
    Ok(())
}

pub type FirestoreSinkDeliveryFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send>>;

impl AsyncTruncateSinkWriter for FirestoreSinkWriter {
    type DeliveryFuture = FirestoreSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.write_chunk_inner(chunk)?.await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use gcloud_sdk::google::firestore::v1::value::ValueType;

    use super::*;

    #[test]
    fn test_encode_pk_document_id_is_unambiguous_and_path_safe() -> Result<()> {
        let first = encode_pk_document_id(vec![
            JsonValue::String("a_b".to_owned()),
            JsonValue::String("c".to_owned()),
        ])?;
        let second = encode_pk_document_id(vec![
            JsonValue::String("a".to_owned()),
            JsonValue::String("b_c".to_owned()),
        ])?;
        let third = encode_pk_document_id(vec![JsonValue::Null])?;
        let fourth = encode_pk_document_id(vec![JsonValue::String("null".to_owned())])?;

        assert_ne!(first, second);
        assert_ne!(third, fourth);
        assert!(!first.contains('/'));
        assert!(!second.contains('/'));
        Ok(())
    }

    #[test]
    fn test_validate_explicit_document_id() {
        assert!(validate_firestore_document_id("doc_1").is_ok());
        assert!(validate_firestore_document_id("a/b").is_err());
        assert!(validate_firestore_document_id("").is_err());
        assert!(validate_firestore_document_id(".").is_err());
        assert!(validate_firestore_document_id("..").is_err());
    }

    #[test]
    fn test_format_int64_as_integer_value() -> Result<()> {
        assert_eq!(
            format_datum(Some(ScalarRefImpl::Int64(i64::MIN)), &DataType::Int64)?,
            JsonValue::Number(i64::MIN.into())
        );
        assert_eq!(
            format_datum(Some(ScalarRefImpl::Int64(i64::MAX)), &DataType::Int64)?,
            JsonValue::Number(i64::MAX.into())
        );
        Ok(())
    }

    #[test]
    fn test_firestore_json_object_serializes_null_as_null_value() {
        let data = HashMap::from([("field".to_owned(), JsonValue::Null)]);
        let doc = FirestoreDb::serialize_to_doc(
            "projects/p/databases/d/documents/c/doc",
            &FirestoreJsonObject(&data),
        )
        .unwrap();

        assert!(matches!(
            doc.fields.get("field").unwrap().value_type,
            Some(ValueType::NullValue(0))
        ));
    }

    #[test]
    fn test_firestore_json_object_preserves_nested_null_values() {
        let data = HashMap::from([(
            "field".to_owned(),
            serde_json::json!({
                "array": [1, null, "x"],
                "object": {
                    "null_field": null,
                    "string_field": "value"
                }
            }),
        )]);
        let doc = FirestoreDb::serialize_to_doc(
            "projects/p/databases/d/documents/c/doc",
            &FirestoreJsonObject(&data),
        )
        .unwrap();

        let Some(ValueType::MapValue(root)) = &doc.fields.get("field").unwrap().value_type else {
            panic!("expected field to be a map");
        };
        let Some(ValueType::ArrayValue(array)) = &root.fields.get("array").unwrap().value_type
        else {
            panic!("expected array field to be an array");
        };
        assert!(matches!(
            array.values[1].value_type,
            Some(ValueType::NullValue(0))
        ));

        let Some(ValueType::MapValue(object)) = &root.fields.get("object").unwrap().value_type
        else {
            panic!("expected object field to be a map");
        };
        assert!(matches!(
            object.fields.get("null_field").unwrap().value_type,
            Some(ValueType::NullValue(0))
        ));
        assert!(matches!(
            object.fields.get("string_field").unwrap().value_type,
            Some(ValueType::StringValue(_))
        ));
    }
}
