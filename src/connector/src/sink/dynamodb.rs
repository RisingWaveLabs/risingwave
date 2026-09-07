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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, anyhow};
use aws_sdk_dynamodb as dynamodb;
use aws_sdk_dynamodb::client::Client;
use aws_smithy_types::Blob;
use dynamodb::types::{AttributeValue, KeySchemaElement, TableStatus, WriteRequest};
use futures::TryFutureExt;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row as _;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;
use write_chunk_future::{DynamoDbPayloadWriter, WriteChunkFuture};

use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::connector_common::AwsAuthProps;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;

pub const DYNAMO_DB_SINK: &str = "dynamodb";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DynamoDbConfig {
    #[serde(rename = "table", alias = "dynamodb.table")]
    pub table: String,

    #[serde(rename = "dynamodb.max_batch_rows", default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    #[deprecated]
    pub max_batch_rows: usize,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(
        rename = "dynamodb.max_batch_item_nums",
        default = "default_max_batch_item_nums"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_item_nums: usize,

    #[serde(
        rename = "dynamodb.max_future_send_nums",
        default = "default_max_future_send_nums"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_future_send_nums: usize,

    #[serde(
        rename = "dynamodb.batch_write_retry_times",
        default = "default_batch_write_retry_times"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub batch_write_retry_times: usize,

    #[serde(
        rename = "dynamodb.batch_write_retry_backoff_ms",
        default = "default_batch_write_retry_backoff_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub batch_write_retry_backoff_ms: u64,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(DynamoDbConfig);

impl EnforceSecret for DynamoDbConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        AwsAuthProps::enforce_one(prop)
    }
}

fn default_max_batch_item_nums() -> usize {
    25
}

fn default_max_future_send_nums() -> usize {
    256
}

fn default_batch_write_retry_times() -> usize {
    3
}

fn default_batch_write_retry_backoff_ms() -> u64 {
    100
}

fn default_max_batch_rows() -> usize {
    1024
}

impl DynamoDbConfig {
    pub async fn build_client(&self) -> ConnectorResult<Client> {
        let config = &self.aws_auth_props;
        let aws_config = config.build_config().await?;

        Ok(Client::new(&aws_config))
    }

    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        serde_json::from_value::<DynamoDbConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
pub struct DynamoDbSink {
    pub config: DynamoDbConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
}

impl EnforceSecret for DynamoDbSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            DynamoDbConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl Sink for DynamoDbSink {
    type LogSinker = AsyncTruncateLogSinkerOf<DynamoDbSinkWriter>;

    const SINK_NAME: &'static str = DYNAMO_DB_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::DynamoDbSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
        let client = (self.config.build_client().await)
            .context("validate DynamoDB sink error")
            .map_err(SinkError::DynamoDb)?;

        let table_name = &self.config.table;
        let output = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| anyhow!(e))?;
        let Some(table) = output.table else {
            return Err(SinkError::DynamoDb(anyhow!(
                "table {} not found",
                table_name
            )));
        };
        if !matches!(table.table_status(), Some(TableStatus::Active)) {
            return Err(SinkError::DynamoDb(anyhow!(
                "table {} is not active",
                table_name
            )));
        }
        let rw_pk_names = rw_pk_names(&self.schema, &self.pk_indices)?;
        let dynamodb_keys = dynamodb_key_schema_names(table_name, table.key_schema())?;
        validate_pk_matches_dynamodb_key_schema(table_name, &rw_pk_names, &dynamodb_keys)?;

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            DynamoDbSinkWriter::new(self.config.clone(), self.schema.clone())
                .await?
                .into_log_sinker(self.config.max_future_send_nums),
        )
    }
}

impl TryFrom<SinkParam> for DynamoDbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = DynamoDbConfig::from_btreemap(param.properties)?;

        Ok(Self {
            config,
            schema,
            pk_indices,
        })
    }
}

#[derive(Debug)]
struct DynamoDbRequest {
    inner: WriteRequest,
    key_items: Vec<String>,
}

impl DynamoDbRequest {
    fn extract_key(&self) -> Option<&HashMap<String, AttributeValue>> {
        match (&self.inner.put_request(), &self.inner.delete_request()) {
            (Some(put_req), None) => Some(&put_req.item),
            (None, Some(del_req)) => Some(&del_req.key),
            _ => None,
        }
    }

    fn has_same_pk(&self, other: &Self) -> bool {
        if self.key_items.is_empty() {
            return false;
        }

        let Some(key) = self.extract_key() else {
            return false;
        };
        let Some(other_key) = other.extract_key() else {
            return false;
        };

        self.key_items.iter().all(|key_item| {
            matches!(
                (key.get(key_item), other_key.get(key_item)),
                (Some(value), Some(other_value)) if value == other_value
            )
        })
    }
}

pub struct DynamoDbSinkWriter {
    payload_writer: DynamoDbPayloadWriter,
    formatter: DynamoDbFormatter,
    max_future_send_nums: usize,
}

impl DynamoDbSinkWriter {
    pub async fn new(config: DynamoDbConfig, schema: Schema) -> Result<Self> {
        let client = config.build_client().await?;
        let table_name = &config.table;
        let output = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| anyhow!(e))?;
        let Some(table) = output.table else {
            return Err(SinkError::DynamoDb(anyhow!(
                "table {} not found",
                table_name
            )));
        };
        let dynamodb_keys = dynamodb_key_schema_names(table_name, table.key_schema())?;

        let payload_writer = DynamoDbPayloadWriter {
            client,
            table: config.table.clone(),
            dynamodb_keys,
            max_batch_item_nums: config.max_batch_item_nums,
            batch_write_retry_times: config.batch_write_retry_times,
            batch_write_retry_backoff_ms: config.batch_write_retry_backoff_ms,
        };

        Ok(Self {
            payload_writer,
            formatter: DynamoDbFormatter { schema },
            max_future_send_nums: config.max_future_send_nums,
        })
    }

    fn write_chunk_inner(&mut self, chunk: StreamChunk) -> Result<WriteChunkFuture> {
        let mut request_items = Vec::new();
        for (op, row) in chunk.rows() {
            let items = self.formatter.format_row(row)?;
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.payload_writer
                        .write_one_insert(items, &mut request_items);
                }
                Op::Delete => {
                    self.payload_writer
                        .write_one_delete(items, &mut request_items);
                }
                Op::UpdateDelete => {}
            }
        }
        Ok(self
            .payload_writer
            .write_chunk(request_items, self.max_future_send_nums))
    }
}

impl AsyncTruncateSinkWriter for DynamoDbSinkWriter {
    type DeliveryFuture = WriteChunkFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.write_chunk_inner(chunk)?.map_ok(|_| ()).await?;
        Ok(())
    }
}

struct DynamoDbFormatter {
    schema: Schema,
}

impl DynamoDbFormatter {
    fn format_row(&self, row: RowRef<'_>) -> Result<HashMap<String, AttributeValue>> {
        row.iter()
            .zip_eq_debug((self.schema.clone()).into_fields())
            .map(|(scalar, field)| {
                map_data(scalar, &field.data_type()).map(|attr| (field.name, attr))
            })
            .collect()
    }
}

fn map_data(scalar_ref: Option<ScalarRefImpl<'_>>, data_type: &DataType) -> Result<AttributeValue> {
    let Some(scalar_ref) = scalar_ref else {
        return Ok(AttributeValue::Null(true));
    };
    let attr = match data_type {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Int256
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal
        | DataType::Serial => AttributeValue::N(scalar_ref.to_text_with_type(data_type)),
        // TODO: jsonb as dynamic type (https://github.com/risingwavelabs/risingwave/issues/11699)
        DataType::Varchar
        | DataType::Interval
        | DataType::Date
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Jsonb => AttributeValue::S(scalar_ref.to_text_with_type(data_type)),
        DataType::Variant => {
            return Err(SinkError::DynamoDb(anyhow!("variant is not supported yet")));
        }
        DataType::Boolean => AttributeValue::Bool(scalar_ref.into_bool()),
        DataType::Bytea => AttributeValue::B(Blob::new(scalar_ref.into_bytea())),
        DataType::List(lt) => {
            let list_attr = scalar_ref
                .into_list()
                .iter()
                .map(|x| map_data(x, lt.elem()))
                .collect::<Result<Vec<_>>>()?;
            AttributeValue::L(list_attr)
        }
        DataType::Struct(st) => {
            let mut map = HashMap::with_capacity(st.len());
            for (sub_datum_ref, (name, data_type)) in scalar_ref
                .into_struct()
                .iter_fields_ref()
                .zip_eq_debug(st.iter())
            {
                let attr = map_data(sub_datum_ref, data_type)?;
                map.insert(name.to_owned(), attr);
            }
            AttributeValue::M(map)
        }
        DataType::Map(_m) => {
            return Err(SinkError::DynamoDb(anyhow!("map is not supported yet")));
        }
        DataType::Vector(_) => {
            return Err(SinkError::DynamoDb(anyhow!("vector is not supported yet")));
        }
    };
    Ok(attr)
}

fn rw_pk_names(schema: &Schema, pk_indices: &[usize]) -> Result<Vec<String>> {
    pk_indices
        .iter()
        .map(|pk_idx| {
            schema
                .fields()
                .get(*pk_idx)
                .map(|field| field.name.clone())
                .ok_or_else(|| {
                    SinkError::DynamoDb(anyhow!(
                        "RisingWave primary key column index {} is out of range",
                        pk_idx
                    ))
                })
        })
        .collect()
}

fn dynamodb_key_schema_names(
    table_name: &str,
    key_schema: &[KeySchemaElement],
) -> Result<Vec<String>> {
    if key_schema.is_empty() {
        return Err(SinkError::DynamoDb(anyhow!(
            "table {} key schema is empty",
            table_name
        )));
    }

    Ok(key_schema
        .iter()
        .map(|key_element| key_element.attribute_name().to_owned())
        .collect())
}

fn validate_pk_matches_dynamodb_key_schema(
    table_name: &str,
    rw_pk_names: &[String],
    dynamodb_keys: &[String],
) -> Result<()> {
    let rw_pk_set = rw_pk_names.iter().collect::<BTreeSet<_>>();
    let dynamodb_key_set = dynamodb_keys.iter().collect::<BTreeSet<_>>();
    if rw_pk_names.len() != dynamodb_keys.len() || rw_pk_set != dynamodb_key_set {
        return Err(SinkError::DynamoDb(anyhow!(
            "DynamoDB table {} primary key {:?} must match RisingWave primary key {:?}",
            table_name,
            dynamodb_keys,
            rw_pk_names
        )));
    }

    Ok(())
}

mod write_chunk_future {
    use std::collections::HashMap;
    use std::time::Duration;

    use anyhow::anyhow;
    use aws_sdk_dynamodb as dynamodb;
    use aws_sdk_dynamodb::client::Client;
    use dynamodb::types::{
        AttributeValue, DeleteRequest, PutRequest, ReturnConsumedCapacity,
        ReturnItemCollectionMetrics, WriteRequest,
    };
    use futures::{FutureExt, StreamExt, TryFuture, TryStreamExt, stream};
    use itertools::Itertools;
    use maplit::hashmap;
    use risingwave_common::util::retry::exponential_backoff;
    use tokio::time::sleep;
    use tokio_retry::strategy::jitter;

    use super::{DynamoDbRequest, SinkError};

    const MAX_BATCH_WRITE_RETRY_DELAY_MS: u64 = 2000;
    const MAX_BATCH_WRITE_CONCURRENCY: usize = 256;

    pub struct DynamoDbPayloadWriter {
        pub client: Client,
        pub table: String,
        pub dynamodb_keys: Vec<String>,
        pub max_batch_item_nums: usize,
        pub batch_write_retry_times: usize,
        pub batch_write_retry_backoff_ms: u64,
    }

    pub type WriteChunkFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + Send + 'static;

    impl DynamoDbPayloadWriter {
        pub fn write_one_insert(
            &mut self,
            item: HashMap<String, AttributeValue>,
            request_items: &mut Vec<DynamoDbRequest>,
        ) {
            let put_req = PutRequest::builder().set_item(Some(item)).build().unwrap();
            let req = WriteRequest::builder().put_request(put_req).build();
            self.write_one_req(req, request_items);
        }

        pub fn write_one_delete(
            &mut self,
            key: HashMap<String, AttributeValue>,
            request_items: &mut Vec<DynamoDbRequest>,
        ) {
            let key = key
                .into_iter()
                .filter(|(k, _)| self.dynamodb_keys.contains(k))
                .collect();
            let del_req = DeleteRequest::builder().set_key(Some(key)).build().unwrap();
            let req = WriteRequest::builder().delete_request(del_req).build();
            self.write_one_req(req, request_items);
        }

        pub fn write_one_req(
            &mut self,
            req: WriteRequest,
            request_items: &mut Vec<DynamoDbRequest>,
        ) {
            let r_req = DynamoDbRequest {
                inner: req,
                key_items: self.dynamodb_keys.clone(),
            };
            request_items.retain(|item| !item.has_same_pk(&r_req));
            request_items.push(r_req);
        }

        #[define_opaque(WriteChunkFuture)]
        pub fn write_chunk(
            &mut self,
            request_items: Vec<DynamoDbRequest>,
            max_future_send_nums: usize,
        ) -> WriteChunkFuture {
            let client = self.client.clone();
            let table = self.table.clone();
            let max_batch_item_nums = self.max_batch_item_nums;
            let batch_write_retry_times = self.batch_write_retry_times;
            let batch_write_retry_backoff_ms = self.batch_write_retry_backoff_ms;
            async move {
                let chunks = request_items
                    .into_iter()
                    .map(|r| r.inner)
                    .chunks(max_batch_item_nums)
                    .into_iter()
                    .map(|chunk| chunk.collect::<Vec<_>>())
                    .collect_vec();
                let max_future_send_nums =
                    max_future_send_nums.clamp(1, MAX_BATCH_WRITE_CONCURRENCY);
                stream::iter(chunks.into_iter().map(|req_items| {
                    let client = client.clone();
                    let table = table.clone();
                    async move {
                        let mut req_items = req_items;
                        let mut retry_count = 0;
                        let mut retry_backoff = exponential_backoff(
                            Duration::from_millis(batch_write_retry_backoff_ms),
                            2,
                            Duration::from_millis(MAX_BATCH_WRITE_RETRY_DELAY_MS),
                        )
                        .map(jitter)
                        .take(batch_write_retry_times);

                        loop {
                            let return_consumed_capacity = if retry_count == 0 {
                                ReturnConsumedCapacity::None
                            } else {
                                ReturnConsumedCapacity::Total
                            };
                            let reqs = hashmap! {
                                table.clone() => req_items.clone(),
                            };
                            let result = client
                                .batch_write_item()
                                .set_request_items(Some(reqs))
                                .return_consumed_capacity(return_consumed_capacity)
                                .return_item_collection_metrics(ReturnItemCollectionMetrics::None)
                                .send()
                                .await;

                            match result {
                                Ok(output) => {
                                    let unprocessed_items =
                                        output.unprocessed_items().cloned().unwrap_or_default();
                                    if unprocessed_items.is_empty() {
                                        if retry_count > 0 {
                                            tracing::warn!(
                                                retry_count,
                                                consumed_capacity = ?output.consumed_capacity(),
                                                "DynamoDB batch write retry succeeded"
                                            );
                                        }
                                        return Ok(());
                                    }

                                    req_items = unprocessed_items.into_values().flatten().collect();
                                    if retry_count >= batch_write_retry_times {
                                        return Err(SinkError::DynamoDb(anyhow!(
                                            "failed to write {} unprocessed items to DynamoDB sink after {} retries",
                                            req_items.len(),
                                            batch_write_retry_times,
                                        )));
                                    }
                                }
                                Err(e) => {
                                    return Err(SinkError::DynamoDb(
                                        anyhow!(e).context("failed to write items to DynamoDB sink"),
                                    ));
                                }
                            }

                            retry_count += 1;
                            let Some(delay) = retry_backoff.next() else {
                                return Err(SinkError::DynamoDb(anyhow!(
                                    "failed to write {} unprocessed items to DynamoDB sink after {} retries",
                                    req_items.len(),
                                    batch_write_retry_times,
                                )));
                            };
                            tracing::warn!(
                                retry_count,
                                delay_ms = delay.as_millis(),
                                unprocessed_items_count = req_items.len(),
                                "retrying DynamoDB batch write"
                            );
                            sleep(delay).await;
                        }
                    }
                }))
                .buffer_unordered(max_future_send_nums)
                .try_collect::<Vec<_>>()
                .await?;
                Ok(())
            }
            .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::{DeleteRequest, KeyType, PutRequest};

    use super::*;

    fn dynamodb_put_request(
        items: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> DynamoDbRequest {
        let item = dynamodb_items(items);
        let put_req = PutRequest::builder().set_item(Some(item)).build().unwrap();
        DynamoDbRequest {
            inner: WriteRequest::builder().put_request(put_req).build(),
            key_items: vec!["pk".to_owned(), "sk".to_owned()],
        }
    }

    fn dynamodb_delete_request(
        items: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> DynamoDbRequest {
        let key = dynamodb_items(items);
        let delete_req = DeleteRequest::builder().set_key(Some(key)).build().unwrap();
        DynamoDbRequest {
            inner: WriteRequest::builder().delete_request(delete_req).build(),
            key_items: vec!["pk".to_owned(), "sk".to_owned()],
        }
    }

    fn dynamodb_items(
        items: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> HashMap<String, AttributeValue> {
        items
            .into_iter()
            .map(|(k, v)| (k.to_owned(), AttributeValue::S(v.to_owned())))
            .collect()
    }

    #[test]
    fn dynamodb_request_compares_pk_by_key_attribute() {
        let req = dynamodb_put_request([("pk", "a"), ("sk", "b")]);
        let swapped_values = dynamodb_put_request([("pk", "b"), ("sk", "a")]);
        let same_pk = dynamodb_put_request([("pk", "a"), ("sk", "b")]);

        assert!(!req.has_same_pk(&swapped_values));
        assert!(req.has_same_pk(&same_pk));
    }

    #[test]
    fn dynamodb_request_empty_key_items_never_match() {
        let mut req = dynamodb_put_request([("pk", "a"), ("sk", "b")]);
        let same_pk = dynamodb_put_request([("pk", "a"), ("sk", "b")]);
        req.key_items.clear();

        assert!(!req.has_same_pk(&same_pk));
    }

    #[test]
    fn dynamodb_request_compares_put_and_delete_by_composite_pk() {
        let put = dynamodb_put_request([("pk", "a"), ("sk", "b"), ("value", "1")]);
        let same_pk_delete = dynamodb_delete_request([("pk", "a"), ("sk", "b")]);
        let different_hash_key_delete = dynamodb_delete_request([("pk", "x"), ("sk", "b")]);
        let different_range_key_delete = dynamodb_delete_request([("pk", "a"), ("sk", "x")]);

        assert!(put.has_same_pk(&same_pk_delete));
        assert!(same_pk_delete.has_same_pk(&put));
        assert!(!put.has_same_pk(&different_hash_key_delete));
        assert!(!put.has_same_pk(&different_range_key_delete));
    }

    #[test]
    fn dynamodb_key_schema_empty_errors() {
        let err = dynamodb_key_schema_names("test_table", &[]).unwrap_err();

        assert!(
            err.to_string()
                .contains("table test_table key schema is empty")
        );
    }

    #[test]
    fn dynamodb_key_schema_must_match_rw_pk() {
        let dynamodb_keys = ["pk".to_owned(), "sk".to_owned()];
        let same_rw_pk = ["sk".to_owned(), "pk".to_owned()];
        let extra_rw_pk = ["pk".to_owned(), "sk".to_owned(), "extra".to_owned()];
        let different_rw_pk = ["pk".to_owned(), "other".to_owned()];

        validate_pk_matches_dynamodb_key_schema("test_table", &same_rw_pk, &dynamodb_keys).unwrap();

        assert!(
            validate_pk_matches_dynamodb_key_schema("test_table", &extra_rw_pk, &dynamodb_keys)
                .unwrap_err()
                .to_string()
                .contains("must match RisingWave primary key")
        );
        assert!(
            validate_pk_matches_dynamodb_key_schema("test_table", &different_rw_pk, &dynamodb_keys)
                .unwrap_err()
                .to_string()
                .contains("must match RisingWave primary key")
        );
    }

    #[test]
    fn dynamodb_key_schema_names_uses_explicit_schema() {
        let key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        ];

        assert_eq!(
            dynamodb_key_schema_names("test_table", &key_schema).unwrap(),
            vec!["pk".to_owned(), "sk".to_owned()]
        );
    }
}
