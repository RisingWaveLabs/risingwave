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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::pending;
use std::pin::Pin;
use std::time::{Duration, Instant as StdInstant};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::types::{DataType, ScalarRefImpl};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::time::Sleep;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::decouple_checkpoint_log_sink::should_force_commit_on_checkpoint_barrier;
use crate::sink::encoder::{JsonEncoder, RowEncoder};
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{
    LogSinker, Result, Sink, SinkError, SinkLogReader, SinkParam, SinkWriterMetrics,
    SinkWriterParam,
};

const DEFAULT_WRITE_BATCH_SIZE: usize = 1000;
const DEFAULT_MAX_LINGER_SECOND: u64 = 1;

pub const TURBOPUFFER_SINK: &str = "turbopuffer";

fn default_write_batch_size() -> usize {
    DEFAULT_WRITE_BATCH_SIZE
}

fn default_max_linger_second() -> u64 {
    DEFAULT_MAX_LINGER_SECOND
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct TurbopufferConfig {
    pub base_url: String,
    pub namespace: Option<String>,
    pub namespace_column: Option<String>,
    pub api_key: String,
    pub distance_metric: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub disable_backpressure: Option<bool>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub num_shards: Option<usize>,
    pub full_text_search_columns: Option<String>,
    pub filterable_columns: Option<String>,
    #[serde(default = "default_write_batch_size")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub write_batch_size: usize,
    #[serde(default = "default_max_linger_second")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub max_linger_second: u64,
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(TurbopufferConfig);

impl EnforceSecret for TurbopufferConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "api_key",
    };
}

impl TurbopufferConfig {
    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<TurbopufferConfig>(
            serde_json::to_value(values).expect("serialize sink properties"),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.write_batch_size == 0 {
            return Err(SinkError::Config(anyhow!(
                "`write_batch_size` must be greater than 0"
            )));
        }
        if config.max_linger_second == 0 {
            return Err(SinkError::Config(anyhow!(
                "`max_linger_second` must be greater than 0"
            )));
        }
        if config.num_shards == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`num_shards` must be greater than 0"
            )));
        }
        Ok(config)
    }
}

#[derive(Clone, Debug)]
enum TurbopufferNamespace {
    Static(String),
    Dynamic { index: usize },
}

#[derive(Clone, Debug)]
pub struct TurbopufferSink {
    config: TurbopufferConfig,
    schema: Schema,
    pk_index: usize,
    namespace: TurbopufferNamespace,
    attribute_indices: Vec<usize>,
    generated_schema: Value,
}

impl EnforceSecret for TurbopufferSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            TurbopufferConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for TurbopufferSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let [pk_index] = pk_indices.as_slice() else {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer sink requires exactly one primary_key column"
            )));
        };
        let pk_index = *pk_index;
        match schema[pk_index].data_type() {
            DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Serial
            | DataType::Varchar => {}
            data_type => {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer document id column must be an integer or varchar, got {:?}",
                    data_type
                )));
            }
        };
        let config = TurbopufferConfig::from_btreemap(param.properties)?;

        let namespace = match (&config.namespace, &config.namespace_column) {
            (Some(namespace), None) => {
                validate_namespace(namespace)?;
                TurbopufferNamespace::Static(namespace.clone())
            }
            (None, Some(namespace_column)) => {
                let index = schema
                    .fields()
                    .iter()
                    .position(|field| field.name == *namespace_column)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Turbopuffer namespace_column '{}' not found in sink schema",
                            namespace_column
                        ))
                    })?;
                if schema[index].data_type != DataType::Varchar {
                    return Err(SinkError::Config(anyhow!(
                        "Turbopuffer namespace_column must be varchar, got {:?}",
                        schema[index].data_type
                    )));
                }
                TurbopufferNamespace::Dynamic { index }
            }
            (Some(_), Some(_)) => {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer sink requires only one of namespace or namespace_column"
                )));
            }
            (None, None) => {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer sink requires either namespace or namespace_column"
                )));
            }
        };

        // Turbopuffer treats `id` as the document ID in write requests; it is not a schema
        // attribute. Dynamic namespace is also metadata for routing, not a document attribute.
        let excluded_indices = match &namespace {
            TurbopufferNamespace::Static(_) => HashSet::from([pk_index]),
            TurbopufferNamespace::Dynamic { index } => HashSet::from([pk_index, *index]),
        };
        let attribute_indices = (0..schema.len())
            .filter(|idx| !excluded_indices.contains(idx))
            .collect_vec();
        for index in &attribute_indices {
            if schema[*index].name == "id" {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer attribute column must not be named id"
                )));
            }
        }
        let full_text_search_columns = parse_column_selection(
            config.full_text_search_columns.as_deref(),
            &schema,
            &attribute_indices,
        )?;
        let filterable_columns = parse_column_selection(
            config.filterable_columns.as_deref(),
            &schema,
            &attribute_indices,
        )?;
        let has_vector = attribute_indices
            .iter()
            .any(|idx| matches!(schema[*idx].data_type, DataType::Vector(_)));
        if has_vector && config.distance_metric.is_none() {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer sink requires distance_metric when sink schema contains vector columns"
            )));
        }
        // This validates every document attribute type before the writer is created:
        // `build_turbopuffer_schema` calls `turbopuffer_type` for each attribute and
        // returns a config error for unsupported types.
        let generated_schema = build_turbopuffer_schema(
            &schema,
            &attribute_indices,
            &full_text_search_columns,
            &filterable_columns,
        )?;

        Ok(Self {
            config,
            schema,
            pk_index,
            namespace,
            attribute_indices,
            generated_schema,
        })
    }
}

impl Sink for TurbopufferSink {
    type LogSinker = TurbopufferLogSinker;

    const SINK_NAME: &'static str = TURBOPUFFER_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn is_sink_decouple(user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default | SinkDecouple::Enable => Ok(true),
            SinkDecouple::Disable => Err(SinkError::Config(anyhow!(
                "Turbopuffer sink can only be created with sink_decouple enabled"
            ))),
        }
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let write_batch_size = self.config.write_batch_size;
        let max_linger = Duration::from_secs(self.config.max_linger_second);
        let writer = TurbopufferSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_index,
            self.namespace.clone(),
            self.attribute_indices.clone(),
            self.generated_schema.clone(),
            write_batch_size,
            max_linger,
        )?;
        Ok(TurbopufferLogSinker::new(
            writer,
            SinkWriterMetrics::new(&writer_param),
        ))
    }
}

pub struct TurbopufferLogSinker {
    writer: TurbopufferSinkWriter,
    sink_writer_metrics: SinkWriterMetrics,
}

impl TurbopufferLogSinker {
    fn new(writer: TurbopufferSinkWriter, sink_writer_metrics: SinkWriterMetrics) -> Self {
        Self {
            writer,
            sink_writer_metrics,
        }
    }

    fn ensure_linger_timer(&self, linger_timer: &mut Pin<&mut Option<Sleep>>) {
        if linger_timer.as_ref().get_ref().is_none() {
            linger_timer
                .as_mut()
                .set(Some(tokio::time::sleep(self.writer.max_linger)));
        }
    }

    async fn flush_all_and_truncate(
        &mut self,
        log_reader: &mut impl SinkLogReader,
        latest_truncate_offset: &mut Option<TruncateOffset>,
        linger_timer: &mut Pin<&mut Option<Sleep>>,
    ) -> Result<()> {
        let start_time = StdInstant::now();
        self.writer.flush_all().await?;
        self.sink_writer_metrics
            .sink_commit_duration
            .observe(start_time.elapsed().as_secs_f64());
        linger_timer.as_mut().set(None);
        if let Some(offset) = latest_truncate_offset.take() {
            log_reader.truncate(offset)?;
        }
        Ok(())
    }
}

#[async_trait]
impl LogSinker for TurbopufferLogSinker {
    async fn consume_log_and_sink(mut self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        let mut latest_truncate_offset = None;
        let linger_timer = None;
        let mut linger_timer = std::pin::pin!(linger_timer);

        loop {
            let (epoch, item) = tokio::select! {
                item = log_reader.next_item() => item?,
                _ = async {
                    match linger_timer.as_mut().as_pin_mut() {
                        Some(timer) => timer.await,
                        None => pending().await,
                    }
                } => {
                    self.flush_all_and_truncate(
                        &mut log_reader,
                        &mut latest_truncate_offset,
                        &mut linger_timer,
                    )
                    .await?;
                    continue;
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    let offset = TruncateOffset::Chunk { epoch, chunk_id };
                    let has_pending_update = self.writer.write_chunk(chunk)?;
                    latest_truncate_offset = Some(offset);
                    if self.writer.should_flush_by_size() {
                        self.flush_all_and_truncate(
                            &mut log_reader,
                            &mut latest_truncate_offset,
                            &mut linger_timer,
                        )
                        .await?;
                    } else if has_pending_update {
                        self.ensure_linger_timer(&mut linger_timer);
                    }
                }
                LogStoreReadItem::Barrier {
                    new_vnode_bitmap,
                    is_stop,
                    schema_change,
                    ..
                } => {
                    let offset = TruncateOffset::Barrier { epoch };
                    let should_flush = should_force_commit_on_checkpoint_barrier(
                        new_vnode_bitmap.is_some(),
                        is_stop,
                        schema_change.is_some(),
                    );
                    if self.writer.is_empty() {
                        log_reader.truncate(offset)?;
                    } else if should_flush {
                        latest_truncate_offset = Some(offset);
                        self.flush_all_and_truncate(
                            &mut log_reader,
                            &mut latest_truncate_offset,
                            &mut linger_timer,
                        )
                        .await?;
                    } else {
                        latest_truncate_offset = Some(offset);
                    }

                    if is_stop {
                        return pending().await;
                    }
                }
            }
        }
    }
}

pub struct TurbopufferSinkWriter {
    client: reqwest::Client,
    base_url: String,
    distance_metric: Option<String>,
    disable_backpressure: Option<bool>,
    num_shards: Option<usize>,
    schema: Value,
    pk_index: usize,
    namespace: TurbopufferNamespace,
    row_encoder: JsonEncoder,
    write_batch_size: usize,
    max_linger: Duration,
    pending_batches: BTreeMap<String, HashMap<DocumentId, CompactedOp>>,
}

impl TurbopufferSinkWriter {
    fn new(
        config: TurbopufferConfig,
        schema: Schema,
        pk_index: usize,
        namespace: TurbopufferNamespace,
        attribute_indices: Vec<usize>,
        generated_schema: Value,
        write_batch_size: usize,
        max_linger: Duration,
    ) -> Result<Self> {
        let mut header_map = HeaderMap::new();
        header_map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let authorization = format!("Bearer {}", config.api_key);
        header_map.insert(
            AUTHORIZATION,
            authorization
                .parse()
                .context("invalid turbopuffer api_key")
                .map_err(SinkError::Config)?,
        );
        let client = reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .context("failed to build turbopuffer HTTP client")
            .map_err(SinkError::Http)?;
        let base_url = config
            .base_url
            .parse::<reqwest::Url>()
            .context("invalid turbopuffer base_url")
            .map_err(SinkError::Config)?
            .to_string()
            .trim_end_matches('/')
            .to_owned();
        let row_encoder = JsonEncoder::new_with_turbopuffer(schema, Some(attribute_indices));
        Ok(Self {
            client,
            base_url,
            distance_metric: config.distance_metric,
            disable_backpressure: config.disable_backpressure,
            num_shards: config.num_shards,
            schema: generated_schema,
            pk_index,
            namespace,
            row_encoder,
            write_batch_size,
            max_linger,
            pending_batches: BTreeMap::new(),
        })
    }

    fn url_for_row(&self, row: &impl Row) -> Result<String> {
        match &self.namespace {
            TurbopufferNamespace::Static(namespace) => Ok(format!(
                "{}/v2/namespaces/{}",
                self.base_url,
                namespace.as_str()
            )),
            TurbopufferNamespace::Dynamic { index } => {
                let namespace = match row.datum_at(*index) {
                    Some(ScalarRefImpl::Utf8(namespace)) => namespace,
                    None => {
                        return Err(SinkError::Http(anyhow!(
                            "Turbopuffer namespace_column cannot be null"
                        )));
                    }
                    Some(_) => {
                        return Err(SinkError::Http(anyhow!(
                            "unexpected namespace_column type, expected varchar"
                        )));
                    }
                };
                validate_namespace(namespace)?;
                Ok(format!("{}/v2/namespaces/{}", self.base_url, namespace))
            }
        }
    }

    // Turbopuffer document IDs are unsigned 64-bit integers, UUIDs, or strings up to 64 bytes.
    // RisingWave UUID IDs can be represented with varchar.
    fn id_for_row(&self, row: &impl Row) -> Result<DocumentId> {
        let datum = row.datum_at(self.pk_index).ok_or_else(|| {
            SinkError::Http(anyhow!("Turbopuffer document id column cannot be null"))
        })?;
        match datum {
            ScalarRefImpl::Int16(value) => Ok(document_id_from_i64(value as i64)),
            ScalarRefImpl::Int32(value) => Ok(document_id_from_i64(value as i64)),
            ScalarRefImpl::Int64(value) => Ok(document_id_from_i64(value)),
            ScalarRefImpl::Serial(value) => Ok(document_id_from_i64(value.into_inner())),
            ScalarRefImpl::Utf8(value) => {
                if value.len() > 64 {
                    return Err(SinkError::Http(anyhow!(
                        "Turbopuffer string document id exceeds 64 bytes"
                    )));
                }
                Ok(DocumentId::String(value.to_owned()))
            }
            _ => Err(SinkError::Http(anyhow!(
                "Turbopuffer document id column must be an integer or varchar"
            ))),
        }
    }

    fn upsert_row(&self, row: &impl Row, id: DocumentId) -> Result<Map<String, Value>> {
        let mut value = self.row_encoder.encode(row)?;
        value.insert(
            "id".to_owned(),
            serde_json::to_value(id).expect("serialize document id"),
        );
        Ok(value)
    }

    fn request_body(
        &self,
        upsert_rows: Vec<Map<String, Value>>,
        deletes: Vec<DocumentId>,
    ) -> Value {
        let mut body = Map::new();
        if let Some(distance_metric) = &self.distance_metric {
            body.insert(
                "distance_metric".to_owned(),
                Value::String(distance_metric.clone()),
            );
        }
        if let Some(num_shards) = self.num_shards {
            let mut sharding = Map::new();
            sharding.insert(
                "num_shards".to_owned(),
                serde_json::to_value(num_shards).expect("serialize num_shards"),
            );
            body.insert("sharding".to_owned(), Value::Object(sharding));
        }
        if !upsert_rows.is_empty() {
            if let Some(disable_backpressure) = self.disable_backpressure {
                body.insert(
                    "disable_backpressure".to_owned(),
                    Value::Bool(disable_backpressure),
                );
            }
            body.insert("schema".to_owned(), self.schema.clone());
            body.insert(
                "upsert_rows".to_owned(),
                Value::Array(upsert_rows.into_iter().map(Value::Object).collect()),
            );
        }
        if !deletes.is_empty() {
            body.insert(
                "deletes".to_owned(),
                Value::Array(
                    deletes
                        .into_iter()
                        .map(|id| serde_json::to_value(id).expect("serialize document id"))
                        .collect(),
                ),
            );
        }
        Value::Object(body)
    }

    fn write_chunk(&mut self, chunk: StreamChunk) -> Result<bool> {
        let mut has_pending_update = false;
        for (op, row) in chunk.rows() {
            let id = match self.id_for_row(&row) {
                Ok(id) => id,
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "skip turbopuffer row with invalid document id");
                    continue;
                }
            };
            let url = match self.url_for_row(&row) {
                Ok(url) => url,
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "skip turbopuffer row with invalid namespace");
                    continue;
                }
            };
            let compacted_op = match op {
                Op::Insert | Op::UpdateInsert => {
                    let upsert_row = match self.upsert_row(&row, id.clone()) {
                        Ok(row) => row,
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(), "skip turbopuffer row failed to encode upsert payload");
                            continue;
                        }
                    };
                    CompactedOp::Upsert(upsert_row)
                }
                Op::Delete | Op::UpdateDelete => CompactedOp::Delete,
            };
            self.pending_batches
                .entry(url)
                .or_default()
                .insert(id, compacted_op);
            has_pending_update = true;
        }

        Ok(has_pending_update)
    }

    fn pending_row_count(&self) -> usize {
        self.pending_batches.values().map(HashMap::len).sum()
    }

    fn should_flush_by_size(&self) -> bool {
        self.pending_row_count() >= self.write_batch_size
    }

    fn is_empty(&self) -> bool {
        self.pending_batches.is_empty()
    }

    async fn flush_all(&mut self) -> Result<()> {
        let batches = std::mem::take(&mut self.pending_batches);
        let client = self.client.clone();
        try_join_all(batches.into_iter().filter_map(|(url, batch)| {
            let (upsert_rows, deletes) = batch_into_request_parts(batch);
            if upsert_rows.is_empty() && deletes.is_empty() {
                return None;
            }

            let client = client.clone();
            let body = self.request_body(upsert_rows, deletes);
            Some(async move { send_turbopuffer_request(client, url, body).await })
        }))
        .await?;
        Ok(())
    }
}

async fn send_turbopuffer_request(client: reqwest::Client, url: String, body: Value) -> Result<()> {
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("turbopuffer write request failed")
        .map_err(SinkError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(SinkError::Http(anyhow!(
            "Turbopuffer sink received non-success response: {} {}",
            status,
            body
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged)]
enum DocumentId {
    U64(u64),
    String(String),
}

#[derive(Debug)]
enum CompactedOp {
    Upsert(Map<String, Value>),
    Delete,
}

fn batch_into_request_parts(
    batch: HashMap<DocumentId, CompactedOp>,
) -> (Vec<Map<String, Value>>, Vec<DocumentId>) {
    let mut upsert_rows = Vec::new();
    let mut deletes = Vec::new();
    for (id, op) in batch {
        match op {
            CompactedOp::Upsert(row) => upsert_rows.push(row),
            CompactedOp::Delete => deletes.push(id),
        }
    }
    (upsert_rows, deletes)
}

fn document_id_from_i64(value: i64) -> DocumentId {
    if value < 0 {
        tracing::warn!(
            value,
            "cast negative turbopuffer integer document id to unsigned integer"
        );
    }
    DocumentId::U64(value as u64)
}

fn validate_namespace(namespace: &str) -> Result<()> {
    if namespace.is_empty() || namespace.len() > 128 {
        return Err(SinkError::Config(anyhow!(
            "Turbopuffer namespace must be 1 to 128 bytes"
        )));
    }
    if !namespace
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(SinkError::Config(anyhow!(
            "Turbopuffer namespace must match [A-Za-z0-9-_.]{{1,128}}"
        )));
    }
    Ok(())
}

fn parse_column_selection(
    value: Option<&str>,
    schema: &Schema,
    attribute_indices: &[usize],
) -> Result<HashSet<String>> {
    let attribute_names = attribute_indices
        .iter()
        .map(|index| schema[*index].name.as_str())
        .collect::<HashSet<_>>();
    let columns: HashSet<String> = match value {
        Some(value) if value.trim() == "*" => {
            return Ok(attribute_names
                .into_iter()
                .map(str::to_owned)
                .collect::<HashSet<_>>());
        }
        Some(value) => value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect(),
        None => return Ok(HashSet::new()),
    };
    for column in &columns {
        if !attribute_names.contains(column.as_str()) {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer schema option references unknown attribute column '{}'",
                column
            )));
        }
    }
    Ok(columns)
}

fn build_turbopuffer_schema(
    schema: &Schema,
    attribute_indices: &[usize],
    full_text_search_columns: &HashSet<String>,
    filterable_columns: &HashSet<String>,
) -> Result<Value> {
    let mut result = Map::new();
    for index in attribute_indices {
        let field = &schema[*index];
        let mut config = Map::new();
        let data_type = field.data_type();
        let turbopuffer_type = turbopuffer_type(&data_type)?;
        let is_vector = matches!(data_type, DataType::Vector(_));
        let is_full_text_search = full_text_search_columns.contains(&field.name);
        if is_full_text_search && !supports_full_text_search(&data_type) {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer full_text_search column '{}' must be string or []string",
                field.name
            )));
        }
        config.insert("type".to_owned(), Value::String(turbopuffer_type));
        if filterable_columns.contains(&field.name) {
            config.insert("filterable".to_owned(), Value::Bool(true));
        }
        if is_full_text_search {
            config.insert("full_text_search".to_owned(), Value::Bool(true));
        }
        if is_vector {
            config.insert("ann".to_owned(), Value::Bool(true));
        }
        result.insert(field.name.clone(), Value::Object(config));
    }
    Ok(Value::Object(result))
}

fn supports_full_text_search(data_type: &DataType) -> bool {
    match data_type {
        DataType::Varchar => true,
        DataType::List(list_type) => matches!(list_type.elem(), DataType::Varchar),
        _ => false,
    }
}

// Mapping from RisingWave attribute types to generated turbopuffer schema types and
// the JSON value shapes sent to turbopuffer:
//
// | RisingWave type                  | turbopuffer type | JSON payload                  |
// |----------------------------------|------------------|-------------------------------|
// | boolean                          | bool             | boolean                       |
// | int16, int32, int64              | int              | number                        |
// | float32, float64                 | float            | number                        |
// | varchar                          | string           | string                        |
// | date                             | datetime         | string: YYYY-MM-DD            |
// | timestamp                        | datetime         | ISO 8601 string without zone  |
// | timestamptz                      | datetime         | RFC3339 UTC string            |
// | boolean[]                        | []bool           | array of booleans             |
// | int16[], int32[], int64[]        | []int            | array of numbers              |
// | float32[], float64[]             | []float          | array of numbers              |
// | varchar[]                        | []string         | array of strings              |
// | date[], timestamp[], timestamptz[] | []datetime      | array of datetime strings     |
// | vector(N)                        | [N]f32           | array of numbers              |
// | serial                           | int              | number                        |
// | decimal                          | float            | number, converted through f64 |
// | serial[]                         | []int            | array of numbers              |
// | decimal[]                        | []float          | array of f64-converted numbers|
//
// The primary key column is encoded separately as the turbopuffer document id, so
// it does not participate in this schema mapping.
fn turbopuffer_type(data_type: &DataType) -> Result<String> {
    match data_type {
        DataType::Boolean => Ok("bool".to_owned()),
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Serial => {
            Ok("int".to_owned())
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal => Ok("float".to_owned()),
        DataType::Varchar => Ok("string".to_owned()),
        DataType::Date | DataType::Timestamp | DataType::Timestamptz => Ok("datetime".to_owned()),
        DataType::List(list_type) => match list_type.elem() {
            DataType::Boolean => Ok("[]bool".to_owned()),
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Serial => {
                Ok("[]int".to_owned())
            }
            DataType::Float32 | DataType::Float64 | DataType::Decimal => Ok("[]float".to_owned()),
            DataType::Varchar => Ok("[]string".to_owned()),
            DataType::Date | DataType::Timestamp | DataType::Timestamptz => {
                Ok("[]datetime".to_owned())
            }
            elem_type => Err(unsupported_type(&format!("list element {:?}", elem_type))),
        },
        DataType::Vector(dimension) => Ok(format!("[{}]f32", dimension)),
        data_type => Err(unsupported_type(&format!("{:?}", data_type))),
    }
}

fn unsupported_type(data_type: &str) -> SinkError {
    SinkError::Config(anyhow!(
        "Turbopuffer sink does not support column type {}",
        data_type
    ))
}

#[cfg(test)]
mod tests {
    #[cfg(not(madsim))]
    use std::collections::VecDeque;
    #[cfg(not(madsim))]
    use std::io::{Read, Write};
    #[cfg(not(madsim))]
    use std::net::TcpListener;
    #[cfg(not(madsim))]
    use std::sync::{Arc, Mutex, mpsc};
    #[cfg(not(madsim))]
    use std::thread;

    #[cfg(not(madsim))]
    use risingwave_common::array::StreamChunk;
    #[cfg(not(madsim))]
    use risingwave_common::array::stream_chunk::StreamChunkTestExt as _;
    use risingwave_common::array::{ListValue, VectorVal};
    #[cfg(not(madsim))]
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{ListType, ScalarImpl, Timestamp, Timestamptz};
    use serde_json::json;

    use super::*;
    #[cfg(not(madsim))]
    use crate::sink::log_store::LogStoreResult;

    #[test]
    fn test_build_schema_flags() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
            Field::with_name(DataType::List(ListType::new(DataType::Varchar)), "tags"),
            Field::with_name(DataType::Boolean, "flag"),
            Field::with_name(DataType::Vector(384), "vector"),
        ]);
        let generated = build_turbopuffer_schema(
            &schema,
            &[1, 2, 3, 4],
            &parse_column_selection(Some("body,tags"), &schema, &[1, 2, 3, 4]).unwrap(),
            &parse_column_selection(Some("*"), &schema, &[1, 2, 3, 4]).unwrap(),
        )
        .unwrap();

        assert_eq!(generated["body"]["type"], json!("string"));
        assert_eq!(generated["body"]["filterable"], json!(true));
        assert_eq!(generated["body"]["full_text_search"], json!(true));
        assert_eq!(generated["tags"]["type"], json!("[]string"));
        assert_eq!(generated["tags"]["full_text_search"], json!(true));
        assert_eq!(generated["vector"]["type"], json!("[384]f32"));
        assert_eq!(generated["vector"]["ann"], json!(true));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_write_chunk_buffers_until_flush_and_posts_payload_and_headers() {
        let (base_url, request_rx, server_thread) = spawn_mock_http_server(1);
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
            Field::with_name(DataType::Varchar, "workspace_id"),
        ]);
        let payload_indices = vec![1];
        let generated_schema = build_turbopuffer_schema(
            &schema,
            &payload_indices,
            &parse_column_selection(Some("body"), &schema, &payload_indices).unwrap(),
            &parse_column_selection(Some("*"), &schema, &payload_indices).unwrap(),
        )
        .unwrap();
        let config = TurbopufferConfig {
            base_url,
            namespace: None,
            namespace_column: Some("workspace_id".to_owned()),
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: None,
            disable_backpressure: Some(true),
            num_shards: Some(8),
            full_text_search_columns: Some("body".to_owned()),
            filterable_columns: Some("*".to_owned()),
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            max_linger_second: DEFAULT_MAX_LINGER_SECOND,
            r#type: "upsert".to_owned(),
            unknown_fields: Default::default(),
        };
        let mut writer = TurbopufferSinkWriter::new(
            config,
            schema,
            0,
            TurbopufferNamespace::Dynamic { index: 2 },
            payload_indices,
            generated_schema,
            DEFAULT_WRITE_BATCH_SIZE,
            Duration::from_secs(DEFAULT_MAX_LINGER_SECOND),
        )
        .unwrap();
        let chunk = StreamChunk::from_pretty(
            "T  T        T
            U- old-id   old_body ns_1
            U+ new-id   new_body ns_1",
        );

        writer.write_chunk(chunk).unwrap();
        assert!(request_rx.try_recv().is_err());
        writer.flush_all().await.unwrap();
        let request = request_rx.recv().unwrap();
        server_thread.join().unwrap();

        assert!(request.starts_with("post /v2/namespaces/ns_1 http/1.1"));
        assert!(request.contains("authorization: bearer tpuf_test_key"));
        assert!(request.contains("content-type: application/json"));

        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["disable_backpressure"], json!(true));
        assert_eq!(body["sharding"]["num_shards"], json!(8));
        assert_eq!(body["schema"]["body"]["type"], json!("string"));
        assert_eq!(body["schema"]["body"]["filterable"], json!(true));
        assert_eq!(body["schema"]["body"]["full_text_search"], json!(true));
        assert_eq!(body["deletes"], json!(["old-id"]));
        assert_eq!(body["upsert_rows"].as_array().unwrap().len(), 1);
        assert_eq!(body["upsert_rows"][0]["id"], json!("new-id"));
        assert_eq!(body["upsert_rows"][0]["body"], json!("new_body"));
        assert!(body.get("distance_metric").is_none());
    }

    #[test]
    fn test_config_defaults_and_validation() {
        let config = TurbopufferConfig::from_btreemap(BTreeMap::from([
            ("base_url".to_owned(), "http://127.0.0.1:0".to_owned()),
            ("namespace".to_owned(), "ns".to_owned()),
            ("api_key".to_owned(), "key".to_owned()),
            ("type".to_owned(), "upsert".to_owned()),
        ]))
        .unwrap();
        assert_eq!(config.write_batch_size, DEFAULT_WRITE_BATCH_SIZE);
        assert_eq!(config.max_linger_second, DEFAULT_MAX_LINGER_SECOND);
        assert_eq!(config.num_shards, None);

        let err = TurbopufferConfig::from_btreemap(BTreeMap::from([
            ("base_url".to_owned(), "http://127.0.0.1:0".to_owned()),
            ("namespace".to_owned(), "ns".to_owned()),
            ("api_key".to_owned(), "key".to_owned()),
            ("type".to_owned(), "upsert".to_owned()),
            ("write_batch_size".to_owned(), "0".to_owned()),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("write_batch_size"));

        let err = TurbopufferConfig::from_btreemap(BTreeMap::from([
            ("base_url".to_owned(), "http://127.0.0.1:0".to_owned()),
            ("namespace".to_owned(), "ns".to_owned()),
            ("api_key".to_owned(), "key".to_owned()),
            ("type".to_owned(), "upsert".to_owned()),
            ("max_linger_second".to_owned(), "0".to_owned()),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("max_linger_second"));

        let err = TurbopufferConfig::from_btreemap(BTreeMap::from([
            ("base_url".to_owned(), "http://127.0.0.1:0".to_owned()),
            ("namespace".to_owned(), "ns".to_owned()),
            ("api_key".to_owned(), "key".to_owned()),
            ("type".to_owned(), "upsert".to_owned()),
            ("num_shards".to_owned(), "0".to_owned()),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("num_shards"));
    }

    #[test]
    fn test_requires_sink_decouple() {
        assert!(TurbopufferSink::is_sink_decouple(&SinkDecouple::Default).unwrap());
        assert!(TurbopufferSink::is_sink_decouple(&SinkDecouple::Enable).unwrap());
        let err = TurbopufferSink::is_sink_decouple(&SinkDecouple::Disable).unwrap_err();
        assert!(
            err.to_string()
                .contains("Turbopuffer sink can only be created with sink_decouple enabled")
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_global_threshold_flushes_all_namespaces() {
        let (base_url, request_rx, server_thread) = spawn_mock_http_server(2);
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
            Field::with_name(DataType::Varchar, "workspace_id"),
        ]);
        let generated_schema =
            build_turbopuffer_schema(&schema, &[1], &HashSet::new(), &HashSet::new()).unwrap();
        let config = TurbopufferConfig {
            base_url,
            namespace: None,
            namespace_column: Some("workspace_id".to_owned()),
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: None,
            disable_backpressure: None,
            num_shards: None,
            full_text_search_columns: None,
            filterable_columns: None,
            write_batch_size: 2,
            max_linger_second: DEFAULT_MAX_LINGER_SECOND,
            r#type: "upsert".to_owned(),
            unknown_fields: Default::default(),
        };
        let mut writer = TurbopufferSinkWriter::new(
            config,
            schema,
            0,
            TurbopufferNamespace::Dynamic { index: 2 },
            vec![1],
            generated_schema,
            2,
            Duration::from_secs(DEFAULT_MAX_LINGER_SECOND),
        )
        .unwrap();

        writer
            .write_chunk(StreamChunk::from_pretty(
                "  T  T    T
                + a1 body ns_a
                + b1 body ns_b",
            ))
            .unwrap();
        assert!(writer.should_flush_by_size());
        writer.flush_all().await.unwrap();

        let requests = [request_rx.recv().unwrap(), request_rx.recv().unwrap()];
        server_thread.join().unwrap();
        assert!(
            requests
                .iter()
                .any(|request| request.starts_with("post /v2/namespaces/ns_a http/1.1"))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.starts_with("post /v2/namespaces/ns_b http/1.1"))
        );
        assert!(writer.pending_batches.is_empty());
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_upsert_compacts_across_chunks() {
        let (base_url, request_rx, server_thread) = spawn_mock_http_server(1);
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
        ]);
        let generated_schema =
            build_turbopuffer_schema(&schema, &[1], &HashSet::new(), &HashSet::new()).unwrap();
        let config = TurbopufferConfig {
            base_url,
            namespace: Some("ns".to_owned()),
            namespace_column: None,
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: None,
            disable_backpressure: None,
            num_shards: None,
            full_text_search_columns: None,
            filterable_columns: None,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            max_linger_second: DEFAULT_MAX_LINGER_SECOND,
            r#type: "upsert".to_owned(),
            unknown_fields: Default::default(),
        };
        let mut writer = TurbopufferSinkWriter::new(
            config,
            schema,
            0,
            TurbopufferNamespace::Static("ns".to_owned()),
            vec![1],
            generated_schema,
            DEFAULT_WRITE_BATCH_SIZE,
            Duration::from_secs(DEFAULT_MAX_LINGER_SECOND),
        )
        .unwrap();

        writer
            .write_chunk(StreamChunk::from_pretty(
                "  T  T
                + id body1",
            ))
            .unwrap();
        writer
            .write_chunk(StreamChunk::from_pretty(
                "  T  T
                - id body1
                + id body2",
            ))
            .unwrap();
        writer.flush_all().await.unwrap();

        let request = request_rx.recv().unwrap();
        server_thread.join().unwrap();
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(body).unwrap();
        assert!(body.get("deletes").is_none());
        assert_eq!(body["upsert_rows"].as_array().unwrap().len(), 1);
        assert_eq!(body["upsert_rows"][0]["id"], json!("id"));
        assert_eq!(body["upsert_rows"][0]["body"], json!("body2"));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_log_sinker_flushes_after_linger() {
        let (base_url, request_rx, server_thread) = spawn_mock_http_server(1);
        let writer = new_test_static_writer_with_linger(
            base_url,
            DEFAULT_WRITE_BATCH_SIZE,
            Duration::from_millis(1),
        );
        let truncates = Arc::new(Mutex::new(Vec::new()));
        let reader = TestSinkLogReader::new(
            vec![
                (
                    1,
                    LogStoreReadItem::StreamChunk {
                        chunk: StreamChunk::from_pretty(
                            "  T  T
                            + id body",
                        ),
                        chunk_id: 0,
                    },
                ),
                (
                    2,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: false,
                        new_vnode_bitmap: None,
                        is_stop: false,
                        schema_change: None,
                    },
                ),
            ],
            truncates.clone(),
        )
        .pending_on_empty();
        tokio::time::timeout(
            Duration::from_millis(200),
            TurbopufferLogSinker::new(writer, SinkWriterMetrics::for_test())
                .consume_log_and_sink(reader),
        )
        .await
        .unwrap_err();

        let request = request_rx.recv().unwrap();
        server_thread.join().unwrap();
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["upsert_rows"].as_array().unwrap().len(), 1);
        assert_eq!(
            *truncates.lock().unwrap(),
            vec![TruncateOffset::Barrier { epoch: 2 }]
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_log_sinker_truncates_latest_chunk_after_threshold_flush() {
        let (base_url, _request_rx, server_thread) = spawn_mock_http_server(1);
        let writer = new_test_static_writer(base_url, 1);
        let truncates = Arc::new(Mutex::new(Vec::new()));
        let reader = TestSinkLogReader::new(
            vec![(
                1,
                LogStoreReadItem::StreamChunk {
                    chunk: StreamChunk::from_pretty(
                        "  T  T
                        + id body",
                    ),
                    chunk_id: 7,
                },
            )],
            truncates.clone(),
        );
        let err = TurbopufferLogSinker::new(writer, SinkWriterMetrics::for_test())
            .consume_log_and_sink(reader)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("done"));
        server_thread.join().unwrap();
        assert_eq!(
            *truncates.lock().unwrap(),
            vec![TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 7
            }]
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_log_sinker_flushes_on_vnode_bitmap_change() {
        let (base_url, request_rx, server_thread) = spawn_mock_http_server(1);
        let writer = new_test_static_writer(base_url, DEFAULT_WRITE_BATCH_SIZE);
        let truncates = Arc::new(Mutex::new(Vec::new()));
        let reader = TestSinkLogReader::new(
            vec![
                (
                    1,
                    LogStoreReadItem::StreamChunk {
                        chunk: StreamChunk::from_pretty(
                            "  T  T
                            + id body",
                        ),
                        chunk_id: 0,
                    },
                ),
                (
                    2,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: false,
                        new_vnode_bitmap: Some(Arc::new(Bitmap::ones(1))),
                        is_stop: false,
                        schema_change: None,
                    },
                ),
            ],
            truncates.clone(),
        );
        let err = TurbopufferLogSinker::new(writer, SinkWriterMetrics::for_test())
            .consume_log_and_sink(reader)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("done"));

        let request = request_rx.recv().unwrap();
        server_thread.join().unwrap();
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["upsert_rows"].as_array().unwrap().len(), 1);
        assert_eq!(
            *truncates.lock().unwrap(),
            vec![TruncateOffset::Barrier { epoch: 2 }]
        );
    }

    #[test]
    fn test_decimal_and_serial_schema_types() {
        assert_eq!(turbopuffer_type(&DataType::Decimal).unwrap(), "float");
        assert_eq!(turbopuffer_type(&DataType::Serial).unwrap(), "int");
        assert_eq!(
            turbopuffer_type(&DataType::List(ListType::new(DataType::Decimal))).unwrap(),
            "[]float"
        );
        assert_eq!(
            turbopuffer_type(&DataType::List(ListType::new(DataType::Serial))).unwrap(),
            "[]int"
        );
    }

    #[test]
    fn test_manual_http_sink_schema_and_payload_shape() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "namespace_id"),
            Field::with_name(DataType::Varchar, "record_id"),
            Field::with_name(DataType::Varchar, "content"),
            Field::with_name(
                DataType::List(ListType::new(DataType::Varchar)),
                "content_segments",
            ),
            Field::with_name(DataType::Varchar, "user_name"),
            Field::with_name(DataType::Varchar, "user_identifier"),
            Field::with_name(DataType::Varchar, "title"),
            Field::with_name(DataType::Boolean, "is_flagged"),
            Field::with_name(DataType::Boolean, "is_resolved"),
            Field::with_name(DataType::Timestamp, "local_event_time"),
            Field::with_name(DataType::Timestamptz, "event_time"),
            Field::with_name(DataType::Int64, "metric_a_count"),
            Field::with_name(DataType::Int64, "metric_b_count"),
            Field::with_name(DataType::List(ListType::new(DataType::Varchar)), "labels"),
            Field::with_name(DataType::Varchar, "group_id"),
            Field::with_name(DataType::Vector(384), "vector"),
        ]);
        let attribute_indices = (2..schema.len()).collect_vec();
        let full_text_search_columns = parse_column_selection(
            Some("content,content_segments,user_name,user_identifier,title"),
            &schema,
            &attribute_indices,
        )
        .unwrap();
        let filterable_columns =
            parse_column_selection(Some("*"), &schema, &attribute_indices).unwrap();
        let generated_schema = build_turbopuffer_schema(
            &schema,
            &attribute_indices,
            &full_text_search_columns,
            &filterable_columns,
        )
        .unwrap();

        assert_eq!(
            generated_schema,
            json!({
                "record_id": {"type": "string", "filterable": true},
                "content": {"type": "string", "filterable": true, "full_text_search": true},
                "content_segments": {"type": "[]string", "filterable": true, "full_text_search": true},
                "user_name": {"type": "string", "filterable": true, "full_text_search": true},
                "user_identifier": {"type": "string", "filterable": true, "full_text_search": true},
                "title": {"type": "string", "filterable": true, "full_text_search": true},
                "is_flagged": {"type": "bool", "filterable": true},
                "is_resolved": {"type": "bool", "filterable": true},
                "local_event_time": {"type": "datetime", "filterable": true},
                "event_time": {"type": "datetime", "filterable": true},
                "metric_a_count": {"type": "int", "filterable": true},
                "metric_b_count": {"type": "int", "filterable": true},
                "labels": {"type": "[]string", "filterable": true},
                "group_id": {"type": "string", "filterable": true},
                "vector": {"type": "[384]f32", "filterable": true, "ann": true}
            })
        );

        let config = TurbopufferConfig {
            base_url: "http://127.0.0.1:0".to_owned(),
            namespace: None,
            namespace_column: Some("namespace_id".to_owned()),
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: Some("cosine_distance".to_owned()),
            disable_backpressure: Some(true),
            num_shards: Some(32),
            full_text_search_columns: Some(
                "content,content_segments,user_name,user_identifier,title".to_owned(),
            ),
            filterable_columns: Some("*".to_owned()),
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            max_linger_second: DEFAULT_MAX_LINGER_SECOND,
            r#type: "upsert".to_owned(),
            unknown_fields: Default::default(),
        };
        let writer = TurbopufferSinkWriter::new(
            config,
            schema,
            0,
            TurbopufferNamespace::Dynamic { index: 1 },
            attribute_indices,
            generated_schema.clone(),
            DEFAULT_WRITE_BATCH_SIZE,
            Duration::from_secs(DEFAULT_MAX_LINGER_SECOND),
        )
        .unwrap();
        let vector =
            VectorVal::from_text(&format!("[{}]", vec!["0.25"; 384].join(",")), 384).unwrap();
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("doc-1".into())),
            Some(ScalarImpl::Utf8("namespace-1".into())),
            Some(ScalarImpl::Utf8("record-1".into())),
            Some(ScalarImpl::Utf8("content text".into())),
            Some(ScalarImpl::List(ListValue::from_iter([
                "segment a",
                "segment b",
            ]))),
            Some(ScalarImpl::Utf8("user-a".into())),
            Some(ScalarImpl::Utf8("user-1".into())),
            Some(ScalarImpl::Utf8("title".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Bool(false)),
            Some(ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(
                1_781_582_706,
                123_456_789,
            ))),
            Some(ScalarImpl::Timestamptz(
                Timestamptz::from_micros(1_781_598_707_000_000).unwrap(),
            )),
            Some(ScalarImpl::Int64(12345)),
            Some(ScalarImpl::Int64(67890)),
            Some(ScalarImpl::List(ListValue::from_iter([
                "label-a", "label-b",
            ]))),
            Some(ScalarImpl::Utf8("group-1".into())),
            Some(ScalarImpl::Vector(vector)),
        ]);
        let id = writer.id_for_row(&row).unwrap();
        let upsert_row = writer.upsert_row(&row, id).unwrap();
        let body = writer.request_body(vec![upsert_row], Vec::new());

        assert_eq!(body["distance_metric"], json!("cosine_distance"));
        assert_eq!(body["disable_backpressure"], json!(true));
        assert_eq!(body["sharding"]["num_shards"], json!(32));
        assert_eq!(body["schema"], generated_schema);
        assert_eq!(body["upsert_rows"][0]["id"], json!("doc-1"));
        assert_eq!(body["upsert_rows"][0]["content"], json!("content text"));
        assert_eq!(
            body["upsert_rows"][0]["content_segments"],
            json!(["segment a", "segment b"])
        );
        assert_eq!(body["upsert_rows"][0]["is_flagged"], json!(true));
        assert_eq!(body["upsert_rows"][0]["is_resolved"], json!(false));
        assert_eq!(body["upsert_rows"][0]["metric_a_count"], json!(12345));
        assert_eq!(
            body["upsert_rows"][0]["local_event_time"],
            json!("2026-06-16T04:05:06.123456")
        );
        assert_eq!(
            body["upsert_rows"][0]["event_time"],
            json!("2026-06-16T08:31:47.000000Z")
        );
        assert_eq!(
            body["upsert_rows"][0]["vector"].as_array().unwrap().len(),
            384
        );
        assert_eq!(body["upsert_rows"][0]["vector"][0], json!(0.25));
    }

    #[cfg(not(madsim))]
    fn spawn_mock_http_server(
        expected_requests: usize,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server_thread = thread::spawn(move || {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = Vec::new();
                let header_end = loop {
                    let mut tmp = [0; 1024];
                    let read = stream.read(&mut tmp).unwrap();
                    assert_ne!(read, 0);
                    buf.extend_from_slice(&tmp[..read]);
                    if let Some(header_end) = find_header_end(&buf) {
                        break header_end;
                    }
                };
                let headers = String::from_utf8_lossy(&buf[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                while buf.len() < header_end + 4 + content_length {
                    let mut tmp = [0; 1024];
                    let read = stream.read(&mut tmp).unwrap();
                    assert_ne!(read, 0);
                    buf.extend_from_slice(&tmp[..read]);
                }
                let request = String::from_utf8(buf[..header_end + 4 + content_length].to_vec())
                    .expect("HTTP request should be utf8");
                request_tx.send(request.to_lowercase()).unwrap();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .unwrap();
            }
        });
        (format!("http://{}", addr), request_rx, server_thread)
    }

    #[cfg(not(madsim))]
    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|window| window == b"\r\n\r\n")
    }

    #[cfg(not(madsim))]
    fn new_test_static_writer(base_url: String, write_batch_size: usize) -> TurbopufferSinkWriter {
        new_test_static_writer_with_linger(
            base_url,
            write_batch_size,
            Duration::from_secs(DEFAULT_MAX_LINGER_SECOND),
        )
    }

    #[cfg(not(madsim))]
    fn new_test_static_writer_with_linger(
        base_url: String,
        write_batch_size: usize,
        max_linger: Duration,
    ) -> TurbopufferSinkWriter {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
        ]);
        let generated_schema =
            build_turbopuffer_schema(&schema, &[1], &HashSet::new(), &HashSet::new()).unwrap();
        let config = TurbopufferConfig {
            base_url,
            namespace: Some("ns".to_owned()),
            namespace_column: None,
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: None,
            disable_backpressure: None,
            num_shards: None,
            full_text_search_columns: None,
            filterable_columns: None,
            write_batch_size,
            max_linger_second: DEFAULT_MAX_LINGER_SECOND,
            r#type: "upsert".to_owned(),
            unknown_fields: Default::default(),
        };
        TurbopufferSinkWriter::new(
            config,
            schema,
            0,
            TurbopufferNamespace::Static("ns".to_owned()),
            vec![1],
            generated_schema,
            write_batch_size,
            max_linger,
        )
        .unwrap()
    }

    #[cfg(not(madsim))]
    struct TestSinkLogReader {
        items: VecDeque<(u64, LogStoreReadItem)>,
        truncates: Arc<Mutex<Vec<TruncateOffset>>>,
        pending_on_empty: bool,
    }

    #[cfg(not(madsim))]
    impl TestSinkLogReader {
        fn new(
            items: Vec<(u64, LogStoreReadItem)>,
            truncates: Arc<Mutex<Vec<TruncateOffset>>>,
        ) -> Self {
            Self {
                items: items.into(),
                truncates,
                pending_on_empty: false,
            }
        }

        fn pending_on_empty(mut self) -> Self {
            self.pending_on_empty = true;
            self
        }
    }

    #[cfg(not(madsim))]
    impl SinkLogReader for TestSinkLogReader {
        async fn start_from(&mut self, _start_offset: Option<u64>) -> LogStoreResult<()> {
            Ok(())
        }

        async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
            match self.items.pop_front() {
                Some(item) => Ok(item),
                None if self.pending_on_empty => pending().await,
                None => Err(anyhow!("done")),
            }
        }

        fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
            self.truncates.lock().unwrap().push(offset);
            Ok(())
        }
    }
}
