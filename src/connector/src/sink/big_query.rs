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

use core::pin::Pin;
use core::time::Duration;
use std::collections::{BTreeMap, HashMap, VecDeque};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use futures::future::pending;
use futures::prelude::Future;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::ResultSet;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use google_cloud_bigquery::grpc::apiv1::conn_pool::ConnectionManager;
use google_cloud_gax::conn::{ConnectionOptions, Environment};
use google_cloud_gax::grpc::{Request, Response, Status};
use google_cloud_googleapis::cloud::bigquery::storage::v1::append_rows_request::{
    MissingValueInterpretation, ProtoData, Rows as AppendRowsRequestRows,
};
use google_cloud_googleapis::cloud::bigquery::storage::v1::{
    AppendRowsRequest, AppendRowsResponse, ProtoRows, ProtoSchema,
};
use google_cloud_pubsub::client::google_cloud_auth;
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use phf::{Set, phf_set};
use prost_reflect::{FieldDescriptor, MessageDescriptor};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto,
};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use simd_json::prelude::ArrayTrait;
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;
use with_options::WithOptions;
use yup_oauth2::ServiceAccountKey;

use super::encoder::{ProtoEncoder, ProtoHeader, RowEncoder, SerTo};
use super::log_store::{LogStoreReadItem, TruncateOffset};
use super::{
    LogSinker, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkLogReader,
};
use crate::aws_utils::load_file_descriptor_from_s3;
use crate::connector_common::AwsAuthProps;
use crate::enforce_secret::EnforceSecret;
use crate::sink::{Result, Sink, SinkParam, SinkWriterParam};

pub const BIGQUERY_SINK: &str = "bigquery";
pub const CHANGE_TYPE: &str = "_CHANGE_TYPE";
const DEFAULT_GRPC_CHANNEL_NUMS: usize = 4;
const CONNECT_TIMEOUT: Option<Duration> = Some(Duration::from_secs(30));
const CONNECTION_TIMEOUT: Option<Duration> = None;
const BIGQUERY_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 65536;
// < 10MB, we set 8MB
const MAX_ROW_SIZE: usize = 8 * 1024 * 1024;

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct BigQueryCommon {
    #[serde(rename = "bigquery.local.path")]
    pub local_path: Option<String>,
    #[serde(rename = "bigquery.s3.path")]
    pub s3_path: Option<String>,
    #[serde(rename = "bigquery.project")]
    pub project: String,
    #[serde(rename = "bigquery.dataset")]
    pub dataset: String,
    #[serde(rename = "bigquery.table")]
    pub table: String,
    #[serde(default, alias = "create_table_if_not_exists")] // default false
    #[serde_as(as = "DisplayFromStr")]
    pub auto_create: bool,
    #[serde(rename = "bigquery.credentials")]
    pub credentials: Option<String>,
}

impl EnforceSecret for BigQueryCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "bigquery.credentials",
    };
}

struct BigQueryFutureManager {
    // `offset_queue` holds the Some corresponding to each future.
    // When TruncateOffset is barrier, the num is 0, we don't need to wait for the return of `resp_stream`.
    // When TruncateOffset is chunk:
    // 1. chunk has no rows. we didn't send, the num is 0, we don't need to wait for the return of `resp_stream`.
    // 2. chunk is less than `MAX_ROW_SIZE`, we only sent once, the num is 1 and we only have to wait once for `resp_stream`.
    // 3. chunk is less than `MAX_ROW_SIZE`, we only sent n, the num is n and we need to wait n times for r.
    offset_queue: VecDeque<(TruncateOffset, usize)>,
    resp_stream: Pin<Box<dyn Stream<Item = Result<()>> + Send>>,
}
impl BigQueryFutureManager {
    pub fn new(
        max_future_num: usize,
        resp_stream: impl Stream<Item = Result<()>> + Send + 'static,
    ) -> Self {
        let offset_queue = VecDeque::with_capacity(max_future_num);
        Self {
            offset_queue,
            resp_stream: Box::pin(resp_stream),
        }
    }

    pub fn add_offset(&mut self, offset: TruncateOffset, resp_num: usize) {
        self.offset_queue.push_back((offset, resp_num));
    }

    pub async fn next_offset(&mut self) -> Result<TruncateOffset> {
        if let Some((_offset, remaining_resp_num)) = self.offset_queue.front_mut() {
            if *remaining_resp_num == 0 {
                return Ok(self.offset_queue.pop_front().unwrap().0);
            }
            while *remaining_resp_num > 0 {
                self.resp_stream
                    .next()
                    .await
                    .ok_or_else(|| SinkError::BigQuery(anyhow::anyhow!("end of stream")))??;
                *remaining_resp_num -= 1;
            }
            Ok(self.offset_queue.pop_front().unwrap().0)
        } else {
            pending().await
        }
    }
}
pub struct BigQueryLogSinker {
    writer: BigQuerySinkWriter,
    bigquery_future_manager: BigQueryFutureManager,
    future_num: usize,
}
impl BigQueryLogSinker {
    pub fn new(
        writer: BigQuerySinkWriter,
        resp_stream: impl Stream<Item = Result<()>> + Send + 'static,
        future_num: usize,
    ) -> Self {
        Self {
            writer,
            bigquery_future_manager: BigQueryFutureManager::new(future_num, resp_stream),
            future_num,
        }
    }
}

#[async_trait]
impl LogSinker for BigQueryLogSinker {
    async fn consume_log_and_sink(mut self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        loop {
            tokio::select!(
                offset = self.bigquery_future_manager.next_offset() => {
                        log_reader.truncate(offset?)?;
                }
                item_result = log_reader.next_item(), if self.bigquery_future_manager.offset_queue.len() <= self.future_num => {
                    let (epoch, item) = item_result?;
                    match item {
                        LogStoreReadItem::StreamChunk { chunk_id, chunk } => {
                            let resp_num = self.writer.write_chunk(chunk)?;
                            self.bigquery_future_manager
                                .add_offset(TruncateOffset::Chunk { epoch, chunk_id },resp_num);
                        }
                        LogStoreReadItem::Barrier { .. } => {
                            self.bigquery_future_manager
                                .add_offset(TruncateOffset::Barrier { epoch },0);
                        }
                    }
                }
            )
        }
    }
}

impl BigQueryCommon {
    async fn build_client(&self, aws_auth_props: &AwsAuthProps) -> Result<Client> {
        let auth_json = self.get_auth_json_from_path(aws_auth_props).await?;

        let service_account =
            if let Ok(auth_json_from_base64) = BASE64_STANDARD.decode(auth_json.clone()) {
                serde_json::from_slice::<ServiceAccountKey>(&auth_json_from_base64)
            } else {
                serde_json::from_str::<ServiceAccountKey>(&auth_json)
            }
            .map_err(|e| SinkError::BigQuery(e.into()))?;

        let client: Client = Client::from_service_account_key(service_account, false)
            .await
            .map_err(|err| SinkError::BigQuery(anyhow::anyhow!(err)))?;
        Ok(client)
    }

    async fn build_writer_client(
        &self,
        aws_auth_props: &AwsAuthProps,
    ) -> Result<(StorageWriterClient, impl Stream<Item = Result<()>> + use<>)> {
        let auth_json = self.get_auth_json_from_path(aws_auth_props).await?;

        let credentials_file =
            if let Ok(auth_json_from_base64) = BASE64_STANDARD.decode(auth_json.clone()) {
                serde_json::from_slice::<CredentialsFile>(&auth_json_from_base64)
            } else {
                serde_json::from_str::<CredentialsFile>(&auth_json)
            }
            .map_err(|e| SinkError::BigQuery(e.into()))?;

        StorageWriterClient::new(credentials_file).await
    }

    async fn get_auth_json_from_path(&self, aws_auth_props: &AwsAuthProps) -> Result<String> {
        if let Some(credentials) = &self.credentials {
            Ok(credentials.clone())
        } else if let Some(local_path) = &self.local_path {
            std::fs::read_to_string(local_path)
                .map_err(|err| SinkError::BigQuery(anyhow::anyhow!(err)))
        } else if let Some(s3_path) = &self.s3_path {
            let url =
                Url::parse(s3_path).map_err(|err| SinkError::BigQuery(anyhow::anyhow!(err)))?;
            let auth_vec = load_file_descriptor_from_s3(&url, aws_auth_props)
                .await
                .map_err(|err| SinkError::BigQuery(anyhow::anyhow!(err)))?;
            Ok(String::from_utf8(auth_vec).map_err(|e| SinkError::BigQuery(e.into()))?)
        } else {
            Err(SinkError::BigQuery(anyhow::anyhow!(
                "`bigquery.local.path` and `bigquery.s3.path` set at least one, configure as needed."
            )))
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct BigQueryConfig {
    #[serde(flatten)]
    pub common: BigQueryCommon,
    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(BigQueryConfig);

impl EnforceSecret for BigQueryConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        BigQueryCommon::enforce_one(prop)?;
        AwsAuthProps::enforce_one(prop)?;
        Ok(())
    }
}

impl BigQueryConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<BigQueryConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct BigQuerySink {
    pub config: BigQueryConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for BigQuerySink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            BigQueryConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl BigQuerySink {
    pub fn new(
        config: BigQueryConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl BigQuerySink {
    fn is_decimal_type_compatible(bigquery_type: &str) -> bool {
        // BigQuery INFORMATION_SCHEMA reports parameterized decimal columns as
        // `NUMERIC(p, s)` or `BIGNUMERIC(p, s)`. RisingWave `Decimal` does not
        // carry typmod, so schema validation accepts the BigQuery decimal type
        // family instead of requiring exact string equality.
        let normalized = bigquery_type.trim().to_ascii_uppercase();
        matches!(
            normalized
                .split_once('(')
                .map_or(normalized.as_str(), |(prefix, _)| prefix),
            "NUMERIC" | "BIGNUMERIC"
        )
    }

    fn is_data_type_compatible(rw_data_type: &DataType, bigquery_type: &str) -> Result<bool> {
        if matches!(rw_data_type, DataType::Decimal) {
            return Ok(Self::is_decimal_type_compatible(bigquery_type));
        }

        Ok(Self::get_string_and_check_support_from_datatype(rw_data_type)? == bigquery_type)
    }

    fn check_column_name_and_type(
        &self,
        big_query_columns_desc: HashMap<String, String>,
    ) -> Result<()> {
        let rw_fields_name = self.schema.fields();
        if big_query_columns_desc.is_empty() {
            return Err(SinkError::BigQuery(anyhow::anyhow!(
                "Cannot find table in bigquery"
            )));
        }
        if rw_fields_name.len().ne(&big_query_columns_desc.len()) {
            return Err(SinkError::BigQuery(anyhow::anyhow!(
                "The length of the RisingWave column {} must be equal to the length of the bigquery column {}",
                rw_fields_name.len(),
                big_query_columns_desc.len()
            )));
        }

        for i in rw_fields_name {
            let value = big_query_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::BigQuery(anyhow::anyhow!(
                    "Column `{:?}` on RisingWave side is not found on BigQuery side.",
                    i.name
                ))
            })?;
            let data_type_string = Self::get_string_and_check_support_from_datatype(&i.data_type)?;
            if !Self::is_data_type_compatible(&i.data_type, value)? {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "Data type mismatch for column `{:?}`. BigQuery side: `{:?}`, RisingWave side: `{:?}`. ",
                    i.name,
                    value,
                    data_type_string
                )));
            };
        }
        Ok(())
    }

    fn get_string_and_check_support_from_datatype(rw_data_type: &DataType) -> Result<String> {
        match rw_data_type {
            DataType::Boolean => Ok("BOOL".to_owned()),
            DataType::Int16 => Ok("INT64".to_owned()),
            DataType::Int32 => Ok("INT64".to_owned()),
            DataType::Int64 => Ok("INT64".to_owned()),
            DataType::Float32 => Err(SinkError::BigQuery(anyhow::anyhow!(
                "REAL is not supported for BigQuery sink. Please convert to FLOAT64 or other supported types."
            ))),
            DataType::Float64 => Ok("FLOAT64".to_owned()),
            DataType::Decimal => Ok("NUMERIC".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            DataType::Varchar => Ok("STRING".to_owned()),
            DataType::Time => Ok("TIME".to_owned()),
            DataType::Timestamp => Ok("DATETIME".to_owned()),
            DataType::Timestamptz => Ok("TIMESTAMP".to_owned()),
            DataType::Interval => Ok("INTERVAL".to_owned()),
            DataType::Struct(structs) => {
                let mut elements_vec = vec![];
                for (name, datatype) in structs.iter() {
                    let element_string =
                        Self::get_string_and_check_support_from_datatype(datatype)?;
                    elements_vec.push(format!("{} {}", name, element_string));
                }
                Ok(format!("STRUCT<{}>", elements_vec.join(", ")))
            }
            DataType::List(l) => {
                let element_string = Self::get_string_and_check_support_from_datatype(l.elem())?;
                Ok(format!("ARRAY<{}>", element_string))
            }
            DataType::Bytea => Ok("BYTES".to_owned()),
            DataType::Jsonb => Ok("JSON".to_owned()),
            DataType::Variant => Err(SinkError::BigQuery(anyhow::anyhow!(
                "VARIANT is not supported for BigQuery sink."
            ))),
            DataType::Serial => Ok("INT64".to_owned()),
            DataType::Int256 => Err(SinkError::BigQuery(anyhow::anyhow!(
                "INT256 is not supported for BigQuery sink."
            ))),
            DataType::Map(_) => Err(SinkError::BigQuery(anyhow::anyhow!(
                "MAP is not supported for BigQuery sink."
            ))),
            DataType::Vector(_) => Err(SinkError::BigQuery(anyhow::anyhow!(
                "VECTOR is not supported for BigQuery sink."
            ))),
        }
    }

    fn map_field(rw_field: &Field) -> Result<TableFieldSchema> {
        let tfs = match &rw_field.data_type {
            DataType::Boolean => TableFieldSchema::bool(&rw_field.name),
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Serial => {
                TableFieldSchema::integer(&rw_field.name)
            }
            DataType::Float32 => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "REAL is not supported for BigQuery sink. Please convert to FLOAT64 or other supported types."
                )));
            }
            DataType::Float64 => TableFieldSchema::float(&rw_field.name),
            DataType::Decimal => TableFieldSchema::numeric(&rw_field.name),
            DataType::Date => TableFieldSchema::date(&rw_field.name),
            DataType::Varchar => TableFieldSchema::string(&rw_field.name),
            DataType::Time => TableFieldSchema::time(&rw_field.name),
            DataType::Timestamp => TableFieldSchema::date_time(&rw_field.name),
            DataType::Timestamptz => TableFieldSchema::timestamp(&rw_field.name),
            DataType::Interval => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "INTERVAL is not supported for BigQuery sink. Please convert to VARCHAR or other supported types."
                )));
            }
            DataType::Struct(st) => {
                let mut sub_fields = Vec::with_capacity(st.len());
                for (name, dt) in st.iter() {
                    let rw_field = Field::with_name(dt.clone(), name);
                    let field = Self::map_field(&rw_field)?;
                    sub_fields.push(field);
                }
                TableFieldSchema::record(&rw_field.name, sub_fields)
            }
            DataType::List(lt) => {
                let inner_field =
                    Self::map_field(&Field::with_name(lt.elem().clone(), &rw_field.name))?;
                TableFieldSchema {
                    mode: Some("REPEATED".to_owned()),
                    ..inner_field
                }
            }

            DataType::Bytea => TableFieldSchema::bytes(&rw_field.name),
            DataType::Jsonb => TableFieldSchema::json(&rw_field.name),
            DataType::Variant => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "VARIANT is not supported for BigQuery sink."
                )));
            }
            DataType::Int256 => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "INT256 is not supported for BigQuery sink."
                )));
            }
            DataType::Map(_) => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "MAP is not supported for BigQuery sink."
                )));
            }
            DataType::Vector(_) => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "VECTOR is not supported for BigQuery sink."
                )));
            }
        };
        Ok(tfs)
    }

    async fn create_table(
        &self,
        client: &Client,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        fields: &Vec<Field>,
    ) -> Result<Table> {
        let dataset = client
            .dataset()
            .get(project_id, dataset_id)
            .await
            .map_err(|e| SinkError::BigQuery(e.into()))?;
        let fields: Vec<_> = fields.iter().map(Self::map_field).collect::<Result<_>>()?;
        let table = Table::from_dataset(&dataset, table_id, TableSchema::new(fields));

        client
            .table()
            .create(table)
            .await
            .map_err(|e| SinkError::BigQuery(e.into()))
    }
}

impl Sink for BigQuerySink {
    type LogSinker = BigQueryLogSinker;

    const SINK_NAME: &'static str = BIGQUERY_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let (writer, resp_stream) = BigQuerySinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?;
        Ok(BigQueryLogSinker::new(
            writer,
            resp_stream,
            BIGQUERY_SEND_FUTURE_BUFFER_MAX_SIZE,
        ))
    }

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::BigQuerySink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert bigquery sink (please define in `primary_key` field)"
            )));
        }
        let client = self
            .config
            .common
            .build_client(&self.config.aws_auth_props)
            .await?;
        let BigQueryCommon {
            project: project_id,
            dataset: dataset_id,
            table: table_id,
            ..
        } = &self.config.common;

        if self.config.common.auto_create {
            match client
                .table()
                .get(project_id, dataset_id, table_id, None)
                .await
            {
                Err(BQError::ResponseError { error }) if error.error.code == 404 => {
                    // early return: no need to query schema to check column and type
                    return self
                        .create_table(
                            &client,
                            project_id,
                            dataset_id,
                            table_id,
                            &self.schema.fields,
                        )
                        .await
                        .map(|_| ());
                }
                Err(e) => return Err(SinkError::BigQuery(e.into())),
                _ => {}
            }
        }

        let rs = client
            .job()
            .query(
                &self.config.common.project,
                QueryRequest::new(format!(
                    "SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{}'",
                    project_id, dataset_id, table_id,
                )),
            ).await.map_err(|e| SinkError::BigQuery(e.into()))?;
        let mut rs = ResultSet::new_from_query_response(rs);

        let mut big_query_schema = HashMap::default();
        while rs.next_row() {
            big_query_schema.insert(
                rs.get_string_by_name("column_name")
                    .map_err(|e| SinkError::BigQuery(e.into()))?
                    .ok_or_else(|| {
                        SinkError::BigQuery(anyhow::anyhow!("Cannot find column_name"))
                    })?,
                rs.get_string_by_name("data_type")
                    .map_err(|e| SinkError::BigQuery(e.into()))?
                    .ok_or_else(|| {
                        SinkError::BigQuery(anyhow::anyhow!("Cannot find column_name"))
                    })?,
            );
        }

        self.check_column_name_and_type(big_query_schema)?;
        Ok(())
    }
}

pub struct BigQuerySinkWriter {
    pub config: BigQueryConfig,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    client: StorageWriterClient,
    is_append_only: bool,
    row_encoder: ProtoEncoder,
    writer_pb_schema: ProtoSchema,
    #[expect(dead_code)]
    message_descriptor: MessageDescriptor,
    write_stream: String,
    proto_field: Option<FieldDescriptor>,
}

impl TryFrom<SinkParam> for BigQuerySink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = BigQueryConfig::from_btreemap(param.properties)?;
        BigQuerySink::new(config, schema, pk_indices, param.sink_type.is_append_only())
    }
}

impl BigQuerySinkWriter {
    pub async fn new(
        config: BigQueryConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<(Self, impl Stream<Item = Result<()>>)> {
        let (client, resp_stream) = config
            .common
            .build_writer_client(&config.aws_auth_props)
            .await?;
        let mut descriptor_proto = build_protobuf_schema(
            schema
                .fields()
                .iter()
                .map(|f| (f.name.as_str(), &f.data_type)),
            config.common.table.clone(),
        )?;

        if !is_append_only {
            let field = FieldDescriptorProto {
                name: Some(CHANGE_TYPE.to_owned()),
                number: Some((schema.len() + 1) as i32),
                r#type: Some(field_descriptor_proto::Type::String.into()),
                ..Default::default()
            };
            descriptor_proto.field.push(field);
        }

        let descriptor_pool = build_protobuf_descriptor_pool(&descriptor_proto)?;
        let message_descriptor = descriptor_pool
            .get_message_by_name(&config.common.table)
            .ok_or_else(|| {
                SinkError::BigQuery(anyhow::anyhow!(
                    "Can't find message proto {}",
                    config.common.table
                ))
            })?;
        let proto_field = if !is_append_only {
            let proto_field = message_descriptor
                .get_field_by_name(CHANGE_TYPE)
                .ok_or_else(|| {
                    SinkError::BigQuery(anyhow::anyhow!("Can't find {}", CHANGE_TYPE))
                })?;
            Some(proto_field)
        } else {
            None
        };
        let row_encoder = ProtoEncoder::new(
            schema.clone(),
            None,
            message_descriptor.clone(),
            ProtoHeader::None,
        )?;
        Ok((
            Self {
                write_stream: format!(
                    "projects/{}/datasets/{}/tables/{}/streams/_default",
                    config.common.project, config.common.dataset, config.common.table
                ),
                config,
                schema,
                pk_indices,
                client,
                is_append_only,
                row_encoder,
                message_descriptor,
                proto_field,
                writer_pb_schema: ProtoSchema {
                    proto_descriptor: Some(descriptor_proto.clone()),
                },
            },
            resp_stream,
        ))
    }

    fn append_only(&mut self, chunk: StreamChunk) -> Result<Vec<Vec<u8>>> {
        let mut serialized_rows: Vec<Vec<u8>> = Vec::with_capacity(chunk.capacity());
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            serialized_rows.push(self.row_encoder.encode(row)?.ser_to()?)
        }
        Ok(serialized_rows)
    }

    fn upsert(&mut self, chunk: StreamChunk) -> Result<Vec<Vec<u8>>> {
        let mut serialized_rows: Vec<Vec<u8>> = Vec::with_capacity(chunk.capacity());
        for (op, row) in chunk.rows() {
            if op == Op::UpdateDelete {
                continue;
            }
            let mut pb_row = self.row_encoder.encode(row)?;
            match op {
                Op::Insert => pb_row
                    .message
                    .try_set_field(
                        self.proto_field.as_ref().unwrap(),
                        prost_reflect::Value::String("UPSERT".to_owned()),
                    )
                    .map_err(|e| SinkError::BigQuery(e.into()))?,
                Op::Delete => pb_row
                    .message
                    .try_set_field(
                        self.proto_field.as_ref().unwrap(),
                        prost_reflect::Value::String("DELETE".to_owned()),
                    )
                    .map_err(|e| SinkError::BigQuery(e.into()))?,
                Op::UpdateDelete => continue,
                Op::UpdateInsert => pb_row
                    .message
                    .try_set_field(
                        self.proto_field.as_ref().unwrap(),
                        prost_reflect::Value::String("UPSERT".to_owned()),
                    )
                    .map_err(|e| SinkError::BigQuery(e.into()))?,
            };

            serialized_rows.push(pb_row.ser_to()?)
        }
        Ok(serialized_rows)
    }

    fn write_chunk(&mut self, chunk: StreamChunk) -> Result<usize> {
        let serialized_rows = if self.is_append_only {
            self.append_only(chunk)?
        } else {
            self.upsert(chunk)?
        };
        if serialized_rows.is_empty() {
            return Ok(0);
        }
        let mut result = Vec::new();
        let mut result_inner = Vec::new();
        let mut size_count = 0;
        for i in serialized_rows {
            size_count += i.len();
            if size_count > MAX_ROW_SIZE {
                result.push(result_inner);
                result_inner = Vec::new();
                size_count = i.len();
            }
            result_inner.push(i);
        }
        if !result_inner.is_empty() {
            result.push(result_inner);
        }
        let len = result.len();
        for serialized_rows in result {
            let rows = AppendRowsRequestRows::ProtoRows(ProtoData {
                writer_schema: Some(self.writer_pb_schema.clone()),
                rows: Some(ProtoRows { serialized_rows }),
            });
            self.client.append_rows(rows, self.write_stream.clone())?;
        }
        Ok(len)
    }
}

#[try_stream(ok = (), error = SinkError)]
pub async fn resp_to_stream(
    resp_stream: impl Future<
        Output = std::result::Result<
            Response<google_cloud_gax::grpc::Streaming<AppendRowsResponse>>,
            Status,
        >,
    >
    + 'static
    + Send,
) {
    let mut resp_stream = resp_stream
        .await
        .map_err(|e| SinkError::BigQuery(e.into()))?
        .into_inner();
    loop {
        match resp_stream
            .message()
            .await
            .map_err(|e| SinkError::BigQuery(e.into()))?
        {
            Some(append_rows_response) => {
                if !append_rows_response.row_errors.is_empty() {
                    return Err(SinkError::BigQuery(anyhow::anyhow!(
                        "bigquery insert error {:?}",
                        append_rows_response.row_errors
                    )));
                }
                if let Some(google_cloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response::Error(status)) = append_rows_response.response{
                            return Err(SinkError::BigQuery(anyhow::anyhow!(
                                "bigquery insert error {:?}",
                                status
                            )));
                        }
                yield ();
            }
            None => {
                return Err(SinkError::BigQuery(anyhow::anyhow!(
                    "bigquery insert error: end of resp stream",
                )));
            }
        }
    }
}

struct StorageWriterClient {
    #[expect(dead_code)]
    environment: Environment,
    request_sender: mpsc::UnboundedSender<AppendRowsRequest>,
}
impl StorageWriterClient {
    pub async fn new(
        credentials: CredentialsFile,
    ) -> Result<(Self, impl Stream<Item = Result<()>>)> {
        let ts_grpc = google_cloud_auth::token::DefaultTokenSourceProvider::new_with_credentials(
            Self::bigquery_grpc_auth_config(),
            Box::new(credentials),
        )
        .await
        .map_err(|e| SinkError::BigQuery(e.into()))?;
        let conn_options = ConnectionOptions {
            connect_timeout: CONNECT_TIMEOUT,
            timeout: CONNECTION_TIMEOUT,
            ..Default::default()
        };
        let environment = Environment::GoogleCloud(Box::new(ts_grpc));
        let conn = ConnectionManager::new(DEFAULT_GRPC_CHANNEL_NUMS, &environment, &conn_options)
            .await
            .map_err(|e| SinkError::BigQuery(e.into()))?;
        let mut client = conn.writer();

        let (tx, rx) = mpsc::unbounded_channel();
        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        let resp = async move { client.append_rows(Request::new(stream)).await };
        let resp_stream = resp_to_stream(resp);

        Ok((
            StorageWriterClient {
                environment,
                request_sender: tx,
            },
            resp_stream,
        ))
    }

    pub fn append_rows(&mut self, row: AppendRowsRequestRows, write_stream: String) -> Result<()> {
        let append_req = AppendRowsRequest {
            write_stream,
            offset: None,
            trace_id: Uuid::new_v4().hyphenated().to_string(),
            missing_value_interpretations: HashMap::default(),
            rows: Some(row),
            default_missing_value_interpretation: MissingValueInterpretation::DefaultValue as i32,
        };
        self.request_sender
            .send(append_req)
            .map_err(|e| SinkError::BigQuery(e.into()))?;
        Ok(())
    }

    fn bigquery_grpc_auth_config() -> google_cloud_auth::project::Config<'static> {
        let mut auth_config = google_cloud_auth::project::Config::default();
        auth_config =
            auth_config.with_audience(google_cloud_bigquery::grpc::apiv1::conn_pool::AUDIENCE);
        auth_config =
            auth_config.with_scopes(&google_cloud_bigquery::grpc::apiv1::conn_pool::SCOPES);
        auth_config
    }
}

fn build_protobuf_descriptor_pool(desc: &DescriptorProto) -> Result<prost_reflect::DescriptorPool> {
    let file_descriptor = FileDescriptorProto {
        message_type: vec![desc.clone()],
        name: Some("bigquery".to_owned()),
        ..Default::default()
    };

    prost_reflect::DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
        file: vec![file_descriptor],
    })
    .context("failed to build descriptor pool")
    .map_err(SinkError::BigQuery)
}

fn build_protobuf_schema<'a>(
    fields: impl Iterator<Item = (&'a str, &'a DataType)>,
    name: String,
) -> Result<DescriptorProto> {
    let mut proto = DescriptorProto {
        name: Some(name),
        ..Default::default()
    };
    let mut struct_vec = vec![];
    let field_vec = fields
        .enumerate()
        .map(|(index, (name, data_type))| {
            let (field, des_proto) =
                build_protobuf_field(data_type, (index + 1) as i32, name.to_owned())?;
            if let Some(sv) = des_proto {
                struct_vec.push(sv);
            }
            Ok(field)
        })
        .collect::<Result<Vec<_>>>()?;
    proto.field = field_vec;
    proto.nested_type = struct_vec;
    Ok(proto)
}

fn build_protobuf_field(
    data_type: &DataType,
    index: i32,
    name: String,
) -> Result<(FieldDescriptorProto, Option<DescriptorProto>)> {
    let mut field = FieldDescriptorProto {
        name: Some(name.clone()),
        number: Some(index),
        ..Default::default()
    };
    match data_type {
        DataType::Boolean => field.r#type = Some(field_descriptor_proto::Type::Bool.into()),
        DataType::Int32 => field.r#type = Some(field_descriptor_proto::Type::Int32.into()),
        DataType::Int16 | DataType::Int64 => {
            field.r#type = Some(field_descriptor_proto::Type::Int64.into())
        }
        DataType::Float64 => field.r#type = Some(field_descriptor_proto::Type::Double.into()),
        DataType::Decimal => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Date => field.r#type = Some(field_descriptor_proto::Type::Int32.into()),
        DataType::Varchar => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Time => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Timestamp => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Timestamptz => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Interval => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Struct(s) => {
            field.r#type = Some(field_descriptor_proto::Type::Message.into());
            let name = format!("Struct{}", name);
            let sub_proto = build_protobuf_schema(s.iter(), name.clone())?;
            field.type_name = Some(name);
            return Ok((field, Some(sub_proto)));
        }
        DataType::List(l) => {
            let (mut field, proto) = build_protobuf_field(l.elem(), index, name)?;
            field.label = Some(field_descriptor_proto::Label::Repeated.into());
            return Ok((field, proto));
        }
        DataType::Bytea => field.r#type = Some(field_descriptor_proto::Type::Bytes.into()),
        DataType::Jsonb => field.r#type = Some(field_descriptor_proto::Type::String.into()),
        DataType::Variant => {
            return Err(SinkError::BigQuery(anyhow::anyhow!("Don't support Variant")));
        }
        DataType::Serial => field.r#type = Some(field_descriptor_proto::Type::Int64.into()),
        DataType::Float32 | DataType::Int256 => {
            return Err(SinkError::BigQuery(anyhow::anyhow!(
                "Don't support Float32 and Int256"
            )));
        }
        DataType::Map(_) => return Err(SinkError::BigQuery(anyhow::anyhow!("Don't support Map"))),
        DataType::Vector(_) => {
            return Err(SinkError::BigQuery(anyhow::anyhow!("Don't support Vector")));
        }
    }
    Ok((field, None))
}

#[cfg(test)]
mod test {

    use std::assert_matches;
    use std::collections::HashMap;

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, StructType};

    use crate::connector_common::AwsAuthProps;
    use crate::sink::big_query::{
        BigQueryCommon, BigQueryConfig, BigQuerySink, build_protobuf_descriptor_pool,
        build_protobuf_schema,
    };

    #[tokio::test]
    async fn test_type_check() {
        let big_query_type_string = "ARRAY<STRUCT<v1 ARRAY<INT64>, v2 STRUCT<v1 INT64, v2 INT64>>>";
        let rw_datatype = DataType::list(DataType::Struct(StructType::new(vec![
            ("v1".to_owned(), DataType::Int64.list()),
            (
                "v2".to_owned(),
                DataType::Struct(StructType::new(vec![
                    ("v1".to_owned(), DataType::Int64),
                    ("v2".to_owned(), DataType::Int64),
                ])),
            ),
        ])));
        assert_eq!(
            BigQuerySink::get_string_and_check_support_from_datatype(&rw_datatype).unwrap(),
            big_query_type_string
        );
    }

    #[tokio::test]
    async fn test_schema_check() {
        let schema = Schema {
            fields: vec![
                Field::with_name(DataType::Int64, "v1"),
                Field::with_name(DataType::Float64, "v2"),
                Field::with_name(
                    DataType::list(DataType::Struct(StructType::new(vec![
                        ("v1".to_owned(), DataType::Int64.list()),
                        (
                            "v3".to_owned(),
                            DataType::Struct(StructType::new(vec![
                                ("v1".to_owned(), DataType::Int64),
                                ("v2".to_owned(), DataType::Int64),
                            ])),
                        ),
                    ]))),
                    "v3",
                ),
            ],
        };
        let fields = schema
            .fields()
            .iter()
            .map(|f| (f.name.as_str(), &f.data_type));
        let desc = build_protobuf_schema(fields, "t1".to_owned()).unwrap();
        let pool = build_protobuf_descriptor_pool(&desc).unwrap();
        let t1_message = pool.get_message_by_name("t1").unwrap();
        assert_matches!(
            t1_message.get_field_by_name("v1").unwrap().kind(),
            prost_reflect::Kind::Int64
        );
        assert_matches!(
            t1_message.get_field_by_name("v2").unwrap().kind(),
            prost_reflect::Kind::Double
        );
        assert_matches!(
            t1_message.get_field_by_name("v3").unwrap().kind(),
            prost_reflect::Kind::Message(_)
        );

        let v3_message = pool.get_message_by_name("t1.Structv3").unwrap();
        assert_matches!(
            v3_message.get_field_by_name("v1").unwrap().kind(),
            prost_reflect::Kind::Int64
        );
        assert!(v3_message.get_field_by_name("v1").unwrap().is_list());

        let v3_v3_message = pool.get_message_by_name("t1.Structv3.Structv3").unwrap();
        assert_matches!(
            v3_v3_message.get_field_by_name("v1").unwrap().kind(),
            prost_reflect::Kind::Int64
        );
        assert_matches!(
            v3_v3_message.get_field_by_name("v2").unwrap().kind(),
            prost_reflect::Kind::Int64
        );
    }

    #[test]
    fn test_decimal_type_family_compatibility() {
        assert!(BigQuerySink::is_decimal_type_compatible("NUMERIC"));
        assert!(BigQuerySink::is_decimal_type_compatible("numeric(31, 2)"));
        assert!(BigQuerySink::is_decimal_type_compatible("BIGNUMERIC"));
        assert!(BigQuerySink::is_decimal_type_compatible(
            "bignumeric(35, 12)"
        ));
        assert!(!BigQuerySink::is_decimal_type_compatible("STRING"));
    }

    #[test]
    fn test_decimal_schema_check_accepts_parameterized_numeric_types() {
        let sink = BigQuerySink {
            config: BigQueryConfig {
                common: BigQueryCommon {
                    local_path: None,
                    s3_path: None,
                    project: "project".to_owned(),
                    dataset: "dataset".to_owned(),
                    table: "table".to_owned(),
                    auto_create: false,
                    credentials: None,
                },
                aws_auth_props: AwsAuthProps {
                    region: None,
                    endpoint: None,
                    access_key: None,
                    secret_key: None,
                    session_token: None,
                    arn: None,
                    external_id: None,
                    profile: None,
                    msk_signer_timeout_sec: None,
                },
                r#type: "append-only".to_owned(),
                unknown_fields: Default::default(),
            },
            schema: Schema {
                fields: vec![Field::with_name(DataType::Decimal, "capitalizedcost")],
            },
            pk_indices: vec![],
            is_append_only: true,
        };

        sink.check_column_name_and_type(HashMap::from([(
            "capitalizedcost".to_owned(),
            "NUMERIC(31, 2)".to_owned(),
        )]))
        .unwrap();

        sink.check_column_name_and_type(HashMap::from([(
            "capitalizedcost".to_owned(),
            "BIGNUMERIC(35, 12)".to_owned(),
        )]))
        .unwrap();
    }
}
