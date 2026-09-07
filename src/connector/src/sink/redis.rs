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

use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use async_trait::async_trait;
use phf::phf_set;
use redis::aio::MultiplexedConnection;
use redis::cluster::{ClusterClient, ClusterConnection, ClusterPipeline};
use redis::{Client as RedisClient, Pipeline};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;
use with_options::WithOptions;

use super::catalog::SinkFormatDesc;
use super::encoder::template::{RedisSinkPayloadWriterInput, TemplateStringEncoder};
use super::formatter::SinkFormatterImpl;
use super::writer::FormattedSink;
use super::{SinkError, SinkParam};
use crate::dispatch_sink_formatter_str_key_impl;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, Sink, SinkWriterParam};

pub const REDIS_SINK: &str = "redis";
pub const KEY_FORMAT: &str = "key_format";
pub const VALUE_FORMAT: &str = "value_format";
pub const REDIS_VALUE_TYPE: &str = "redis_value_type";
pub const REDIS_VALUE_TYPE_STRING: &str = "string";
pub const REDIS_VALUE_TYPE_GEO: &str = "geospatial";
pub const REDIS_VALUE_TYPE_PUBSUB: &str = "pubsub";
pub const REDIS_VALUE_TYPE_STREAM: &str = "stream";
pub const LON_NAME: &str = "longitude";
pub const LAT_NAME: &str = "latitude";
pub const MEMBER_NAME: &str = "member";
pub const CHANNEL: &str = "channel";
pub const CHANNEL_COLUMN: &str = "channel_column";
pub const STREAM: &str = "stream";
pub const STREAM_COLUMN: &str = "stream_column";

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct RedisCommon {
    #[serde(rename = "redis.url")]
    pub url: String,
}

impl EnforceSecret for RedisCommon {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf_set! {
        "redis.url"
    };
}

pub enum RedisPipe {
    Cluster(ClusterPipeline),
    Single(Pipeline),
}
impl RedisPipe {
    pub async fn query<T: redis::FromRedisValue>(
        &self,
        conn: &mut RedisConn,
    ) -> ConnectorResult<T> {
        match (self, conn) {
            (RedisPipe::Cluster(pipe), RedisConn::Cluster(conn)) => Ok(pipe.query(conn)?),
            (RedisPipe::Single(pipe), RedisConn::Single(conn)) => {
                Ok(pipe.query_async(conn).await?)
            }
            _ => Err(SinkError::Redis("RedisPipe and RedisConn not match".to_owned()).into()),
        }
    }

    pub fn clear(&mut self) {
        match self {
            RedisPipe::Cluster(pipe) => pipe.clear(),
            RedisPipe::Single(pipe) => pipe.clear(),
        }
    }

    pub fn set(
        &mut self,
        k: RedisSinkPayloadWriterInput,
        v: RedisSinkPayloadWriterInput,
    ) -> Result<()> {
        match self {
            RedisPipe::Cluster(pipe) => match (k, v) {
                (
                    RedisSinkPayloadWriterInput::String(k),
                    RedisSinkPayloadWriterInput::String(v),
                ) => {
                    pipe.set(k, v);
                }
                (
                    RedisSinkPayloadWriterInput::RedisGeoKey((key, member)),
                    RedisSinkPayloadWriterInput::RedisGeoValue((lat, lon)),
                ) => {
                    pipe.geo_add(key, (lon, lat, member));
                }
                (
                    RedisSinkPayloadWriterInput::RedisPubSubStreamKey(key),
                    RedisSinkPayloadWriterInput::String(v),
                ) => {
                    pipe.publish(key, v);
                }
                (
                    RedisSinkPayloadWriterInput::RedisPubSubStreamKey(key),
                    RedisSinkPayloadWriterInput::RedisStreamValue((field, value)),
                ) => {
                    pipe.xadd(key, "*", &[(&field, &value)]);
                }
                _ => return Err(SinkError::Redis("RedisPipe set not match".to_owned())),
            },
            RedisPipe::Single(pipe) => match (k, v) {
                (
                    RedisSinkPayloadWriterInput::String(k),
                    RedisSinkPayloadWriterInput::String(v),
                ) => {
                    pipe.set(k, v);
                }
                (
                    RedisSinkPayloadWriterInput::RedisGeoKey((key, member)),
                    RedisSinkPayloadWriterInput::RedisGeoValue((lat, lon)),
                ) => {
                    pipe.geo_add(key, (lon, lat, member));
                }
                (
                    RedisSinkPayloadWriterInput::RedisPubSubStreamKey(key),
                    RedisSinkPayloadWriterInput::String(v),
                ) => {
                    pipe.publish(key, v);
                }
                (
                    RedisSinkPayloadWriterInput::RedisPubSubStreamKey(key),
                    RedisSinkPayloadWriterInput::RedisStreamValue((field, value)),
                ) => {
                    pipe.xadd(key, "*", &[(&field, &value)]);
                }
                _ => return Err(SinkError::Redis("RedisPipe set not match".to_owned())),
            },
        };
        Ok(())
    }

    pub fn del(&mut self, k: RedisSinkPayloadWriterInput) -> Result<()> {
        match self {
            RedisPipe::Cluster(pipe) => match k {
                RedisSinkPayloadWriterInput::String(k) => {
                    pipe.del(k);
                }
                RedisSinkPayloadWriterInput::RedisGeoKey((key, member)) => {
                    pipe.zrem(key, member);
                }
                _ => return Err(SinkError::Redis("RedisPipe del not match".to_owned())),
            },
            RedisPipe::Single(pipe) => match k {
                RedisSinkPayloadWriterInput::String(k) => {
                    pipe.del(k);
                }
                RedisSinkPayloadWriterInput::RedisGeoKey((key, member)) => {
                    pipe.zrem(key, member);
                }
                _ => return Err(SinkError::Redis("RedisPipe del not match".to_owned())),
            },
        };
        Ok(())
    }
}
pub enum RedisConn {
    // Redis deployed as a cluster, clusters with only one node should also use this conn
    Cluster(ClusterConnection),
    // Redis is not deployed as a cluster
    Single(MultiplexedConnection),
}

impl RedisCommon {
    pub async fn build_conn_and_pipe(&self) -> ConnectorResult<(RedisConn, RedisPipe)> {
        match serde_json::from_str(&self.url).map_err(|e| SinkError::Config(anyhow!(e))) {
            Ok(v) => {
                if let Value::Array(list) = v {
                    let list = list
                        .into_iter()
                        .map(|s| {
                            if let Value::String(s) = s {
                                Ok(s)
                            } else {
                                Err(SinkError::Redis(
                                    "redis.url must be array of string".to_owned(),
                                )
                                .into())
                            }
                        })
                        .collect::<ConnectorResult<Vec<String>>>()?;

                    let client = ClusterClient::new(list)?;
                    Ok((
                        RedisConn::Cluster(client.get_connection()?),
                        RedisPipe::Cluster(redis::cluster::cluster_pipe()),
                    ))
                } else {
                    Err(SinkError::Redis("redis.url must be array or string".to_owned()).into())
                }
            }
            Err(_) => {
                let client = RedisClient::open(self.url.clone())?;
                Ok((
                    RedisConn::Single(client.get_multiplexed_async_connection().await?),
                    RedisPipe::Single(redis::pipe()),
                ))
            }
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct RedisConfig {
    #[serde(flatten)]
    pub common: RedisCommon,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(RedisConfig);

impl EnforceSecret for RedisConfig {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            RedisCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl RedisConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<RedisConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct RedisSink {
    config: RedisConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl EnforceSecret for RedisSink {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            RedisConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

#[async_trait]
impl TryFrom<SinkParam> for RedisSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let Some(pk_indices) = param.downstream_pk.clone() else {
            return Err(SinkError::Config(anyhow!(
                "Redis Sink Primary Key must be specified."
            )));
        };
        let config = RedisConfig::from_btreemap(param.properties.clone())?;
        Ok(Self {
            config,
            schema: param.schema(),
            pk_indices,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

impl Sink for RedisSink {
    type LogSinker = AsyncTruncateLogSinkerOf<RedisSinkWriter>;

    const SINK_NAME: &'static str = "redis";

    crate::impl_validate_sink_unknown_fields!();

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(RedisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(usize::MAX))
    }

    async fn validate(&self) -> Result<()> {
        let all_map: HashMap<String, DataType> = self
            .schema
            .fields()
            .iter()
            .map(|f| (f.name.clone(), f.data_type.clone()))
            .collect();
        let pk_map: HashMap<String, DataType> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(k, _)| self.pk_indices.contains(k))
            .map(|(_, v)| (v.name.clone(), v.data_type.clone()))
            .collect();
        if matches!(
            self.format_desc.encode,
            super::catalog::SinkEncode::Template
        ) {
            match self
                .format_desc
                .options
                .get(REDIS_VALUE_TYPE)
                .map(|s| s.as_str())
            {
                // if not set, default to string
                Some(REDIS_VALUE_TYPE_STRING) | None => {
                    let key_format = self.format_desc.options.get(KEY_FORMAT).ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Cannot find '{KEY_FORMAT}', please set it or use JSON"
                        ))
                    })?;
                    TemplateStringEncoder::check_string_format(key_format, &pk_map)?;
                    let value_format =
                        self.format_desc.options.get(VALUE_FORMAT).ok_or_else(|| {
                            SinkError::Config(anyhow!(
                                "Cannot find `{VALUE_FORMAT}`, please set it or use JSON"
                            ))
                        })?;
                    TemplateStringEncoder::check_string_format(value_format, &all_map)?;
                }
                Some(REDIS_VALUE_TYPE_GEO) => {
                    let key_format = self.format_desc.options.get(KEY_FORMAT).ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Cannot find '{KEY_FORMAT}', please set it or use JSON"
                        ))
                    })?;
                    TemplateStringEncoder::check_string_format(key_format, &pk_map)?;

                    let lon_name = self.format_desc.options.get(LON_NAME).ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Cannot find `{LON_NAME}`, please set it or use JSON or set `{REDIS_VALUE_TYPE}` to `{REDIS_VALUE_TYPE_STRING}`"
                        ))
                    })?;
                    let lat_name = self.format_desc.options.get(LAT_NAME).ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Cannot find `{LAT_NAME}`, please set it or use JSON or set `{REDIS_VALUE_TYPE}` to `{REDIS_VALUE_TYPE_STRING}`"
                        ))
                    })?;
                    let member_name = self.format_desc.options.get(MEMBER_NAME).ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Cannot find `{MEMBER_NAME}`, please set it or use JSON or set `{REDIS_VALUE_TYPE}` to `{REDIS_VALUE_TYPE_STRING}`"
                        ))
                    })?;
                    if let Some(lon_type) = all_map.get(lon_name)
                        && (lon_type == &DataType::Float64
                            || lon_type == &DataType::Float32
                            || lon_type == &DataType::Varchar)
                    {
                        // do nothing
                    } else {
                        return Err(SinkError::Config(anyhow!(
                            "`{LON_NAME}` must be set to `float64` or `float32` or `varchar`"
                        )));
                    }
                    if let Some(lat_type) = all_map.get(lat_name)
                        && (lat_type == &DataType::Float64
                            || lat_type == &DataType::Float32
                            || lat_type == &DataType::Varchar)
                    {
                        // do nothing
                    } else {
                        return Err(SinkError::Config(anyhow!(
                            "`{LAT_NAME}` must be set to `float64` or `float32` or `varchar`"
                        )));
                    }
                    if let Some(member_type) = pk_map.get(member_name)
                        && member_type == &DataType::Varchar
                    {
                        // do nothing
                    } else {
                        return Err(SinkError::Config(anyhow!(
                            "`{MEMBER_NAME}` must be set to `varchar` and `primary_key`"
                        )));
                    }
                }
                Some(REDIS_VALUE_TYPE_PUBSUB) => {
                    let channel = self.format_desc.options.get(CHANNEL);
                    let channel_column = self.format_desc.options.get(CHANNEL_COLUMN);
                    if (channel.is_none() && channel_column.is_none())
                        || (channel.is_some() && channel_column.is_some())
                    {
                        return Err(SinkError::Config(anyhow!(
                            "`{CHANNEL}` and `{CHANNEL_COLUMN}` only one can be set"
                        )));
                    }

                    if let Some(channel_column) = channel_column
                        && let Some(channel_column_type) = all_map.get(channel_column)
                        && (channel_column_type != &DataType::Varchar)
                    {
                        return Err(SinkError::Config(anyhow!(
                            "`{CHANNEL_COLUMN}` must be set to `varchar`"
                        )));
                    }

                    let value_format =
                        self.format_desc.options.get(VALUE_FORMAT).ok_or_else(|| {
                            SinkError::Config(anyhow!("Cannot find `{VALUE_FORMAT}`"))
                        })?;
                    TemplateStringEncoder::check_string_format(value_format, &all_map)?;
                }
                Some(REDIS_VALUE_TYPE_STREAM) => {
                    tracing::error!("test:for bug");
                    risingwave_common::license::Feature::RedisSinkStream
                        .check_available()
                        .map_err(|e| anyhow::anyhow!(e))?;
                    let stream = self.format_desc.options.get(STREAM);
                    let stream_column = self.format_desc.options.get(STREAM_COLUMN);
                    if (stream.is_none() && stream_column.is_none())
                        || (stream.is_some() && stream_column.is_some())
                    {
                        return Err(SinkError::Config(anyhow!(
                            "Please specific either `{STREAM}` or `{STREAM_COLUMN}`. They are mutually exclusive options."
                        )));
                    }

                    if let Some(stream_column) = stream_column
                        && let Some(stream_column_type) = all_map.get(stream_column)
                        && (stream_column_type != &DataType::Varchar)
                    {
                        return Err(SinkError::Config(anyhow!(
                            "`{STREAM_COLUMN}` must be set to `varchar`"
                        )));
                    }

                    let value_format =
                        self.format_desc.options.get(VALUE_FORMAT).ok_or_else(|| {
                            SinkError::Config(anyhow!("Cannot find `{VALUE_FORMAT}`"))
                        })?;
                    let key_format = self.format_desc.options.get(KEY_FORMAT).ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Cannot find '{KEY_FORMAT}', please set it or use JSON"
                        ))
                    })?;
                    TemplateStringEncoder::check_string_format(key_format, &pk_map)?;
                    TemplateStringEncoder::check_string_format(value_format, &all_map)?;
                }
                _ => {
                    return Err(SinkError::Config(anyhow!(
                        "`{REDIS_VALUE_TYPE}` must be set to `{REDIS_VALUE_TYPE_STRING}` or `{REDIS_VALUE_TYPE_GEO}` or `{REDIS_VALUE_TYPE_PUBSUB}` or `{REDIS_VALUE_TYPE_STREAM}`"
                    )));
                }
            }
        }
        self.config.common.build_conn_and_pipe().await?;
        Ok(())
    }
}

pub struct RedisSinkWriter {
    #[expect(dead_code)]
    epoch: u64,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    formatter: SinkFormatterImpl,
    payload_writer: RedisSinkPayloadWriter,
}

struct RedisSinkPayloadWriter {
    // connection to redis, one per executor
    conn: Option<RedisConn>,
    // the command pipeline for write-commit
    pipe: RedisPipe,
}

impl RedisSinkPayloadWriter {
    pub async fn new(config: RedisConfig) -> Result<Self> {
        let (conn, pipe) = config.common.build_conn_and_pipe().await?;
        let conn = Some(conn);

        Ok(Self { conn, pipe })
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        let conn = None;
        let pipe = RedisPipe::Single(redis::pipe());
        Self { conn, pipe }
    }

    pub async fn commit(&mut self) -> Result<()> {
        #[cfg(test)]
        {
            if self.conn.is_none() {
                return Ok(());
            }
        }
        self.pipe.query::<()>(self.conn.as_mut().unwrap()).await?;
        self.pipe.clear();
        Ok(())
    }
}

impl FormattedSink for RedisSinkPayloadWriter {
    type K = RedisSinkPayloadWriterInput;
    type V = RedisSinkPayloadWriterInput;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        let k = k.ok_or_else(|| SinkError::Redis("The redis key cannot be null".to_owned()))?;
        match v {
            Some(v) => self.pipe.set(k, v)?,
            None => self.pipe.del(k)?,
        };
        Ok(())
    }
}

impl RedisSinkWriter {
    pub async fn new(
        config: RedisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let payload_writer = RedisSinkPayloadWriter::new(config.clone()).await?;
        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema.clone(),
            pk_indices.clone(),
            db_name,
            sink_from_name,
            "NO_TOPIC",
        )
        .await?;

        Ok(Self {
            schema,
            pk_indices,
            epoch: 0,
            formatter,
            payload_writer,
        })
    }

    #[cfg(test)]
    pub async fn mock(
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
    ) -> Result<Self> {
        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema.clone(),
            pk_indices.clone(),
            "d1".to_owned(),
            "t1".to_owned(),
            "NO_TOPIC",
        )
        .await?;
        Ok(Self {
            schema,
            pk_indices,
            epoch: 0,
            formatter,
            payload_writer: RedisSinkPayloadWriter::mock(),
        })
    }
}

impl AsyncTruncateSinkWriter for RedisSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        dispatch_sink_formatter_str_key_impl!(&self.formatter, formatter, {
            self.payload_writer.write_chunk(chunk, formatter).await?;
            self.payload_writer.commit().await
        })
    }
}

#[cfg(test)]
mod test {
    use core::panic;

    use rdkafka::message::FromBytes;
    use risingwave_common::array::{Array, I32Array, Op, Utf8Array};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use super::*;
    use crate::sink::catalog::{SinkEncode, SinkFormat};
    use crate::sink::log_store::DeliveryFutureManager;

    #[tokio::test]
    async fn test_write() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_owned(),
            },
        ]);

        let format_desc = SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Json,
            options: BTreeMap::default(),
            secret_refs: BTreeMap::default(),
            key_encode: None,
            connection_id: None,
        };

        let mut redis_sink_writer = RedisSinkWriter::mock(schema, vec![0], &format_desc)
            .await
            .unwrap();

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );

        let mut manager = DeliveryFutureManager::new(0);

        redis_sink_writer
            .write_chunk(chunk_a, manager.start_write_chunk(0, 0))
            .await
            .expect("failed to write batch");
        let expected_a = vec![
            (
                0,
                "*3\r\n$3\r\nSET\r\n$8\r\n{\"id\":1}\r\n$23\r\n{\"id\":1,\"name\":\"Alice\"}\r\n",
            ),
            (
                1,
                "*3\r\n$3\r\nSET\r\n$8\r\n{\"id\":2}\r\n$21\r\n{\"id\":2,\"name\":\"Bob\"}\r\n",
            ),
            (
                2,
                "*3\r\n$3\r\nSET\r\n$8\r\n{\"id\":3}\r\n$23\r\n{\"id\":3,\"name\":\"Clare\"}\r\n",
            ),
        ];

        if let RedisPipe::Single(pipe) = &redis_sink_writer.payload_writer.pipe {
            pipe.cmd_iter()
                .enumerate()
                .zip_eq_debug(expected_a.clone())
                .for_each(|((i, cmd), (exp_i, exp_cmd))| {
                    if exp_i == i {
                        assert_eq!(exp_cmd, str::from_bytes(&cmd.get_packed_command()).unwrap())
                    }
                });
        } else {
            panic!("pipe type not match")
        }
    }

    #[tokio::test]
    async fn test_format_write() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_owned(),
            },
        ]);

        let mut btree_map = BTreeMap::default();
        btree_map.insert(KEY_FORMAT.to_owned(), "key-{id}".to_owned());
        btree_map.insert(
            VALUE_FORMAT.to_owned(),
            "values:\\{id:{id},name:{name}\\}".to_owned(),
        );
        let format_desc = SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Template,
            options: btree_map,
            secret_refs: Default::default(),
            key_encode: None,
            connection_id: None,
        };

        let mut redis_sink_writer = RedisSinkWriter::mock(schema, vec![0], &format_desc)
            .await
            .unwrap();

        let mut future_manager = DeliveryFutureManager::new(0);

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );

        redis_sink_writer
            .write_chunk(chunk_a, future_manager.start_write_chunk(0, 0))
            .await
            .expect("failed to write batch");
        let expected_a = vec![
            (
                0,
                "*3\r\n$3\r\nSET\r\n$5\r\nkey-1\r\n$24\r\nvalues:{id:1,name:Alice}\r\n",
            ),
            (
                1,
                "*3\r\n$3\r\nSET\r\n$5\r\nkey-2\r\n$22\r\nvalues:{id:2,name:Bob}\r\n",
            ),
            (
                2,
                "*3\r\n$3\r\nSET\r\n$5\r\nkey-3\r\n$24\r\nvalues:{id:3,name:Clare}\r\n",
            ),
        ];

        if let RedisPipe::Single(pipe) = &redis_sink_writer.payload_writer.pipe {
            pipe.cmd_iter()
                .enumerate()
                .zip_eq_debug(expected_a.clone())
                .for_each(|((i, cmd), (exp_i, exp_cmd))| {
                    if exp_i == i {
                        assert_eq!(exp_cmd, str::from_bytes(&cmd.get_packed_command()).unwrap())
                    }
                });
        } else {
            panic!("pipe type not match")
        };
    }
}
