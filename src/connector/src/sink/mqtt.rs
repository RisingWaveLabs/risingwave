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

use core::fmt::Debug;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context as _, anyhow};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::id::ActorId;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use rumqttc::v5::ConnectionError;
use rumqttc::v5::mqttbytes::QoS;
use serde::Deserialize;
use serde_with::serde_as;
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::SinkWriterParam;
use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc, SinkId};
use super::encoder::{
    DateHandlingMode, JsonEncoder, JsonbHandlingMode, ProtoEncoder, ProtoHeader, RowEncoder, SerTo,
    TimeHandlingMode, TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::writer::AsyncTruncateSinkWriterExt;
use crate::connector_common::MqttCommon;
use crate::deserialize_bool_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter};
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam};

pub const MQTT_SINK: &str = "mqtt";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MqttConfig {
    #[serde(flatten)]
    pub common: MqttCommon,

    /// The topic name to subscribe or publish to. When subscribing, it can be a wildcard topic. e.g /topic/#
    pub topic: Option<String>,

    /// Whether the message should be retained by the broker
    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub retain: bool,

    // accept "append-only"
    pub r#type: String,

    // if set, will use a field value as the topic name, if topic is also set it will be used as a fallback
    #[serde(rename = "topic.field")]
    pub topic_field: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(MqttConfig);

impl EnforceSecret for MqttConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        MqttCommon::enforce_one(prop)
    }
}

pub enum RowEncoderWrapper {
    Json(JsonEncoder),
    Proto(ProtoEncoder),
}

impl RowEncoder for RowEncoderWrapper {
    type Output = Vec<u8>;

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        match self {
            RowEncoderWrapper::Json(json) => json.encode_cols(row, col_indices)?.ser_to(),
            RowEncoderWrapper::Proto(proto) => proto.encode_cols(row, col_indices)?.ser_to(),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            RowEncoderWrapper::Json(json) => json.schema(),
            RowEncoderWrapper::Proto(proto) => proto.schema(),
        }
    }

    fn col_indices(&self) -> Option<&[usize]> {
        match self {
            RowEncoderWrapper::Json(json) => json.col_indices(),
            RowEncoderWrapper::Proto(proto) => proto.col_indices(),
        }
    }

    fn encode(&self, row: impl Row) -> Result<Self::Output> {
        match self {
            RowEncoderWrapper::Json(json) => json.encode(row)?.ser_to(),
            RowEncoderWrapper::Proto(proto) => proto.encode(row)?.ser_to(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MqttSink {
    pub config: MqttConfig,
    schema: Schema,
    format_desc: SinkFormatDesc,
    is_append_only: bool,
    name: String,
}

impl EnforceSecret for MqttSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            MqttConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

// sink write
pub struct MqttSinkWriter {
    pub config: MqttConfig,
    payload_writer: MqttSinkPayloadWriter,
    #[expect(dead_code)]
    schema: Schema,
    encoder: RowEncoderWrapper,
    stopped: Arc<AtomicBool>,
}

/// Basic data types for use with the mqtt interface
impl MqttConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<MqttConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY {
            Err(SinkError::Config(anyhow!(
                "MQTT sink only supports append-only mode"
            )))
        } else {
            Ok(config)
        }
    }
}

impl TryFrom<SinkParam> for MqttSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = MqttConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            name: param.sink_name,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for MqttSink {
    type LogSinker = AsyncTruncateLogSinkerOf<MqttSinkWriter>;

    const SINK_NAME: &'static str = MQTT_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Mqtt(anyhow!(
                "MQTT sink only supports append-only mode"
            )));
        }

        if let Some(field) = &self.config.topic_field {
            let _ = get_topic_field_index_path(&self.schema, field.as_str())?;
        } else if self.config.topic.is_none() {
            return Err(SinkError::Config(anyhow!(
                "either topic or topic.field must be set"
            )));
        }

        let _client = (self.config.common.build_client(0.into(), 0))
            .context("validate mqtt sink error")
            .map_err(SinkError::Mqtt)?;

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(MqttSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            &self.format_desc,
            &self.name,
            writer_param.sink_id,
            writer_param.actor_id,
        )
        .await?
        .into_log_sinker(usize::MAX))
    }
}

impl MqttSinkWriter {
    pub async fn new(
        config: MqttConfig,
        schema: Schema,
        format_desc: &SinkFormatDesc,
        name: &str,
        sink_id: SinkId,
        actor_id: ActorId,
    ) -> Result<Self> {
        let mut topic_index_path = vec![];
        if let Some(field) = &config.topic_field {
            topic_index_path = get_topic_field_index_path(&schema, field.as_str())?;
        }

        let timestamptz_mode = TimestamptzHandlingMode::from_options(&format_desc.options)?;
        let jsonb_handling_mode = JsonbHandlingMode::from_options(&format_desc.options)?;
        let encoder = match format_desc.format {
            SinkFormat::AppendOnly => match format_desc.encode {
                SinkEncode::Json => RowEncoderWrapper::Json(JsonEncoder::new(
                    schema.clone(),
                    None,
                    DateHandlingMode::FromCe,
                    TimestampHandlingMode::Milli,
                    timestamptz_mode,
                    TimeHandlingMode::Milli,
                    jsonb_handling_mode,
                )),
                SinkEncode::Protobuf => {
                    let (descriptor, sid) = crate::schema::protobuf::fetch_descriptor(
                        &format_desc.options,
                        config.topic.as_deref().unwrap_or(name),
                        None,
                    )
                    .await
                    .map_err(|e| SinkError::Config(anyhow!(e)))?;
                    let header = match sid {
                        None => ProtoHeader::None,
                        Some(sid) => ProtoHeader::ConfluentSchemaRegistry(sid),
                    };
                    RowEncoderWrapper::Proto(ProtoEncoder::new(
                        schema.clone(),
                        None,
                        descriptor,
                        header,
                    )?)
                }
                _ => {
                    return Err(SinkError::Config(anyhow!(
                        "mqtt sink encode unsupported: {:?}",
                        format_desc.encode,
                    )));
                }
            },
            _ => {
                return Err(SinkError::Config(anyhow!(
                    "MQTT sink only supports append-only mode"
                )));
            }
        };
        let qos = config.common.qos();

        let (client, mut eventloop) = config
            .common
            .build_client(actor_id, sink_id.as_raw_id())
            .map_err(|e| SinkError::Mqtt(anyhow!(e)))?;

        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();
        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match eventloop.poll().await {
                    Ok(_) => (),
                    Err(err) => match err {
                        ConnectionError::Timeout(_) => (),
                        ConnectionError::MqttState(rumqttc::v5::StateError::Io(err))
                        | ConnectionError::Io(err)
                            if err.kind() == std::io::ErrorKind::ConnectionAborted
                                || err.kind() == std::io::ErrorKind::ConnectionReset =>
                        {
                            continue;
                        }
                        err => {
                            tracing::error!(
                                "failed to poll the MQTT event loop: {}",
                                err.as_report()
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    },
                }
            }
        });

        let payload_writer = MqttSinkPayloadWriter {
            topic: config.topic.clone(),
            client,
            qos,
            retain: config.retain,
            topic_index_path,
        };

        Ok::<_, SinkError>(Self {
            config: config.clone(),
            payload_writer,
            schema: schema.clone(),
            stopped,
            encoder,
        })
    }
}

impl AsyncTruncateSinkWriter for MqttSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.payload_writer.write_chunk(chunk, &self.encoder).await
    }
}

impl Drop for MqttSinkWriter {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

struct MqttSinkPayloadWriter {
    // connection to mqtt, one per executor
    client: rumqttc::v5::AsyncClient,
    topic: Option<String>,
    qos: QoS,
    retain: bool,
    topic_index_path: Vec<usize>,
}

impl MqttSinkPayloadWriter {
    async fn write_chunk(&mut self, chunk: StreamChunk, encoder: &RowEncoderWrapper) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let topic = match get_topic_from_index_path(
                &self.topic_index_path,
                self.topic.as_deref(),
                &row,
            ) {
                Some(s) => s,
                None => {
                    tracing::error!("topic field not found in row; skipping row: {:?}", row);
                    return Ok(());
                }
            };

            let v = encoder.encode(row)?;

            self.client
                .publish(topic, self.qos, self.retain, v)
                .await
                .context("mqtt sink error")
                .map_err(SinkError::Mqtt)?;
        }

        Ok(())
    }
}

fn get_topic_from_index_path<'s>(
    path: &[usize],
    default_topic: Option<&'s str>,
    row: &'s RowRef<'s>,
) -> Option<&'s str> {
    if let Some(topic) = default_topic
        && path.is_empty()
    {
        Some(topic)
    } else {
        let mut iter = path.iter();
        let scalar = iter
            .next()
            .and_then(|pos| row.datum_at(*pos))
            .and_then(|d| {
                iter.try_fold(d, |d, pos| match d {
                    ScalarRefImpl::Struct(struct_ref) => {
                        struct_ref.iter_fields_ref().nth(*pos).flatten()
                    }
                    _ => None,
                })
            });
        match scalar {
            Some(ScalarRefImpl::Utf8(s)) => Some(s),
            _ => {
                if let Some(topic) = default_topic {
                    Some(topic)
                } else {
                    None
                }
            }
        }
    }
}

// This function returns the index path to the topic field in the schema, validating that the field exists and is of type string
// the returnent path can be used to extract the topic field from a row. The path is a list of indexes to be used to navigate the row
// to the topic field.
fn get_topic_field_index_path(schema: &Schema, topic_field: &str) -> Result<Vec<usize>> {
    let mut iter = topic_field.split('.');
    let mut path = vec![];
    let dt =
        iter.next()
            .and_then(|field| {
                // Extract the field from the schema
                schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name == field)
                    .map(|(pos, f)| {
                        path.push(pos);
                        &f.data_type
                    })
            })
            .and_then(|dt| {
                // Iterate over the next fields to extract the fields from the nested structs
                iter.try_fold(dt, |dt, field| match dt {
                    DataType::Struct(st) => {
                        st.iter().enumerate().find(|(_, (s, _))| *s == field).map(
                            |(pos, (_, dt))| {
                                path.push(pos);
                                dt
                            },
                        )
                    }
                    _ => None,
                })
            });

    match dt {
        Some(DataType::Varchar) => Ok(path),
        Some(dt) => Err(SinkError::Config(anyhow!(
            "topic field `{}` must be of type string but got {:?}",
            topic_field,
            dt
        ))),
        None => Err(SinkError::Config(anyhow!(
            "topic field `{}`  not found",
            topic_field
        ))),
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::array::{DataChunk, DataChunkTestExt, RowRef};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, StructType};

    use super::{get_topic_field_index_path, get_topic_from_index_path};

    #[test]
    fn test_single_field_extraction() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "topic")]);
        let path = get_topic_field_index_path(&schema, "topic").unwrap();
        assert_eq!(path, vec![0]);

        let chunk = DataChunk::from_pretty(
            "T
            test",
        );

        let row = RowRef::new(&chunk, 0);

        assert_eq!(get_topic_from_index_path(&path, None, &row), Some("test"));

        let result = get_topic_field_index_path(&schema, "other_field");
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_field_extraction() {
        let schema = Schema::new(vec![Field::with_name(
            DataType::Struct(StructType::new(vec![
                ("field", DataType::Int32),
                ("subtopic", DataType::Varchar),
            ])),
            "topic",
        )]);
        let path = get_topic_field_index_path(&schema, "topic.subtopic").unwrap();
        assert_eq!(path, vec![0, 1]);

        let chunk = DataChunk::from_pretty(
            "<i,T>
            (1,test)",
        );

        let row = RowRef::new(&chunk, 0);

        assert_eq!(get_topic_from_index_path(&path, None, &row), Some("test"));

        let result = get_topic_field_index_path(&schema, "topic.other_field");
        assert!(result.is_err());
    }

    #[test]
    fn test_null_values_extraction() {
        let path = vec![0];
        let chunk = DataChunk::from_pretty(
            "T
            .",
        );
        let row = RowRef::new(&chunk, 0);
        assert_eq!(
            get_topic_from_index_path(&path, Some("default"), &row),
            Some("default")
        );
        assert_eq!(get_topic_from_index_path(&path, None, &row), None);

        let path = vec![0, 1];
        let chunk = DataChunk::from_pretty(
            "<i,T>
            (1,)",
        );
        let row = RowRef::new(&chunk, 0);
        assert_eq!(
            get_topic_from_index_path(&path, Some("default"), &row),
            Some("default")
        );
        assert_eq!(get_topic_from_index_path(&path, None, &row), None);
    }

    #[test]
    fn test_multiple_levels() {
        let schema = Schema::new(vec![
            Field::with_name(
                DataType::Struct(StructType::new(vec![
                    ("field", DataType::Int32),
                    (
                        "subtopic",
                        DataType::Struct(StructType::new(vec![
                            ("int_field", DataType::Int32),
                            ("boolean_field", DataType::Boolean),
                            ("string_field", DataType::Varchar),
                        ])),
                    ),
                ])),
                "topic",
            ),
            Field::with_name(DataType::Varchar, "other_field"),
        ]);

        let path = get_topic_field_index_path(&schema, "topic.subtopic.string_field").unwrap();
        assert_eq!(path, vec![0, 1, 2]);

        assert!(get_topic_field_index_path(&schema, "topic.subtopic.boolean_field").is_err());

        assert!(get_topic_field_index_path(&schema, "topic.subtopic.int_field").is_err());

        assert!(get_topic_field_index_path(&schema, "topic.field").is_err());

        let path = get_topic_field_index_path(&schema, "other_field").unwrap();
        assert_eq!(path, vec![1]);

        let chunk = DataChunk::from_pretty(
            "<i,<T>> T
            (1,(test)) other",
        );

        let row = RowRef::new(&chunk, 0);

        // topic.subtopic.string_field
        assert_eq!(
            get_topic_from_index_path(&[0, 1, 0], None, &row),
            Some("test")
        );

        // topic.field
        assert_eq!(get_topic_from_index_path(&[0, 0], None, &row), None);

        // other_field
        assert_eq!(get_topic_from_index_path(&[1], None, &row), Some("other"));
    }
}
