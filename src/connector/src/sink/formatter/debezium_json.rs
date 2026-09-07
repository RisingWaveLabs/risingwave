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

use std::time::{SystemTime, UNIX_EPOCH};

use risingwave_common::array::Op;
use risingwave_common::catalog::{Field, Schema};
use serde_json::{Map, Value, json};
use tracing::warn;

use super::{Result, SinkFormatter, StreamChunk};
use crate::sink::encoder::{
    DateHandlingMode, JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode,
    TimestampHandlingMode, TimestamptzHandlingMode,
};
use crate::tri;

const DEBEZIUM_NAME_FIELD_PREFIX: &str = "RisingWave";

pub struct DebeziumAdapterOpts {
    gen_tombstone: bool,
}

impl Default for DebeziumAdapterOpts {
    fn default() -> Self {
        Self {
            gen_tombstone: true,
        }
    }
}

fn concat_debezium_name_field(db_name: &str, sink_from_name: &str, value: &str) -> String {
    DEBEZIUM_NAME_FIELD_PREFIX.to_owned() + "." + db_name + "." + sink_from_name + "." + value
}

pub struct DebeziumJsonFormatter {
    schema: Schema,
    pk_indices: Vec<usize>,
    db_name: String,
    sink_from_name: String,
    opts: DebeziumAdapterOpts,
    key_encoder: JsonEncoder,
    val_encoder: JsonEncoder,
}

impl DebeziumJsonFormatter {
    pub fn new(
        schema: Schema,
        pk_indices: Vec<usize>,
        db_name: String,
        sink_from_name: String,
        opts: DebeziumAdapterOpts,
    ) -> Self {
        let key_encoder = JsonEncoder::new(
            schema.clone(),
            Some(pk_indices.clone()),
            DateHandlingMode::FromEpoch,
            TimestampHandlingMode::Milli,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::Milli,
            JsonbHandlingMode::String,
        );
        let val_encoder = JsonEncoder::new(
            schema.clone(),
            None,
            DateHandlingMode::FromEpoch,
            TimestampHandlingMode::Milli,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::Milli,
            JsonbHandlingMode::String,
        );
        Self {
            schema,
            pk_indices,
            db_name,
            sink_from_name,
            opts,
            key_encoder,
            val_encoder,
        }
    }
}

impl SinkFormatter for DebeziumJsonFormatter {
    type K = Value;
    type V = Value;

    fn format_chunk(
        &self,
        chunk: &StreamChunk,
    ) -> impl Iterator<Item = Result<(Option<Value>, Option<Value>)>> {
        std::iter::from_coroutine(
            #[coroutine]
            || {
                let DebeziumJsonFormatter {
                    schema,
                    pk_indices,
                    db_name,
                    sink_from_name,
                    opts,
                    key_encoder,
                    val_encoder,
                } = self;
                let ts_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let source_field = json!({
                    // todo: still some missing fields in source field
                    // ref https://debezium.io/documentation/reference/2.4/connectors/postgresql.html#postgresql-create-events
                    "db": db_name,
                    "table": sink_from_name,
                    "ts_ms": ts_ms,
                });

                let mut update_cache: Option<Map<String, Value>> = None;

                for (op, row) in chunk.rows() {
                    let event_key_object: Option<Value> = Some(json!({
                        "schema": json!({
                            "type": "struct",
                            "fields": fields_pk_to_json(&schema.fields, pk_indices),
                            "optional": false,
                            "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
                        }),
                        "payload": tri!(key_encoder.encode(row)),
                    }));
                    let event_object: Option<Value> = match op {
                        Op::Insert => Some(json!({
                            "schema": schema_to_json(schema, db_name, sink_from_name),
                            "payload": {
                                "before": null,
                                "after": tri!(val_encoder.encode(row)),
                                "op": "c",
                                "ts_ms": ts_ms,
                                "source": source_field,
                            }
                        })),
                        Op::Delete => {
                            let value_obj = Some(json!({
                                "schema": schema_to_json(schema, db_name, sink_from_name),
                                "payload": {
                                    "before": tri!(val_encoder.encode(row)),
                                    "after": null,
                                    "op": "d",
                                    "ts_ms": ts_ms,
                                    "source": source_field,
                                }
                            }));
                            yield Ok((event_key_object.clone(), value_obj));

                            if opts.gen_tombstone {
                                // Tomestone event
                                // https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-delete-events
                                yield Ok((event_key_object, None));
                            }

                            continue;
                        }
                        Op::UpdateDelete => {
                            update_cache = Some(tri!(val_encoder.encode(row)));
                            continue;
                        }
                        Op::UpdateInsert => {
                            if let Some(before) = update_cache.take() {
                                Some(json!({
                                    "schema": schema_to_json(schema, db_name, sink_from_name),
                                    "payload": {
                                        "before": before,
                                        "after": tri!(val_encoder.encode(row)),
                                        "op": "u",
                                        "ts_ms": ts_ms,
                                        "source": source_field,
                                    }
                                }))
                            } else {
                                warn!(
                                    "not found UpdateDelete in prev row, skipping, row index {:?}",
                                    row.index()
                                );
                                continue;
                            }
                        }
                    };
                    yield Ok((event_key_object, event_object));
                }
            },
        )
    }
}

pub(crate) fn schema_to_json(schema: &Schema, db_name: &str, sink_from_name: &str) -> Value {
    let mut schema_fields = Vec::new();
    schema_fields.push(json!({
        "type": "struct",
        "fields": fields_to_json(&schema.fields),
        "optional": true,
        "field": "before",
        "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
    }));
    schema_fields.push(json!({
        "type": "struct",
        "fields": fields_to_json(&schema.fields),
        "optional": true,
        "field": "after",
        "name": concat_debezium_name_field(db_name, sink_from_name, "Key"),
    }));

    schema_fields.push(json!({
        "type": "struct",
        "optional": false,
        "name": concat_debezium_name_field(db_name, sink_from_name, "Source"),
        "fields": vec![
            json!({
                "type": "string",
                "optional": false,
                "field": "db"
            }),
            json!({
                "type": "string",
                "optional": true,
                "field": "table"
            }),
            json!({
                "type": "int64",
                "optional": false,
                "field": "ts_ms"
            }),
        ],

        "field": "source"
    }));
    schema_fields.push(json!({
        "type": "string",
        "optional": false,
        "field": "op"
    }));
    schema_fields.push(json!({
        "type": "int64",
        "optional": false,
        "field": "ts_ms"
    }));

    json!({
        "type": "struct",
        "fields": schema_fields,
        "optional": false,
        "name": concat_debezium_name_field(db_name, sink_from_name, "Envelope"),
    })
}

pub(crate) fn fields_pk_to_json(fields: &[Field], pk_indices: &[usize]) -> Value {
    let mut res = Vec::new();
    for idx in pk_indices {
        res.push(field_to_json(&fields[*idx]));
    }
    json!(res)
}

pub(crate) fn fields_to_json(fields: &[Field]) -> Value {
    let mut res = Vec::new();

    fields
        .iter()
        .for_each(|field| res.push(field_to_json(field)));

    json!(res)
}

pub(crate) fn field_to_json(field: &Field) -> Value {
    // mapping from 'https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-data-types'
    let (r#type, name) = match field.data_type() {
        risingwave_common::types::DataType::Boolean => ("boolean", ""),
        risingwave_common::types::DataType::Int16 => ("int16", ""),
        risingwave_common::types::DataType::Int32 => ("int32", ""),
        risingwave_common::types::DataType::Int64 => ("int64", ""),
        risingwave_common::types::DataType::Int256 => ("string", ""),
        risingwave_common::types::DataType::Float32 => ("float", ""),
        risingwave_common::types::DataType::Float64 => ("double", ""),
        // currently, we only support handling decimal as string.
        // https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-decimal-types
        risingwave_common::types::DataType::Decimal => ("string", ""),

        risingwave_common::types::DataType::Varchar => ("string", ""),

        // use built-in Kafka Connect logical types.
        risingwave_common::types::DataType::Date => ("int32", "org.apache.kafka.connect.data.Date"),
        // may lose precision when Time/Timestamp has a fractional second precision value that is greater than 3. But for TimestampHandlingMode:Milli, it is sufficient.
        risingwave_common::types::DataType::Time => ("int64", "org.apache.kafka.connect.data.Time"),
        risingwave_common::types::DataType::Timestamp => {
            ("int64", "org.apache.kafka.connect.data.Timestamp")
        }

        risingwave_common::types::DataType::Timestamptz => {
            ("string", "io.debezium.time.ZonedTimestamp")
        }
        risingwave_common::types::DataType::Interval => ("string", "io.debezium.time.Interval"),

        risingwave_common::types::DataType::Bytea => ("bytes", ""),
        risingwave_common::types::DataType::Jsonb => ("string", "io.debezium.data.Json"),
        risingwave_common::types::DataType::Variant => ("string", "io.debezium.data.Json"),
        risingwave_common::types::DataType::Serial => ("int32", ""),
        // since the original debezium pg support HSTORE via encoded as json string by default,
        // we do the same here
        risingwave_common::types::DataType::Struct(_) => ("string", ""),
        risingwave_common::types::DataType::List { .. } => ("string", ""),
        risingwave_common::types::DataType::Map(_) => ("string", ""),
        risingwave_common::types::DataType::Vector(_) => ("string", ""),
    };

    if name.is_empty() {
        json!({
            "field": field.name,
            "optional": true,
            "type": r#type
        })
    } else {
        json!({
            "field": field.name,
            "optional": true,
            "type": r#type,
            "name": name
        })
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, StructType};

    use super::*;
    use crate::sink::utils::chunk_to_json;

    const SCHEMA_JSON_RESULT: &str = r#"{"fields":[{"field":"before","fields":[{"field":"v1","optional":true,"type":"int32"},{"field":"v2","optional":true,"type":"float"},{"field":"v3","optional":true,"type":"string"}],"name":"RisingWave.test_db.test_table.Key","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"v1","optional":true,"type":"int32"},{"field":"v2","optional":true,"type":"float"},{"field":"v3","optional":true,"type":"string"}],"name":"RisingWave.test_db.test_table.Key","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"db","optional":false,"type":"string"},{"field":"table","optional":true,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"}],"name":"RisingWave.test_db.test_table.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"}],"name":"RisingWave.test_db.test_table.Envelope","optional":false,"type":"struct"}"#;

    #[test]
    fn test_chunk_to_json() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " i   f   <i,f>
            + 0 0.0 (0,0.0)
            + 1 1.0 (1,1.0)
            + 2 2.0 (2,2.0)
            + 3 3.0 (3,3.0)
            + 4 4.0 (4,4.0)
            + 5 5.0 (5,5.0)
            + 6 6.0 (6,6.0)
            + 7 7.0 (7,7.0)
            + 8 8.0 (8,8.0)
            + 9 9.0 (9,9.0)",
        );

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "v1".into(),
            },
            Field {
                data_type: DataType::Float32,
                name: "v2".into(),
            },
            Field {
                data_type: StructType::new(vec![
                    ("v4", DataType::Int32),
                    ("v5", DataType::Float32),
                ])
                .into(),
                name: "v3".into(),
            },
        ]);

        let encoder = JsonEncoder::new(
            schema.clone(),
            None,
            DateHandlingMode::FromEpoch,
            TimestampHandlingMode::Milli,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::Milli,
            JsonbHandlingMode::String,
        );
        let json_chunk = chunk_to_json(chunk, &encoder).unwrap();
        let schema_json = schema_to_json(&schema, "test_db", "test_table");
        assert_eq!(
            schema_json,
            serde_json::from_str::<Value>(SCHEMA_JSON_RESULT).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<Value>(&json_chunk[0]).unwrap(),
            serde_json::from_str::<Value>("{\"v1\":0,\"v2\":0.0,\"v3\":{\"v4\":0,\"v5\":0.0}}")
                .unwrap()
        );

        Ok(())
    }
}
