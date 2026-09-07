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

#[cfg(test)]
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use base64::Engine as _;
use base64::engine::general_purpose;
use chrono::{DateTime, Datelike, Timelike};
use chrono_tz::Tz;
use indexmap::IndexMap;
use itertools::Itertools;
use risingwave_common::array::{ArrayError, ArrayResult};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, JsonbVal, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde_json::{Map, Value, json};
use thiserror_ext::AsReport;

use super::{
    CustomJsonType, DateHandlingMode, DorisJsonConfig, JsonbHandlingMode, KafkaConnectParams,
    KafkaConnectParamsRef, Result, RowEncoder, SerTo, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::SinkError;

pub struct JsonEncoderConfig {
    time_handling_mode: TimeHandlingMode,
    date_handling_mode: DateHandlingMode,
    timestamp_handling_mode: TimestampHandlingMode,
    timestamptz_handling_mode: TimestamptzHandlingMode,
    custom_json_type: CustomJsonType,
    jsonb_handling_mode: JsonbHandlingMode,
}

pub struct JsonEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    kafka_connect: Option<KafkaConnectParamsRef>,
    config: JsonEncoderConfig,
}

impl JsonEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        date_handling_mode: DateHandlingMode,
        timestamp_handling_mode: TimestampHandlingMode,
        timestamptz_handling_mode: TimestamptzHandlingMode,
        time_handling_mode: TimeHandlingMode,
        jsonb_handling_mode: JsonbHandlingMode,
    ) -> Self {
        let config = JsonEncoderConfig {
            time_handling_mode,
            date_handling_mode,
            timestamp_handling_mode,
            timestamptz_handling_mode,
            custom_json_type: CustomJsonType::None,
            jsonb_handling_mode,
        };
        Self {
            schema,
            col_indices,
            kafka_connect: None,
            config,
        }
    }

    pub fn new_with_es(schema: Schema, col_indices: Option<Vec<usize>>) -> Self {
        let config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcWithoutSuffix,
            custom_json_type: CustomJsonType::Es,
            jsonb_handling_mode: JsonbHandlingMode::Dynamic,
        };
        Self {
            schema,
            col_indices,
            kafka_connect: None,
            config,
        }
    }

    pub fn new_with_doris(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        doris_config: DorisJsonConfig,
    ) -> Self {
        let config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::Milli,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcWithoutSuffix,
            custom_json_type: CustomJsonType::Doris(doris_config),
            jsonb_handling_mode: JsonbHandlingMode::String,
        };
        Self {
            schema,
            col_indices,
            kafka_connect: None,
            config,
        }
    }

    pub fn new_with_starrocks(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        time_zone: Tz,
    ) -> Self {
        let config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::Milli,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::SpecifiedTimezoneWithoutSuffix(
                time_zone,
            ),
            custom_json_type: CustomJsonType::StarRocks,
            jsonb_handling_mode: JsonbHandlingMode::Dynamic,
        };
        Self {
            schema,
            col_indices,
            kafka_connect: None,
            config,
        }
    }

    pub fn new_with_turbopuffer(schema: Schema, col_indices: Option<Vec<usize>>) -> Self {
        let config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::Iso8601String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::Turbopuffer,
            jsonb_handling_mode: JsonbHandlingMode::Dynamic,
        };
        Self {
            schema,
            col_indices,
            kafka_connect: None,
            config,
        }
    }

    pub fn with_kafka_connect(self, kafka_connect: KafkaConnectParams) -> Self {
        Self {
            kafka_connect: Some(Arc::new(kafka_connect)),
            ..self
        }
    }
}

impl JsonEncoderConfig {
    fn jsonb_handling_mode_for_field(&self, field_name: &str) -> JsonbHandlingMode {
        match &self.custom_json_type {
            CustomJsonType::Doris(config) if config.variant_columns.contains(field_name) => {
                JsonbHandlingMode::Dynamic
            }
            _ => self.jsonb_handling_mode,
        }
    }
}

impl RowEncoder for JsonEncoder {
    type Output = Map<String, Value>;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_ref().map(Vec::as_ref)
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        let mut mappings = Map::with_capacity(self.schema.len());
        let col_indices = col_indices.collect_vec();
        for idx in &col_indices {
            let field = &self.schema[*idx];
            let key = field.name.clone();
            let value = datum_to_json_object(field, row.datum_at(*idx), &self.config)
                .map_err(|e| SinkError::Encode(e.to_report_string()))?;
            mappings.insert(key, value);
        }

        Ok(if let Some(param) = &self.kafka_connect {
            json_converter_with_schema(
                Value::Object(mappings),
                param.schema_name.clone(),
                col_indices.into_iter().map(|i| &self.schema[i]),
            )
        } else {
            mappings
        })
    }
}

impl SerTo<String> for Map<String, Value> {
    fn ser_to(self) -> Result<String> {
        Value::Object(self).ser_to()
    }
}

impl SerTo<String> for Value {
    fn ser_to(self) -> Result<String> {
        Ok(self.to_string())
    }
}

fn datum_to_json_object(
    field: &Field,
    datum: DatumRef<'_>,
    config: &JsonEncoderConfig,
) -> ArrayResult<Value> {
    let scalar_ref = match datum {
        None => {
            return Ok(Value::Null);
        }
        Some(datum) => datum,
    };

    let data_type = field.data_type();

    tracing::trace!("datum_to_json_object: {:?}, {:?}", data_type, scalar_ref);

    let value = match (data_type, scalar_ref) {
        (DataType::Boolean, ScalarRefImpl::Bool(v)) => {
            json!(v)
        }
        (DataType::Int16, ScalarRefImpl::Int16(v)) => {
            json!(v)
        }
        (DataType::Int32, ScalarRefImpl::Int32(v)) => {
            json!(v)
        }
        (DataType::Int64, ScalarRefImpl::Int64(v)) => {
            json!(v)
        }
        (DataType::Serial, ScalarRefImpl::Serial(v)) => {
            if matches!(&config.custom_json_type, CustomJsonType::Turbopuffer) {
                json!(v.into_inner())
            } else {
                // The serial type needs to be handled as a string to prevent primary key conflicts caused by the precision issues of JSON numbers.
                json!(format!("{:#018x}", v.into_inner()))
            }
        }
        (DataType::Float32, ScalarRefImpl::Float32(v)) => {
            json!(f32::from(v))
        }
        (DataType::Float64, ScalarRefImpl::Float64(v)) => {
            json!(f64::from(v))
        }
        (DataType::Varchar, ScalarRefImpl::Utf8(v)) => {
            json!(v)
        }
        // Doris/Starrocks will convert out-of-bounds decimal and -INF, INF, NAN to NULL
        (DataType::Decimal, ScalarRefImpl::Decimal(mut v)) => match &config.custom_json_type {
            CustomJsonType::Doris(config) => {
                let s = config.decimal_scale.get(&field.name).unwrap();
                v.rescale(*s as u32);
                json!(v.to_text())
            }
            CustomJsonType::Turbopuffer => {
                let value = f64::try_from(v).map_err(|err| {
                    ArrayError::internal(format!(
                        "failed to convert decimal to f64: {}",
                        err.as_report()
                    ))
                })?;
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .ok_or_else(|| {
                        ArrayError::internal(
                            "failed to encode non-finite decimal as JSON number".to_owned(),
                        )
                    })?
            }
            CustomJsonType::Es | CustomJsonType::None | CustomJsonType::StarRocks => {
                json!(v.to_text())
            }
        },
        (DataType::Timestamptz, ScalarRefImpl::Timestamptz(v)) => {
            match config.timestamptz_handling_mode {
                TimestamptzHandlingMode::UtcString => {
                    let parsed = v.to_datetime_utc();
                    let v = parsed.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
                    json!(v)
                }
                TimestamptzHandlingMode::UtcWithoutSuffix => {
                    let parsed = v.to_datetime_utc().naive_utc();
                    let v = parsed.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                    json!(v)
                }
                TimestamptzHandlingMode::SpecifiedTimezoneWithoutSuffix(time_zone) => {
                    let parsed = v.to_datetime_in_zone(time_zone).naive_local();
                    let v = parsed.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                    json!(v)
                }
                TimestamptzHandlingMode::Micro => json!(v.timestamp_micros()),
                TimestamptzHandlingMode::Milli => json!(v.timestamp_millis()),
            }
        }
        (DataType::Time, ScalarRefImpl::Time(v)) => match config.time_handling_mode {
            TimeHandlingMode::Milli => {
                // todo: just ignore the nanos part to avoid leap second complex
                json!(v.0.num_seconds_from_midnight() as i64 * 1000)
            }
            TimeHandlingMode::String => {
                let a = v.0.format("%H:%M:%S%.6f").to_string();
                json!(a)
            }
        },
        (DataType::Date, ScalarRefImpl::Date(v)) => match config.date_handling_mode {
            DateHandlingMode::FromCe => json!(v.0.num_days_from_ce()),
            DateHandlingMode::FromEpoch => {
                let duration = v.0 - DateTime::UNIX_EPOCH.date_naive();
                json!(duration.num_days())
            }
            DateHandlingMode::String => {
                let a = v.0.format("%Y-%m-%d").to_string();
                json!(a)
            }
        },
        (DataType::Timestamp, ScalarRefImpl::Timestamp(v)) => {
            match config.timestamp_handling_mode {
                TimestampHandlingMode::Milli => json!(v.0.and_utc().timestamp_millis()),
                TimestampHandlingMode::String => {
                    json!(v.0.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                }
                TimestampHandlingMode::Iso8601String => {
                    json!(v.0.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
                }
            }
        }
        (DataType::Bytea, ScalarRefImpl::Bytea(v)) => {
            json!(general_purpose::STANDARD.encode(v))
        }
        // P<years>Y<months>M<days>DT<hours>H<minutes>M<seconds>S
        (DataType::Interval, ScalarRefImpl::Interval(v)) => {
            json!(v.as_iso_8601())
        }

        (DataType::Jsonb, ScalarRefImpl::Jsonb(jsonb_ref)) => {
            match config.jsonb_handling_mode_for_field(&field.name) {
                JsonbHandlingMode::String => {
                    json!(jsonb_ref.to_string())
                }
                JsonbHandlingMode::Dynamic => JsonbVal::from(jsonb_ref).take(),
            }
        }
        (DataType::List(lt), ScalarRefImpl::List(list_ref)) => {
            let elems = list_ref.iter();
            let mut vec = Vec::with_capacity(elems.len());
            let inner_field = Field::unnamed(lt.into_elem());
            for sub_datum_ref in elems {
                let value = datum_to_json_object(&inner_field, sub_datum_ref, config)?;
                vec.push(value);
            }
            json!(vec)
        }
        (DataType::Vector(_), ScalarRefImpl::Vector(vector)) => {
            let elems = vector.as_raw_slice();
            let mut vec = Vec::with_capacity(elems.len());
            for v in elems {
                let value = serde_json::Number::from_f64(*v as _)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
                vec.push(value);
            }
            json!(vec)
        }
        (DataType::Struct(st), ScalarRefImpl::Struct(struct_ref)) => {
            match config.custom_json_type {
                CustomJsonType::Doris(_) => {
                    // We need to ensure that the order of elements in the json matches the insertion order.
                    let mut map = IndexMap::with_capacity(st.len());
                    for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                        st.iter()
                            .map(|(name, dt)| Field::with_name(dt.clone(), name)),
                    ) {
                        let value = datum_to_json_object(&sub_field, sub_datum_ref, config)?;
                        map.insert(sub_field.name.clone(), value);
                    }
                    Value::String(
                        serde_json::to_string(&map).context("failed to serialize into JSON")?,
                    )
                }
                CustomJsonType::StarRocks => {
                    return Err(ArrayError::internal(
                        "starrocks can't support struct".to_owned(),
                    ));
                }
                CustomJsonType::Es | CustomJsonType::None | CustomJsonType::Turbopuffer => {
                    let mut map = Map::with_capacity(st.len());
                    for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                        st.iter()
                            .map(|(name, dt)| Field::with_name(dt.clone(), name)),
                    ) {
                        let value = datum_to_json_object(&sub_field, sub_datum_ref, config)?;
                        map.insert(sub_field.name.clone(), value);
                    }
                    json!(map)
                }
            }
        }
        // TODO(map): support map
        (data_type, scalar_ref) => {
            return Err(ArrayError::internal(format!(
                "datum_to_json_object: unsupported data type: field name: {:?}, logical type: {:?}, physical type: {:?}",
                field.name, data_type, scalar_ref
            )));
        }
    };

    Ok(value)
}

fn json_converter_with_schema<'a>(
    object: Value,
    name: String,
    fields: impl Iterator<Item = &'a Field>,
) -> Map<String, Value> {
    let mut mapping = Map::with_capacity(2);
    mapping.insert(
        "schema".to_owned(),
        json!({
            "type": "struct",
            "fields": fields.map(|field| {
                let mut mapping = type_as_json_schema(&field.data_type);
                mapping.insert("field".to_owned(), json!(field.name));
                mapping
            }).collect_vec(),
            "optional": false,
            "name": name,
        }),
    );
    mapping.insert("payload".to_owned(), object);
    mapping
}

// reference: https://github.com/apache/kafka/blob/80982c4ae3fe6be127b48ec09caff11ab5f87c69/connect/json/src/main/java/org/apache/kafka/connect/json/JsonSchema.java#L39
pub(crate) fn schema_type_mapping(rw_type: &DataType) -> &'static str {
    match rw_type {
        DataType::Boolean => "boolean",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Decimal => "string",
        DataType::Date => "int32",
        DataType::Varchar => "string",
        DataType::Time => "int64",
        DataType::Timestamp => "int64",
        DataType::Timestamptz => "string",
        DataType::Interval => "string",
        DataType::Struct(_) => "struct",
        DataType::List(_) => "array",
        DataType::Vector(_) => "array",
        DataType::Bytea => "bytes",
        DataType::Jsonb => "string",
        DataType::Variant => "string",
        DataType::Serial => "string",
        DataType::Int256 => "string",
        DataType::Map(_) => "map",
    }
}

fn type_as_json_schema(rw_type: &DataType) -> Map<String, Value> {
    let mut mapping = Map::with_capacity(4); // type + optional + fields/items + field
    mapping.insert("type".to_owned(), json!(schema_type_mapping(rw_type)));
    mapping.insert("optional".to_owned(), json!(true));
    match rw_type {
        DataType::Struct(struct_type) => {
            let sub_fields = struct_type
                .iter()
                .map(|(sub_name, sub_type)| {
                    let mut sub_mapping = type_as_json_schema(sub_type);
                    sub_mapping.insert("field".to_owned(), json!(sub_name));
                    sub_mapping
                })
                .collect_vec();
            mapping.insert("fields".to_owned(), json!(sub_fields));
        }
        DataType::List(list_type) => {
            mapping.insert(
                "items".to_owned(),
                json!(type_as_json_schema(list_type.elem())),
            );
        }
        _ => {}
    }

    mapping
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{
        Date, Decimal, Interval, Scalar, ScalarImpl, StructRef, StructType, StructValue, Time,
        Timestamp,
    };

    use super::*;

    #[test]
    fn test_starrocks_timestamptz_encoding() {
        let schema = Schema::new(vec![Field::with_name(DataType::Timestamptz, "ts")]);
        let encoder = JsonEncoder::new_with_starrocks(schema, None, chrono_tz::Asia::Shanghai);
        let tstz = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let row = OwnedRow::new(vec![Some(ScalarImpl::Timestamptz(tstz))]);

        let encoded = encoder.encode(row).unwrap();

        assert_eq!(
            encoded.get("ts"),
            Some(&json!("2018-01-27 02:30:09.453000"))
        );
    }

    #[test]
    fn test_to_json_basic_type() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
        };

        let config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::Milli,
            date_handling_mode: DateHandlingMode::FromCe,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::None,
            jsonb_handling_mode: JsonbHandlingMode::String,
        };

        let boolean_value = datum_to_json_object(
            &Field {
                data_type: DataType::Boolean,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Bool(false).as_scalar_ref_impl()),
            &config,
        )
        .unwrap();
        assert_eq!(boolean_value, json!(false));

        let int16_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int16,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int16(16).as_scalar_ref_impl()),
            &config,
        )
        .unwrap();
        assert_eq!(int16_value, json!(16));

        let int64_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int64,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int64(i64::MAX).as_scalar_ref_impl()),
            &config,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&int64_value).unwrap(),
            i64::MAX.to_string()
        );

        let serial_value = datum_to_json_object(
            &Field {
                data_type: DataType::Serial,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Serial(i64::MAX.into()).as_scalar_ref_impl()),
            &config,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&serial_value).unwrap(),
            format!("\"{:#018x}\"", i64::MAX)
        );

        let turbopuffer_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::Turbopuffer,
            jsonb_handling_mode: JsonbHandlingMode::Dynamic,
        };
        let turbopuffer_serial_value = datum_to_json_object(
            &Field {
                data_type: DataType::Serial,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Serial(i64::MAX.into()).as_scalar_ref_impl()),
            &turbopuffer_config,
        )
        .unwrap();
        assert_eq!(turbopuffer_serial_value, json!(i64::MAX));
        let turbopuffer_decimal_value = datum_to_json_object(
            &Field {
                data_type: DataType::Decimal,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Decimal(Decimal::try_from(1.25).unwrap()).as_scalar_ref_impl()),
            &turbopuffer_config,
        )
        .unwrap();
        assert_eq!(turbopuffer_decimal_value, json!(1.25));
        assert!(
            datum_to_json_object(
                &Field {
                    data_type: DataType::Decimal,
                    ..mock_field.clone()
                },
                Some(ScalarImpl::Decimal(Decimal::NaN).as_scalar_ref_impl()),
                &turbopuffer_config,
            )
            .unwrap_err()
            .to_string()
            .contains("non-finite decimal")
        );

        // https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/time/ZonedTimestamp.java
        let tstz_inner = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let tstz_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Timestamptz(tstz_inner).as_scalar_ref_impl()),
            &config,
        )
        .unwrap();
        assert_eq!(tstz_value, "2018-01-26T18:30:09.453000Z");

        let unix_wo_suffix_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::Milli,
            date_handling_mode: DateHandlingMode::FromCe,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcWithoutSuffix,
            custom_json_type: CustomJsonType::None,
            jsonb_handling_mode: JsonbHandlingMode::String,
        };

        let tstz_inner = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let tstz_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Timestamptz(tstz_inner).as_scalar_ref_impl()),
            &unix_wo_suffix_config,
        )
        .unwrap();
        assert_eq!(tstz_value, "2018-01-26 18:30:09.453000");

        let timestamp_milli_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::FromCe,
            timestamp_handling_mode: TimestampHandlingMode::Milli,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::None,
            jsonb_handling_mode: JsonbHandlingMode::String,
        };
        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            &timestamp_milli_config,
        )
        .unwrap();
        assert_eq!(ts_value, json!(1000 * 1000));

        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            &config,
        )
        .unwrap();
        assert_eq!(ts_value, json!("1970-01-01 00:16:40.000000".to_owned()));

        // Represents the number of milliseconds past midnigh, org.apache.kafka.connect.data.Time
        let time_value = datum_to_json_object(
            &Field {
                data_type: DataType::Time,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Time(Time::from_num_seconds_from_midnight_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            &config,
        )
        .unwrap();
        assert_eq!(time_value, json!(1000 * 1000));

        let interval_value = datum_to_json_object(
            &Field {
                data_type: DataType::Interval,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Interval(Interval::from_month_day_usec(13, 2, 1000000))
                    .as_scalar_ref_impl(),
            ),
            &config,
        )
        .unwrap();
        assert_eq!(interval_value, json!("P1Y1M2DT0H0M1S"));

        let mut decimal_scale = HashMap::default();
        decimal_scale.insert("aaa".to_owned(), 5_u8);
        let doris_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::Doris(DorisJsonConfig {
                decimal_scale,
                variant_columns: HashSet::default(),
            }),
            jsonb_handling_mode: JsonbHandlingMode::String,
        };
        let decimal = datum_to_json_object(
            &Field {
                data_type: DataType::Decimal,
                name: "aaa".to_owned(),
            },
            Some(ScalarImpl::Decimal(Decimal::try_from(1.1111111).unwrap()).as_scalar_ref_impl()),
            &doris_config,
        )
        .unwrap();
        assert_eq!(decimal, json!("1.11111"));

        let date_value = datum_to_json_object(
            &Field {
                data_type: DataType::Date,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Date(Date::from_ymd_uncheck(1970, 1, 1)).as_scalar_ref_impl()),
            &config,
        )
        .unwrap();
        assert_eq!(date_value, json!(719163));

        let from_epoch_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::FromEpoch,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::None,
            jsonb_handling_mode: JsonbHandlingMode::String,
        };
        let date_value = datum_to_json_object(
            &Field {
                data_type: DataType::Date,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Date(Date::from_ymd_uncheck(1970, 1, 1)).as_scalar_ref_impl()),
            &from_epoch_config,
        )
        .unwrap();
        assert_eq!(date_value, json!(0));

        let doris_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::Doris(DorisJsonConfig {
                decimal_scale: HashMap::default(),
                variant_columns: HashSet::default(),
            }),
            jsonb_handling_mode: JsonbHandlingMode::String,
        };
        let date_value = datum_to_json_object(
            &Field {
                data_type: DataType::Date,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Date(Date::from_ymd_uncheck(2010, 10, 10)).as_scalar_ref_impl()),
            &doris_config,
        )
        .unwrap();
        assert_eq!(date_value, json!("2010-10-10"));

        let value = StructValue::new(vec![
            Some(3_i32.to_scalar_value()),
            Some(2_i32.to_scalar_value()),
            Some(1_i32.to_scalar_value()),
        ]);

        let interval_value = datum_to_json_object(
            &Field {
                data_type: DataType::Struct(StructType::new(vec![
                    ("v3", DataType::Int32),
                    ("v2", DataType::Int32),
                    ("v1", DataType::Int32),
                ])),
                ..mock_field.clone()
            },
            Some(ScalarRefImpl::Struct(StructRef::ValueRef { val: &value })),
            &doris_config,
        )
        .unwrap();
        assert_eq!(interval_value, json!("{\"v3\":3,\"v2\":2,\"v1\":1}"));

        let encode_jsonb_obj_config = JsonEncoderConfig {
            time_handling_mode: TimeHandlingMode::String,
            date_handling_mode: DateHandlingMode::String,
            timestamp_handling_mode: TimestampHandlingMode::String,
            timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
            custom_json_type: CustomJsonType::None,
            jsonb_handling_mode: JsonbHandlingMode::Dynamic,
        };
        let json_value = datum_to_json_object(
            &Field {
                data_type: DataType::Jsonb,
                ..mock_field
            },
            Some(ScalarImpl::Jsonb(JsonbVal::from(json!([1, 2, 3]))).as_scalar_ref_impl()),
            &encode_jsonb_obj_config,
        )
        .unwrap();
        assert_eq!(json_value, json!([1, 2, 3]));

        let variant_json_value = datum_to_json_object(
            &Field {
                data_type: DataType::Jsonb,
                name: "variant_col".into(),
            },
            Some(
                ScalarImpl::Jsonb(JsonbVal::from(json!({"nested": [1, 2, 3]})))
                    .as_scalar_ref_impl(),
            ),
            &JsonEncoderConfig {
                time_handling_mode: TimeHandlingMode::String,
                date_handling_mode: DateHandlingMode::String,
                timestamp_handling_mode: TimestampHandlingMode::String,
                timestamptz_handling_mode: TimestamptzHandlingMode::UtcString,
                custom_json_type: CustomJsonType::Doris(DorisJsonConfig {
                    decimal_scale: HashMap::default(),
                    variant_columns: HashSet::from(["variant_col".to_owned()]),
                }),
                jsonb_handling_mode: JsonbHandlingMode::String,
            },
        )
        .unwrap();
        assert_eq!(variant_json_value, json!({"nested": [1, 2, 3]}));
    }

    #[test]
    fn test_generate_json_converter_schema() {
        let fields = vec![
            Field {
                data_type: DataType::Boolean,
                name: "v1".into(),
            },
            Field {
                data_type: DataType::Int16,
                name: "v2".into(),
            },
            Field {
                data_type: DataType::Int32,
                name: "v3".into(),
            },
            Field {
                data_type: DataType::Float32,
                name: "v4".into(),
            },
            Field {
                data_type: DataType::Decimal,
                name: "v5".into(),
            },
            Field {
                data_type: DataType::Date,
                name: "v6".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "v7".into(),
            },
            Field {
                data_type: DataType::Time,
                name: "v8".into(),
            },
            Field {
                data_type: DataType::Interval,
                name: "v9".into(),
            },
            Field {
                data_type: DataType::Struct(StructType::new(vec![
                    ("a", DataType::Timestamp),
                    ("b", DataType::Timestamptz),
                    (
                        "c",
                        DataType::Struct(StructType::new(vec![
                            ("aa", DataType::Int64),
                            ("bb", DataType::Float64),
                        ])),
                    ),
                ])),
                name: "v10".into(),
            },
            Field {
                data_type: DataType::list(DataType::list(DataType::Struct(StructType::new(vec![
                    ("aa", DataType::Int64),
                    ("bb", DataType::Float64),
                ])))),
                name: "v11".into(),
            },
            Field {
                data_type: DataType::Jsonb,
                name: "12".into(),
            },
            Field {
                data_type: DataType::Serial,
                name: "13".into(),
            },
            Field {
                data_type: DataType::Int256,
                name: "14".into(),
            },
        ];
        let schema =
            json_converter_with_schema(json!({}), "test".to_owned(), fields.iter())["schema"]
                .to_string();
        let ans = r#"{"fields":[{"field":"v1","optional":true,"type":"boolean"},{"field":"v2","optional":true,"type":"int16"},{"field":"v3","optional":true,"type":"int32"},{"field":"v4","optional":true,"type":"float"},{"field":"v5","optional":true,"type":"string"},{"field":"v6","optional":true,"type":"int32"},{"field":"v7","optional":true,"type":"string"},{"field":"v8","optional":true,"type":"int64"},{"field":"v9","optional":true,"type":"string"},{"field":"v10","fields":[{"field":"a","optional":true,"type":"int64"},{"field":"b","optional":true,"type":"string"},{"field":"c","fields":[{"field":"aa","optional":true,"type":"int64"},{"field":"bb","optional":true,"type":"double"}],"optional":true,"type":"struct"}],"optional":true,"type":"struct"},{"field":"v11","items":{"items":{"fields":[{"field":"aa","optional":true,"type":"int64"},{"field":"bb","optional":true,"type":"double"}],"optional":true,"type":"struct"},"optional":true,"type":"array"},"optional":true,"type":"array"},{"field":"12","optional":true,"type":"string"},{"field":"13","optional":true,"type":"string"},{"field":"14","optional":true,"type":"string"}],"name":"test","optional":false,"type":"struct"}"#;
        assert_eq!(schema, ans);
    }
}
