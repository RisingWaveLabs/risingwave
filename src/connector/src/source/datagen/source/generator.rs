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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_async_stream::try_stream;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::field_generator::FieldGeneratorImpl;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;

use crate::error::ConnectorResult;
use crate::parser::{EncodingProperties, ProtocolProperties, SpecificParserConfig};
use crate::source::{SourceMessage, SourceMeta, SplitId};

pub enum FieldDesc {
    // field is invisible, generate None
    Invisible,
    Visible(FieldGeneratorImpl),
}

pub struct DatagenEventGenerator {
    // fields_map: HashMap<String, FieldGeneratorImpl>,
    field_names: Vec<String>,
    fields_vec: Vec<FieldDesc>,
    source_format: SpecificParserConfig,
    data_types: Vec<DataType>,
    offset: u64,
    split_id: SplitId,
    partition_rows_per_second: u64,
}

#[derive(Debug, Clone)]
pub struct DatagenMeta {
    // timestamp(milliseconds) of the data generated
    pub timestamp: Option<i64>,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_vec: Vec<FieldDesc>,
        field_names: Vec<String>,
        source_format: SpecificParserConfig,
        data_types: Vec<DataType>,
        rows_per_second: u64,
        offset: u64,
        split_id: SplitId,
        split_num: u64,
        split_index: u64,
    ) -> ConnectorResult<Self> {
        let partition_rows_per_second = if rows_per_second % split_num > split_index {
            rows_per_second / split_num + 1
        } else {
            rows_per_second / split_num
        };
        Ok(Self {
            field_names,
            fields_vec,
            source_format,
            data_types,
            offset,
            split_id,
            partition_rows_per_second,
        })
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn into_msg_stream(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        const MAX_ROWS_PER_YIELD: u64 = 1024;
        let mut reach_end = false;
        loop {
            // generate `partition_rows_per_second` rows per second
            interval.tick().await;
            let mut rows_generated_this_second = 0;
            while rows_generated_this_second < self.partition_rows_per_second {
                let num_rows_to_generate = std::cmp::min(
                    MAX_ROWS_PER_YIELD,
                    self.partition_rows_per_second - rows_generated_this_second,
                );
                let mut msgs = Vec::with_capacity(num_rows_to_generate as usize);
                'outer: for _ in 0..num_rows_to_generate {
                    let payload = match (
                        &self.source_format.protocol_config,
                        &self.source_format.encoding_config,
                    ) {
                        (ProtocolProperties::Plain, EncodingProperties::Json(_)) => {
                            let mut map = serde_json::Map::with_capacity(self.fields_vec.len());
                            for (name, field_generator) in self
                                .field_names
                                .iter()
                                .zip_eq_fast(self.fields_vec.iter_mut())
                            {
                                let value = match field_generator {
                                    FieldDesc::Invisible => continue,
                                    FieldDesc::Visible(field_generator) => {
                                        let value = field_generator.generate_json(self.offset);
                                        if value.is_null() {
                                            reach_end = true;
                                            tracing::info!(
                                                "datagen split {} stop generate, offset {}",
                                                self.split_id,
                                                self.offset
                                            );
                                            break 'outer;
                                        }
                                        value
                                    }
                                };

                                map.insert(name.clone(), value);
                            }
                            serde_json::Value::from(map).to_string().as_bytes().to_vec()
                        }
                        _ => {
                            unimplemented!("only json format is supported for now")
                        }
                    };
                    msgs.push(SourceMessage {
                        key: None,
                        payload: Some(payload),
                        offset: self.offset.to_string(),
                        split_id: self.split_id.clone(),
                        meta: SourceMeta::Datagen(DatagenMeta {
                            timestamp: Some(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as i64,
                            ),
                        }),
                    });
                    self.offset += 1;
                    rows_generated_this_second += 1;
                }
                if !msgs.is_empty() {
                    yield msgs;
                }

                if reach_end {
                    return Ok(());
                }
            }
        }
    }

    #[try_stream(ok = StreamChunk, error = crate::error::ConnectorError)]
    pub async fn into_native_stream(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        const MAX_ROWS_PER_YIELD: u64 = 1024;
        let mut reach_end = false;
        loop {
            // generate `partition_rows_per_second` rows per second
            interval.tick().await;
            let mut rows_generated_this_second = 0;
            let mut chunk_builder =
                StreamChunkBuilder::new(MAX_ROWS_PER_YIELD as usize, self.data_types.clone());
            while rows_generated_this_second < self.partition_rows_per_second {
                let num_rows_to_generate = std::cmp::min(
                    MAX_ROWS_PER_YIELD,
                    self.partition_rows_per_second - rows_generated_this_second,
                );
                'outer: for _ in 0..num_rows_to_generate {
                    let mut row = Vec::with_capacity(self.data_types.len());
                    for (field_generator, field_name) in
                        self.fields_vec.iter_mut().zip_eq_fast(&self.field_names)
                    {
                        let datum = match field_generator {
                            // TODO: avoid distinguishing hidden partition/offset columns by name
                            FieldDesc::Invisible => match field_name.as_str() {
                                "_rw_datagen_partition" => {
                                    Some(ScalarImpl::Utf8(self.split_id.as_ref().into()))
                                }
                                "_rw_datagen_offset" => {
                                    Some(ScalarImpl::Utf8(self.offset.to_string().into_boxed_str()))
                                }
                                _ => None,
                            },
                            FieldDesc::Visible(field_generator) => {
                                let datum = field_generator.generate_datum(self.offset);
                                if datum.is_none() {
                                    reach_end = true;
                                    tracing::info!(
                                        "datagen split {} stop generate, offset {}",
                                        self.split_id,
                                        self.offset
                                    );
                                    break 'outer;
                                };

                                datum
                            }
                        };

                        row.push(datum);
                    }

                    self.offset += 1;
                    rows_generated_this_second += 1;
                    if let Some(chunk) = chunk_builder.append_row(Op::Insert, OwnedRow::new(row)) {
                        yield chunk;
                    }
                }

                if let Some(chunk) = chunk_builder.take() {
                    yield chunk;
                }

                if reach_end {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;

    use super::*;

    async fn check_sequence_partition_result(
        split_num: u64,
        split_index: u64,
        rows_per_second: u64,
        expected_length: usize,
    ) {
        let split_id = format!("{}-{}", split_num, split_index).into();
        let start = 1;
        let end = 10;

        let data_types = vec![DataType::Int32, DataType::Float32];
        let fields_vec = vec![
            FieldDesc::Visible(
                FieldGeneratorImpl::with_number_sequence(
                    data_types[0].clone(),
                    Some(start.to_string()),
                    Some(end.to_string()),
                    split_index,
                    split_num,
                    0,
                )
                .unwrap(),
            ),
            FieldDesc::Visible(
                FieldGeneratorImpl::with_number_sequence(
                    data_types[1].clone(),
                    Some(start.to_string()),
                    Some(end.to_string()),
                    split_index,
                    split_num,
                    0,
                )
                .unwrap(),
            ),
        ];

        let generator = DatagenEventGenerator::new(
            fields_vec,
            vec!["c1".to_owned(), "c2".to_owned()],
            SpecificParserConfig {
                protocol_config: ProtocolProperties::Plain,
                encoding_config: EncodingProperties::Json(crate::parser::JsonProperties {
                    use_schema_registry: false,
                    timestamptz_handling: None,
                    timestamp_handling: None,
                    time_handling: None,
                    bigint_unsigned_handling: None,
                    handle_toast_columns: false,
                    numeric_string_parsing: false,
                }),
            },
            data_types,
            rows_per_second,
            0,
            split_id,
            split_num,
            split_index,
        )
        .unwrap();

        let mut stream = generator.into_msg_stream().boxed();

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(expected_length, chunk.len());

        let empty_chunk = stream.next().await;
        if rows_per_second >= (end - start + 1) {
            assert!(empty_chunk.is_none());
        } else {
            assert!(empty_chunk.is_some());
        }
    }

    #[tokio::test]
    async fn test_one_partition_sequence() {
        check_sequence_partition_result(1, 0, 10, 10).await;
    }

    #[tokio::test]
    async fn test_two_partition_sequence() {
        check_sequence_partition_result(2, 0, 10, 5).await;
        check_sequence_partition_result(2, 1, 10, 5).await;
    }

    #[tokio::test]
    async fn test_three_partition_sequence() {
        check_sequence_partition_result(3, 0, 10, 4).await;
        check_sequence_partition_result(3, 1, 10, 3).await;
        check_sequence_partition_result(3, 2, 10, 3).await;
    }

    #[tokio::test]
    async fn test_one_partition_sequence_reach_end() {
        check_sequence_partition_result(1, 0, 15, 10).await;
    }

    #[tokio::test]
    async fn test_two_partition_sequence_reach_end() {
        check_sequence_partition_result(2, 0, 15, 5).await;
        check_sequence_partition_result(2, 1, 15, 5).await;
    }

    #[tokio::test]
    async fn test_three_partition_sequence_reach_end() {
        check_sequence_partition_result(3, 0, 15, 4).await;
        check_sequence_partition_result(3, 1, 15, 3).await;
        check_sequence_partition_result(3, 2, 15, 3).await;
    }
}
