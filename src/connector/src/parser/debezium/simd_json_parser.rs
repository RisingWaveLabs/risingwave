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

use std::fmt::Debug;

use anyhow::Context;
use simd_json::BorrowedValue;
use simd_json::prelude::MutableObject;

use crate::error::ConnectorResult;
use crate::parser::unified::AccessImpl;
use crate::parser::unified::debezium::MongoJsonAccess;
use crate::parser::unified::json::{
    BigintUnsignedHandlingMode, JsonAccess, JsonParseOptions, TimeHandling, TimestampHandling,
    TimestamptzHandling,
};
use crate::parser::{AccessBuilder, MongoProperties};

#[derive(Debug)]
pub struct DebeziumJsonAccessBuilder {
    value: Option<Vec<u8>>,
    json_parse_options: JsonParseOptions,
}

impl DebeziumJsonAccessBuilder {
    pub fn new(
        timestamptz_handling: TimestamptzHandling,
        timestamp_handling: TimestampHandling,
        time_handling: TimeHandling,
        bigint_unsigned_handling: BigintUnsignedHandlingMode,
        handle_toast_columns: bool,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            value: None,
            json_parse_options: JsonParseOptions::new_for_debezium(
                timestamptz_handling,
                timestamp_handling,
                time_handling,
                bigint_unsigned_handling,
                handle_toast_columns,
            ),
        })
    }

    pub fn new_for_schema_event() -> ConnectorResult<Self> {
        Ok(Self {
            value: None,
            json_parse_options: JsonParseOptions::default(),
        })
    }
}

impl AccessBuilder for DebeziumJsonAccessBuilder {
    async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        _: &crate::source::SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>> {
        self.value = Some(payload);
        let mut event: BorrowedValue<'_> =
            simd_json::to_borrowed_value(self.value.as_mut().unwrap())
                .context("failed to parse debezium json payload")?;

        let payload = if let Some(payload) = event.get_mut("payload") {
            std::mem::take(payload)
        } else {
            event
        };

        Ok(AccessImpl::Json(JsonAccess::new_with_options(
            payload,
            &self.json_parse_options,
        )))
    }
}

#[derive(Debug)]
pub struct DebeziumMongoJsonAccessBuilder {
    value: Option<Vec<u8>>,
    json_parse_options: JsonParseOptions,
    strong_schema: bool,
}

impl DebeziumMongoJsonAccessBuilder {
    pub fn new(props: MongoProperties) -> anyhow::Result<Self> {
        Ok(Self {
            value: None,
            json_parse_options: JsonParseOptions::new_for_debezium(
                TimestamptzHandling::GuessNumberUnit,
                TimestampHandling::GuessNumberUnit,
                TimeHandling::Micro,
                BigintUnsignedHandlingMode::Long,
                false,
            ),
            strong_schema: props.strong_schema,
        })
    }
}

impl AccessBuilder for DebeziumMongoJsonAccessBuilder {
    async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        _: &crate::source::SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>> {
        self.value = Some(payload);
        let mut event: BorrowedValue<'_> =
            simd_json::to_borrowed_value(self.value.as_mut().unwrap())
                .context("failed to parse debezium mongo json payload")?;

        let payload = if let Some(payload) = event.get_mut("payload") {
            std::mem::take(payload)
        } else {
            event
        };

        Ok(AccessImpl::MongoJson(MongoJsonAccess::new(
            JsonAccess::new_with_options(payload, &self.json_parse_options),
            self.strong_schema,
        )))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use risingwave_common::array::{Op, StructValue};
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, Date, Interval, Scalar, ScalarImpl, Time, Timestamp};
    use serde_json::Value;
    use thiserror_ext::AsReport;

    use crate::parser::{
        DebeziumParser, DebeziumProps, EncodingProperties, JsonProperties, ProtocolProperties,
        SourceColumnDesc, SourceStreamChunkBuilder, SpecificParserConfig,
    };
    use crate::source::{SourceContext, SourceCtrlOpts};

    fn assert_json_eq(parse_result: &Option<ScalarImpl>, json_str: &str) {
        if let Some(ScalarImpl::Jsonb(json_val)) = parse_result {
            let mut json_string = String::new();
            json_val
                .as_scalar_ref()
                .force_str(&mut json_string)
                .unwrap();
            let val1: Value = serde_json::from_str(json_string.as_str()).unwrap();
            let val2: Value = serde_json::from_str(json_str).unwrap();
            assert_eq!(val1, val2);
        }
    }

    async fn build_parser(rw_columns: Vec<SourceColumnDesc>) -> DebeziumParser {
        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
                timestamp_handling: None,
                time_handling: None,
                bigint_unsigned_handling: None,
                handle_toast_columns: false,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        DebeziumParser::new(props, rw_columns, SourceContext::dummy().into())
            .await
            .unwrap()
    }

    async fn parse_one(
        mut parser: DebeziumParser,
        columns: Vec<SourceColumnDesc>,
        payload: Vec<u8>,
    ) -> Vec<(Op, OwnedRow)> {
        let mut builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());
        parser
            .parse_inner(None, Some(payload), builder.row_writer())
            .await
            .unwrap();
        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
        chunk
            .rows()
            .map(|(op, row_ref)| (op, row_ref.into_owned_row()))
            .collect::<Vec<_>>()
    }

    mod test1_basic {
        use super::*;

        fn get_test1_columns() -> Vec<SourceColumnDesc> {
            vec![
                SourceColumnDesc::simple("id", DataType::Int32, ColumnId::from(0)),
                SourceColumnDesc::simple("name", DataType::Varchar, ColumnId::from(1)),
                SourceColumnDesc::simple("description", DataType::Varchar, ColumnId::from(2)),
                SourceColumnDesc::simple("weight", DataType::Float64, ColumnId::from(3)),
            ]
        }

        #[tokio::test]
        async fn test1_debezium_json_parser_read() {
            //     "before": null,
            //     "after": {
            //       "id": 101,
            //       "name": "scooter",
            //       "description": "Small 2-wheel scooter",
            //       "weight": 1.234
            //     },
            let input = vec![
                // data with payload field
                br#"{"payload":{"before":null,"after":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639547113601,"snapshot":"true","db":"inventory","sequence":null,"table":"products","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":156,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1639547113602,"transaction":null}}"#.to_vec(),
                // data without payload field
                br#"{"before":null,"after":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639547113601,"snapshot":"true","db":"inventory","sequence":null,"table":"products","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":156,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1639547113602,"transaction":null}"#.to_vec()];

            let columns = get_test1_columns();

            for data in input {
                let parser = build_parser(columns.clone()).await;
                let [(_op, row)]: [_; 1] = parse_one(parser, columns.clone(), data)
                    .await
                    .try_into()
                    .unwrap();

                assert!(row[0].eq(&Some(ScalarImpl::Int32(101))));
                assert!(row[1].eq(&Some(ScalarImpl::Utf8("scooter".into()))));
                assert!(row[2].eq(&Some(ScalarImpl::Utf8("Small 2-wheel scooter".into()))));
                assert!(row[3].eq(&Some(ScalarImpl::Float64(1.234.into()))));
            }
        }

        #[tokio::test]
        async fn test1_debezium_json_parser_insert() {
            //     "before": null,
            //     "after": {
            //       "id": 102,
            //       "name": "car battery",
            //       "description": "12V car battery",
            //       "weight": 8.1
            //     },
            let input = vec![
                // data with payload field
                br#"{"payload":{"before":null,"after":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551564000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":717,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1639551564960,"transaction":null}}"#.to_vec(),
                // data without payload field
                br#"{"before":null,"after":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551564000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":717,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1639551564960,"transaction":null}"#.to_vec()];

            let columns = get_test1_columns();

            for data in input {
                let parser = build_parser(columns.clone()).await;
                let [(op, row)]: [_; 1] = parse_one(parser, columns.clone(), data)
                    .await
                    .try_into()
                    .unwrap();
                assert_eq!(op, Op::Insert);

                assert!(row[0].eq(&Some(ScalarImpl::Int32(102))));
                assert!(row[1].eq(&Some(ScalarImpl::Utf8("car battery".into()))));
                assert!(row[2].eq(&Some(ScalarImpl::Utf8("12V car battery".into()))));
                assert!(row[3].eq(&Some(ScalarImpl::Float64(8.1.into()))));
            }
        }

        #[tokio::test]
        async fn test1_debezium_json_parser_delete() {
            //     "before": {
            //       "id": 101,
            //       "name": "scooter",
            //       "description": "Small 2-wheel scooter",
            //       "weight": 1.234
            //     },
            //     "after": null,
            let input = vec![
                // data with payload field
                br#"{"payload":{"before":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"after":null,"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551767000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1045,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1639551767775,"transaction":null}}"#.to_vec(),
                // data without payload field
                br#"{"before":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"after":null,"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551767000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1045,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1639551767775,"transaction":null}"#.to_vec()];

            for data in input {
                let columns = get_test1_columns();
                let parser = build_parser(columns.clone()).await;
                let [(op, row)]: [_; 1] = parse_one(parser, columns.clone(), data)
                    .await
                    .try_into()
                    .unwrap();

                assert_eq!(op, Op::Delete);

                assert!(row[0].eq(&Some(ScalarImpl::Int32(101))));
                assert!(row[1].eq(&Some(ScalarImpl::Utf8("scooter".into()))));
                assert!(row[2].eq(&Some(ScalarImpl::Utf8("Small 2-wheel scooter".into()))));
                assert!(row[3].eq(&Some(ScalarImpl::Float64(1.234.into()))));
            }
        }

        #[tokio::test]
        async fn test1_debezium_json_parser_update() {
            //     "before": {
            //       "id": 102,
            //       "name": "car battery",
            //       "description": "12V car battery",
            //       "weight": 8.1
            //     },
            //     "after": {
            //       "id": 102,
            //       "name": "car battery",
            //       "description": "24V car battery",
            //       "weight": 9.1
            //     },
            let input = vec![
                // data with payload field
                br#"{"payload":{"before":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"after":{"id":102,"name":"car battery","description":"24V car battery","weight":9.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551901000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1382,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1639551901165,"transaction":null}}"#.to_vec(),
                // data without payload field
                br#"{"before":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"after":{"id":102,"name":"car battery","description":"24V car battery","weight":9.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551901000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1382,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1639551901165,"transaction":null}"#.to_vec()];

            let columns = get_test1_columns();

            for data in input {
                let parser = build_parser(columns.clone()).await;
                let [(op, row)]: [_; 1] = parse_one(parser, columns.clone(), data)
                    .await
                    .try_into()
                    .unwrap();

                assert_eq!(op, Op::Insert);

                assert!(row[0].eq(&Some(ScalarImpl::Int32(102))));
                assert!(row[1].eq(&Some(ScalarImpl::Utf8("car battery".into()))));
                assert!(row[2].eq(&Some(ScalarImpl::Utf8("24V car battery".into()))));
                assert!(row[3].eq(&Some(ScalarImpl::Float64(9.1.into()))));
            }
        }
    }
    // test2 covers read/insert/update/delete event on the following MySQL table for debezium json:
    // CREATE TABLE IF NOT EXISTS orders (
    //     O_KEY BIGINT NOT NULL,
    //     O_BOOL BOOLEAN,
    //     O_TINY TINYINT,
    //     O_INT INT,
    //     O_REAL REAL,
    //     O_DOUBLE DOUBLE,
    //     O_DECIMAL DECIMAL(15, 2),
    //     O_CHAR CHAR(15),
    //     O_DATE DATE,
    //     O_TIME TIME,
    //     O_DATETIME DATETIME,
    //     O_TIMESTAMP TIMESTAMP,
    //     O_JSON JSON,
    //     PRIMARY KEY (O_KEY));
    // test2 also covers overflow tests on basic types
    mod test2_mysql {
        use super::*;

        fn get_test2_columns() -> Vec<SourceColumnDesc> {
            vec![
                SourceColumnDesc::simple("O_KEY", DataType::Int64, ColumnId::from(0)),
                SourceColumnDesc::simple("O_BOOL", DataType::Boolean, ColumnId::from(1)),
                SourceColumnDesc::simple("O_TINY", DataType::Int16, ColumnId::from(2)),
                SourceColumnDesc::simple("O_INT", DataType::Int32, ColumnId::from(3)),
                SourceColumnDesc::simple("O_REAL", DataType::Float32, ColumnId::from(4)),
                SourceColumnDesc::simple("O_DOUBLE", DataType::Float64, ColumnId::from(5)),
                SourceColumnDesc::simple("O_DECIMAL", DataType::Decimal, ColumnId::from(6)),
                SourceColumnDesc::simple("O_CHAR", DataType::Varchar, ColumnId::from(7)),
                SourceColumnDesc::simple("O_DATE", DataType::Date, ColumnId::from(8)),
                SourceColumnDesc::simple("O_TIME", DataType::Time, ColumnId::from(9)),
                SourceColumnDesc::simple("O_DATETIME", DataType::Timestamp, ColumnId::from(10)),
                SourceColumnDesc::simple("O_TIMESTAMP", DataType::Timestamptz, ColumnId::from(11)),
                SourceColumnDesc::simple("O_JSON", DataType::Jsonb, ColumnId::from(12)),
            ]
        }

        #[tokio::test]
        async fn test2_debezium_json_parser_read() {
            let data = br#"{"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":-1,"O_INT":-1111,"O_REAL":-11.11,"O_DOUBLE":-111.11111,"O_DECIMAL":-111.11,"O_CHAR":"yes please","O_DATE":"1000-01-01","O_TIME":0,"O_DATETIME":0,"O_TIMESTAMP":"1970-01-01T00:00:01Z","O_JSON":"{\"k1\": \"v1\", \"k2\": 11}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678090651000,"snapshot":"last","db":"test","sequence":null,"table":"orders","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":951,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1678090651640,"transaction":null}}"#;

            let columns = get_test2_columns();

            let parser = build_parser(columns.clone()).await;

            let [(_op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();

            assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
            assert!(row[1].eq(&Some(ScalarImpl::Bool(true))));
            assert!(row[2].eq(&Some(ScalarImpl::Int16(-1))));
            assert!(row[3].eq(&Some(ScalarImpl::Int32(-1111))));
            assert!(row[4].eq(&Some(ScalarImpl::Float32((-11.11).into()))));
            assert!(row[5].eq(&Some(ScalarImpl::Float64((-111.11111).into()))));
            assert!(row[6].eq(&Some(ScalarImpl::Decimal("-111.11".parse().unwrap()))));
            assert!(row[7].eq(&Some(ScalarImpl::Utf8("yes please".into()))));
            assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
                NaiveDate::from_ymd_opt(1000, 1, 1).unwrap()
            )))));
            assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()
            )))));
            assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
                "1970-01-01T00:00:00".parse().unwrap()
            )))));
            assert!(row[11].eq(&Some(ScalarImpl::Timestamptz(
                "1970-01-01T00:00:01Z".parse().unwrap()
            ))));
            assert_json_eq(&row[12], "{\"k1\": \"v1\", \"k2\": 11}");
        }

        #[tokio::test]
        async fn test2_debezium_json_parser_insert() {
            let data = br#"{"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":-1,"O_INT":-1111,"O_REAL":-11.11,"O_DOUBLE":-111.11111,"O_DECIMAL":-111.11,"O_CHAR":"yes please","O_DATE":"1000-01-01","O_TIME":0,"O_DATETIME":0,"O_TIMESTAMP":"1970-01-01T00:00:01Z","O_JSON":"{\"k1\": \"v1\", \"k2\": 11}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678088861000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":789,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678088861249,"transaction":null}}"#;

            let columns = get_test2_columns();
            let parser = build_parser(columns.clone()).await;
            let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();
            assert_eq!(op, Op::Insert);

            assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
            assert!(row[1].eq(&Some(ScalarImpl::Bool(true))));
            assert!(row[2].eq(&Some(ScalarImpl::Int16(-1))));
            assert!(row[3].eq(&Some(ScalarImpl::Int32(-1111))));
            assert!(row[4].eq(&Some(ScalarImpl::Float32((-11.11).into()))));
            assert!(row[5].eq(&Some(ScalarImpl::Float64((-111.11111).into()))));
            assert!(row[6].eq(&Some(ScalarImpl::Decimal("-111.11".parse().unwrap()))));
            assert!(row[7].eq(&Some(ScalarImpl::Utf8("yes please".into()))));
            assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
                NaiveDate::from_ymd_opt(1000, 1, 1).unwrap()
            )))));
            assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()
            )))));
            assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
                "1970-01-01T00:00:00".parse().unwrap()
            )))));
            assert!(row[11].eq(&Some(ScalarImpl::Timestamptz(
                "1970-01-01T00:00:01Z".parse().unwrap()
            ))));
            assert_json_eq(&row[12], "{\"k1\": \"v1\", \"k2\": 11}");
        }

        #[tokio::test]
        async fn test2_debezium_json_parser_delete() {
            let data = br#"{"payload":{"before":{"O_KEY":111,"O_BOOL":0,"O_TINY":3,"O_INT":3333,"O_REAL":33.33,"O_DOUBLE":333.33333,"O_DECIMAL":333.33,"O_CHAR":"no thanks","O_DATE":"9999-12-31","O_TIME":86399000000,"O_DATETIME":99999999999000,"O_TIMESTAMP":"2038-01-09T03:14:07Z","O_JSON":"{\"k1\":\"v1_updated\",\"k2\":33}"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678090653000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1643,"row":0,"thread":4,"query":null},"op":"d","ts_ms":1678090653611,"transaction":null}}"#;

            let columns = get_test2_columns();
            let parser = build_parser(columns.clone()).await;
            let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();

            assert_eq!(op, Op::Delete);

            assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
            assert!(row[1].eq(&Some(ScalarImpl::Bool(false))));
            assert!(row[2].eq(&Some(ScalarImpl::Int16(3))));
            assert!(row[3].eq(&Some(ScalarImpl::Int32(3333))));
            assert!(row[4].eq(&Some(ScalarImpl::Float32((33.33).into()))));
            assert!(row[5].eq(&Some(ScalarImpl::Float64((333.33333).into()))));
            assert!(row[6].eq(&Some(ScalarImpl::Decimal("333.33".parse().unwrap()))));
            assert!(row[7].eq(&Some(ScalarImpl::Utf8("no thanks".into()))));
            assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
                NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()
            )))));
            assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(23, 59, 59, 0).unwrap()
            )))));
            assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
                "5138-11-16T09:46:39".parse().unwrap()
            )))));
            assert!(row[11].eq(&Some(ScalarImpl::Timestamptz(
                "2038-01-09T03:14:07Z".parse().unwrap()
            ))));
            assert_json_eq(&row[12], "{\"k1\":\"v1_updated\",\"k2\":33}");
        }

        #[tokio::test]
        async fn test2_debezium_json_parser_update() {
            let data = br#"{"payload":{"before":{"O_KEY":111,"O_BOOL":1,"O_TINY":-1,"O_INT":-1111,"O_REAL":-11.11,"O_DOUBLE":-111.11111,"O_DECIMAL":-111.11,"O_CHAR":"yes please","O_DATE":"1000-01-01","O_TIME":0,"O_DATETIME":0,"O_TIMESTAMP":"1970-01-01T00:00:01Z","O_JSON":"{\"k1\": \"v1\", \"k2\": 11}"},"after":{"O_KEY":111,"O_BOOL":0,"O_TINY":3,"O_INT":3333,"O_REAL":33.33,"O_DOUBLE":333.33333,"O_DECIMAL":333.33,"O_CHAR":"no thanks","O_DATE":"9999-12-31","O_TIME":86399000000,"O_DATETIME":99999999999000,"O_TIMESTAMP":"2038-01-09T03:14:07Z","O_JSON":"{\"k1\": \"v1_updated\", \"k2\": 33}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678089331000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1168,"row":0,"thread":4,"query":null},"op":"u","ts_ms":1678089331464,"transaction":null}}"#;

            let columns = get_test2_columns();

            let parser = build_parser(columns.clone()).await;
            let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();

            assert_eq!(op, Op::Insert);

            assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
            assert!(row[1].eq(&Some(ScalarImpl::Bool(false))));
            assert!(row[2].eq(&Some(ScalarImpl::Int16(3))));
            assert!(row[3].eq(&Some(ScalarImpl::Int32(3333))));
            assert!(row[4].eq(&Some(ScalarImpl::Float32((33.33).into()))));
            assert!(row[5].eq(&Some(ScalarImpl::Float64((333.33333).into()))));
            assert!(row[6].eq(&Some(ScalarImpl::Decimal("333.33".parse().unwrap()))));
            assert!(row[7].eq(&Some(ScalarImpl::Utf8("no thanks".into()))));
            assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
                NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()
            )))));
            assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(23, 59, 59, 0).unwrap()
            )))));
            assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
                "5138-11-16T09:46:39".parse().unwrap()
            )))));
            assert!(row[11].eq(&Some(ScalarImpl::Timestamptz(
                "2038-01-09T03:14:07Z".parse().unwrap()
            ))));
            assert_json_eq(&row[12], "{\"k1\": \"v1_updated\", \"k2\": 33}");
        }

        #[cfg(not(madsim))] // Traced test does not work with madsim
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn test2_debezium_json_parser_overflow() {
            let columns = vec![
                SourceColumnDesc::simple("O_KEY", DataType::Int64, ColumnId::from(0)),
                SourceColumnDesc::simple("O_BOOL", DataType::Boolean, ColumnId::from(1)),
                SourceColumnDesc::simple("O_TINY", DataType::Int16, ColumnId::from(2)),
                SourceColumnDesc::simple("O_INT", DataType::Int32, ColumnId::from(3)),
                SourceColumnDesc::simple("O_REAL", DataType::Float32, ColumnId::from(4)),
                SourceColumnDesc::simple("O_DOUBLE", DataType::Float64, ColumnId::from(5)),
            ];
            let mut parser = build_parser(columns.clone()).await;

            let mut dummy_builder =
                SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

            let normal_values = ["111", "1", "33", "444", "555.0", "666.0"];
            let overflow_values = [
                "9223372036854775808",
                "2",
                "32768",
                "2147483648",
                "3.80282347E38",
                "1.797695E308",
            ];

            for i in 0..6 {
                let mut values = normal_values;
                values[i] = overflow_values[i];
                let data = format!(
                    r#"{{"payload":{{"before":null,"after":{{"O_KEY":{},"O_BOOL":{},"O_TINY":{},"O_INT":{},"O_REAL":{},"O_DOUBLE":{}}},"source":{{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678158055000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":637,"row":0,"thread":4,"query":null}},"op":"c","ts_ms":1678158055464,"transaction":null}}}}"#,
                    values[0], values[1], values[2], values[3], values[4], values[5]
                ).as_bytes().to_vec();

                let res = parser
                    .parse_inner(None, Some(data), dummy_builder.row_writer())
                    .await;
                if i < 5 {
                    // For other overflow, the parsing succeeds but the type conversion fails
                    // The errors are ignored and logged.
                    res.unwrap();
                    assert!(logs_contain("expected type"), "{i}");
                } else {
                    // For f64 overflow, the parsing fails
                    let e = res.unwrap_err();
                    assert!(e.to_report_string().contains("InvalidNumber"), "{i}: {e}");
                }
            }
        }
    }

    // postgres-specific data-type mapping tests
    mod test3_postgres {
        use risingwave_pb::plan_common::AdditionalColumn;

        use super::*;
        use crate::connector_common::postgres::postgres_point_type;
        use crate::source::SourceColumnType;

        // schema for temporal-type test
        fn get_temporal_test_columns() -> Vec<SourceColumnDesc> {
            vec![
                SourceColumnDesc::simple("o_key", DataType::Int32, ColumnId::from(0)),
                SourceColumnDesc::simple("o_time_0", DataType::Time, ColumnId::from(1)),
                SourceColumnDesc::simple("o_time_6", DataType::Time, ColumnId::from(2)),
                SourceColumnDesc::simple("o_timez_0", DataType::Time, ColumnId::from(3)),
                SourceColumnDesc::simple("o_timez_6", DataType::Time, ColumnId::from(4)),
                SourceColumnDesc::simple("o_timestamp_0", DataType::Timestamp, ColumnId::from(5)),
                SourceColumnDesc::simple("o_timestamp_6", DataType::Timestamp, ColumnId::from(6)),
                SourceColumnDesc::simple(
                    "o_timestampz_0",
                    DataType::Timestamptz,
                    ColumnId::from(7),
                ),
                SourceColumnDesc::simple(
                    "o_timestampz_6",
                    DataType::Timestamptz,
                    ColumnId::from(8),
                ),
                SourceColumnDesc::simple("o_interval", DataType::Interval, ColumnId::from(9)),
                SourceColumnDesc::simple("o_date", DataType::Date, ColumnId::from(10)),
            ]
        }

        // schema for numeric-type test
        fn get_numeric_test_columns() -> Vec<SourceColumnDesc> {
            vec![
                SourceColumnDesc::simple("o_key", DataType::Int32, ColumnId::from(0)),
                SourceColumnDesc::simple("o_smallint", DataType::Int16, ColumnId::from(1)),
                SourceColumnDesc::simple("o_integer", DataType::Int32, ColumnId::from(2)),
                SourceColumnDesc::simple("o_bigint", DataType::Int64, ColumnId::from(3)),
                SourceColumnDesc::simple("o_real", DataType::Float32, ColumnId::from(4)),
                SourceColumnDesc::simple("o_double", DataType::Float64, ColumnId::from(5)),
                SourceColumnDesc::simple("o_numeric", DataType::Decimal, ColumnId::from(6)),
                SourceColumnDesc::simple("o_numeric_6_3", DataType::Decimal, ColumnId::from(7)),
                SourceColumnDesc::simple("o_money", DataType::Decimal, ColumnId::from(8)),
            ]
        }

        // schema for the remaining types
        fn get_other_types_test_columns() -> Vec<SourceColumnDesc> {
            vec![
                SourceColumnDesc::simple("o_key", DataType::Int32, ColumnId::from(0)),
                SourceColumnDesc::simple("o_boolean", DataType::Boolean, ColumnId::from(1)),
                SourceColumnDesc::simple("o_bit", DataType::Boolean, ColumnId::from(2)),
                SourceColumnDesc::simple("o_bytea", DataType::Bytea, ColumnId::from(3)),
                SourceColumnDesc::simple("o_json", DataType::Jsonb, ColumnId::from(4)),
                SourceColumnDesc::simple("o_xml", DataType::Varchar, ColumnId::from(5)),
                SourceColumnDesc::simple("o_uuid", DataType::Varchar, ColumnId::from(6)),
                SourceColumnDesc {
                    name: "o_point".to_owned(),
                    data_type: postgres_point_type(),
                    column_id: 7.into(),
                    column_type: SourceColumnType::Normal,
                    is_pk: false,
                    is_hidden_addition_col: false,
                    additional_column: AdditionalColumn { column_type: None },
                },
                SourceColumnDesc::simple("o_enum", DataType::Varchar, ColumnId::from(8)),
                SourceColumnDesc::simple("o_char", DataType::Varchar, ColumnId::from(9)),
                SourceColumnDesc::simple("o_varchar", DataType::Varchar, ColumnId::from(10)),
                SourceColumnDesc::simple("o_character", DataType::Varchar, ColumnId::from(11)),
                SourceColumnDesc::simple(
                    "o_character_varying",
                    DataType::Varchar,
                    ColumnId::from(12),
                ),
            ]
        }

        #[tokio::test]
        async fn test_temporal_types() {
            // this test includes all supported temporal types, with the schema
            // CREATE TABLE orders (
            //     o_key integer,
            //     o_time_0 time(0),
            //     o_time_6 time(6),
            //     o_timez_0 time(0) with time zone,
            //     o_timez_6 time(6) with time zone,
            //     o_timestamp_0 timestamp(0),
            //     o_timestamp_6 timestamp(6),
            //     o_timestampz_0 timestamp(0) with time zone,
            //     o_timestampz_6 timestamp(6) with time zone,
            //     o_interval interval,
            //     o_date date,
            //     PRIMARY KEY (o_key)
            // );
            // this test covers an insert event on the table above
            let data = br#"{"payload":{"before":null,"after":{"o_key":0,"o_time_0":40271000000,"o_time_6":40271000010,"o_timez_0":"11:11:11Z","o_timez_6":"11:11:11.00001Z","o_timestamp_0":1321009871000,"o_timestamp_6":1321009871123456,"o_timestampz_0":"2011-11-11T03:11:11Z","o_timestampz_6":"2011-11-11T03:11:11.123456Z","o_interval":"P1Y2M3DT4H5M6.78S","o_date":"1999-09-09"},"source":{"version":"1.9.7.Final","connector":"postgresql","name":"RW_CDC_localhost.test.orders","ts_ms":1684733351963,"snapshot":"last","db":"test","sequence":"[null,\"26505352\"]","schema":"public","table":"orders","txId":729,"lsn":26505352,"xmin":null},"op":"r","ts_ms":1684733352110,"transaction":null}}"#;
            let columns = get_temporal_test_columns();
            let parser = build_parser(columns.clone()).await;
            let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();
            assert_eq!(op, Op::Insert);
            assert!(row[0].eq(&Some(ScalarImpl::Int32(0))));
            assert!(row[1].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(11, 11, 11, 0).unwrap()
            )))));
            assert!(row[2].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(11, 11, 11, 10).unwrap()
            )))));
            assert!(row[3].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(11, 11, 11, 0).unwrap()
            )))));
            assert!(row[4].eq(&Some(ScalarImpl::Time(Time::new(
                NaiveTime::from_hms_micro_opt(11, 11, 11, 10).unwrap()
            )))));
            assert!(row[5].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
                "2011-11-11T11:11:11".parse().unwrap()
            )))));
            assert!(row[6].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
                "2011-11-11T11:11:11.123456".parse().unwrap()
            )))));
            assert!(
                row[9].eq(&Some(ScalarImpl::Interval(Interval::from_month_day_usec(
                    14,
                    3,
                    14706780000
                ))))
            );
            assert!(row[10].eq(&Some(ScalarImpl::Date(Date::new(
                NaiveDate::from_ymd_opt(1999, 9, 9).unwrap()
            )))));
        }

        #[tokio::test]
        async fn test_numeric_types() {
            // this test includes all supported numeric types, with the schema
            // CREATE TABLE orders (
            //     o_key integer,
            //     o_smallint smallint,
            //     o_integer integer,
            //     o_bigint bigint,
            //     o_real real,
            //     o_double double precision,
            //     o_numeric numeric,
            //     o_numeric_6_3 numeric(6,3),
            //     o_money money,
            //     PRIMARY KEY (o_key)
            // );
            // this test covers an insert event on the table above
            let data = br#"{"payload":{"before":null,"after":{"o_key":0,"o_smallint":32767,"o_integer":2147483647,"o_bigint":9223372036854775807,"o_real":9.999,"o_double":9.999999,"o_numeric":123456.789,"o_numeric_6_3":123.456,"o_money":123.12},"source":{"version":"1.9.7.Final","connector":"postgresql","name":"RW_CDC_localhost.test.orders","ts_ms":1684404343201,"snapshot":"last","db":"test","sequence":"[null,\"26519216\"]","schema":"public","table":"orders","txId":729,"lsn":26519216,"xmin":null},"op":"r","ts_ms":1684404343349,"transaction":null}}"#;
            let columns = get_numeric_test_columns();
            let parser = build_parser(columns.clone()).await;
            let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();
            assert_eq!(op, Op::Insert);
            assert!(row[0].eq(&Some(ScalarImpl::Int32(0))));
            assert!(row[1].eq(&Some(ScalarImpl::Int16(32767))));
            assert!(row[2].eq(&Some(ScalarImpl::Int32(2147483647))));
            assert!(row[3].eq(&Some(ScalarImpl::Int64(9223372036854775807))));
            assert!(row[4].eq(&Some(ScalarImpl::Float32((9.999).into()))));
            assert!(row[5].eq(&Some(ScalarImpl::Float64((9.999999).into()))));
            assert!(row[6].eq(&Some(ScalarImpl::Decimal("123456.7890".parse().unwrap()))));
            assert!(row[7].eq(&Some(ScalarImpl::Decimal("123.456".parse().unwrap()))));
            assert!(row[8].eq(&Some(ScalarImpl::Decimal("123.12".parse().unwrap()))));
        }

        #[tokio::test]
        async fn test_other_types() {
            // this test includes the remaining types, with the schema
            // CREATE TABLE orders (
            //     o_key integer,
            //     o_boolean boolean,
            //     o_bit bit,
            //     o_bytea bytea,
            //     o_json jsonb,
            //     o_xml xml,
            //     o_uuid uuid,
            //     o_point point,
            //     o_enum bear,
            //     o_char char,
            //     o_varchar varchar,
            //     o_character character,
            //     o_character_varying character varying,
            //     PRIMARY KEY (o_key)
            //  );
            // this test covers an insert event on the table above
            let data = br#"{"payload":{"before":null,"after":{"o_key":1,"o_boolean":false,"o_bit":true,"o_bytea":"ASNFZ4mrze8=","o_json":"{\"k1\": \"v1\", \"k2\": 11}","o_xml":"<!--hahaha-->","o_uuid":"60f14fe2-f857-404a-b586-3b5375b3259f","o_point":{"x":1.0,"y":2.0,"wkb":"AQEAAAAAAAAAAADwPwAAAAAAAABA","srid":null},"o_enum":"polar","o_char":"h","o_varchar":"ha","o_character":"h","o_character_varying":"hahaha"},"source":{"version":"1.9.7.Final","connector":"postgresql","name":"RW_CDC_localhost.test.orders","ts_ms":1684743927178,"snapshot":"last","db":"test","sequence":"[null,\"26524528\"]","schema":"public","table":"orders","txId":730,"lsn":26524528,"xmin":null},"op":"r","ts_ms":1684743927343,"transaction":null}}"#;
            let columns = get_other_types_test_columns();
            let parser = build_parser(columns.clone()).await;
            let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
                .await
                .try_into()
                .unwrap();
            assert_eq!(op, Op::Insert);
            assert!(row[0].eq(&Some(ScalarImpl::Int32(1))));
            assert!(row[1].eq(&Some(ScalarImpl::Bool(false))));
            assert!(row[2].eq(&Some(ScalarImpl::Bool(true))));
            assert!(row[3].eq(&Some(ScalarImpl::Bytea(Box::new([
                u8::from_str_radix("01", 16).unwrap(),
                u8::from_str_radix("23", 16).unwrap(),
                u8::from_str_radix("45", 16).unwrap(),
                u8::from_str_radix("67", 16).unwrap(),
                u8::from_str_radix("89", 16).unwrap(),
                u8::from_str_radix("AB", 16).unwrap(),
                u8::from_str_radix("CD", 16).unwrap(),
                u8::from_str_radix("EF", 16).unwrap()
            ])))));
            assert_json_eq(&row[4], "{\"k1\": \"v1\", \"k2\": 11}");
            assert!(row[5].eq(&Some(ScalarImpl::Utf8("<!--hahaha-->".into()))));
            assert!(row[6].eq(&Some(ScalarImpl::Utf8(
                "60f14fe2-f857-404a-b586-3b5375b3259f".into()
            ))));
            assert!(row[7].eq(&Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Float64(1.into())),
                Some(ScalarImpl::Float64(2.into()))
            ])))));
            assert!(row[8].eq(&Some(ScalarImpl::Utf8("polar".into()))));
            assert!(row[9].eq(&Some(ScalarImpl::Utf8("h".into()))));
            assert!(row[10].eq(&Some(ScalarImpl::Utf8("ha".into()))));
            assert!(row[11].eq(&Some(ScalarImpl::Utf8("h".into()))));
            assert!(row[12].eq(&Some(ScalarImpl::Utf8("hahaha".into()))));
        }
    }
}
