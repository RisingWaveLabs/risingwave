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

use std::collections::BTreeMap;

use risingwave_common::bail;

use super::simd_json_parser::DebeziumJsonAccessBuilder;
use super::{DebeziumAvroAccessBuilder, DebeziumAvroParserConfig};
use crate::error::ConnectorResult;
use crate::parser::unified::debezium::DebeziumChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType, ParseResult,
    ParserFormat, ProtocolProperties, SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,

    props: DebeziumProps,
}

pub const DEBEZIUM_IGNORE_KEY: &str = "ignore_key";

#[derive(Debug, Clone, Default)]
pub struct DebeziumProps {
    // Ignore the key part of the message.
    // If enabled, we don't take the key part into message accessor.
    pub ignore_key: bool,
}

impl DebeziumProps {
    pub fn from(props: &BTreeMap<String, String>) -> Self {
        let ignore_key = props
            .get(DEBEZIUM_IGNORE_KEY)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self { ignore_key }
    }
}

async fn build_accessor_builder(
    config: EncodingProperties,
    encoding_type: EncodingType,
) -> ConnectorResult<AccessBuilderImpl> {
    match config {
        EncodingProperties::Avro(_) => {
            let config = DebeziumAvroParserConfig::new(config).await?;
            Ok(AccessBuilderImpl::DebeziumAvro(
                DebeziumAvroAccessBuilder::new(config, encoding_type)?,
            ))
        }
        EncodingProperties::Json(json_config) => Ok(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new(json_config)?,
        )),
        _ => bail!("unsupported encoding for Debezium"),
    }
}

impl DebeziumParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let key_builder =
            build_accessor_builder(props.encoding_config.clone(), EncodingType::Key).await?;
        let payload_builder =
            build_accessor_builder(props.encoding_config, EncodingType::Value).await?;
        let debezium_props = if let ProtocolProperties::Debezium(props) = props.protocol_config {
            props
        } else {
            unreachable!(
                "expecting Debezium protocol properties but got {:?}",
                props.protocol_config
            )
        };
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
            props: debezium_props,
        })
    }

    pub async fn new_for_test(rw_columns: Vec<SourceColumnDesc>) -> ConnectorResult<Self> {
        use crate::parser::JsonProperties;

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
                timestamp_handling: None,
                time_handling: None,
                bigint_unsigned_handling: None,
                handle_toast_columns: false,
                numeric_string_parsing: false,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        Self::new(props, rw_columns, SourceContext::dummy().into()).await
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<ParseResult> {
        let meta = writer.source_meta();
        // tombetone messages are handled implicitly by these accessors
        let key_accessor = match (key, self.props.ignore_key) {
            (None, false) => None,
            (Some(data), false) => Some(self.key_builder.generate_accessor(data, meta).await?),
            (_, true) => None,
        };
        let payload_accessor = match payload {
            None => None,
            Some(data) => Some(self.payload_builder.generate_accessor(data, meta).await?),
        };
        let row_op = DebeziumChangeEvent::new(key_accessor, payload_accessor);

        match apply_row_operation_on_stream_chunk_writer(&row_op, &mut writer) {
            Ok(_) => Ok(ParseResult::Rows),
            Err(err) => {
                // Only try to access transaction control message if the row operation access failed
                // to make it a fast path.
                if let Some(transaction_control) =
                    row_op.transaction_control(&self.source_ctx.connector_props)
                {
                    Ok(ParseResult::TransactionControl(transaction_control))
                } else {
                    Err(err)?
                }
            }
        }
    }
}

impl ByteStreamSourceParser for DebeziumParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Debezium
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        _payload: Option<Vec<u8>>,
        _writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<()> {
        unreachable!("should call `parse_one_with_txn` instead")
    }

    async fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<ParseResult> {
        self.parse_inner(key, payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use risingwave_common::array::Op;
    use risingwave_common::catalog::{CDC_SOURCE_COLUMN_NUM, ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, Timestamptz};
    use risingwave_pb::plan_common::{
        AdditionalColumn, AdditionalColumnTimestamp, additional_column,
    };

    use super::*;
    use crate::parser::{JsonProperties, SourceStreamChunkBuilder, TransactionControl};
    use crate::source::{ConnectorProperties, SourceCtrlOpts};

    #[tokio::test]
    async fn test_parse_transaction_metadata() {
        let schema = ColumnCatalog::debezium_cdc_source_cols();

        let columns = schema
            .iter()
            .map(|c| SourceColumnDesc::from(&c.column_desc))
            .collect::<Vec<_>>();

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
                timestamp_handling: None,
                time_handling: None,
                bigint_unsigned_handling: None,
                handle_toast_columns: false,
                numeric_string_parsing: false,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::PostgresCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut dummy_builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        // "id":"35352:3962948040" Postgres transaction ID itself and LSN of given operation separated by colon, i.e. the format is txID:LSN
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"35352:3962948040","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"35352:3962950064","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;
        let res = parser
            .parse_one_with_txn(
                None,
                Some(begin_msg.as_bytes().to_vec()),
                dummy_builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Begin { id })) => {
                assert_eq!(id.deref(), "35352");
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
        let res = parser
            .parse_one_with_txn(
                None,
                Some(commit_msg.as_bytes().to_vec()),
                dummy_builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Commit { id })) => {
                assert_eq!(id.deref(), "35352");
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
    }

    /// Verifies that committed Oracle create, update, and delete events decode `NUMBER` values
    /// emitted as JSON strings into RisingWave integer and decimal types while preserving the
    /// accompanying `RAW` and `VARCHAR2` values.
    #[tokio::test]
    async fn test_parse_oracle_row_changes_with_numeric_strings() {
        let columns = [
            ColumnDesc::named("ID", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("QUANTITY", ColumnId::new(2), DataType::Int32),
            ColumnDesc::named("AMOUNT", ColumnId::new(3), DataType::Decimal),
            ColumnDesc::named("PAYLOAD", ColumnId::new(4), DataType::Bytea),
            ColumnDesc::named("NAME", ColumnId::new(5), DataType::Varchar),
        ];
        let columns = columns
            .iter()
            .map(SourceColumnDesc::from)
            .collect::<Vec<_>>();
        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
                timestamp_handling: None,
                time_handling: None,
                bigint_unsigned_handling: None,
                handle_toast_columns: false,
                numeric_string_parsing: true,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::OracleCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());
        let events = [
            // INSERT INTO APP.CUSTOMERS (ID, QUANTITY, AMOUNT, PAYLOAD, NAME)
            // VALUES (1, 7, 123.45, HEXTORAW('010203'), 'before');
            // COMMIT;
            r#"{"payload":{"before":null,"after":{"ID":"1","QUANTITY":"7","AMOUNT":"123.45","PAYLOAD":"AQID","NAME":"before"},"source":{"connector":"oracle","ts_ms":1788480000000,"schema":"APP","table":"CUSTOMERS"},"op":"c","ts_ms":1788480000010}}"#,
            // UPDATE APP.CUSTOMERS
            // SET QUANTITY = 8, AMOUNT = 456.78, PAYLOAD = HEXTORAW('040506'), NAME = 'after'
            // WHERE ID = 1;
            // COMMIT;
            r#"{"payload":{"before":{"ID":"1","QUANTITY":"7","AMOUNT":"123.45","PAYLOAD":"AQID","NAME":"before"},"after":{"ID":"1","QUANTITY":"8","AMOUNT":"456.78","PAYLOAD":"BAUG","NAME":"after"},"source":{"connector":"oracle","ts_ms":1788480001000,"schema":"APP","table":"CUSTOMERS"},"op":"u","ts_ms":1788480001010}}"#,
            // DELETE FROM APP.CUSTOMERS WHERE ID = 1;
            // COMMIT;
            r#"{"payload":{"before":{"ID":"1","QUANTITY":"8","AMOUNT":"456.78","PAYLOAD":"BAUG","NAME":"after"},"after":null,"source":{"connector":"oracle","ts_ms":1788480002000,"schema":"APP","table":"CUSTOMERS"},"op":"d","ts_ms":1788480002010}}"#,
        ];

        for event in events {
            let result = parser
                .parse_one_with_txn(None, Some(event.as_bytes().to_vec()), builder.row_writer())
                .await
                .unwrap();
            assert!(matches!(result, ParseResult::Rows));
        }

        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
        assert_eq!(chunk.ops(), &[Op::Insert, Op::Insert, Op::Delete]);
        let rows = chunk
            .rows()
            .map(|(_, row)| row.to_owned_row())
            .collect::<Vec<_>>();
        assert_eq!(rows[0][0], Some(ScalarImpl::Int64(1)));
        assert_eq!(rows[0][1], Some(ScalarImpl::Int32(7)));
        assert_eq!(
            rows[0][2],
            Some(ScalarImpl::Decimal("123.45".parse().unwrap()))
        );
        assert_eq!(rows[0][3], Some(ScalarImpl::Bytea(vec![1, 2, 3].into())));
        assert_eq!(rows[1][1], Some(ScalarImpl::Int32(8)));
        assert_eq!(
            rows[1][2],
            Some(ScalarImpl::Decimal("456.78".parse().unwrap()))
        );
        assert_eq!(rows[2], rows[1]);
    }

    #[tokio::test]
    async fn test_parse_additional_columns() {
        let columns = [
            ColumnDesc::named("O_ORDERKEY", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("O_CUSTKEY", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("O_ORDERSTATUS", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("O_TOTALPRICE", ColumnId::new(4), DataType::Decimal),
            ColumnDesc::named("O_ORDERDATE", ColumnId::new(5), DataType::Date),
            ColumnDesc::named_with_additional_column(
                "commit_ts",
                ColumnId::new(6),
                DataType::Timestamptz,
                AdditionalColumn {
                    column_type: Some(additional_column::ColumnType::Timestamp(
                        AdditionalColumnTimestamp {},
                    )),
                },
            ),
        ];

        let columns = columns
            .iter()
            .map(SourceColumnDesc::from)
            .collect::<Vec<_>>();

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
                timestamp_handling: None,
                time_handling: None,
                bigint_unsigned_handling: None,
                handle_toast_columns: false,
                numeric_string_parsing: false,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::PostgresCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        let payload = r#"{ "payload": { "before": null, "after": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" }, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null }, "op": "c", "ts_ms": 1695277757017, "transaction": null } }"#;

        let res = parser
            .parse_one_with_txn(
                None,
                Some(payload.as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::Rows) => {
                builder.finish_current_chunk();
                let chunk = builder.consume_ready_chunks().next().unwrap();
                for (_, row) in chunk.rows() {
                    let commit_ts = row.datum_at(5).unwrap().into_timestamptz();
                    assert_eq!(commit_ts, Timestamptz::from_millis(1695277757000).unwrap());
                }
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cdc_source_job_schema() {
        let columns = ColumnCatalog::debezium_cdc_source_cols();
        // make sure it doesn't broken by future PRs
        assert_eq!(CDC_SOURCE_COLUMN_NUM, columns.len() as u32);
    }
}
