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

use risingwave_common::bail;
use thiserror_ext::AsReport;

use super::unified::kv_event::KvEvent;
use super::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, SourceStreamChunkRowWriter,
    SpecificParserConfig,
};
use crate::error::ConnectorResult;
use crate::parser::bytes_parser::BytesAccessBuilder;
use crate::parser::simd_json_parser::DebeziumJsonAccessBuilder;
use crate::parser::unified::AccessImpl;
use crate::parser::unified::debezium::{parse_schema_change, parse_transaction_meta};
use crate::parser::upsert_parser::get_key_column_name;
use crate::parser::{BytesProperties, JsonProperties, ParseResult, ParserFormat};
use crate::source::cdc::CdcMessageType;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef, SourceMeta};

/// Parser for `FORMAT PLAIN`, i.e., append-only source.
#[derive(Debug)]
pub struct PlainParser {
    pub key_builder: Option<AccessBuilderImpl>,
    pub payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    pub source_ctx: SourceContextRef,
    // parsing transaction metadata for shared cdc source
    pub transaction_meta_builder: Option<AccessBuilderImpl>,
    pub schema_change_builder: Option<AccessBuilderImpl>,
}

impl PlainParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let key_builder = if let Some(key_column_name) = get_key_column_name(&rw_columns) {
            Some(AccessBuilderImpl::Bytes(BytesAccessBuilder::new(
                EncodingProperties::Bytes(BytesProperties {
                    column_name: Some(key_column_name),
                }),
            )?))
        } else {
            None
        };

        let payload_builder = match props.encoding_config {
            EncodingProperties::Json(_)
            | EncodingProperties::Protobuf(_)
            | EncodingProperties::Avro(_)
            | EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::new_default(props.encoding_config).await?
            }
            _ => bail!("Unsupported encoding for Plain"),
        };

        let transaction_meta_builder = Some(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new(JsonProperties::default())?,
        ));

        let schema_change_builder = Some(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new_for_schema_event()?,
        ));

        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
            transaction_meta_builder,
            schema_change_builder,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<ParseResult> {
        // plain parser also used in the shared cdc source,
        // we need to handle transaction metadata and schema change messages here
        if let Some(msg_meta) = writer.row_meta()
            && let SourceMeta::DebeziumCdc(cdc_meta) = msg_meta.source_meta
            && let Some(data) = payload
        {
            match cdc_meta.msg_type {
                CdcMessageType::Data | CdcMessageType::Heartbeat => {
                    return self.parse_rows(key, Some(data), writer).await;
                }
                CdcMessageType::TransactionMeta => {
                    let accessor = self
                        .transaction_meta_builder
                        .as_mut()
                        .expect("expect transaction metadata access builder")
                        .generate_accessor(data, writer.source_meta())
                        .await?;
                    return match parse_transaction_meta(&accessor, &self.source_ctx.connector_props)
                    {
                        Ok(transaction_control) => {
                            Ok(ParseResult::TransactionControl(transaction_control))
                        }
                        Err(err) => Err(err)?,
                    };
                }
                CdcMessageType::SchemaChange => {
                    let accessor = self
                        .schema_change_builder
                        .as_mut()
                        .expect("expect schema change access builder")
                        .generate_accessor(data, writer.source_meta())
                        .await?;

                    return match parse_schema_change(
                        &accessor,
                        self.source_ctx.source_id,
                        &self.source_ctx.source_name,
                        &self.source_ctx.connector_props,
                    )
                    .await
                    {
                        Ok(schema_change) => Ok(ParseResult::SchemaChange(schema_change)),
                        Err(err) => {
                            // Report CDC auto schema change fail event
                            let (fail_info, table_name, cdc_table_id) = match &err {
                                crate::parser::AccessError::CdcAutoSchemaChangeError {
                                    ty,
                                    table_name,
                                    ..
                                } => {
                                    // Parse table_name format: "schema"."table" -> schema.table
                                    let clean_table_name =
                                        table_name.trim_matches('"').replace("\".\"", ".");
                                    let fail_info = format!(
                                        "Unsupported data type '{}' in source '{}' table '{}'",
                                        ty, self.source_ctx.source_name, clean_table_name
                                    );
                                    // Build cdc_table_id: source_name.schema.table_name
                                    let cdc_table_id = format!(
                                        "{}.{}",
                                        self.source_ctx.source_name, clean_table_name
                                    );

                                    (fail_info, clean_table_name, cdc_table_id)
                                }
                                _ => {
                                    let fail_info = format!(
                                        "Failed to parse schema change: {:?}, source: {}",
                                        err.as_report(),
                                        self.source_ctx.source_name
                                    );
                                    (fail_info, "".to_owned(), "".to_owned())
                                }
                            };
                            self.source_ctx.on_cdc_auto_schema_change_failure(
                                self.source_ctx.source_id,
                                table_name,
                                cdc_table_id,
                                "".to_owned(), // upstream_ddl is not available in this context
                                fail_info,
                            );

                            Err(err)?
                        }
                    };
                }
                CdcMessageType::Unspecified => {
                    unreachable!()
                }
            }
        }

        // for non-cdc source messages
        self.parse_rows(key, payload, writer).await
    }

    async fn parse_rows(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<ParseResult> {
        let meta = writer.source_meta();
        let mut row_op: KvEvent<AccessImpl<'_>, AccessImpl<'_>> = KvEvent::default();

        if let Some(data) = key
            && let Some(key_builder) = self.key_builder.as_mut()
        {
            // key is optional in format plain
            row_op.with_key(key_builder.generate_accessor(data, meta).await?);
        }
        if let Some(data) = payload {
            // the data part also can be an empty vec
            row_op.with_value(self.payload_builder.generate_accessor(data, meta).await?);
        }

        writer.do_insert(|column: &SourceColumnDesc| row_op.access_field::<false>(column))?;

        Ok(ParseResult::Rows)
    }
}

impl ByteStreamSourceParser for PlainParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Plain
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

    use expect_test::expect;
    use futures::executor::block_on;
    use futures::{StreamExt, TryStreamExt};
    use futures_async_stream::try_stream;
    use itertools::Itertools;
    use risingwave_common::catalog::ColumnCatalog;
    use risingwave_pb::connector_service::{SourceType, cdc_message};

    use super::*;
    use crate::parser::{MessageMeta, SourceStreamChunkBuilder, TransactionControl};
    use crate::source::cdc::DebeziumCdcMeta;
    use crate::source::{
        ConnectorProperties, SourceCtrlOpts, SourceMessage, SourceMessageEvent, SourceReaderEvent,
        SplitId,
    };

    #[tokio::test]
    async fn test_emit_transactional_chunk() {
        let schema = ColumnCatalog::debezium_cdc_source_cols();

        let columns = schema
            .iter()
            .map(|c| SourceColumnDesc::from(&c.column_desc))
            .collect::<Vec<_>>();

        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::PostgresCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let source_ctx = Arc::new(source_ctx);
        // format plain encode json parser
        let parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            source_ctx.clone(),
        )
        .await
        .unwrap();

        let mut transactional = false;
        // for untransactional source, we expect emit a chunk for each message batch
        let message_stream = source_message_stream(transactional);
        let chunk_stream = crate::parser::parse_message_stream(
            parser,
            message_stream.map_ok(SourceMessageEvent::Data).boxed(),
            SourceCtrlOpts::for_test(),
        );
        let output: std::result::Result<Vec<_>, _> = block_on(chunk_stream.try_collect::<Vec<_>>());
        let output = output
            .unwrap()
            .into_iter()
            .filter_map(|event| match event {
                SourceReaderEvent::DataChunk(chunk) if chunk.cardinality() > 0 => Some(chunk),
                SourceReaderEvent::DataChunk(_) | SourceReaderEvent::SplitProgress(_) => None,
            })
            .enumerate()
            .map(|(i, c)| {
                if i == 0 {
                    // begin + 3 data messages
                    assert_eq!(4, c.cardinality());
                }
                if i == 1 {
                    // 2 data messages + 1 end
                    assert_eq!(3, c.cardinality());
                }
                c
            })
            .collect_vec();

        // 2 chunks for 2 message batches
        assert_eq!(2, output.len());

        // format plain encode json parser
        let parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            source_ctx,
        )
        .await
        .unwrap();

        // for transactional source, we expect emit a single chunk for the transaction
        transactional = true;
        let message_stream = source_message_stream(transactional);
        let chunk_stream = crate::parser::parse_message_stream(
            parser,
            message_stream.map_ok(SourceMessageEvent::Data).boxed(),
            SourceCtrlOpts::for_test(),
        );
        let output: std::result::Result<Vec<_>, _> = block_on(chunk_stream.try_collect::<Vec<_>>());
        let output = output
            .unwrap()
            .into_iter()
            .filter_map(|event| match event {
                SourceReaderEvent::DataChunk(chunk) if chunk.cardinality() > 0 => Some(chunk),
                SourceReaderEvent::DataChunk(_) | SourceReaderEvent::SplitProgress(_) => None,
            })
            .inspect(|c| {
                // 5 data messages in a single chunk
                assert_eq!(5, c.cardinality());
            })
            .collect_vec();

        // a single transactional chunk
        assert_eq!(1, output.len());
    }

    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn source_message_stream(transactional: bool) {
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"35352:3962948040","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"35352:3962950064","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;
        let data_batches = [
            vec![
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a1", "order_date": "2020-01-30", "order_id": 10021, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a2", "order_date": "2020-02-30", "order_id": 10022, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a3", "order_date": "2020-03-30", "order_id": 10023, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
            ],
            vec![
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a4", "order_date": "2020-04-30", "order_id": 10024, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a5", "order_date": "2020-05-30", "order_id": 10025, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
            ],
        ];
        for (i, batch) in data_batches.iter().enumerate() {
            let mut source_msg_batch = vec![];
            if i == 0 {
                // put begin message at first
                source_msg_batch.push(SourceMessage {
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                        "orders".to_owned(),
                        0,
                        if transactional {
                            cdc_message::CdcMessageType::TransactionMeta
                        } else {
                            cdc_message::CdcMessageType::Data
                        },
                        SourceType::Unspecified,
                    )),
                    split_id: SplitId::from("1001"),
                    offset: "0".into(),
                    key: None,
                    payload: Some(begin_msg.as_bytes().to_vec()),
                });
            }
            // put data messages
            for data_msg in batch {
                source_msg_batch.push(SourceMessage {
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                        "orders".to_owned(),
                        0,
                        cdc_message::CdcMessageType::Data,
                        SourceType::Unspecified,
                    )),
                    split_id: SplitId::from("1001"),
                    offset: "0".into(),
                    key: None,
                    payload: Some(data_msg.as_bytes().to_vec()),
                });
            }
            if i == data_batches.len() - 1 {
                // put commit message at last
                source_msg_batch.push(SourceMessage {
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                        "orders".to_owned(),
                        0,
                        if transactional {
                            cdc_message::CdcMessageType::TransactionMeta
                        } else {
                            cdc_message::CdcMessageType::Data
                        },
                        SourceType::Unspecified,
                    )),
                    split_id: SplitId::from("1001"),
                    offset: "0".into(),
                    key: None,
                    payload: Some(commit_msg.as_bytes().to_vec()),
                });
            }
            yield source_msg_batch;
        }
    }

    #[tokio::test]
    async fn test_parse_transaction_metadata() {
        let schema = ColumnCatalog::debezium_cdc_source_cols();

        let columns = schema
            .iter()
            .map(|c| SourceColumnDesc::from(&c.column_desc))
            .collect::<Vec<_>>();

        // format plain encode json parser
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::MysqlCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            Arc::new(source_ctx),
        )
        .await
        .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        // "id":"35352:3962948040" Postgres transaction ID itself and LSN of given operation separated by colon, i.e. the format is txID:LSN
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"3E11FA47-71CA-11E1-9E33-C80AA9429562:23","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"3E11FA47-71CA-11E1-9E33-C80AA9429562:23","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;

        let cdc_meta = SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            "orders".to_owned(),
            0,
            cdc_message::CdcMessageType::TransactionMeta,
            SourceType::Unspecified,
        ));
        let msg_meta = MessageMeta {
            source_meta: &cdc_meta,
            split_id: "1001",
            offset: "",
        };

        let expect_tx_id = "3E11FA47-71CA-11E1-9E33-C80AA9429562:23";
        let res = parser
            .parse_one_with_txn(
                None,
                Some(begin_msg.as_bytes().to_vec()),
                builder.row_writer().with_meta(msg_meta),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Begin { id })) => {
                assert_eq!(id.deref(), expect_tx_id);
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
        let res = parser
            .parse_one_with_txn(
                None,
                Some(commit_msg.as_bytes().to_vec()),
                builder.row_writer().with_meta(msg_meta),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Commit { id })) => {
                assert_eq!(id.deref(), expect_tx_id);
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }

        builder.finish_current_chunk();
        assert!(builder.consume_ready_chunks().next().is_none());
    }

    #[tokio::test]
    async fn test_parse_schema_change() {
        let schema = ColumnCatalog::debezium_cdc_source_cols();

        let columns = schema
            .iter()
            .map(|c| SourceColumnDesc::from(&c.column_desc))
            .collect::<Vec<_>>();

        // format plain encode json parser
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::MysqlCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            Arc::new(source_ctx),
        )
        .await
        .unwrap();
        let mut dummy_builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        let msg = r#"{"schema":null,"payload": { "databaseName": "mydb", "ddl": "ALTER TABLE test add column v2 varchar(32)", "schemaName": null, "source": { "connector": "mysql", "db": "mydb", "file": "binlog.000065", "gtid": null, "name": "RW_CDC_0", "pos": 234, "query": null, "row": 0, "sequence": null, "server_id": 1, "snapshot": "false", "table": "test", "thread": null, "ts_ms": 1718354727000, "version": "2.4.2.Final" }, "tableChanges": [ { "id": "\"mydb\".\"test\"", "table": { "columns": [ { "autoIncremented": false, "charsetName": null, "comment": null, "defaultValueExpression": null, "enumValues": null, "generated": false, "jdbcType": 4, "length": null, "name": "id", "nativeType": null, "optional": false, "position": 1, "scale": null, "typeExpression": "INT", "typeName": "INT" }, { "autoIncremented": false, "charsetName": null, "comment": null, "defaultValueExpression": null, "enumValues": null, "generated": false, "jdbcType": 2014, "length": null, "name": "v1", "nativeType": null, "optional": true, "position": 2, "scale": null, "typeExpression": "TIMESTAMP", "typeName": "TIMESTAMP" }, { "autoIncremented": false, "charsetName": "utf8mb4", "comment": null, "defaultValueExpression": null, "enumValues": null, "generated": false, "jdbcType": 12, "length": 32, "name": "v2", "nativeType": null, "optional": true, "position": 3, "scale": null, "typeExpression": "VARCHAR", "typeName": "VARCHAR" } ], "comment": null, "defaultCharsetName": "utf8mb4", "primaryKeyColumnNames": [ "id" ] }, "type": "ALTER" } ], "ts_ms": 1718354727594 }}"#;
        let cdc_meta = SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            "mydb.test".to_owned(),
            0,
            cdc_message::CdcMessageType::SchemaChange,
            SourceType::Mysql,
        ));
        let msg_meta = MessageMeta {
            source_meta: &cdc_meta,
            split_id: "1001",
            offset: "",
        };

        let res = parser
            .parse_one_with_txn(
                None,
                Some(msg.as_bytes().to_vec()),
                dummy_builder.row_writer().with_meta(msg_meta),
            )
            .await;

        let res = res.unwrap();
        expect![[r#"
            SchemaChange(
                SchemaChangeEnvelope {
                    table_changes: [
                        TableSchemaChange {
                            cdc_table_id: "0.mydb.test",
                            columns: [
                                ColumnCatalog {
                                    column_desc: ColumnDesc {
                                        data_type: Int32,
                                        column_id: #2147483646,
                                        name: "id",
                                        generated_or_default_column: None,
                                        description: None,
                                        additional_column: AdditionalColumn {
                                            column_type: None,
                                        },
                                        version: Pr13707,
                                        system_column: None,
                                        nullable: true,
                                    },
                                    is_hidden: false,
                                },
                                ColumnCatalog {
                                    column_desc: ColumnDesc {
                                        data_type: Timestamptz,
                                        column_id: #2147483646,
                                        name: "v1",
                                        generated_or_default_column: None,
                                        description: None,
                                        additional_column: AdditionalColumn {
                                            column_type: None,
                                        },
                                        version: Pr13707,
                                        system_column: None,
                                        nullable: true,
                                    },
                                    is_hidden: false,
                                },
                                ColumnCatalog {
                                    column_desc: ColumnDesc {
                                        data_type: Varchar,
                                        column_id: #2147483646,
                                        name: "v2",
                                        generated_or_default_column: None,
                                        description: None,
                                        additional_column: AdditionalColumn {
                                            column_type: None,
                                        },
                                        version: Pr13707,
                                        system_column: None,
                                        nullable: true,
                                    },
                                    is_hidden: false,
                                },
                            ],
                            change_type: Alter,
                            upstream_ddl: "ALTER TABLE test add column v2 varchar(32)",
                        },
                    ],
                },
            )
        "#]]
        .assert_debug_eq(&res);
    }
}
