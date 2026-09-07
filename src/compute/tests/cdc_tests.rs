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

#![feature(coroutines)]

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch_executors::{Executor as BatchExecutor, RowSeqScanExecutor, ScanRange};
use risingwave_common::array::{
    Array, ArrayBuilder, DataChunk, DataChunkTestExt, Op, StreamChunk, Utf8ArrayBuilder,
};
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, JsonbVal, ScalarImpl};
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_connector::source::cdc::external::{
    DebeziumOffset, DebeziumSourceOffset, ExternalCdcTableType, ExternalTableConfig,
    SchemaTableName,
};
use risingwave_connector::source::cdc::{
    CdcScanOptions, CdcTableSnapshotSplitAssignmentWithGeneration, DebeziumCdcSplit,
};
use risingwave_connector::source::{CdcTableSnapshotSplitRaw, SplitImpl};
use risingwave_hummock_sdk::test_batch_query_epoch;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::{MessageSender, MockSource};
use risingwave_stream::executor::{
    ActorContext, AddMutation, Barrier, BoxedMessageStream, CdcBackfillExecutor, Execute,
    Executor as StreamExecutor, ExecutorInfo, ExternalStorageTable, MaterializeExecutor, Message,
    Mutation, ParallelizedCdcBackfillExecutor, StreamExecutorError, UpdateMutation,
    expect_first_barrier,
};
use risingwave_stream::task::ActorId;

// mock upstream binlog offset starting from "1.binlog, pos=0"
pub struct MockOffsetGenExecutor {
    upstream: Option<StreamExecutor>,

    start_offset: u32,
}

impl MockOffsetGenExecutor {
    pub fn new(upstream: StreamExecutor) -> Self {
        Self {
            upstream: Some(upstream),
            start_offset: 0,
        }
    }

    fn next_offset(&mut self) -> anyhow::Result<String> {
        let start_offset = self.start_offset;
        let dbz_offset = DebeziumOffset {
            source_partition: Default::default(),
            source_offset: DebeziumSourceOffset {
                last_snapshot_record: None,
                snapshot: None,
                file: Some("1.binlog".to_owned()),
                pos: Some(start_offset as _),
                lsn: None,
                lsn_commit: None,
                lsn_proc: None,
                txid: None,
                tx_usec: None,
                change_lsn: None,
                commit_lsn: None,
            },
            is_heartbeat: false,
        };

        self.start_offset += 1;
        let out = serde_json::to_string(&dbz_offset)?;
        Ok(out)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // fake offset in stream chunk
        let mut upstream = self.upstream.take().unwrap().execute();

        // The first barrier mush propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Chunk(chunk) => {
                    let mut offset_builder = Utf8ArrayBuilder::new(chunk.cardinality());
                    assert!(chunk.is_vis_compacted());
                    let (ops, mut columns, vis) = chunk.into_inner();

                    for _ in 0..ops.len() {
                        let offset_str = self.next_offset()?;
                        offset_builder.append(Some(&offset_str));
                    }

                    let offsets = offset_builder.finish();
                    columns.push(offsets.into_ref());
                    yield Message::Chunk(StreamChunk::with_visibility(ops, columns, vis));
                }
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => yield Message::Watermark(watermark),
            }
        }
    }
}

impl Execute for MockOffsetGenExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[tokio::test]
async fn test_cdc_backfill() -> StreamResult<()> {
    let memory_state_store = MemoryStateStore::new();

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(
        Schema::new(vec![
            Field::unnamed(DataType::Jsonb), // payload
        ]),
        vec![0],
    );

    // mock upstream offset (start from "1.binlog, pos=0") for ingested chunks
    let mock_offset_executor = StreamExecutor::new(
        ExecutorInfo::for_test(
            Schema::new(vec![
                Field::unnamed(DataType::Jsonb),   // payload
                Field::unnamed(DataType::Varchar), // _rw_offset
            ]),
            vec![0],
            "MockOffsetGenExecutor".to_owned(),
            0,
        ),
        MockOffsetGenExecutor::new(source).boxed(),
    );

    let table_name = SchemaTableName {
        schema_name: "public".to_owned(),
        table_name: "mock_table".to_owned(),
    };
    let table_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "id"), // primary key
        Field::with_name(DataType::Float64, "price"),
    ]);
    let table_pk_indices = vec![0];
    let table_pk_order_types = vec![OrderType::ascending()];
    let config = ExternalTableConfig::default();

    let external_table = ExternalStorageTable::new(
        TableId::new(1234),
        table_name,
        "mydb".to_owned(),
        config,
        ExternalCdcTableType::Mock,
        table_schema.clone(),
        table_pk_order_types,
        table_pk_indices.clone(),
    );

    let actor_id = 0x1a;

    // create state table
    let state_schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "split_id"),
        Field::with_name(DataType::Int64, "id"), // pk
        Field::with_name(DataType::Boolean, "backfill_finished"),
        Field::with_name(DataType::Int64, "row_count"),
        Field::with_name(DataType::Jsonb, "cdc_offset"),
    ]);

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), state_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), state_schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), state_schema[2].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(3), state_schema[3].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(4), state_schema[4].data_type.clone()),
    ];

    let state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::from(0x42),
            column_descs,
            vec![OrderType::ascending()],
            vec![0],
            0,
        ),
        memory_state_store.clone(),
        None,
    )
    .await;

    let output_columns = vec![
        ColumnDesc::named("id", ColumnId::new(1), DataType::Int64), // primary key
        ColumnDesc::named("price", ColumnId::new(2), DataType::Float64),
    ];

    let cdc_backfill = StreamExecutor::new(
        ExecutorInfo::for_test(
            table_schema.clone(),
            table_pk_indices,
            "CdcBackfillExecutor".to_owned(),
            0,
        ),
        CdcBackfillExecutor::new(
            ActorContext::for_test(actor_id),
            external_table,
            mock_offset_executor,
            vec![0, 1],
            output_columns,
            None,
            Arc::new(StreamingMetrics::unused()),
            state_table,
            Some(4), // limit a snapshot chunk to have <= 4 rows by rate limit
            CdcScanOptions {
                disable_backfill: false,
                snapshot_barrier_interval: 1,
                snapshot_batch_size: 4,
                ..Default::default()
            },
            BTreeMap::default(),
        )
        .boxed(),
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let materialize_table_id = TableId::new(5678);
    let mut materialize = MaterializeExecutor::for_test(
        cdc_backfill,
        memory_state_store.clone(),
        materialize_table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        vec![0.into(), 1.into()],
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::Overwrite,
    )
    .await
    .boxed()
    .execute();

    // construct upstream chunks
    let chunk1_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 10.01}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 2, "price": 22.22}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 3, "price": 3.03}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 4, "price": 4.04}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 5, "price": 5.05}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 6.06}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];

    let chunk2_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 11.11}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 10.08}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 199, "price": 40.5}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 978, "price": 72.6}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 134, "price": 41.7}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];

    let chunk1_datums: Vec<Datum> = chunk1_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();

    let chunk2_datums: Vec<Datum> = chunk2_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();

    let chunk_schema = Schema::new(vec![
        Field::unnamed(DataType::Jsonb), // payload
    ]);

    let stream_chunk1 = create_stream_chunk(chunk1_datums, &chunk_schema);
    let stream_chunk2 = create_stream_chunk(chunk2_datums, &chunk_schema);

    // The first barrier
    let mut curr_epoch = test_epoch(11);
    let mut splits = HashMap::new();
    splits.insert(
        actor_id.into(),
        vec![SplitImpl::MysqlCdc(DebeziumCdcSplit::new(0, None, None))],
    );
    let init_barrier =
        Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add(AddMutation {
            splits,
            ..Default::default()
        }));

    tx.send_barrier(init_barrier);

    // assert barrier is forwarded to mview
    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: Some(_),
            ..
        }) if epoch.curr == curr_epoch
    ));

    // start the stream pipeline src -> backfill -> mview
    let mview_handle = tokio::spawn(consume_message_stream(materialize));

    // ingest data and barrier
    let interval = Duration::from_millis(10);

    // send a dummy barrier to trigger the backfill, since
    // cdc backfill will wait for a barrier before start
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);

    // first chunk
    tx.push_chunk(stream_chunk1);

    tokio::time::sleep(interval).await;

    // barrier to trigger emit buffered events
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);

    // second chunk
    tx.push_chunk(stream_chunk2);

    tokio::time::sleep(interval).await;
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);

    tokio::time::sleep(interval).await;
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, true);

    // scan the final result of the mv table
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), table_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), table_schema[1].data_type.clone()),
    ];
    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = BatchTable::for_test(
        memory_state_store.clone(),
        materialize_table_id,
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        value_indices,
    );

    let scan = Box::new(RowSeqScanExecutor::new(
        table.clone(),
        vec![ScanRange::full()],
        true,
        test_batch_query_epoch(),
        1024,
        "RowSeqExecutor2".to_owned(),
        None,
        None,
    ));

    // check result
    let mut stream = scan.execute();
    while let Some(message) = stream.next().await {
        let chunk = message.expect("scan a chunk");
        let expect = DataChunk::from_pretty(
            "I F
                1 11.11
                2 22.22
                3 3.03
                4 4.04
                5 5.05
                6 10.08
                8 1.0008
                134 41.7
                199 40.5
                978 72.6",
        );
        assert_eq!(expect, chunk);
    }

    mview_handle.await.unwrap()?;

    Ok(())
}

fn create_stream_chunk(datums: Vec<Datum>, schema: &Schema) -> StreamChunk {
    let mut builders = schema.create_array_builders(8);
    for datum in &datums {
        builders[0].append(datum);
    }
    let columns = builders
        .into_iter()
        .map(|builder| builder.finish().into())
        .collect();

    let ops = vec![Op::Insert; datums.len()];
    StreamChunk::from_parts(ops, DataChunk::new(columns, datums.len()))
}

async fn consume_message_stream(mut stream: BoxedMessageStream) -> StreamResult<()> {
    while let Some(message) = stream.next().await {
        let message = message?;
        match message {
            Message::Watermark(_) => {
                break;
            }
            Message::Chunk(c) => {
                println!("[mv] chunk: {:#?}", c);
            }
            Message::Barrier(b) => {
                if let Some(m) = b.mutation
                    && matches!(*m, Mutation::Stop(_))
                {
                    println!("encounter stop barrier");
                    break;
                }
            }
        }
    }
    Ok(())
}

struct ParallelizedCdcBackfillTestContext {
    memory_state_store: MemoryStateStore,
    actor_id: ActorId,
    tx: MessageSender,
    materialize: BoxedMessageStream,
    materialize_table_id: TableId,
    table_schema: Schema,
}

async fn setup_parallelized_cdc_backfill_test_context() -> ParallelizedCdcBackfillTestContext {
    let memory_state_store = MemoryStateStore::new();

    let (tx, source) = MockSource::channel();
    let source = source.into_executor(Schema::new(vec![]), vec![]);
    let cdc_source = StreamExecutor::new(
        ExecutorInfo::for_test(
            Schema::new(vec![
                Field::unnamed(DataType::Jsonb),   // payload
                Field::unnamed(DataType::Varchar), // _rw_offset
            ]),
            vec![0],
            "MockOffsetGenExecutor".to_owned(),
            0,
        ),
        MockOffsetGenExecutor::new(source).boxed(),
    );
    let table_name = SchemaTableName {
        schema_name: "public".to_owned(),
        table_name: "mock_table".to_owned(),
    };
    let table_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "id"), // primary key
        Field::with_name(DataType::Float64, "price"),
    ]);
    let table_pk_indices = vec![0];
    let table_pk_order_types = vec![OrderType::ascending()];
    let config = ExternalTableConfig::default();
    let external_table = ExternalStorageTable::new(
        TableId::new(1234),
        table_name,
        "mydb".to_owned(),
        config,
        ExternalCdcTableType::Mock,
        table_schema.clone(),
        table_pk_order_types,
        table_pk_indices.clone(),
    );
    let actor_id = 0x1a.into();

    // create state table
    let state_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "split_id"),
        Field::with_name(DataType::Boolean, "backfill_finished"),
        Field::with_name(DataType::Int64, "row_count"),
    ]);
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), state_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), state_schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), state_schema[2].data_type.clone()),
    ];
    let state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::from(0x42),
            column_descs,
            vec![OrderType::ascending()],
            vec![0],
            0,
        ),
        memory_state_store.clone(),
        None,
    )
    .await;

    let output_columns = vec![
        ColumnDesc::named("id", ColumnId::new(1), DataType::Int64), // primary key
        ColumnDesc::named("price", ColumnId::new(2), DataType::Float64),
    ];
    let cdc_backfill = StreamExecutor::new(
        ExecutorInfo::for_test(
            table_schema.clone(),
            table_pk_indices,
            "ParallelizedCdcBackfillExecutor".to_owned(),
            0,
        ),
        ParallelizedCdcBackfillExecutor::new(
            ActorContext::for_test(actor_id),
            external_table,
            cdc_source,
            vec![0, 1],
            output_columns,
            Arc::new(StreamingMetrics::unused()),
            state_table,
            Some(4), // limit a snapshot chunk to have <= 4 rows by rate limit,
            CdcScanOptions {
                backfill_parallelism: 1,
                backfill_num_rows_per_split: 100,
                ..Default::default()
            },
            BTreeMap::default(),
            None,
        )
        .boxed(),
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let materialize_table_id = TableId::new(5678);
    let materialize = MaterializeExecutor::for_test(
        cdc_backfill,
        memory_state_store.clone(),
        materialize_table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        vec![0.into(), 1.into()],
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::Overwrite,
    )
    .await
    .boxed()
    .execute();
    ParallelizedCdcBackfillTestContext {
        memory_state_store,
        actor_id,
        tx,
        materialize,
        materialize_table_id,
        table_schema,
    }
}

fn parallelized_cdc_backfill_upstream_data() -> Vec<StreamChunk> {
    // construct upstream chunks
    let chunk1_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 10.01}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 2, "price": 22.22}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 3, "price": 3.03}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 1000, "price": 3.03}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 4, "price": 4.04}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 5, "price": 5.05}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 6.06}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];
    let chunk1_datums: Vec<Datum> = chunk1_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();
    let chunk2_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 11.11}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 10.08}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 199, "price": 40.5}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 978, "price": 72.6}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 134, "price": 41.7}, "source": { "version": "1.9.7.Final", "connector": "postgres", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];
    let chunk2_datums: Vec<Datum> = chunk2_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();
    let chunk_schema = Schema::new(vec![
        Field::unnamed(DataType::Jsonb), // payload
    ]);
    let stream_chunk1 = create_stream_chunk(chunk1_datums, &chunk_schema);
    let stream_chunk2 = create_stream_chunk(chunk2_datums, &chunk_schema);
    vec![stream_chunk1, stream_chunk2]
}

#[tokio::test]
async fn test_parallelized_cdc_backfill() {
    let ParallelizedCdcBackfillTestContext {
        memory_state_store,
        actor_id,
        mut tx,
        mut materialize,
        materialize_table_id,
        table_schema,
    } = setup_parallelized_cdc_backfill_test_context().await;
    let mut upstream_data = parallelized_cdc_backfill_upstream_data();
    let stream_chunk1 = upstream_data.swap_remove(0);
    let stream_chunk2 = upstream_data.swap_remove(0);

    // The first barrier to initialized CDC table snapshot splits.
    let mut curr_epoch = test_epoch(11);
    let mut source_splits = HashMap::new();
    source_splits.insert(
        actor_id,
        vec![SplitImpl::PostgresCdc(DebeziumCdcSplit::new(0, None, None))],
    );
    let splits = [(
        actor_id,
        (
            vec![CdcTableSnapshotSplitRaw {
                split_id: 1,
                left_bound_inclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(1))])
                    .value_serialize(),
                right_bound_exclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(10))])
                    .value_serialize(),
            }],
            10,
        ),
    )]
    .into_iter()
    .collect();
    let actor_cdc_table_snapshot_splits = CdcTableSnapshotSplitAssignmentWithGeneration { splits };
    let init_barrier =
        Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add(AddMutation {
            splits: source_splits,
            actor_cdc_table_snapshot_splits,
            ..Default::default()
        }));
    tx.send_barrier(init_barrier);
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Barrier(Barrier {
            epoch,
            mutation: Some(_),
            ..
        }) if epoch.curr == curr_epoch
    ));
    assert_mv(
        None,
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // The backfill executor should process 4 rows (limited by chunk size) from snapshot stream.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    assert_mv(
        None,
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;
    send_and_poll_barrier(&mut curr_epoch, &mut tx, &mut materialize).await;
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 11.00
            2 22.00
            5 1.0005",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // Push first WAL chunk. It should be buffered until split backfill is completed.
    tx.push_chunk(stream_chunk1);
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 11.00
            2 22.00
            5 1.0005",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // The backfill executor should process remaining 2 rows from snapshot stream.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    send_and_poll_chunk_then_barrier(&mut curr_epoch, &mut tx, &mut materialize).await;
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 10.01
            2 22.22
            3 3.03
            4 4.04
            5 5.05
            6 6.06
            8 1.0008",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // Push second WAL chunk. It should be processed immediately.
    tx.push_chunk(stream_chunk2);

    // The backfill executor should process second WAL chunk.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    send_and_poll_barrier(&mut curr_epoch, &mut tx, &mut materialize).await;
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 11.11
            2 22.22
            3 3.03
            4 4.04
            5 5.05
            6 10.08
            8 1.0008",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;
}

#[tokio::test]
async fn test_parallelized_cdc_backfill_reschedule() {
    let ParallelizedCdcBackfillTestContext {
        memory_state_store,
        actor_id,
        mut tx,
        mut materialize,
        materialize_table_id,
        table_schema,
    } = setup_parallelized_cdc_backfill_test_context().await;
    let mut upstream_data = parallelized_cdc_backfill_upstream_data();
    let stream_chunk1 = upstream_data.swap_remove(0);
    let stream_chunk2 = upstream_data.swap_remove(0);

    // The first barrier to initialized CDC table snapshot splits.
    let mut curr_epoch = test_epoch(11);
    let mut source_splits = HashMap::new();
    source_splits.insert(
        actor_id,
        vec![SplitImpl::PostgresCdc(DebeziumCdcSplit::new(0, None, None))],
    );
    let splits = [(
        actor_id,
        (
            vec![CdcTableSnapshotSplitRaw {
                split_id: 2,
                left_bound_inclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(1))])
                    .value_serialize(),
                right_bound_exclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(6))])
                    .value_serialize(),
            }],
            10,
        ),
    )]
    .into_iter()
    .collect();
    let actor_cdc_table_snapshot_splits = CdcTableSnapshotSplitAssignmentWithGeneration { splits };
    let init_barrier =
        Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add(AddMutation {
            splits: source_splits.clone(),
            actor_cdc_table_snapshot_splits,
            ..Default::default()
        }));
    tx.send_barrier(init_barrier);
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Barrier(Barrier {
            epoch,
            mutation: Some(_),
            ..
        }) if epoch.curr == curr_epoch
    ));
    assert_mv(
        None,
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // The backfill executor should process 4 rows (limited by chunk size) from snapshot stream.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Barrier(Barrier {
            epoch,
            ..
        }) if epoch.curr == curr_epoch
    ));
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 11.00
            2 22.00
            5 1.0005",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // Push first WAL chunk. It should be buffered until split backfill is completed.
    tx.push_chunk(stream_chunk1);

    // Send reschedule barrier.
    let splits = [(
        actor_id,
        (
            vec![
                CdcTableSnapshotSplitRaw {
                    split_id: 3,
                    left_bound_inclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(6))])
                        .value_serialize(),
                    right_bound_exclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(100))])
                        .value_serialize(),
                },
                CdcTableSnapshotSplitRaw {
                    split_id: 4,
                    left_bound_inclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(100))])
                        .value_serialize(),
                    right_bound_exclusive: OwnedRow::new(vec![Some(ScalarImpl::Int64(500))])
                        .value_serialize(),
                },
            ],
            10,
        ),
    )]
    .into_iter()
    .collect();
    let actor_cdc_table_snapshot_splits = CdcTableSnapshotSplitAssignmentWithGeneration { splits };
    curr_epoch.inc_epoch();
    let reschedule_barrier =
        Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Update(UpdateMutation {
            actor_cdc_table_snapshot_splits,
            ..Default::default()
        }));
    tx.send_barrier(reschedule_barrier);

    // Buffered WAL should have been flushed.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Barrier(Barrier {
            epoch,
            mutation: Some(_),
            ..
        }) if epoch.curr == curr_epoch
    ));
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 10.01
            2 22.22
            3 3.03
            4 4.04
            5 5.05",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // The backfill executor should process all rows of first split from snapshot stream.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    send_and_poll_barrier(&mut curr_epoch, &mut tx, &mut materialize).await;
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 10.01
            2 22.22
            3 3.03
            4 4.04
            5 5.05
            6 1.0006
            8 1.0008",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // The backfill executor should process all rows of second split from snapshot stream.
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    send_and_poll_barrier(&mut curr_epoch, &mut tx, &mut materialize).await;
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 10.01
            2 22.22
            3 3.03
            4 4.04
            5 5.05
            6 1.0006
            8 1.0008
            400 400.1",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;

    // Push second WAL chunk. It should be processed immediately.
    tx.push_chunk(stream_chunk2);
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    send_and_poll_chunk_then_barrier(&mut curr_epoch, &mut tx, &mut materialize).await;
    assert_mv(
        DataChunk::from_pretty(
            "I F
            1 10.01
            2 22.22
            3 3.03
            4 4.04
            5 5.05
            6 10.08
            8 1.0008
            134 41.7
            199 40.5
            400 400.1",
        )
        .into(),
        &table_schema,
        memory_state_store.clone(),
        materialize_table_id,
    )
    .await;
}

async fn send_and_poll_barrier(
    curr_epoch: &mut u64,
    tx: &mut MessageSender,
    materialize: &mut BoxedMessageStream,
) {
    curr_epoch.inc_epoch();
    tx.push_barrier(*curr_epoch, false);
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Barrier(Barrier {
            epoch,
            ..
        }) if epoch.curr == *curr_epoch
    ));
}

async fn send_and_poll_chunk_then_barrier(
    curr_epoch: &mut u64,
    tx: &mut MessageSender,
    materialize: &mut BoxedMessageStream,
) {
    curr_epoch.inc_epoch();
    tx.push_barrier(*curr_epoch, false);
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Chunk(_)
    ));
    assert!(matches!(
        materialize.next().await.unwrap().unwrap(),
        Message::Barrier(Barrier {
            epoch,
            ..
        }) if epoch.curr == *curr_epoch
    ));
}

async fn assert_mv(
    expect: Option<DataChunk>,
    table_schema: &Schema,
    memory_state_store: MemoryStateStore,
    materialize_table_id: TableId,
) {
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), table_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), table_schema[1].data_type.clone()),
    ];
    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = BatchTable::for_test(
        memory_state_store,
        materialize_table_id,
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        value_indices,
    );
    let scan = Box::new(RowSeqScanExecutor::new(
        table.clone(),
        vec![ScanRange::full()],
        true,
        test_batch_query_epoch(),
        1024,
        "RowSeqExecutor2".to_owned(),
        None,
        None,
    ));
    let mut stream = scan.execute();
    match expect {
        None => {
            assert!(stream.next().await.is_none());
        }
        Some(expect) => {
            let message = stream.next().await.unwrap();
            let chunk = message.unwrap();
            assert_eq!(expect, chunk);
        }
    }
}
