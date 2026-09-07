// Copyright 2026 RisingWave Labs
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

use std::sync::{Arc, Mutex};

use futures::FutureExt;
use iceberg::writer::PositionDeleteInput;
use risingwave_common::array::Op;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::id::SinkId;
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::test_epoch;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;

use super::*;
use crate::common::table::test_utils::gen_pbtable;
use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
use crate::executor::{IcebergPkIndexCompactionContext, Mutation, UpdateMutation};
use crate::task::LocalBarrierManager;

const CHUNK_SIZE: usize = 1024;
const TEST_FILE_PATH: &str = "file1.parquet";

fn compaction_context(
    sink_id: SinkId,
    task_id: IcebergCompactionTaskId,
    phase: Phase,
) -> IcebergPkIndexCompactionContext {
    IcebergPkIndexCompactionContext {
        sink_id,
        task_id,
        phase: phase as i32,
    }
}

struct IcebergWriterMock {
    file_path: String,
    next_offset: i64,
    written_chunks: Arc<Mutex<Vec<StreamChunk>>>,
}

impl IcebergWriterMock {
    fn new(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_owned(),
            next_offset: 0,
            written_chunks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn written_chunks(&self) -> Arc<Mutex<Vec<StreamChunk>>> {
        self.written_chunks.clone()
    }
}

#[async_trait::async_trait]
impl IcebergWriter for IcebergWriterMock {
    async fn write_chunk(
        &mut self,
        chunk: DataChunk,
    ) -> StreamExecutorResult<Vec<PositionDeleteInput>> {
        let row_count = chunk.cardinality();
        let mut positions = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            positions.push(PositionDeleteInput::new(
                Arc::<str>::from(self.file_path.as_str()),
                self.next_offset,
            ));
            self.next_offset += 1;
        }
        self.written_chunks.lock().unwrap().push(chunk.into());
        Ok(positions)
    }

    async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>> {
        Ok(None)
    }
}

async fn create_pk_index_state_table(
    store: MemoryStateStore,
    table_id: TableId,
) -> StateTable<MemoryStateStore> {
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar),
        ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
    ];
    let order_types = vec![OrderType::ascending()];
    let pk_indices = vec![0];

    StateTable::from_table_catalog_inconsistent_op(
        &gen_pbtable(table_id, column_descs, order_types, pk_indices, 0),
        store,
        None,
    )
    .await
}

fn input_schema() -> Schema {
    Schema::new(vec![
        Field::unnamed(DataType::Int64),
        Field::unnamed(DataType::Int64),
    ])
}

fn decode_chunk(chunk: StreamChunk) -> Vec<(String, i64)> {
    chunk
        .rows()
        .map(|(op, row)| {
            assert_eq!(op, Op::Insert);
            let file_path = row.datum_at(0).unwrap().into_utf8().to_owned();
            let position = row.datum_at(1).unwrap().into_int64();
            (file_path, position)
        })
        .collect()
}

fn test_file_position(position: i64) -> (String, i64) {
    (TEST_FILE_PATH.to_owned(), position)
}

struct WriterTestHarness {
    left_tx: MessageSender,
    right_tx: MessageSender,
    executor: BoxedMessageStream,
    written_chunks: Arc<Mutex<Vec<StreamChunk>>>,
}

impl WriterTestHarness {
    async fn new() -> Self {
        Self::with_schema(input_schema()).await
    }

    /// Build a harness with a custom input schema. The PK is always the first column (Int64)
    /// so the shared `create_pk_index_state_table` schema applies.
    async fn with_schema(input_schema: Schema) -> Self {
        let store = MemoryStateStore::new();
        let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
        let writer = IcebergWriterMock::new(TEST_FILE_PATH);
        let written_chunks = writer.written_chunks();

        let (left_tx, left_source) = MockSource::channel();
        let left_input = left_source.into_executor(input_schema, vec![0]);
        let (right_tx, right_source) = MockSource::channel();
        let right_input = right_source.into_executor(
            Schema::new(vec![
                Field::with_name(DataType::Int64, "pk"),
                Field::with_name(DataType::Varchar, "file_path"),
                Field::with_name(DataType::Int64, "position"),
            ]),
            vec![0],
        );
        let executor = WriterExecutor::new(
            ActorContext::for_test(123),
            left_input,
            right_input,
            vec![0],
            state_table,
            writer,
            CHUNK_SIZE,
            SinkId::new(0),
            LocalBarrierManager::for_test(),
        )
        .boxed()
        .execute();

        Self {
            left_tx,
            right_tx,
            executor,
            written_chunks,
        }
    }

    async fn init(&mut self) {
        self.left_tx.push_barrier(test_epoch(1), false);
        self.right_tx.push_barrier(test_epoch(1), false);
        self.executor.expect_barrier().await;
    }

    fn push_chunk(&mut self, chunk: StreamChunk) {
        self.left_tx.push_chunk(chunk);
    }

    fn push_pretty_chunk(&mut self, pretty: &str) {
        self.push_chunk(StreamChunk::from_pretty(pretty));
    }

    fn push_barrier(&mut self, epoch: u64) {
        self.left_tx.push_barrier(test_epoch(epoch), false);
        self.right_tx.push_barrier(test_epoch(epoch), false);
    }

    fn push_left_compaction_barrier(&mut self, epoch: u64, task_id: u64, phase: Phase) {
        self.left_tx.send_barrier(
            Barrier::new_test_barrier(test_epoch(epoch)).with_iceberg_pk_index_compaction(
                compaction_context(SinkId::new(0), task_id.into(), phase),
            ),
        );
    }

    fn push_right_barrier(&mut self, epoch: u64, task_id: u64, phase: Phase) {
        self.right_tx.send_barrier(
            Barrier::new_test_barrier(test_epoch(epoch)).with_iceberg_pk_index_compaction(
                compaction_context(SinkId::new(0), task_id.into(), phase),
            ),
        );
    }

    fn push_compaction_begin(&mut self, epoch: u64, task_id: u64) {
        self.push_left_compaction_barrier(epoch, task_id, Phase::Begin);
        self.push_right_barrier(epoch, task_id, Phase::Begin);
    }

    fn push_compaction_seal(&mut self, epoch: u64, task_id: u64) {
        self.push_right_barrier(epoch, task_id, Phase::End);
        self.push_left_compaction_barrier(epoch, task_id, Phase::End);
    }

    fn push_resolver_chunk(&mut self, pretty: &str) {
        self.right_tx.push_chunk(StreamChunk::from_pretty(pretty));
    }

    async fn expect_barrier(&mut self) {
        self.executor.expect_barrier().await;
    }

    async fn expect_position_chunk(&mut self, expected: Vec<(String, i64)>) {
        assert_eq!(decode_chunk(self.executor.expect_chunk().await), expected);
    }

    fn written_chunks(&self) -> Vec<StreamChunk> {
        self.written_chunks.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn test_writer_executor_insert_only() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10
            + 2 20
            + 3 30",
    );
    harness.push_barrier(2);

    harness.expect_barrier().await;
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
                + 1 10
                + 2 20
                + 3 30",
        )]
    );
}

#[tokio::test]
async fn test_writer_executor_insert_then_delete() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10
            + 2 20
            + 3 30",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
            - 2 20",
    );
    harness.push_barrier(3);

    harness
        .expect_position_chunk(vec![test_file_position(1)])
        .await;
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_update_rewrites_position() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
            U- 1 10
            U+ 1 99",
    );
    harness.push_barrier(3);

    harness
        .expect_position_chunk(vec![test_file_position(0)])
        .await;
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
            - 1 99",
    );
    harness.push_barrier(4);

    harness
        .expect_position_chunk(vec![test_file_position(1)])
        .await;
    harness.expect_barrier().await;

    assert_eq!(
        harness.written_chunks(),
        vec![
            StreamChunk::from_pretty(
                " I I
                    + 1 10",
            ),
            StreamChunk::from_pretty(
                " I I
                    + 1 99",
            ),
        ]
    );
}

#[tokio::test]
async fn test_writer_executor_delete_then_insert_without_existing_row_is_fresh_insert() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            - 1 10
            + 1 99",
    );
    harness.push_barrier(2);

    harness.expect_barrier().await;
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
                + 1 99",
        )]
    );
}

#[tokio::test]
async fn test_writer_executor_delete_then_insert_rewrites_existing_row() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
            - 1 10
            + 1 99",
    );
    harness.push_barrier(3);

    harness
        .expect_position_chunk(vec![test_file_position(0)])
        .await;
    harness.expect_barrier().await;

    assert_eq!(
        harness.written_chunks(),
        vec![
            StreamChunk::from_pretty(
                " I I
                    + 1 10",
            ),
            StreamChunk::from_pretty(
                " I I
                    + 1 99",
            ),
        ]
    );
}

/// Two deletes for the same PK within one chunk are inconsistent input: the PK is derived from
/// the upstream stream key, which guarantees uniqueness within a chunk. The writer panics on
/// compaction rather than silently swallowing the duplicate.
#[tokio::test]
#[should_panic(expected = "inconsistency happened")]
async fn test_writer_executor_duplicate_delete_in_same_chunk_panics() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
            - 1 10
            - 1 10",
    );
    harness.push_barrier(3);

    // Processing the duplicate-delete chunk panics during compaction.
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_insert_then_delete_in_different_chunks_same_checkpoint() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10",
    );
    harness.push_pretty_chunk(
        " I I
            - 1 10",
    );
    harness.push_barrier(2);

    harness
        .expect_position_chunk(vec![test_file_position(0)])
        .await;
    harness.expect_barrier().await;
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
                + 1 10",
        )]
    );
}

/// Two inserts for the same PK within one chunk are inconsistent input: the upstream stream key
/// guarantees PK uniqueness within a chunk, so the writer panics on compaction.
#[tokio::test]
#[should_panic(expected = "inconsistency happened")]
async fn test_writer_executor_duplicate_insert_in_same_chunk_panics() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10
            + 1 99",
    );
    harness.push_barrier(2);

    // Processing the duplicate-insert chunk panics during compaction.
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_insert_then_delete_in_same_chunk_is_cancelled() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
            + 1 10
            - 1 10",
    );
    harness.push_barrier(2);

    harness.expect_barrier().await;
    assert!(harness.written_chunks().is_empty());
}

#[tokio::test]
async fn test_writer_executor_normal_watermark_does_not_poll_right_input() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.left_tx.push_int64_watermark(0, 42);
    assert!(matches!(
        harness.executor.next().now_or_never(),
        Some(Some(Ok(Message::Watermark(_))))
    ));

    harness.push_barrier(2);
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_rejects_mismatched_ordinary_update_mutation() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    let mut left_update = UpdateMutation::default();
    left_update.dropped_actors.insert(ActorId::new(456));
    let mut right_update = UpdateMutation::default();
    right_update.dropped_actors.insert(ActorId::new(789));
    let left =
        Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Update(left_update));
    let right =
        Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Update(right_update));
    harness.left_tx.send_barrier(left);
    harness.right_tx.send_barrier(right);

    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("mismatched left/right barriers"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_accepts_equivalent_unordered_update_mutations() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    let mut left_update = UpdateMutation::default();
    left_update
        .dropped_actors
        .extend([ActorId::new(456), ActorId::new(789), ActorId::new(1011)]);
    let mut right_update = UpdateMutation::default();
    right_update
        .dropped_actors
        .extend([ActorId::new(1011), ActorId::new(789), ActorId::new(456)]);
    let left =
        Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Update(left_update));
    let right =
        Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Update(right_update));
    harness.left_tx.send_barrier(left);
    harness.right_tx.send_barrier(right);
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_stray_seal_in_normal() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_compaction_seal(2, 7);

    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("unexpected End in Normal mode"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_allows_other_sink_seal_in_normal() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    let barrier = Barrier::new_test_barrier(test_epoch(2)).with_iceberg_pk_index_compaction(
        compaction_context(SinkId::new(99), 7.into(), Phase::End),
    );
    harness.left_tx.send_barrier(barrier.clone());
    harness.right_tx.send_barrier(barrier);
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_left_watermark_before_b2() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_compaction_begin(2, 7);
    harness.expect_barrier().await;
    harness.push_right_barrier(3, 7, Phase::End);
    harness.left_tx.push_int64_watermark(0, 42);

    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string()
            .contains("received watermark on left input while draining"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_applies_right_survivors_before_left_e1_delete() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_pretty_chunk(
        " I I
          + 1 10",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    harness.push_compaction_begin(3, 7);
    harness.expect_barrier().await;
    harness.push_pretty_chunk(
        " I I
          - 1 10",
    );
    harness.push_resolver_chunk(
        " I T              I
          + 1 output.parquet 100",
    );
    harness.push_compaction_seal(4, 7);

    harness
        .expect_position_chunk(vec![("output.parquet".to_owned(), 100)])
        .await;
    harness.expect_barrier().await;
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
              + 1 10",
        )]
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_does_not_poll_left_before_right_b2() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_compaction_begin(2, 7);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
          + 1 10",
    );
    assert!(harness.executor.next().now_or_never().is_none());
    assert!(harness.written_chunks().is_empty());

    harness.push_right_barrier(3, 7, Phase::End);
    assert!(harness.executor.next().now_or_never().is_none());
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
              + 1 10",
        )]
    );
    harness.push_left_compaction_barrier(3, 7, Phase::End);
    harness.expect_barrier().await;
}

#[tokio::test]
async fn test_writer_executor_compaction_forwards_b2_and_immediately_processes_e2() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_compaction_begin(2, 7);
    harness.expect_barrier().await;
    harness.push_compaction_seal(3, 7);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
          + 1 10",
    );
    harness.push_barrier(4);
    harness.expect_barrier().await;
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
              + 1 10",
        )]
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_mismatched_left_and_right_b2() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_compaction_begin(2, 7);
    harness.expect_barrier().await;
    harness.push_right_barrier(3, 7, Phase::End);
    harness.push_left_compaction_barrier(3, 8, Phase::End);

    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("mismatched left/right barriers"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_right_eof_before_b2() {
    let store = MemoryStateStore::new();
    let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
    let writer = IcebergWriterMock::new(TEST_FILE_PATH);
    let (mut left_tx, left_source) = MockSource::channel();
    let (mut right_tx, right_source) = MockSource::channel();
    let left_input = left_source.into_executor(input_schema(), vec![0]);
    let right_input = right_source
        .stop_on_finish(false)
        .into_executor(Schema::empty().clone(), vec![]);
    let mut executor = WriterExecutor::new(
        ActorContext::for_test(123),
        left_input,
        right_input,
        vec![0],
        state_table,
        writer,
        CHUNK_SIZE,
        SinkId::new(0),
        LocalBarrierManager::for_test(),
    )
    .boxed()
    .execute();

    left_tx.push_barrier(test_epoch(1), false);
    right_tx.push_barrier(test_epoch(1), false);
    executor.expect_barrier().await;
    let begin = Barrier::new_test_barrier(test_epoch(2)).with_iceberg_pk_index_compaction(
        compaction_context(SinkId::new(0), 7.into(), Phase::Begin),
    );
    left_tx.send_barrier(begin.clone());
    right_tx.send_barrier(begin);
    executor.expect_barrier().await;
    drop(right_tx);

    let err = executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("right input closed before End"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_left_eof_before_b2() {
    let store = MemoryStateStore::new();
    let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
    let writer = IcebergWriterMock::new(TEST_FILE_PATH);
    let (mut left_tx, left_source) = MockSource::channel();
    let (mut right_tx, right_source) = MockSource::channel();
    let left_input = left_source
        .stop_on_finish(false)
        .into_executor(input_schema(), vec![0]);
    let right_input = right_source.into_executor(Schema::empty().clone(), vec![]);
    let mut executor = WriterExecutor::new(
        ActorContext::for_test(123),
        left_input,
        right_input,
        vec![0],
        state_table,
        writer,
        CHUNK_SIZE,
        SinkId::new(0),
        LocalBarrierManager::for_test(),
    )
    .boxed()
    .execute();

    left_tx.push_barrier(test_epoch(1), false);
    right_tx.push_barrier(test_epoch(1), false);
    executor.expect_barrier().await;
    let begin = Barrier::new_test_barrier(test_epoch(2)).with_iceberg_pk_index_compaction(
        compaction_context(SinkId::new(0), 7.into(), Phase::Begin),
    );
    left_tx.send_barrier(begin.clone());
    right_tx.send_barrier(begin);
    executor.expect_barrier().await;

    right_tx.send_barrier(
        Barrier::new_test_barrier(test_epoch(3)).with_iceberg_pk_index_compaction(
            compaction_context(SinkId::new(0), 7.into(), Phase::End),
        ),
    );
    drop(left_tx);

    let err = executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("left input closed before End"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_two_consecutive_compactions_reuse_same_state_table_handle() {
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_pretty_chunk(
        " I I
          + 1 10",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    harness.push_compaction_begin(3, 7);
    harness.expect_barrier().await;
    harness.push_resolver_chunk(
        " I T                I
          + 1 output-1.parquet 100",
    );
    harness.push_compaction_seal(4, 7);
    harness.expect_barrier().await;

    harness.push_compaction_begin(5, 8);
    harness.expect_barrier().await;
    harness.push_resolver_chunk(
        " I T                I
          + 1 output-2.parquet 200",
    );
    harness.push_compaction_seal(6, 8);
    harness.expect_barrier().await;

    harness.push_pretty_chunk(
        " I I
          - 1 10",
    );
    harness.push_barrier(7);
    harness
        .expect_position_chunk(vec![("output-2.parquet".to_owned(), 200)])
        .await;
    harness.expect_barrier().await;
}
