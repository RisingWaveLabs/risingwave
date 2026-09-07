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

use anyhow::Context;
use iceberg::writer::PositionDeleteInput;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::bail;
use risingwave_common::id::SinkId;
use risingwave_common::row::{Project, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_pb::stream_plan::iceberg_pk_index_compaction_context::Phase;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_storage::StateStore;

use crate::common::change_buffer::output_kind;
use crate::common::compact_chunk::{InconsistencyBehavior, compact_chunk_inline};
use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

type PkRow<'a> = Project<'a, RowRef<'a>>;

fn new_chunk_builder(chunk_size: usize) -> DataChunkBuilder {
    DataChunkBuilder::new(vec![DataType::Varchar, DataType::Int64], chunk_size)
}

fn append_row(builder: &mut DataChunkBuilder, file_path: &str, position: i64) -> Option<DataChunk> {
    builder.append_one_row([
        Some(ScalarRefImpl::Utf8(file_path)),
        Some(ScalarRefImpl::Int64(position)),
    ])
}

/// Input-selection state for the writer's dual-input compaction protocol.
#[derive(Debug)]
pub enum WriterInputMode {
    Normal,
    ResolvingRight {
        task_id: IcebergCompactionTaskId,
        begin_epoch: EpochPair,
    },
    DrainingLeft {
        task_id: IcebergCompactionTaskId,
        barrier: Barrier,
    },
}

/// Trait abstracting the Iceberg data file writing for testability.
///
/// Implementations are responsible for writing rows to Iceberg data files
/// and tracking row positions. Commit is handled by the executor, not the writer.
#[async_trait::async_trait]
pub trait IcebergWriter: Send + 'static {
    /// Write a batch of insert rows. Returns the position of each row in the chunk (in order).
    async fn write_chunk(
        &mut self,
        chunk: DataChunk,
    ) -> StreamExecutorResult<Vec<PositionDeleteInput>>;

    /// Flush current data files on barrier. Returns serialized commit metadata,
    /// or `None` if no data was written since the last flush.
    async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>>;
}

/// Writer Executor for iceberg pk-index sink with PK index
///
/// This stateful executor maintains a PK index that maps primary key values to
/// their position in data files (`file_path`, `position`). It processes change logs
/// from upstream:
///
/// - **Insert**: Writes the row to a data file via [`IcebergWriter`], records the
///   position in the PK index state table.
/// - **Delete**: Looks up the PK index to find the data file position, emits a
///   delete position message downstream to the position-delete merger, removes from index.
/// - **Update**: Treated as Delete + Insert. The planner guarantees the old and
///   new rows share the same PK, so the executor can reuse the projected PK from
///   the old row when updating the PK index.
pub struct WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    ctx: ActorContextRef,
    input: Option<Executor>,
    resolver_input: Option<Executor>,
    /// Column indices of the primary key in the input schema.
    pk_indices: Vec<usize>,
    /// State table storing the PK index: `pk_columns` -> (`file_path`, `position`).
    /// Schema: [`pk_col_0`, ..., `pk_col_n`, `file_path`: Varchar, `position`: Int64]
    pk_index_state_table: StateTable<S>,
    /// The Iceberg data file writer.
    writer: W,
    /// Buffer for accumulating delete position messages before the next barrier flush.
    delete_position_buffer: Option<DataChunkBuilder>,
    mode: WriterInputMode,
    chunk_size: usize,
    sink_id: SinkId,
    local_barrier_manager: LocalBarrierManager,
}

impl<S, W> WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        resolver_input: Executor,
        pk_indices: Vec<usize>,
        pk_index_state_table: StateTable<S>,
        writer: W,
        chunk_size: usize,
        sink_id: SinkId,
        local_barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            resolver_input: Some(resolver_input),
            pk_indices,
            pk_index_state_table,
            writer,
            delete_position_buffer: None,
            mode: WriterInputMode::Normal,
            chunk_size,
            sink_id,
            local_barrier_manager,
        }
    }

    async fn delete_existing_row(
        &mut self,
        pk_row: PkRow<'_>,
        delete_position_buffer: &mut DataChunkBuilder,
    ) -> StreamExecutorResult<Option<DataChunk>> {
        let Some(index_row) = self.pk_index_state_table.get_row(pk_row).await? else {
            return Ok(None);
        };

        let num_cols = index_row.len();
        let file_path = index_row
            .datum_at(num_cols - 2)
            .context("file_path should not be null")?
            .into_utf8();
        let position = index_row
            .datum_at(num_cols - 1)
            .context("position should not be null")?
            .into_int64();
        let chunk = append_row(delete_position_buffer, file_path, position);
        self.pk_index_state_table.delete(index_row);
        Ok(chunk)
    }

    // Process one stream chunk:
    //
    // 1. Compact the chunk by `pk_indices` so each PK appears at most once and any intra-chunk
    //    `+/-` cancellations are absorbed up front. After this step every record is either a
    //    standalone `Insert`, `Delete`, or `Update {old, new}` whose old and new rows share the
    //    same PK.
    // 2. For each record: `Insert` is buffered into a single batched write; `Delete` looks up
    //    `pk_index_state_table` to emit a position delete and clears the entry; `Update` is
    //    handled as a position delete for the old row plus a buffered insert for the new row.
    // 3. After the scan, write all buffered inserts in one `write_chunk` call and persist the
    //    returned Iceberg positions back to `pk_index_state_table`.
    //
    // `pk_index_state_table` and `delete_position_buffer` live until the next barrier, so a later
    // chunk in the same checkpoint observes earlier writes/deletes via the state table.
    #[try_stream(ok = DataChunk, error = StreamExecutorError)]
    async fn process_chunk(&mut self, chunk: StreamChunk) {
        let chunk = compact_chunk_inline::<{ output_kind::RETRACT }>(
            chunk,
            &self.pk_indices,
            InconsistencyBehavior::Panic,
        );

        let mut delete_position_buffer = self
            .delete_position_buffer
            .take()
            .unwrap_or_else(|| new_chunk_builder(self.chunk_size));
        let pk_indices = self.pk_indices.clone();

        // Invariant: every input column is visible and written to Iceberg verbatim. The planner
        // (`promote_iceberg_pk_index_stream_key` in `stream_sink.rs`) enforces this by promoting
        // hidden stream-key columns to visible and by not adding the extra partition column for
        // pk-index sinks, so the writer has no hidden-column projection and writes the whole row.
        // `chunk.capacity() + 1` is an upper bound on appended rows: each surviving record
        // contributes at most one row (Insert / Update::new), and `records()` yields at most
        // `capacity` records.
        let mut insert_chunk =
            DataChunkBuilder::new(chunk.data_chunk().data_types(), chunk.capacity() + 1);
        let mut insert_pks: Vec<PkRow<'_>> = Vec::new();

        for record in chunk.records() {
            match record {
                Record::Insert { new_row } => {
                    let overflow = insert_chunk.append_one_row(new_row);
                    debug_assert!(overflow.is_none(), "insert chunk exceeds capacity");
                    insert_pks.push(new_row.project(&pk_indices));
                }
                Record::Delete { old_row } => {
                    let pk_row = old_row.project(&pk_indices);
                    if let Some(chunk) = self
                        .delete_existing_row(pk_row, &mut delete_position_buffer)
                        .await?
                    {
                        yield chunk;
                    }
                }
                Record::Update { new_row, .. } => {
                    // The compactor groups by `pk_indices`, so old and new share the same PK.
                    let pk_row = new_row.project(&pk_indices);
                    if let Some(chunk) = self
                        .delete_existing_row(pk_row, &mut delete_position_buffer)
                        .await?
                    {
                        yield chunk;
                    }
                    let overflow = insert_chunk.append_one_row(new_row);
                    debug_assert!(overflow.is_none(), "insert chunk exceeds capacity");
                    insert_pks.push(pk_row);
                }
            }
        }

        if !insert_pks.is_empty() {
            let write_chunk = insert_chunk.finish();
            let positions = self.writer.write_chunk(write_chunk).await?;

            for (pk, pos) in insert_pks.into_iter().zip_eq_fast(positions) {
                let mut index_row_data = Vec::with_capacity(pk_indices.len() + 2);
                for datum in pk.iter() {
                    index_row_data.push(datum);
                }
                index_row_data.push(Some(ScalarRefImpl::Utf8(&pos.path)));
                index_row_data.push(Some(ScalarRefImpl::Int64(pos.pos)));
                self.pk_index_state_table.insert(index_row_data.as_slice());
            }
        }

        self.delete_position_buffer = Some(delete_position_buffer);
        self.pk_index_state_table.try_flush().await?;
    }

    async fn apply_resolver_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                bail!(
                    "iceberg pk-index writer {} expected resolver inserts, got {op:?}",
                    self.sink_id
                );
            }
            self.pk_index_state_table.insert(row);
        }
        self.pk_index_state_table.try_flush().await?;
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn checkpoint_barrier(&mut self, barrier: Barrier) {
        barrier.assume_no_update_vnode_bitmap(self.ctx.id)?;
        let mut metadata = None;
        if barrier.is_checkpoint() {
            if let Some(chunk) = self
                .delete_position_buffer
                .take()
                .and_then(|mut builder| builder.consume_all())
            {
                yield Message::Chunk(chunk.into());
            }
            metadata = self.writer.flush().await?;
        }

        self.pk_index_state_table
            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
            .await?;
        if let Some(metadata) = metadata
            && metadata.metadata.is_some()
        {
            self.local_barrier_manager
                .report_iceberg_pk_index_sink_metadata(
                    barrier.epoch,
                    self.sink_id,
                    self.ctx.id,
                    PbIcebergPkIndexSinkRole::Writer,
                    Some(metadata),
                );
        }
        yield Message::Barrier(barrier);
    }

    fn validate_compaction_barrier(
        &self,
        barrier: &Barrier,
        expected_task: IcebergCompactionTaskId,
        expected_phase: Phase,
        expected_prev: u64,
    ) -> StreamExecutorResult<()> {
        if !barrier.is_checkpoint() || barrier.epoch.prev != expected_prev {
            bail!(
                "iceberg pk-index writer {} expected checkpoint {:?} starting at {}, got {:?}",
                self.sink_id,
                expected_phase,
                expected_prev,
                barrier
            );
        }
        match barrier.iceberg_pk_index_compaction() {
            Some(context)
                if context.sink_id == self.sink_id
                    && context.task_id == expected_task
                    && context.phase == expected_phase as i32 =>
            {
                Ok(())
            }
            _ => bail!(
                "iceberg pk-index writer {} expected matching {:?} context for task {}, got {:?}",
                self.sink_id,
                expected_phase,
                expected_task,
                barrier
            ),
        }
    }

    fn validate_aligned_barriers(
        &self,
        left: &Barrier,
        right: &Barrier,
    ) -> StreamExecutorResult<()> {
        let context_matches = match (
            left.iceberg_pk_index_compaction(),
            right.iceberg_pk_index_compaction(),
        ) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                left.sink_id == right.sink_id
                    && left.task_id == right.task_id
                    && left.phase == right.phase
            }
            _ => false,
        };
        if left.epoch != right.epoch
            || left.kind != right.kind
            || left.mutation != right.mutation
            || !context_matches
        {
            bail!(
                "iceberg pk-index writer {} received mismatched left/right barriers: left={:?}, right={:?}",
                self.sink_id,
                left,
                right
            );
        }
        Ok(())
    }

    fn compaction_begin(
        &self,
        barrier: &Barrier,
    ) -> StreamExecutorResult<Option<IcebergCompactionTaskId>> {
        let context = match barrier.iceberg_pk_index_compaction() {
            Some(context) if context.sink_id == self.sink_id => context,
            _ => return Ok(None),
        };
        if context.phase == Phase::End as i32 {
            bail!(
                "iceberg pk-index writer {} received unexpected End in Normal mode for task {}",
                self.sink_id,
                context.task_id
            );
        }
        if context.phase != Phase::Begin as i32 {
            bail!(
                "iceberg pk-index writer {} expected Begin context for task {}, got {:?}",
                self.sink_id,
                context.task_id,
                context.phase
            );
        }
        Ok(Some(context.task_id))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();
        let mut resolver_input = self.resolver_input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        let remap_first = expect_first_barrier(&mut resolver_input).await?;
        self.validate_aligned_barriers(&barrier, &remap_first)?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.pk_index_state_table.init_epoch(first_epoch).await?;

        loop {
            let mode = std::mem::replace(&mut self.mode, WriterInputMode::Normal);
            match mode {
                WriterInputMode::Normal => {
                    let mut completed = false;
                    #[for_await]
                    for msg in self.execute_normal(&mut input, &mut resolver_input, &mut completed)
                    {
                        yield msg?;
                    }
                    if completed {
                        break;
                    }
                }
                WriterInputMode::ResolvingRight {
                    task_id,
                    begin_epoch,
                } => {
                    #[for_await]
                    for msg in
                        self.execute_resolving_right(&mut resolver_input, task_id, begin_epoch)
                    {
                        yield msg?;
                    }
                }
                WriterInputMode::DrainingLeft { task_id, barrier } => {
                    #[for_await]
                    for msg in self.execute_draining_left(&mut input, task_id, barrier) {
                        yield msg?;
                    }
                }
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_normal<'a>(
        &'a mut self,
        input: &'a mut BoxedMessageStream,
        resolver_input: &'a mut BoxedMessageStream,
        completed: &'a mut bool,
    ) {
        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) =>
                {
                    #[for_await]
                    for chunk in self.process_chunk(chunk) {
                        yield Message::Chunk(chunk?.into());
                    }
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
                Message::Barrier(barrier) => {
                    let msg = next_msg(resolver_input).await?;
                    let remap_barrier = match msg {
                        Message::Barrier(b) => b,
                        _ => bail!(
                            "iceberg pk-index writer {} expected barrier on remap input, got {msg:?}",
                            self.sink_id
                        ),
                    };
                    self.validate_aligned_barriers(&barrier, &remap_barrier)?;
                    let begin = self.compaction_begin(&barrier)?;
                    let epoch = barrier.epoch;
                    #[for_await]
                    for msg in self.checkpoint_barrier(barrier) {
                        yield msg?;
                    }

                    if let Some(task_id) = begin {
                        self.mode = WriterInputMode::ResolvingRight {
                            task_id,
                            begin_epoch: epoch,
                        };
                        return Ok(());
                    }
                }
            }
        }

        *completed = true;
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_resolving_right<'a>(
        &'a mut self,
        resolver_input: &'a mut BoxedMessageStream,
        task_id: IcebergCompactionTaskId,
        begin_epoch: EpochPair,
    ) {
        #[for_await]
        for msg in resolver_input {
            match msg? {
                Message::Chunk(chunk) => {
                    self.apply_resolver_chunk(chunk).await?;
                }
                Message::Watermark(_) => bail!(
                    "iceberg pk-index writer {} received watermark on remap input while resolving task {}",
                    self.sink_id,
                    task_id
                ),
                Message::Barrier(barrier) => {
                    self.validate_compaction_barrier(
                        &barrier,
                        task_id,
                        Phase::End,
                        begin_epoch.curr,
                    )?;
                    self.mode = WriterInputMode::DrainingLeft { task_id, barrier };
                    return Ok(());
                }
            }
        }

        bail!(
            "iceberg pk-index writer {} right input closed before End for task {}",
            self.sink_id,
            task_id
        );
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_draining_left<'a>(
        &'a mut self,
        input: &'a mut BoxedMessageStream,
        task_id: IcebergCompactionTaskId,
        expected: Barrier,
    ) {
        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) =>
                {
                    #[for_await]
                    for chunk in self.process_chunk(chunk) {
                        yield Message::Chunk(chunk?.into());
                    }
                }
                Message::Watermark(_) => bail!(
                    "iceberg pk-index writer {} received watermark on left input while draining task {}",
                    self.sink_id,
                    task_id
                ),
                Message::Barrier(barrier) => {
                    self.validate_aligned_barriers(&barrier, &expected)?;
                    #[for_await]
                    for msg in self.checkpoint_barrier(barrier) {
                        yield msg?;
                    }
                    self.mode = WriterInputMode::Normal;
                    return Ok(());
                }
            }
        }

        bail!(
            "iceberg pk-index writer {} left input closed before End for task {}",
            self.sink_id,
            task_id
        );
    }
}

impl<S, W> Execute for WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

async fn next_msg(input: &mut BoxedMessageStream) -> StreamExecutorResult<Message> {
    input
        .next()
        .await
        .ok_or_else(|| {
            StreamExecutorError::channel_closed(
                "iceberg pk-index writer input channel closed unexpectedly",
            )
        })
        .flatten()
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod tests;
