// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;

use risingwave_common::array::Op;
use risingwave_common::array::stream_record::Record;
use risingwave_common::gap_fill::{
    FillStrategy, apply_interpolation_step, calculate_interpolation_step,
};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, RowExt};
use risingwave_common::types::{CheckedAdd, Interval, ToOwnedDatum};
use risingwave_expr::ExprError;
use risingwave_expr::expr::NonStrictExpression;
use tracing::warn;

use crate::executor::prelude::*;

pub struct EowcGapFillExecutor<S: StateStore> {
    input: Executor,
    inner: ExecutorInner<S>,
}

pub struct EowcGapFillExecutorArgs<S: StateStore> {
    pub actor_ctx: ActorContextRef,

    pub input: Executor,

    pub schema: Schema,
    pub prev_row_table: StateTable<S>,
    pub chunk_size: usize,
    pub time_column_index: usize,
    pub fill_columns: HashMap<usize, FillStrategy>,
    pub gap_interval: NonStrictExpression,
    pub high_gap_fill_amplification_threshold: usize,
    pub partition_by_indices: Vec<usize>,
}

pub struct GapFillMetrics {
    pub gap_fill_generated_rows_count: LabelGuardedIntCounter,
}

struct GapFillGenerationContext<'a> {
    metrics: &'a GapFillMetrics,
    high_amplification_threshold: usize,
    actor_ctx: &'a ActorContextRef,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,

    schema: Schema,
    prev_row_table: StateTable<S>,
    chunk_size: usize,
    time_column_index: usize,
    fill_columns: HashMap<usize, FillStrategy>,
    gap_interval: NonStrictExpression,
    high_gap_fill_amplification_threshold: usize,
    partition_by_indices: Vec<usize>,
    partition_col_mask: Vec<bool>,

    // Metrics
    metrics: GapFillMetrics,
}

struct ExecutionVars {
    staging_prev_rows: HashMap<OwnedRow, OwnedRow>,
}

impl<S: StateStore> ExecutorInner<S> {
    fn generate_filled_rows(
        prev_row: &OwnedRow,
        curr_row: &OwnedRow,
        time_column_index: usize,
        partition_col_mask: &[bool],
        fill_columns: &HashMap<usize, FillStrategy>,
        interval: risingwave_common::types::Interval,
        generation_context: &GapFillGenerationContext<'_>,
    ) -> Result<Vec<OwnedRow>, ExprError> {
        let mut filled_rows = Vec::new();
        let (Some(prev_time_scalar), Some(curr_time_scalar)) = (
            prev_row.datum_at(time_column_index),
            curr_row.datum_at(time_column_index),
        ) else {
            return Ok(filled_rows);
        };

        let prev_time = match prev_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!(
                    "Failed to convert time column to timestamp, got {:?}. Skipping gap fill.",
                    prev_time_scalar
                );
                return Ok(filled_rows);
            }
        };

        let curr_time = match curr_time_scalar {
            ScalarRefImpl::Timestamp(ts) => ts,
            ScalarRefImpl::Timestamptz(ts) => {
                match risingwave_common::types::Timestamp::with_micros(ts.timestamp_micros()) {
                    Ok(timestamp) => timestamp,
                    Err(_) => {
                        warn!("Failed to convert timestamptz to timestamp: {:?}", ts);
                        return Ok(filled_rows);
                    }
                }
            }
            _ => {
                warn!(
                    "Failed to convert time column to timestamp, got {:?}. Skipping gap fill.",
                    curr_time_scalar
                );
                return Ok(filled_rows);
            }
        };
        if prev_time >= curr_time {
            return Ok(filled_rows);
        }

        let mut fill_time = match prev_time.checked_add(interval) {
            Some(t) => t,
            None => {
                return Ok(filled_rows);
            }
        };
        if fill_time >= curr_time {
            return Ok(filled_rows);
        }

        // Calculate the number of rows to fill
        let mut row_count = 0;
        let mut temp_time = fill_time;
        while temp_time < curr_time {
            row_count += 1;
            temp_time = match temp_time.checked_add(interval) {
                Some(t) => t,
                None => break,
            };
        }

        // Pre-compute interpolation steps for each column that requires interpolation
        let mut interpolation_steps: Vec<Option<ScalarImpl>> = Vec::new();
        let mut interpolation_states: Vec<Datum> = Vec::new();

        for i in 0..prev_row.len() {
            if let Some(strategy) = fill_columns.get(&i) {
                if matches!(strategy, FillStrategy::Interpolate) {
                    let step = calculate_interpolation_step(
                        prev_row.datum_at(i),
                        curr_row.datum_at(i),
                        row_count + 1,
                    );
                    interpolation_steps.push(step.clone());
                    interpolation_states.push(prev_row.datum_at(i).to_owned_datum());
                } else {
                    interpolation_steps.push(None);
                    interpolation_states.push(None);
                }
            } else {
                interpolation_steps.push(None);
                interpolation_states.push(None);
            }
        }

        // Generate filled rows, applying the appropriate strategy for each column
        while fill_time < curr_time {
            let mut new_row_data = Vec::with_capacity(prev_row.len());

            for col_idx in 0..prev_row.len() {
                let datum = if col_idx == time_column_index {
                    // Time column: use the incremented timestamp
                    let fill_time_scalar = match prev_time_scalar {
                        ScalarRefImpl::Timestamp(_) => ScalarImpl::Timestamp(fill_time),
                        ScalarRefImpl::Timestamptz(_) => {
                            let micros = fill_time.0.and_utc().timestamp_micros();
                            ScalarImpl::Timestamptz(
                                risingwave_common::types::Timestamptz::from_micros_uncheck(micros),
                            )
                        }
                        _ => unreachable!("Time column should be Timestamp or Timestamptz"),
                    };
                    Some(fill_time_scalar)
                } else if partition_col_mask[col_idx] {
                    prev_row.datum_at(col_idx).to_owned_datum()
                } else if let Some(strategy) = fill_columns.get(&col_idx) {
                    // Apply the fill strategy for this column
                    match strategy {
                        FillStrategy::Locf => prev_row.datum_at(col_idx).to_owned_datum(),
                        FillStrategy::Null => None,
                        FillStrategy::Interpolate => {
                            // Apply interpolation step and update cumulative value
                            if let Some(step) = &interpolation_steps[col_idx] {
                                apply_interpolation_step(&mut interpolation_states[col_idx], step);
                                interpolation_states[col_idx].clone()
                            } else {
                                // If interpolation step is None, fill with NULL
                                None
                            }
                        }
                    }
                } else {
                    // No strategy specified, default to NULL
                    None
                };
                new_row_data.push(datum);
            }

            filled_rows.push(OwnedRow::new(new_row_data));

            fill_time = match fill_time.checked_add(interval) {
                Some(t) => t,
                None => {
                    // Time overflow during iteration, stop filling
                    warn!(
                        "Gap fill stopped due to timestamp overflow after generating {} rows.",
                        filled_rows.len()
                    );
                    break;
                }
            };
        }

        // Update metrics with the number of generated rows
        generation_context
            .metrics
            .gap_fill_generated_rows_count
            .inc_by(filled_rows.len() as u64);

        if filled_rows.len() > generation_context.high_amplification_threshold {
            tracing::warn!(target: "high_gap_fill_amplification",
                generated_rows_len = filled_rows.len(),
                prev_time = ?prev_time,
                curr_time = ?curr_time,
                gap_interval = ?interval,
                actor_id = %generation_context.actor_ctx.id,
                fragment_id = %generation_context.actor_ctx.fragment_id,
                "large rows generated by gap fill"
            );
        }

        Ok(filled_rows)
    }
}

impl<S: StateStore> Execute for EowcGapFillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> EowcGapFillExecutor<S> {
    pub fn new(args: EowcGapFillExecutorArgs<S>) -> Self {
        let metrics = args.actor_ctx.streaming_metrics.clone();
        let actor_id = args.actor_ctx.id.to_string();
        let fragment_id = args.actor_ctx.fragment_id.to_string();
        let gap_fill_metrics = GapFillMetrics {
            gap_fill_generated_rows_count: metrics
                .gap_fill_generated_rows_count
                .with_guarded_label_values(&[&actor_id, &fragment_id]),
        };
        let mut partition_col_mask = vec![false; args.schema.len()];
        for &idx in &args.partition_by_indices {
            partition_col_mask[idx] = true;
        }

        Self {
            input: args.input,

            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                schema: args.schema,
                prev_row_table: args.prev_row_table,
                chunk_size: args.chunk_size,
                time_column_index: args.time_column_index,
                fill_columns: args.fill_columns,
                gap_interval: args.gap_interval,
                high_gap_fill_amplification_threshold: args.high_gap_fill_amplification_threshold,
                partition_by_indices: args.partition_by_indices,
                partition_col_mask,
                metrics: gap_fill_metrics,
            },
        }
    }

    async fn load_prev_row_for_partition(
        partition_by_indices: &[usize],
        partition_key: &OwnedRow,
        prev_row_table: &StateTable<S>,
        staging_prev_rows: &mut HashMap<OwnedRow, OwnedRow>,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        if let Some(row) = staging_prev_rows.get(partition_key) {
            return Ok(Some(row.clone()));
        }

        let row = if partition_by_indices.is_empty() {
            prev_row_table.get_from_one_row_table().await?
        } else {
            prev_row_table.get_row(partition_key).await?
        };

        if let Some(row) = row {
            staging_prev_rows.insert(partition_key.clone(), row.clone());
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn store_prev_row_for_partition(
        partition_key: OwnedRow,
        prev_row: Option<&OwnedRow>,
        current_row: OwnedRow,
        prev_row_table: &mut StateTable<S>,
        staging_prev_rows: &mut HashMap<OwnedRow, OwnedRow>,
    ) {
        if let Some(old_row) = prev_row {
            prev_row_table.delete(old_row);
        }
        prev_row_table.insert(&current_row);
        staging_prev_rows.insert(partition_key, current_row);
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            input,
            inner: mut this,
        } = self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        this.prev_row_table.init_epoch(first_epoch).await?;

        // Calculate and validate gap interval once at initialization
        let dummy_row = OwnedRow::new(vec![]);
        let interval_datum = this.gap_interval.eval_row_infallible(&dummy_row).await;
        let interval = interval_datum
            .ok_or_else(|| anyhow::anyhow!("Gap interval expression returned null"))?
            .into_interval();

        // Validate that gap interval is positive.
        if interval <= Interval::from_month_day_usec(0, 0, 0) {
            Err(anyhow::anyhow!("Gap interval must be positive"))?;
        }

        let mut vars = ExecutionVars {
            staging_prev_rows: HashMap::new(),
        };

        #[for_await]
        for msg in input {
            match msg? {
                // Drop the time watermark: a late anchor makes gap fill back-fill below it.
                // PARTITION BY column watermarks (if any) pass through unchanged.
                Message::Watermark(watermark)
                    if this.partition_by_indices.contains(&watermark.col_idx) =>
                {
                    yield Message::Watermark(watermark);
                }
                Message::Watermark(_) => continue,
                Message::Chunk(chunk) => {
                    let mut chunk_builder =
                        StreamChunkBuilder::new(this.chunk_size, this.schema.data_types());

                    for record in chunk.records() {
                        let current_row =
                            must_match!(record, Record::Insert { new_row } => new_row)
                                .into_owned_row();
                        let partition_key = (&current_row)
                            .project(&this.partition_by_indices)
                            .into_owned_row();
                        let prev_row = Self::load_prev_row_for_partition(
                            &this.partition_by_indices,
                            &partition_key,
                            &this.prev_row_table,
                            &mut vars.staging_prev_rows,
                        )
                        .await?;
                        if let Some(p_row) = &prev_row {
                            let generation_context = GapFillGenerationContext {
                                metrics: &this.metrics,
                                high_amplification_threshold: this
                                    .high_gap_fill_amplification_threshold,
                                actor_ctx: &this.actor_ctx,
                            };
                            let filled_rows = ExecutorInner::<S>::generate_filled_rows(
                                p_row,
                                &current_row,
                                this.time_column_index,
                                &this.partition_col_mask,
                                &this.fill_columns,
                                interval,
                                &generation_context,
                            )?;
                            for filled_row in filled_rows {
                                if let Some(chunk) =
                                    chunk_builder.append_row(Op::Insert, &filled_row)
                                {
                                    yield Message::Chunk(chunk);
                                }
                            }
                        }
                        if let Some(chunk) = chunk_builder.append_row(Op::Insert, &current_row) {
                            yield Message::Chunk(chunk);
                        }
                        Self::store_prev_row_for_partition(
                            partition_key,
                            prev_row.as_ref(),
                            current_row,
                            &mut this.prev_row_table,
                            &mut vars.staging_prev_rows,
                        );
                    }
                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }
                    this.prev_row_table.try_flush().await?;
                }
                Message::Barrier(barrier) => {
                    let prev_row_post_commit = this.prev_row_table.commit(barrier.epoch).await?;
                    // The prev-row state table is the source of truth after commit. Drop the
                    // per-epoch cache so high-cardinality historical partitions do not remain
                    // resident forever; the next active partition will be loaded on demand.
                    vars.staging_prev_rows.clear();

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(this.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    if prev_row_post_commit
                        .post_yield_barrier(update_vnode_bitmap)
                        .await?
                        .is_some()
                    {
                        // Vnode ownership changed. The cache is already cleared after commit, so
                        // subsequent rows reload only partitions owned by the current actor.
                        vars.staging_prev_rows.clear();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::Interval;
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::LiteralExpression;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_executor<S: StateStore>(
        time_column_index: usize,
        fill_columns: HashMap<usize, FillStrategy>,
        gap_interval: NonStrictExpression,
        store: S,
    ) -> (MessageSender, BoxedMessageStream) {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Float32),
            Field::unnamed(DataType::Float64),
        ]);
        let input_stream_key = vec![time_column_index];

        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Timestamp),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Float32),
            ColumnDesc::unnamed(ColumnId::new(4), DataType::Float64),
        ];

        let prev_row_pk_indices = vec![0];
        let prev_row_order_types = vec![OrderType::ascending()];
        let prev_row_table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(1),
                table_columns,
                prev_row_order_types,
                prev_row_pk_indices,
                0,
                vec![],
            ),
            store,
            None,
        )
        .await;

        let (tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema, input_stream_key);
        let gap_fill_executor = EowcGapFillExecutor::new(EowcGapFillExecutorArgs {
            actor_ctx: ActorContext::for_test(123),
            schema: source.schema().clone(),
            input: source,
            prev_row_table,
            chunk_size: 1024,
            time_column_index,
            fill_columns,
            gap_interval,
            high_gap_fill_amplification_threshold: 2048,
            partition_by_indices: vec![],
        });

        (tx, gap_fill_executor.boxed().execute())
    }

    async fn create_partitioned_executor<S: StateStore>(
        store: S,
    ) -> (MessageSender, BoxedMessageStream) {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Timestamp),
            Field::unnamed(DataType::Int64),
        ]);
        let input_stream_key = vec![0, 1];

        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Timestamp),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
        ];

        let prev_row_table = StateTable::from_table_catalog(
            &gen_pbtable_with_dist_key(
                TableId::new(11),
                table_columns,
                vec![OrderType::ascending()],
                vec![0],
                0,
                vec![0],
            ),
            store,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into()),
        )
        .await;

        let (tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema, input_stream_key);
        let gap_fill_executor = EowcGapFillExecutor::new(EowcGapFillExecutorArgs {
            actor_ctx: ActorContext::for_test(123),
            schema: source.schema().clone(),
            input: source,
            prev_row_table,
            chunk_size: 1024,
            time_column_index: 1,
            fill_columns: HashMap::from([(2, FillStrategy::Locf)]),
            gap_interval: NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(Interval::from_days(1).into()),
            )),
            high_gap_fill_amplification_threshold: 2048,
            partition_by_indices: vec![0],
        });

        (tx, gap_fill_executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_gap_fill_interpolate() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Interpolate),
            (3, FillStrategy::Interpolate),
            (4, FillStrategy::Interpolate),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 125 2.0 125.0
                + 2023-04-03T10:00:00 30 150 3.0 150.0
                + 2023-04-04T10:00:00 40 175 4.0 175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
    }

    #[tokio::test]
    async fn test_gap_fill_locf() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Locf),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Locf),
            (4, FillStrategy::Locf),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 10 100 1.0 100.0
                + 2023-04-03T10:00:00 10 100 1.0 100.0
                + 2023-04-04T10:00:00 10 100 1.0 100.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
    }

    #[tokio::test]
    async fn test_gap_fill_prev_row_reloaded_after_barrier() {
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_partitioned_executor(store).await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I TS                  I
            + 1 2023-04-01T00:00:00 10
            + 1 2023-04-03T00:00:00 30",
        ));
        tx.push_watermark(
            1,
            DataType::Timestamp,
            "2023-04-04 00:00:00"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        let expected = StreamChunk::from_pretty(
            " I TS                  I
            + 1 2023-04-01T00:00:00 10
            + 1 2023-04-02T00:00:00 10
            + 1 2023-04-03T00:00:00 30",
        );
        assert_eq!(
            chunk,
            expected,
            "\nactual:\n{}\nexpected:\n{}",
            chunk.to_pretty(),
            expected.to_pretty()
        );
        tx.push_barrier(test_epoch(2), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I TS                  I
            + 1 2023-04-05T00:00:00 50",
        ));
        tx.push_watermark(
            1,
            DataType::Timestamp,
            "2023-04-06 00:00:00"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        let expected = StreamChunk::from_pretty(
            " I TS                  I
            + 1 2023-04-04T00:00:00 30
            + 1 2023-04-05T00:00:00 50",
        );
        assert_eq!(
            chunk,
            expected,
            "\nactual:\n{}\nexpected:\n{}",
            chunk.to_pretty(),
            expected.to_pretty()
        );
    }

    #[tokio::test]
    async fn test_gap_fill_locf_partition_by() {
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_partitioned_executor(store).await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I TS                  I
            + 1 2023-04-01T00:00:00 10
            + 2 2023-04-01T00:00:00 100
            + 1 2023-04-03T00:00:00 30
            + 2 2023-04-04T00:00:00 400",
        ));

        tx.push_int64_watermark(2, 0_i64);
        tx.push_watermark(
            1,
            DataType::Timestamp,
            "2023-04-05 00:00:00"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        let expected = StreamChunk::from_pretty(
            " I TS                  I
            + 1 2023-04-01T00:00:00 10
            + 2 2023-04-01T00:00:00 100
            + 1 2023-04-02T00:00:00 10
            + 1 2023-04-03T00:00:00 30
            + 2 2023-04-02T00:00:00 100
            + 2 2023-04-03T00:00:00 100
            + 2 2023-04-04T00:00:00 400",
        );
        assert_eq!(
            chunk,
            expected,
            "\nactual:\n{}\nexpected:\n{}",
            chunk.to_pretty(),
            expected.to_pretty()
        );
    }

    #[tokio::test]
    async fn test_gap_fill_null() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Null),
            (2, FillStrategy::Null),
            (3, FillStrategy::Null),
            (4, FillStrategy::Null),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 .  .    .    .
                + 2023-04-03T10:00:00 .  .    .    .
                + 2023-04-04T10:00:00 .  .    .    .
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
    }

    #[tokio::test]
    async fn test_gap_fill_mixed_strategy() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Interpolate),
            (2, FillStrategy::Locf),
            (3, FillStrategy::Null),
            (4, FillStrategy::Interpolate),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-03-06 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        tx.push_int64_watermark(1, 0_i64);
        tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-05 18:27:03"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 20 100 .    125.0
                + 2023-04-03T10:00:00 30 100 .    150.0
                + 2023-04-04T10:00:00 40 100 .    175.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );
    }

    #[tokio::test]
    async fn test_gap_fill_fail_over() {
        let time_column_index = 0;
        let gap_interval = Interval::from_days(1);
        let fill_columns = HashMap::from([
            (1, FillStrategy::Locf),
            (2, FillStrategy::Interpolate),
            (3, FillStrategy::Locf),
            (4, FillStrategy::Locf),
        ]);
        let store = MemoryStateStore::new();
        let (mut tx, mut gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns.clone(),
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        tx.push_barrier(test_epoch(1), false);
        gap_fill_executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-01T10:00:00 10 100 1.0 100.0
            + 2023-04-05T10:00:00 50 200 5.0 200.0",
        ));

        let chunk = gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-01T10:00:00 10 100 1.0 100.0
                + 2023-04-02T10:00:00 10 125 1.0 100.0
                + 2023-04-03T10:00:00 10 150 1.0 100.0
                + 2023-04-04T10:00:00 10 175 1.0 100.0
                + 2023-04-05T10:00:00 50 200 5.0 200.0",
            )
        );

        tx.push_barrier(test_epoch(2), false);
        gap_fill_executor.expect_barrier().await;

        let (mut recovered_tx, mut recovered_gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns.clone(),
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store.clone(),
        )
        .await;

        recovered_tx.push_barrier(test_epoch(2), false);
        recovered_gap_fill_executor.expect_barrier().await;

        recovered_tx.push_watermark(
            0,
            DataType::Timestamp,
            "2023-04-06T10:00:00"
                .parse::<risingwave_common::types::Timestamp>()
                .unwrap()
                .into(),
        );
        recovered_tx.push_chunk(StreamChunk::from_pretty(
            " TS                  i   I    f     F
            + 2023-04-08T10:00:00 80 500 8.0 500.0",
        ));

        let chunk = recovered_gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                  i   I    f     F
                + 2023-04-06T10:00:00 50 300 5.0 200.0
                + 2023-04-07T10:00:00 50 400 5.0 200.0
                + 2023-04-08T10:00:00 80 500 8.0 500.0"
            )
        );

        recovered_tx.push_barrier(test_epoch(3), false);
        recovered_gap_fill_executor.expect_barrier().await;

        let (mut final_recovered_tx, mut final_recovered_gap_fill_executor) = create_executor(
            time_column_index,
            fill_columns,
            NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Interval,
                Some(gap_interval.into()),
            )),
            store,
        )
        .await;

        final_recovered_tx.push_barrier(test_epoch(3), false);
        final_recovered_gap_fill_executor.expect_barrier().await;

        final_recovered_tx.push_chunk(StreamChunk::from_pretty(
            " TS                   i   I    f     F
            + 2023-04-10T10:00:00 100 700 10.0 700.0",
        ));

        let chunk = final_recovered_gap_fill_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " TS                   i   I    f     F
                + 2023-04-09T10:00:00  80 600  8.0 500.0
                + 2023-04-10T10:00:00 100 700 10.0 700.0"
            )
        );
    }
}
