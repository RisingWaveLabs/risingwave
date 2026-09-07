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

use std::cmp;

use futures::future::{BoxFuture, Either, select, try_join, try_join_all};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::DefaultOrd;
use risingwave_common::{bail, row};
use risingwave_expr::Result as ExprResult;
use risingwave_expr::expr::{
    InputRefExpression, LiteralExpression, NonStrictExpression, SyncExpressionBoxExt,
    build_func_non_strict,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::expr::expr_node::Type;
use risingwave_storage::table::batch_table::BatchTable;

use super::filter::FilterExecutorInner;
use crate::executor::prelude::*;
use crate::task::ActorEvalErrorReport;

type WatermarkRefreshFuture = BoxFuture<'static, StreamExecutorResult<Option<ScalarImpl>>>;

enum WatermarkFilterEvent {
    GlobalRefresh(StreamExecutorResult<Option<ScalarImpl>>),
    Input(Option<MessageStreamItem>),
}

/// The executor will generate a `Watermark` after each chunk.
/// This will also guarantee all later rows with event time **less than** the watermark will be
/// filtered.
pub struct WatermarkFilterExecutorInner<S: StateStore, const UPSERT: bool> {
    ctx: ActorContextRef,

    input: Executor,
    /// The expression used to calculate the watermark value.
    watermark_expr: NonStrictExpression,
    /// The column we should generate watermark and filter on.
    event_time_col_idx: usize,
    table: StateTable<S>,
    global_watermark_table: BatchTable<S>,

    eval_error_report: ActorEvalErrorReport,
}

pub type WatermarkFilterExecutor<S> = WatermarkFilterExecutorInner<S, false>;
pub type UpsertWatermarkFilterExecutor<S> = WatermarkFilterExecutorInner<S, true>;

impl<S: StateStore, const UPSERT: bool> WatermarkFilterExecutorInner<S, UPSERT> {
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        watermark_expr: NonStrictExpression,
        event_time_col_idx: usize,
        table: StateTable<S>,
        global_watermark_table: BatchTable<S>,
        eval_error_report: ActorEvalErrorReport,
    ) -> Self {
        Self {
            ctx,
            input,
            watermark_expr,
            event_time_col_idx,
            table,
            global_watermark_table,
            eval_error_report,
        }
    }
}

impl<S: StateStore, const UPSERT: bool> Execute for WatermarkFilterExecutorInner<S, UPSERT> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
const UPDATE_GLOBAL_WATERMARK_FREQUENCY_WHEN_IDLE: usize = 5;

impl<S: StateStore, const UPSERT: bool> WatermarkFilterExecutorInner<S, UPSERT> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            input,
            event_time_col_idx,
            watermark_expr,
            ctx,
            mut table,
            mut global_watermark_table,
            eval_error_report,
        } = *self;

        let watermark_type = watermark_expr.return_type();
        assert_eq!(
            watermark_type,
            input.schema().data_types()[event_time_col_idx]
        );
        let mut input = input.execute();

        let first_barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = first_barrier.epoch;
        let mut is_paused = first_barrier.is_pause_on_startup();
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);
        let prev_epoch = first_epoch.prev;
        table.init_epoch(first_epoch).await?;

        // Initiate and yield the first watermark.
        let mut current_watermark =
            Self::get_global_max_watermark(&table, &global_watermark_table, prev_epoch).await?;

        let mut last_checkpoint_watermark = None;

        if let Some(watermark) = current_watermark.clone()
            && !is_paused
        {
            yield Message::Watermark(Watermark::new(
                event_time_col_idx,
                watermark_type.clone(),
                watermark.clone(),
            ));
        }

        // If the input is idle
        let mut idle_input = true;
        let mut barrier_num_during_idle = 0;
        let mut pending_global_refresh: Option<WatermarkRefreshFuture> = None;

        loop {
            let event = if let Some(refresh) = pending_global_refresh.as_mut() {
                let next_msg = input.next();
                pin_mut!(next_msg);
                match select(refresh, next_msg).await {
                    Either::Left((refresh_result, _)) => {
                        pending_global_refresh = None;
                        WatermarkFilterEvent::GlobalRefresh(refresh_result)
                    }
                    Either::Right((msg, _)) => WatermarkFilterEvent::Input(msg),
                }
            } else {
                WatermarkFilterEvent::Input(input.next().await)
            };

            let msg = match event {
                WatermarkFilterEvent::GlobalRefresh(refresh_result) => {
                    let global_max_watermark = refresh_result?;
                    current_watermark = match (current_watermark, global_max_watermark) {
                        (Some(watermark), Some(global_max_watermark)) => Some(cmp::max_by(
                            watermark,
                            global_max_watermark,
                            DefaultOrd::default_cmp,
                        )),
                        (Some(watermark), None) => Some(watermark),
                        (None, global_max_watermark) => global_max_watermark,
                    };
                    if let Some(watermark) = current_watermark.clone()
                        && !is_paused
                    {
                        yield Message::Watermark(Watermark::new(
                            event_time_col_idx,
                            watermark_type.clone(),
                            watermark,
                        ));
                    }
                    continue;
                }
                WatermarkFilterEvent::Input(msg) => {
                    let Some(msg) = msg else {
                        break;
                    };
                    msg?
                }
            };

            match msg {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact_vis();

                    // Empty chunk should not be processed.
                    if chunk.cardinality() == 0 {
                        continue;
                    }

                    let watermark_array = watermark_expr.eval_infallible(chunk.data_chunk()).await;

                    // Build the expression to calculate watermark filter.
                    let watermark_filter_expr = current_watermark
                        .clone()
                        .map(|watermark| {
                            Self::build_watermark_filter_expr(
                                watermark_type.clone(),
                                event_time_col_idx,
                                watermark,
                                eval_error_report.clone(),
                            )
                        })
                        .transpose()?;

                    // NULL watermark should not be considered.
                    let max_watermark = watermark_array
                        .iter()
                        .flatten() // skip NULL values
                        .max_by(DefaultOrd::default_cmp);

                    if let Some(max_watermark) = max_watermark {
                        // Assign a new watermark.
                        current_watermark = Some(current_watermark.map_or(
                            max_watermark.into_scalar_impl(),
                            |watermark| {
                                cmp::max_by(
                                    watermark,
                                    max_watermark.into_scalar_impl(),
                                    DefaultOrd::default_cmp,
                                )
                            },
                        ));
                    }

                    if let Some(expr) = watermark_filter_expr {
                        let pred_output = expr.eval_infallible(chunk.data_chunk()).await;

                        if let Some(output_chunk) =
                            FilterExecutorInner::<UPSERT>::filter(chunk, pred_output)?
                        {
                            yield Message::Chunk(output_chunk);
                        };
                    } else {
                        // No watermark
                        yield Message::Chunk(chunk);
                    }

                    if let Some(watermark) = current_watermark.clone() {
                        idle_input = false;
                        yield Message::Watermark(Watermark::new(
                            event_time_col_idx,
                            watermark_type.clone(),
                            watermark,
                        ));
                    }
                    table.try_flush().await?;
                }
                Message::Watermark(watermark) => {
                    if watermark.col_idx == event_time_col_idx {
                        tracing::warn!(
                            "WatermarkFilterExecutor received a watermark on the event it is filtering."
                        );
                        let watermark = watermark.val;
                        if let Some(cur_watermark) = current_watermark.clone()
                            && cur_watermark.default_cmp(&watermark).is_lt()
                        {
                            current_watermark = Some(watermark.clone());
                            idle_input = false;
                            yield Message::Watermark(Watermark::new(
                                event_time_col_idx,
                                watermark_type.clone(),
                                watermark,
                            ));
                        }
                    } else {
                        yield Message::Watermark(watermark)
                    }
                }
                Message::Barrier(barrier) => {
                    let prev_epoch = barrier.epoch.prev;
                    let is_checkpoint = barrier.kind.is_checkpoint();

                    if is_checkpoint && last_checkpoint_watermark != current_watermark {
                        last_checkpoint_watermark.clone_from(&current_watermark);
                        // Persist the watermark when checkpoint arrives.
                        if let Some(watermark) = current_watermark.clone() {
                            for vnode in table.vnodes().clone().iter_vnodes() {
                                let pk = vnode.to_datum();
                                let row = [pk, Some(watermark.clone())];
                                // This is an upsert.
                                table.insert(row);
                            }
                        }
                    }
                    let post_commit = table.commit(barrier.epoch).await?;

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause => {
                                is_paused = true;
                            }
                            Mutation::Resume => {
                                is_paused = false;
                            }
                            _ => (),
                        }
                    }

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(ctx.id);
                    yield Message::Barrier(barrier);

                    let mut need_update_global_max_watermark = false;
                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(((vnode_bitmap, previous_vnode_bitmap, _), _cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                    {
                        let other_vnodes_bitmap = Arc::new(!(*vnode_bitmap).clone());
                        let _ = global_watermark_table.update_vnode_bitmap(other_vnodes_bitmap);

                        // Take the global max watermark when scaling happens.
                        if previous_vnode_bitmap != vnode_bitmap {
                            need_update_global_max_watermark = true;
                        }
                    }

                    if need_update_global_max_watermark {
                        current_watermark = Self::get_global_max_watermark(
                            &table,
                            &global_watermark_table,
                            prev_epoch,
                        )
                        .await?;
                    }

                    if is_checkpoint && !is_paused {
                        if idle_input {
                            barrier_num_during_idle += 1;

                            if barrier_num_during_idle
                                >= UPDATE_GLOBAL_WATERMARK_FREQUENCY_WHEN_IDLE
                                && pending_global_refresh.is_none()
                            {
                                barrier_num_during_idle = 0;
                                let global_watermark_table = global_watermark_table.clone();
                                pending_global_refresh = Some(Box::pin(async move {
                                    Self::read_global_max_watermark(
                                        &global_watermark_table,
                                        prev_epoch,
                                    )
                                    .await
                                }));
                            }
                        } else {
                            idle_input = true;
                            barrier_num_during_idle = 0;
                        }
                    }
                }
            }
        }
    }

    fn build_watermark_filter_expr(
        watermark_type: DataType,
        event_time_col_idx: usize,
        watermark: ScalarImpl,
        eval_error_report: ActorEvalErrorReport,
    ) -> ExprResult<NonStrictExpression> {
        build_func_non_strict(
            Type::GreaterThanOrEqual,
            DataType::Boolean,
            vec![
                InputRefExpression::new(watermark_type.clone(), event_time_col_idx).boxed(),
                LiteralExpression::new(watermark_type, Some(watermark)).boxed(),
            ],
            eval_error_report,
        )
    }

    fn decode_watermark_row(
        watermark_row: Option<OwnedRow>,
    ) -> StreamExecutorResult<Option<ScalarImpl>> {
        match watermark_row {
            Some(row) => {
                if row.len() == 1 {
                    Ok(row[0].clone())
                } else {
                    bail!("The watermark row should only contain 1 datum");
                }
            }
            _ => Ok(None),
        }
    }

    async fn read_global_max_watermark(
        global_watermark_table: &BatchTable<S>,
        read_epoch: u64,
    ) -> StreamExecutorResult<Option<ScalarImpl>> {
        let global_watermark_iter_futures =
            global_watermark_table
                .vnodes()
                .iter_vnodes()
                .map(|vnode| async move {
                    let pk = row::once(vnode.to_datum());
                    let watermark_row: Option<OwnedRow> = global_watermark_table
                        .get_row(pk, HummockReadEpoch::Committed(read_epoch))
                        .await?;
                    Self::decode_watermark_row(watermark_row)
                });
        let global_watermarks = try_join_all(global_watermark_iter_futures).await?;

        Ok(global_watermarks
            .into_iter()
            .flatten()
            .max_by(DefaultOrd::default_cmp))
    }

    /// If the returned if `Ok(None)`, it means there is no global max watermark.
    async fn get_global_max_watermark(
        table: &StateTable<S>,
        global_watermark_table: &BatchTable<S>,
        read_epoch: u64,
    ) -> StreamExecutorResult<Option<ScalarImpl>> {
        let local_watermark_iter_futures = table.vnodes().iter_vnodes().map(|vnode| async move {
            let pk = row::once(vnode.to_datum());
            let watermark_row: Option<OwnedRow> = table.get_row(pk).await?;
            Self::decode_watermark_row(watermark_row)
        });
        let (global_watermarks, local_watermarks) = try_join(
            Self::read_global_max_watermark(global_watermark_table, read_epoch),
            try_join_all(local_watermark_iter_futures),
        )
        .await?;

        // Return the minimal value if the remote max watermark is Null.
        let watermark = global_watermarks
            .into_iter()
            .chain(local_watermarks.into_iter().flatten())
            .max_by(DefaultOrd::default_cmp);

        Ok(watermark)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableDesc};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::Date;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::Table;
    use risingwave_pb::common::ColumnOrder;
    use risingwave_pb::plan_common::PbColumnCatalog;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::test_utils::{MessageSender, MockSource};

    const WATERMARK_TYPE: DataType = DataType::Timestamp;

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        val_indices: &[usize],
        table_id: u32,
    ) -> (BatchTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        let table = Table {
            id: table_id.into(),
            columns: data_types
                .iter()
                .enumerate()
                .map(|(id, data_type)| PbColumnCatalog {
                    column_desc: Some(
                        ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone())
                            .to_protobuf(),
                    ),
                    is_hidden: false,
                })
                .collect(),
            pk: pk_indices
                .iter()
                .zip_eq(order_types.iter())
                .map(|(pk, order)| ColumnOrder {
                    column_index: *pk as _,
                    order_type: Some(order.to_protobuf()),
                })
                .collect(),
            distribution_key: vec![],
            stream_key: vec![0],
            append_only: false,
            vnode_col_index: Some(0),
            value_indices: val_indices.iter().map(|i| *i as _).collect(),
            read_prefix_len_hint: 0,
            ..Default::default()
        };

        // TODO: use consistent operations for watermark filter after we have upsert.
        let state_table = StateTable::from_table_catalog_inconsistent_op(
            &table,
            mem_state.clone(),
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into()),
        )
        .await;

        let desc = TableDesc::from_pb_table(&table).try_to_protobuf().unwrap();

        let storage_table = BatchTable::new_partial(
            mem_state,
            val_indices.iter().map(|i| ColumnId::new(*i as _)).collect(),
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into()),
            &desc,
        );
        (storage_table, state_table)
    }

    async fn create_watermark_filter_executor(
        mem_state: MemoryStateStore,
    ) -> (Box<dyn Execute>, MessageSender) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int16),        // pk
                Field::unnamed(WATERMARK_TYPE.clone()), // watermark column
            ],
        };

        let watermark_expr = build_from_pretty("(subtract:timestamp $1:timestamp 1day:interval)");

        let (storage_table, table) = create_in_memory_state_table(
            mem_state,
            &[DataType::Int16, WATERMARK_TYPE],
            &[OrderType::ascending()],
            &[0],
            &[1],
            0,
        )
        .await;

        let (tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![0]);

        let ctx = ActorContext::for_test(123);
        let info = ExecutorInfo::for_test(
            source.schema().clone(),
            source.stream_key().to_vec(),
            "WatermarkFilterExecutor".to_owned(),
            0,
        );
        let eval_error_report = ActorEvalErrorReport {
            actor_context: ctx.clone(),
            identity: info.identity.into(),
        };

        (
            WatermarkFilterExecutor::new(
                ctx,
                source,
                watermark_expr,
                1,
                table,
                storage_table,
                eval_error_report,
            )
            .boxed(),
            tx,
        )
    }

    #[tokio::test]
    async fn test_watermark_filter() {
        let chunk1 = StreamChunk::from_pretty(
            "  I TS
             + 1 2022-11-07T00:00:00
             + 2 2022-11-08T00:00:00
             + 3 2022-11-06T00:00:00",
        );
        let chunk2 = StreamChunk::from_pretty(
            "  I TS
             + 4 2022-11-07T00:00:00
             + 5 2022-11-06T00:00:00
             + 6 2022-11-10T00:00:00",
        );
        let chunk3 = StreamChunk::from_pretty(
            "  I TS
             + 7 2022-11-14T00:00:00
             + 8 2022-11-09T00:00:00
             + 9 2022-11-08T00:00:00",
        );

        let mem_state = MemoryStateStore::new();

        let (executor, mut tx) = create_watermark_filter_executor(mem_state.clone()).await;
        let mut executor = executor.execute();

        // push the init barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        macro_rules! watermark {
            ($scalar:expr) => {
                Watermark::new(1, WATERMARK_TYPE.clone(), $scalar)
            };
        }

        // push the 1st chunk
        tx.push_chunk(chunk1);
        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I TS
                 + 1 2022-11-07T00:00:00
                 + 2 2022-11-08T00:00:00
                 + 3 2022-11-06T00:00:00",
            )
        );
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                Date::from_ymd_uncheck(2022, 11, 7).and_hms_uncheck(0, 0, 0)
            ))
        );

        // push the 2nd barrier
        tx.push_barrier(test_epoch(2), false);
        executor.next().await.unwrap().unwrap();

        // push the 2nd chunk
        tx.push_chunk(chunk2);
        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I TS
                 + 4 2022-11-07T00:00:00
                 + 6 2022-11-10T00:00:00",
            )
        );
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                Date::from_ymd_uncheck(2022, 11, 9).and_hms_uncheck(0, 0, 0)
            ))
        );

        // push the 3nd barrier
        tx.push_barrier(test_epoch(3), false);
        executor.next().await.unwrap().unwrap();

        // Drop executor
        drop(executor);

        // Build new executor
        let (executor, mut tx) = create_watermark_filter_executor(mem_state.clone()).await;
        let mut executor = executor.execute();

        // push the 1st barrier after failover
        tx.push_barrier(test_epoch(3), false);
        executor.next().await.unwrap().unwrap();

        // Init watermark after failover
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                Date::from_ymd_uncheck(2022, 11, 9).and_hms_uncheck(0, 0, 0)
            ))
        );

        // push the 3rd chunk
        tx.push_chunk(chunk3);
        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I TS
                 + 7 2022-11-14T00:00:00
                 + 8 2022-11-09T00:00:00",
            )
        );
        let watermark = executor.next().await.unwrap().unwrap();
        assert_eq!(
            watermark.into_watermark().unwrap(),
            watermark!(ScalarImpl::Timestamp(
                Date::from_ymd_uncheck(2022, 11, 13).and_hms_uncheck(0, 0, 0)
            ))
        );
    }
}
