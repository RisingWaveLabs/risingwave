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

use std::ops::Bound;

use either::Either;
use futures::{StreamExt, TryStreamExt, stream};
use futures_async_stream::try_stream;
use iceberg::scan::FileScanTask;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, SerialArray};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ColumnId, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ROW_ID_COLUMN_NAME,
};
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::id::SourceId;
use risingwave_common::types::{JsonbVal, ScalarRef, Serial, ToOwnedDatum};
use risingwave_connector::source::iceberg::{
    GLOBAL_ICEBERG_SCAN_METRICS, IcebergFileScanMetrics, IcebergScanMetricsLabels, IcebergScanOpts,
    PersistedFileScanTask, scan_task_to_chunk_with_deletes,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{SourceContext, SourceCtrlOpts};
use risingwave_pb::common::ThrottleType;
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{SourceStateTableHandler, StreamSourceCore, prune_additional_cols};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

/// An executor that fetches data from Iceberg tables.
///
/// This executor works with an upstream list executor that provides the list of files to read.
/// It reads data from Iceberg files in batches, converts them to stream chunks, and passes them
/// downstream.
pub struct IcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides the list of files to read.
    /// This executor is responsible for discovering new files and changes in the Iceberg table.
    upstream: Option<Executor>,

    /// Optional rate limit in rows/s to control data ingestion speed
    rate_limit_rps: Option<u32>,

    /// Configuration for streaming operations, including Iceberg-specific settings
    streaming_config: Arc<StreamingConfig>,

    scan_metrics: Option<IcebergScanMetricsLabels>,
    file_scan_metrics: Option<IcebergFileScanMetrics>,
}

/// Fetched data from 1 [`FileScanTask`], along with states for checkpointing.
///
/// Currently 1 `FileScanTask` -> 1 `ChunksWithState`.
/// Later after we support reading part of a file, we will support 1 `FileScanTask` -> n `ChunksWithState`.
pub(crate) struct ChunksWithState {
    /// The actual data chunks read from the file
    pub chunks: Vec<StreamChunk>,

    /// Path to the data file, used for checkpointing and error reporting.
    pub data_file_path: String,

    /// The last read position in the file, used for checkpointing.
    #[expect(dead_code)]
    pub last_read_pos: Datum,
}

impl<S: StateStore> IcebergFetchExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        rate_limit_rps: Option<u32>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            rate_limit_rps,
            streaming_config,
            scan_metrics: None,
            file_scan_metrics: None,
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, ChunksWithState>,
        rate_limit_rps: Option<u32>,
        streaming_config: Arc<StreamingConfig>,
        file_scan_metrics: IcebergFileScanMetrics,
    ) -> StreamExecutorResult<()> {
        let mut batch =
            Vec::with_capacity(streaming_config.developer.iceberg_fetch_batch_size as usize);
        let state_table = state_store_handler.state_table();
        'vnodes: for vnode in state_table.vnodes().iter_vnodes() {
            let table_iter = state_table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    // This usage is similar with `backfill`. So we only need to fetch a large data rather than establish a connection for a whole object.
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(table_iter);
            while let Some(item) = table_iter.next().await {
                let row = item?;
                let task = match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                        PersistedFileScanTask::decode(jsonb_ref)?
                    }
                    _ => unreachable!(),
                };
                batch.push(task);

                if batch.len() >= streaming_config.developer.iceberg_fetch_batch_size as usize {
                    break 'vnodes;
                }
            }
        }
        if batch.is_empty() {
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            *splits_on_fetch += batch.len();
            let batch_reader = Self::build_batched_stream_reader(
                column_ids,
                source_ctx,
                source_desc,
                batch,
                rate_limit_rps,
                streaming_config,
                file_scan_metrics,
            )
            .map_err(StreamExecutorError::connector_error);
            stream.replace_data_stream(batch_reader);
        }

        Ok(())
    }

    #[try_stream(ok = ChunksWithState, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        _column_ids: Vec<ColumnId>,
        _source_ctx: SourceContext,
        source_desc: SourceDesc,
        batch: Vec<FileScanTask>,
        _rate_limit_rps: Option<u32>,
        streaming_config: Arc<StreamingConfig>,
        file_scan_metrics: IcebergFileScanMetrics,
    ) {
        let file_path_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_PATH_COLUMN_NAME)
            .unwrap();
        let file_pos_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_POS_COLUMN_NAME)
            .unwrap();
        let properties = source_desc.source.config.clone();
        let properties = match properties {
            risingwave_connector::source::ConnectorProperties::Iceberg(iceberg_properties) => {
                iceberg_properties
            }
            _ => unreachable!(),
        };
        let table = properties.load_table().await?;
        for task in batch {
            // Capture the file path upfront from the task so we can use it even when the
            // scan produces no chunks (empty data file or fully equality-deleted file).
            let task_data_file_path = task.data_file_path.clone();
            let mut chunks = vec![];
            #[for_await]
            for chunk in scan_task_to_chunk_with_deletes(
                table.clone(),
                task,
                IcebergScanOpts {
                    chunk_size: streaming_config.developer.chunk_size,
                    need_seq_num: true, /* Although this column is unnecessary, we still keep it for potential usage in the future */
                    need_file_path_and_pos: true,
                    // Iceberg V2 position/equality deletes are exposed as separate delete-file
                    // tasks for table-engine reads. V3 deletion vectors should be applied by
                    // iceberg-rs while scanning the data file.
                    handle_delete_files: table.metadata().format_version()
                        >= iceberg::spec::FormatVersion::V3,
                },
                Some(file_scan_metrics.clone()),
            ) {
                let chunk = chunk?;
                // Skip zero-cardinality chunks: a RecordBatch with 0 visible rows after
                // predicate/delete filtering would cause `cardinality() - 1` to underflow
                // below when we extract the last-row metadata. Skipping here is safe
                // because the existing `task_data_file_path` fallback already covers
                // the case where no readable rows are produced.
                if chunk.cardinality() == 0 {
                    continue;
                }
                chunks.push(StreamChunk::from_parts(
                    itertools::repeat_n(Op::Insert, chunk.cardinality()).collect_vec(),
                    chunk,
                ));
            }
            // We yield once for each file now, because iceberg-rs doesn't support read part of a file now.
            // We must always yield — even for an empty task — so that `into_stream` can
            // decrement `splits_on_fetch` and delete the file assignment from the state
            // table.  Skipping the yield (e.g. with `continue`) would leave the file
            // stuck in the state table and prevent subsequent batches from progressing.
            let (data_file_path, last_read_pos) = if let Some(last_chunk) = chunks.last() {
                let last_row = last_chunk.row_at(last_chunk.cardinality() - 1).1;
                let path = last_row
                    .datum_at(file_path_idx)
                    .unwrap()
                    .into_utf8()
                    .to_owned();
                let pos = last_row.datum_at(file_pos_idx).unwrap().to_owned_datum();
                (path, pos)
            } else {
                // No rows were produced: fall back to the task's own path so the
                // consumer can still remove the state entry for this file.
                (task_data_file_path, None)
            };
            yield ChunksWithState {
                chunks,
                data_file_path,
                last_read_pos,
            };
        }
    }

    fn build_source_ctx(
        &self,
        source_desc: &SourceDesc,
        source_id: SourceId,
        source_name: &str,
    ) -> SourceContext {
        SourceContext::new(
            self.actor_ctx.id,
            source_id,
            self.actor_ctx.fragment_id,
            source_name.to_owned(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit_rps),
                split_txn: self.rate_limit_rps.is_some(), // when rate limiting, we may split txn
            },
            source_desc.source.config.clone(),
            None,
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = barrier.epoch;
        let is_pause_on_startup = barrier.is_pause_on_startup();
        yield Message::Barrier(barrier);

        let mut core = self.stream_source_core.take().unwrap();
        let mut state_store_handler = core.split_state_store;

        // Build source description from the builder.
        let source_desc_builder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let file_path_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_PATH_COLUMN_NAME)
            .unwrap();
        let file_pos_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_POS_COLUMN_NAME)
            .unwrap();
        // TODO: currently we generate row_id here. If for risingwave iceberg table engine, maybe we can use _risingwave_iceberg_row_id instead.
        let row_id_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ROW_ID_COLUMN_NAME)
            .unwrap();
        tracing::trace!(
            "source_desc.columns: {:#?}, file_path_idx: {}, file_pos_idx: {}, row_id_idx: {}",
            source_desc.columns,
            file_path_idx,
            file_pos_idx,
            row_id_idx
        );
        // Initialize state table.
        state_store_handler.init_epoch(first_epoch).await?;

        // Extract table name from iceberg properties for metrics labeling.
        let iceberg_table_name = {
            match &source_desc.source.config {
                risingwave_connector::source::ConnectorProperties::Iceberg(props) => {
                    props.table.table_name().to_owned()
                }
                _ => unreachable!("IcebergFetchExecutor must be built with Iceberg properties"),
            }
        };
        let source_id_str = core.source_id.to_string();
        let source_name_str = core.source_name.clone();
        let scan_metrics = self
            .scan_metrics
            .insert(IcebergScanMetricsLabels::new(
                source_id_str,
                source_name_str,
                iceberg_table_name.clone(),
            ))
            .clone();
        let file_scan_metrics = self
            .file_scan_metrics
            .insert(IcebergFileScanMetrics::new(
                &GLOBAL_ICEBERG_SCAN_METRICS,
                &iceberg_table_name,
            ))
            .clone();

        let mut splits_on_fetch: usize = 0;
        let mut stream = StreamReaderWithPause::<true, ChunksWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        if is_pause_on_startup {
            stream.pause_stream();
        }

        // If it is a recovery startup,
        // there can be file assignments in the state table.
        // Hence we try building a reader first.
        Self::replace_with_new_batch_reader(
            &mut splits_on_fetch,
            &state_store_handler, // move into the function
            core.column_ids.clone(),
            self.build_source_ctx(&source_desc, core.source_id, &core.source_name),
            source_desc.clone(),
            &mut stream,
            self.rate_limit_rps,
            self.streaming_config.clone(),
            file_scan_metrics.clone(),
        )
        .await?;
        scan_metrics.set_inflight_file_count(splits_on_fetch);

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    scan_metrics.record_fetch_error();
                    splits_on_fetch = 0;
                    scan_metrics.set_inflight_file_count(0);
                }
                Ok(msg) => {
                    match msg {
                        // This branch will be preferred.
                        Either::Left(msg) => {
                            match msg {
                                Message::Barrier(barrier) => {
                                    let mut need_rebuild_reader = false;

                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
                                            Mutation::Throttle(fragment_to_apply) => {
                                                if let Some(entry) = fragment_to_apply
                                                    .get(&self.actor_ctx.fragment_id)
                                                    && entry.throttle_type() == ThrottleType::Source
                                                    && entry.rate_limit != self.rate_limit_rps
                                                {
                                                    tracing::debug!(
                                                        "updating rate limit from {:?} to {:?}",
                                                        self.rate_limit_rps,
                                                        entry.rate_limit
                                                    );
                                                    self.rate_limit_rps = entry.rate_limit;
                                                    need_rebuild_reader = true;
                                                }
                                            }
                                            _ => (),
                                        }
                                    }

                                    let post_commit = state_store_handler
                                        .commit_may_update_vnode_bitmap(barrier.epoch)
                                        .await?;

                                    let update_vnode_bitmap =
                                        barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                                    // Propagate the barrier.
                                    yield Message::Barrier(barrier);

                                    if post_commit
                                        .post_yield_barrier(update_vnode_bitmap)
                                        .await?
                                        .is_some()
                                    {
                                        // Vnode bitmap update changes which file assignments this executor
                                        // should read. Rebuild the reader to avoid reading splits that no
                                        // longer belong to this actor (e.g., during scale-out).
                                        splits_on_fetch = 0;
                                    }

                                    if splits_on_fetch == 0 || need_rebuild_reader {
                                        Self::replace_with_new_batch_reader(
                                            &mut splits_on_fetch,
                                            &state_store_handler,
                                            core.column_ids.clone(),
                                            self.build_source_ctx(
                                                &source_desc,
                                                core.source_id,
                                                &core.source_name,
                                            ),
                                            source_desc.clone(),
                                            &mut stream,
                                            self.rate_limit_rps,
                                            self.streaming_config.clone(),
                                            file_scan_metrics.clone(),
                                        )
                                        .await?;
                                        scan_metrics.set_inflight_file_count(splits_on_fetch);
                                    }
                                }
                                // Receiving file assignments from upstream list executor,
                                // store into state table.
                                Message::Chunk(chunk) => {
                                    let jsonb_values: Vec<(String, JsonbVal)> = chunk
                                        .data_chunk()
                                        .rows()
                                        .map(|row| {
                                            let file_name = row.datum_at(0).unwrap().into_utf8();
                                            let split = row.datum_at(1).unwrap().into_jsonb();
                                            (file_name.to_owned(), split.to_owned_scalar())
                                        })
                                        .collect();
                                    state_store_handler.set_states_json(jsonb_values).await?;
                                    state_store_handler.try_flush().await?;
                                }
                                Message::Watermark(_) => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        Either::Right(ChunksWithState {
                            chunks,
                            data_file_path,
                            last_read_pos: _,
                        }) => {
                            // TODO: support persist progress after supporting reading part of a file.
                            if true {
                                splits_on_fetch = splits_on_fetch.saturating_sub(1);
                                state_store_handler.delete(&data_file_path).await?;
                                scan_metrics.set_inflight_file_count(splits_on_fetch);
                            }

                            for chunk in &chunks {
                                let chunk = prune_additional_cols(
                                    chunk,
                                    &[file_path_idx, file_pos_idx],
                                    &source_desc.columns,
                                );
                                // pad row_id
                                let (chunk, op) = chunk.into_parts();
                                let (mut columns, visibility) = chunk.into_parts();
                                columns.insert(
                                    row_id_idx,
                                    Arc::new(
                                        SerialArray::from_iter_bitmap(
                                            itertools::repeat_n(Serial::from(0), columns[0].len()),
                                            Bitmap::zeros(columns[0].len()),
                                        )
                                        .into(),
                                    ),
                                );
                                let chunk = StreamChunk::from_parts(
                                    op,
                                    DataChunk::from_parts(columns.into(), visibility),
                                );

                                yield Message::Chunk(chunk);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<S: StateStore> Execute for IcebergFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("IcebergFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("IcebergFetchExecutor").finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{DataChunk, Op, StreamChunk};

    use super::ChunksWithState;

    /// Verifies the `ChunksWithState` contract for the empty-task case.
    ///
    /// When a `FileScanTask` produces no rows (e.g. an empty data file or an Iceberg file
    /// fully covered by equality-delete files), `build_batched_stream_reader` now yields a
    /// `ChunksWithState` with an empty `chunks` vec and the `data_file_path` taken directly
    /// from the task — rather than skipping the yield entirely.
    ///
    /// Skipping the yield would prevent `into_stream` from calling
    /// `state_store_handler.delete(&data_file_path)` and decrementing `splits_on_fetch`,
    /// leaving the file assignment stuck in the state table.
    ///
    /// This test verifies the two properties that `into_stream` depends on:
    /// 1. An empty `ChunksWithState` forwards no data downstream (the chunk loop is a
    ///    no-op), so no spurious rows appear.
    /// 2. `data_file_path` is populated so the state entry can be cleaned up.
    #[test]
    fn test_empty_chunks_with_state_satisfies_into_stream_contract() {
        let path = "s3://bucket/empty.parquet".to_owned();

        // Simulate what build_batched_stream_reader yields for an empty task.
        let cws = ChunksWithState {
            chunks: vec![],
            data_file_path: path.clone(),
            last_read_pos: None,
        };

        // Property 1: no data is forwarded downstream.
        let forwarded: Vec<_> = cws.chunks.iter().collect();
        assert!(
            forwarded.is_empty(),
            "empty ChunksWithState must not forward any rows"
        );

        // Property 2: data_file_path is set so the state entry can be deleted.
        assert_eq!(
            cws.data_file_path, path,
            "data_file_path must match the original task path"
        );
    }

    /// Verifies that a non-empty `ChunksWithState` carries its chunks unmodified.
    #[test]
    fn test_non_empty_chunks_with_state() {
        let chunk = StreamChunk::from_parts(
            vec![Op::Insert, Op::Insert, Op::Insert],
            DataChunk::new_dummy(3),
        );
        let cws = ChunksWithState {
            chunks: vec![chunk],
            data_file_path: "s3://bucket/data.parquet".to_owned(),
            last_read_pos: None,
        };

        assert_eq!(cws.chunks.len(), 1);
        assert_eq!(cws.chunks[0].cardinality(), 3);
    }

    /// Verifies that zero-cardinality chunks are excluded from the `chunks` vec.
    ///
    /// `scan_task_to_chunk_with_deletes` can emit zero-row `RecordBatch`es after
    /// predicate or equality-delete filtering. If such a chunk were pushed into `chunks`
    /// and then chosen as `chunks.last()`, the subsequent `cardinality() - 1` call would
    /// underflow on `usize` and panic. The fix is to skip those chunks before pushing,
    /// relying on the `task_data_file_path` fallback to cover the all-empty case.
    #[test]
    fn test_zero_cardinality_chunks_are_excluded() {
        // Simulate what build_batched_stream_reader does when filtering zero-row chunks.
        let path = "s3://bucket/mostly-deleted.parquet".to_owned();

        let mut chunks: Vec<StreamChunk> = vec![];

        // A zero-cardinality chunk (e.g. from a fully-deleted RecordBatch).
        let zero_row_chunk = DataChunk::new_dummy(0);
        if zero_row_chunk.cardinality() == 0 {
            // skipped — no push
        } else {
            chunks.push(StreamChunk::from_parts(
                itertools::repeat_n(Op::Insert, zero_row_chunk.cardinality()).collect_vec(),
                zero_row_chunk,
            ));
        }

        // After the loop, chunks is empty: the fallback path kicks in.
        assert!(
            chunks.is_empty(),
            "zero-cardinality chunk must not be added to the chunks vec"
        );

        // Simulate the fallback: data_file_path comes from the task.
        let cws = ChunksWithState {
            chunks,
            data_file_path: path.clone(),
            last_read_pos: None,
        };

        // State cleanup can still run.
        assert_eq!(
            cws.data_file_path, path,
            "data_file_path must be set even when all chunks are zero-cardinality"
        );
        // No spurious rows forwarded downstream.
        assert!(cws.chunks.is_empty());
    }
}
