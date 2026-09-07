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

use std::sync::Arc;

use anyhow::anyhow;
use either::Either;
use futures_async_stream::try_stream;
use parking_lot::Mutex;
use risingwave_common::array::Op;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::config::StreamingConfig;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    IcebergIncrementalScan, IcebergScanMetricsLabels, IcebergScanPlanner, IcebergScanProjection,
    PersistedFileScanTask,
};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

pub struct IcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Columns of fetch executor, used to plan files.
    /// For backward compatibility, this can be None, meaning all columns are needed.
    downstream_columns: Option<Vec<ColumnCatalog>>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    #[expect(dead_code)]
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    #[expect(dead_code)]
    rate_limit_rps: Option<u32>,

    /// Streaming config
    streaming_config: Arc<StreamingConfig>,
}

impl<S: StateStore> IcebergListExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        downstream_columns: Option<Vec<ColumnCatalog>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            downstream_columns,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
            streaming_config,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let first_barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;
        let first_epoch = first_barrier.epoch;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::Iceberg(iceberg_properties) = config else {
            unreachable!()
        };

        let scan_projection =
            IcebergScanProjection::from_downstream_columns(self.downstream_columns.as_deref());

        tracing::debug!("scan_projection: {:?}", scan_projection);

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let source_id_str = self.stream_source_core.source_id.to_string();
        let source_name_str = self.stream_source_core.source_name.clone();
        let table_name = iceberg_properties.table.table_name().to_owned();
        let scan_metrics = IcebergScanMetricsLabels::new(
            source_id_str.clone(),
            source_name_str.clone(),
            table_name,
        );
        let scan_planner = IcebergScanPlanner::new(
            (*iceberg_properties).clone(),
            scan_projection,
            Some(scan_metrics.clone()),
        );

        let state_table = self.stream_source_core.split_state_store.state_table_mut();
        state_table.init_epoch(first_epoch).await?;
        let state_row = state_table.get_from_one_value_table().await?;
        // last_snapshot is EXCLUSIVE (i.e., already scanned)
        let mut last_snapshot: Option<i64> = state_row.map(|s| *s.as_int64());
        let mut prev_persisted_snapshot = last_snapshot;

        if last_snapshot.is_none() {
            // do a regular scan, then switch to incremental scan
            // TODO: we may support starting from a specific snapshot/timestamp later
            // If the current snapshot is None (empty table), go to incremental scan directly.
            if let Some(snapshot_plan) = scan_planner.plan_current_snapshot().await? {
                last_snapshot = Some(snapshot_plan.snapshot_id);
                let mut chunk_builder = StreamChunkBuilder::new(
                    self.streaming_config.developer.chunk_size,
                    vec![DataType::Varchar, DataType::Jsonb],
                );
                #[for_await]
                for scan_task in snapshot_plan.tasks {
                    let scan_task = scan_task?;
                    let data_file_path = scan_task.data_file_path.clone();
                    let persisted_task = PersistedFileScanTask::encode(scan_task)?;
                    if let Some(chunk) = chunk_builder.append_row(
                        Op::Insert,
                        &[
                            Some(ScalarImpl::Utf8(data_file_path.into())),
                            Some(ScalarImpl::Jsonb(persisted_task)),
                        ],
                    ) {
                        yield Message::Chunk(chunk);
                    }
                }
                if let Some(chunk) = chunk_builder.take() {
                    yield Message::Chunk(chunk);
                }
            }
        }

        let last_snapshot = Arc::new(Mutex::new(last_snapshot));
        let build_incremental_stream = || {
            incremental_scan_stream(
                scan_planner.clone(),
                last_snapshot.clone(),
                self.streaming_config.developer.iceberg_list_interval_sec,
            )
            .map(|res| match res {
                Ok(scan_task) => {
                    let data_file_path = scan_task.data_file_path.clone();
                    let persisted_task = PersistedFileScanTask::encode(scan_task)?;
                    let row = (
                        Op::Insert,
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(data_file_path.into())),
                            Some(ScalarImpl::Jsonb(persisted_task)),
                        ]),
                    );
                    Ok(StreamChunk::from_rows(
                        &[row],
                        &[DataType::Varchar, DataType::Jsonb],
                    ))
                }
                Err(e) => Err(e),
            })
        };

        let mut stream =
            StreamReaderWithPause::<true, _>::new(barrier_stream, build_incremental_stream());

        // TODO: support pause (incl. pause on startup)/resume/rate limit

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(
                        error = %e.as_report(),
                        "incremental iceberg list stream errored, rebuilding"
                    );
                    scan_metrics.record_scan_error("list_error");
                    stream.replace_data_stream(build_incremental_stream());
                }
                Ok(msg) => match msg {
                    // Barrier arrives.
                    Either::Left(msg) => match &msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    _ => (),
                                }
                            }
                            if let Some(last_snapshot) = *last_snapshot.lock() {
                                let state_row =
                                    OwnedRow::new(vec![ScalarImpl::Int64(last_snapshot).into()]);
                                if let Some(prev_persisted_snapshot_id) = prev_persisted_snapshot {
                                    let prev_state_row = OwnedRow::new(vec![
                                        ScalarImpl::Int64(prev_persisted_snapshot_id).into(),
                                    ]);
                                    state_table.update(prev_state_row, state_row);
                                } else {
                                    state_table.insert(state_row);
                                }
                                prev_persisted_snapshot = Some(last_snapshot);
                            }
                            state_table
                                .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                                .await?;
                            // Propagate the barrier.
                            yield msg;
                        }
                        // Only barrier can be received.
                        _ => unreachable!(),
                    },
                    // Data arrives.
                    Either::Right(chunk) => {
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }
}

/// `last_snapshot` is EXCLUSIVE (i.e., already scanned)
#[try_stream(
    boxed,
    ok = iceberg::scan::FileScanTask,
    error = StreamExecutorError
)]
async fn incremental_scan_stream(
    scan_planner: IcebergScanPlanner,
    last_snapshot_lock: Arc<Mutex<Option<i64>>>,
    list_interval_sec: u64,
) {
    let mut last_snapshot: Option<i64> = *last_snapshot_lock.lock();
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(list_interval_sec)).await;

        match scan_planner.plan_incremental(last_snapshot).await? {
            IcebergIncrementalScan::EmptyTable | IcebergIncrementalScan::UpToDate { .. } => {}
            IcebergIncrementalScan::Planned(plan) => {
                #[for_await]
                for scan_task in plan.tasks {
                    yield scan_task?;
                }

                last_snapshot = Some(plan.snapshot_id);
                *last_snapshot_lock.lock() = last_snapshot;
                scan_planner.record_caught_up();
            }
        }
    }
}

impl<S: StateStore> Execute for IcebergListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}
