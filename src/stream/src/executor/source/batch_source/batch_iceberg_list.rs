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

use anyhow::anyhow;
use either::Either;
use parking_lot::RwLock;
use risingwave_common::array::Op;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::id::TableId;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    IcebergScanMetricsLabels, IcebergScanPlanner, IcebergScanProjection, PersistedFileScanTask,
};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

pub struct BatchIcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,
    /// Columns of fetch executor, used to plan files.
    /// For backward compatibility, this can be None, meaning all columns are needed.
    downstream_columns: Option<Vec<ColumnCatalog>>,
    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// Local barrier manager for reporting list finished
    barrier_manager: LocalBarrierManager,
    associated_table_id: TableId,
    /// Metrics for monitor.
    _metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> BatchIcebergListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        downstream_columns: Option<Vec<ColumnCatalog>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        barrier_manager: LocalBarrierManager,
        associated_table_id: Option<TableId>,
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core,
            downstream_columns,
            barrier_receiver: Some(barrier_receiver),
            barrier_manager,
            associated_table_id: associated_table_id.unwrap(),
            _metrics: metrics,
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

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::Iceberg(iceberg_properties) = config else {
            unreachable!()
        };

        let iceberg_table_name = iceberg_properties.table.table_name().to_owned();
        let source_id_str = self.stream_source_core.source_id.to_string();
        let source_name_str = self.stream_source_core.source_name.clone();
        let scan_metrics =
            IcebergScanMetricsLabels::new(source_id_str, source_name_str, iceberg_table_name);
        let scan_planner = IcebergScanPlanner::new(
            (*iceberg_properties).clone(),
            IcebergScanProjection::from_downstream_columns(self.downstream_columns.as_deref()),
            Some(scan_metrics.clone()),
        );

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let mut stream = StreamReaderWithPause::<true, _>::new(
            barrier_stream,
            futures::stream::pending().boxed(),
        );
        let mut is_refreshing = false;
        let is_list_finished = Arc::new(RwLock::new(false));

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "encountered an error in batch iceberg list");

                    GLOBAL_ERROR_METRICS.user_source_error.report([
                        e.variant_name().to_owned(),
                        self.stream_source_core.source_id.to_string(),
                        self.actor_ctx.fragment_id.to_string(),
                        self.associated_table_id.to_string(),
                    ]);

                    scan_metrics.record_scan_error("list_error");

                    if is_refreshing {
                        tracing::info!(
                            source_id = %self.stream_source_core.source_id,
                            table_id = %self.associated_table_id,
                            "re-listing iceberg scan tasks due to error"
                        );
                        let iceberg_list_stream = Self::list_iceberg_scan_task(
                            scan_planner.clone(),
                            is_list_finished.clone(),
                        )
                        .boxed();
                        stream.replace_data_stream(iceberg_list_stream);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(e);
                }
                Ok(msg) => match msg {
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    Mutation::RefreshStart {
                                        associated_source_id,
                                        ..
                                    } if associated_source_id
                                        == &self.stream_source_core.source_id =>
                                    {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %self.stream_source_core.source_id,
                                            table_id = %self.associated_table_id,
                                            "RefreshStart triggered file re-listing"
                                        );
                                        is_refreshing = true;

                                        *is_list_finished.write() = false;

                                        // re-list iceberg scan tasks
                                        let iceberg_list_stream = Self::list_iceberg_scan_task(
                                            scan_planner.clone(),
                                            is_list_finished.clone(),
                                        )
                                        .boxed();
                                        stream.replace_data_stream(iceberg_list_stream);
                                    }
                                    _ => (),
                                }
                            }

                            if is_refreshing && *is_list_finished.read() && barrier.is_checkpoint()
                            {
                                tracing::info!(
                                    ?barrier.epoch,
                                    source_id = %self.stream_source_core.source_id,
                                    table_id = %self.associated_table_id,
                                    "reporting batch iceberg list finished"
                                );
                                self.barrier_manager.report_source_list_finished(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.associated_table_id,
                                    self.stream_source_core.source_id,
                                );
                                is_refreshing = false;
                            }

                            yield Message::Barrier(barrier);
                        }
                        _ => unreachable!(),
                    },
                    Either::Right(file_scan_task) => {
                        let data_file_path = file_scan_task.data_file_path.clone();
                        let persisted_task = PersistedFileScanTask::encode(file_scan_task)?;
                        let chunk = StreamChunk::from_rows(
                            &[(
                                Op::Insert,
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Utf8(data_file_path.into())),
                                    Some(ScalarImpl::Jsonb(persisted_task)),
                                ]),
                            )],
                            &[DataType::Varchar, DataType::Jsonb],
                        );

                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }

    #[try_stream(
        ok = iceberg::scan::FileScanTask,
        error = StreamExecutorError
    )]
    async fn list_iceberg_scan_task(
        scan_planner: IcebergScanPlanner,
        is_list_finished: Arc<RwLock<bool>>,
    ) {
        if let Some(snapshot_plan) = scan_planner.plan_current_snapshot().await? {
            #[for_await]
            for scan_task in snapshot_plan.tasks {
                let scan_task = scan_task?;
                tracing::debug!(
                    "list_iceberg_scan_task: data_file_path={}, scan_task={:?}",
                    scan_task.data_file_path,
                    scan_task
                );
                yield scan_task;
            }
        }
        *is_list_finished.write() = true;
    }
}

impl<S: StateStore> Execute for BatchIcebergListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchIcebergListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchIcebergListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .field("downstream_columns", &self.downstream_columns)
            .finish()
    }
}
