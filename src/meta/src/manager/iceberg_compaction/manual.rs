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

use anyhow::anyhow;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use risingwave_pb::id::IcebergCompactionTaskId;

use super::*;

impl IcebergCompactionManager {
    pub(super) fn complete_manual_task_waiter(
        waiter: ManualCompactionWaiter,
        report: &IcebergReportTask,
    ) {
        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        let result = match status {
            IcebergReportTaskStatus::Success => Ok(report.task_id),
            IcebergReportTaskStatus::Drained
            | IcebergReportTaskStatus::Failed
            | IcebergReportTaskStatus::Unspecified => {
                let message = report
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "manual iceberg compaction failed".to_owned());
                Err(anyhow!(
                    "Manual iceberg compaction task {} for sink {} failed: {}",
                    report.task_id,
                    report.sink_id,
                    message
                )
                .into())
            }
        };

        let _ = waiter.send(result);
    }

    pub async fn trigger_manual_compaction(
        &self,
        sink_id: SinkId,
    ) -> MetaResult<IcebergCompactionTaskId> {
        // Fast-fail before registering a waiter when no compactor can pull the
        // scheduler task.
        if self.iceberg_compactor_manager.compactor_num() == 0 {
            return Err(anyhow!("No iceberg compactor available").into());
        }

        let waiter = self.start_manual_compaction(sink_id).await?;
        let cleanup_guard = scopeguard::guard(sink_id, |sink_id| {
            self.cancel_manual_compaction_waiter(sink_id)
        });

        tracing::info!(
            "Manual compaction requested for sink {}, waiting for completion...",
            sink_id
        );

        match waiter.await {
            Ok(result) => {
                let _ = scopeguard::ScopeGuard::into_inner(cleanup_guard);
                result
            }
            Err(_) => {
                let _ = scopeguard::ScopeGuard::into_inner(cleanup_guard);
                Err(anyhow!(
                    "Manual iceberg compaction waiter dropped unexpectedly for sink {}",
                    sink_id,
                )
                .into())
            }
        }
    }
}
