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

use std::collections::VecDeque;
#[cfg(madsim)]
use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;

use iceberg::spec::{DataFile, SerializedDataFile};
use iceberg::table::Table;
use risingwave_connector::sink::iceberg::IcebergCommitResult;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::iceberg_compaction::{
    PkIndexCompactionResult as PbPkIndexCompactionResult, SubscribeIcebergCompactionEventRequest,
    subscribe_iceberg_compaction_event_request,
};
use risingwave_pb::id::IcebergCompactionTaskId;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::TaskKey;
use crate::hummock::{HummockError, HummockResult};

/// Per-plan result of a pk-index coordinated compaction run (rewrite without commit).
///
/// Produced by the compactor when the dispatched task has `pk_index_coordinated == true`. The
/// actual iceberg commit is performed later by meta's iceberg pk-index sink coordinator, so the
/// compactor only surfaces the rewrite output, the input file paths, and the snapshot it read
/// from.
#[derive(Clone)]
pub(crate) struct PkIndexCompactionResult {
    /// Newly written data files produced by the rewrite.
    pub(crate) output_files: Vec<SerializedDataFile>,
    pub(crate) schema_id: i32,
    pub(crate) partition_spec_id: i32,
    /// Paths of all input files (data + delete) consumed by the rewrite, taken directly from the
    /// compaction plan's `FileGroup`. No manifest walk required.
    pub(crate) input_file_paths: Vec<String>,
    /// Snapshot the rewrite plan read from.
    pub(crate) read_snapshot_id: i64,
}

// Manual Debug: `SerializedDataFile` does not implement `Debug`.
impl std::fmt::Debug for PkIndexCompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkIndexCompactionResult")
            .field("output_file_count", &self.output_files.len())
            .field("input_file_count", &self.input_file_paths.len())
            .field("read_snapshot_id", &self.read_snapshot_id)
            .finish()
    }
}

/// Builds the pk-index coordinated report payload from a no-commit rewrite.
///
/// - Output files come from `data_files` (the rewrite's output, taken from `CompactionResult`)
///   and are converted to [`SerializedDataFile`] for JSON serialization, using `table`'s partition
///   type and format version.
/// - `input_file_paths` and `read_snapshot_id` must be captured by the caller *before* the
///   compaction plan is consumed by `compact_with_plan` (the plan's `FileGroup` already carries
///   each input file's path via `FileScanTask::data_file_path` — no manifest walk needed).
pub(crate) fn build_pk_index_compaction_result(
    table: &Table,
    data_files: Vec<DataFile>,
    input_file_paths: Vec<String>,
    read_snapshot_id: i64,
) -> HummockResult<PkIndexCompactionResult> {
    let partition_type = table.metadata().default_partition_type();
    let format_version = table.metadata().format_version();

    let output_files = data_files
        .into_iter()
        .map(|data_file| {
            SerializedDataFile::try_from(data_file, partition_type, format_version)
                .map_err(|e| HummockError::compaction_executor(e.as_report()))
        })
        .collect::<HummockResult<Vec<_>>>()?;

    Ok(PkIndexCompactionResult {
        output_files,
        schema_id: table.metadata().current_schema_id(),
        partition_spec_id: table.metadata().default_partition_spec_id(),
        input_file_paths,
        read_snapshot_id,
    })
}

#[derive(Debug)]
pub(crate) struct IcebergPlanCompletion {
    pub(crate) task_key: TaskKey,
    pub(crate) error_message: Option<String>,
    /// Present only for pk-index coordinated plans that completed successfully.
    pub(crate) pk_index_result: Option<PkIndexCompactionResult>,
}

pub(crate) type IcebergTaskReport = subscribe_iceberg_compaction_event_request::ReportTask;

#[cfg(madsim)]
static SIMULATED_PK_INDEX_RESULT: LazyLock<Mutex<Option<PbPkIndexCompactionResult>>> =
    LazyLock::new(|| Mutex::new(None));

#[cfg(madsim)]
pub fn set_simulated_pk_index_compaction_result(result: Option<PbPkIndexCompactionResult>) {
    *SIMULATED_PK_INDEX_RESULT.lock().unwrap() = result;
}

#[cfg(madsim)]
pub(crate) fn simulated_pk_index_compaction_result() -> Option<PbPkIndexCompactionResult> {
    SIMULATED_PK_INDEX_RESULT.lock().unwrap().clone()
}

pub(crate) enum ReportSendResult {
    Sent,
    RestartStream,
}

pub(crate) struct IcebergTaskTracker {
    sink_id: u32,
    admitted_plans: usize,
    fully_admitted_bounded_round: bool,
    remaining_admitted_plans: usize,
    successful_plans: usize,
    failed_plans: usize,
    first_error: Option<String>,
    /// Pk-index coordinated rewrite results, aggregated across all plans of the task. Empty for
    /// non-coordinated tasks.
    pk_index_results: Vec<PkIndexCompactionResult>,
}

impl IcebergTaskTracker {
    pub(crate) fn new(
        sink_id: u32,
        admitted_plans: usize,
        fully_admitted_bounded_round: bool,
    ) -> Self {
        Self {
            sink_id,
            admitted_plans,
            fully_admitted_bounded_round,
            remaining_admitted_plans: admitted_plans,
            successful_plans: 0,
            failed_plans: 0,
            first_error: None,
            pk_index_results: Vec::new(),
        }
    }

    pub(crate) fn record_completion(
        &mut self,
        error_message: Option<String>,
        pk_index_result: Option<PkIndexCompactionResult>,
    ) {
        debug_assert!(self.remaining_admitted_plans > 0);
        self.remaining_admitted_plans -= 1;
        if let Some(error_message) = error_message {
            self.failed_plans += 1;
            if self.first_error.is_none() {
                self.first_error = Some(error_message);
            }
        } else {
            self.successful_plans += 1;
            if let Some(pk_index_result) = pk_index_result {
                self.pk_index_results.push(pk_index_result);
            }
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        // This only proves that the admitted batch finished. The report also
        // considers whether this task covered every plan in a bounded round.
        self.remaining_admitted_plans == 0
    }

    pub(crate) fn sink_id(&self) -> u32 {
        self.sink_id
    }

    pub(crate) fn admitted_plans(&self) -> usize {
        self.admitted_plans
    }

    pub(crate) fn successful_plans(&self) -> usize {
        self.successful_plans
    }

    pub(crate) fn failed_plans(&self) -> usize {
        self.failed_plans
    }

    pub(crate) fn into_report(self, task_id: IcebergCompactionTaskId) -> IcebergTaskReport {
        let is_drained =
            self.fully_admitted_bounded_round && self.successful_plans == self.admitted_plans;
        let error_message = if self.successful_plans > 0 {
            None
        } else {
            Some(
                self.first_error
                    .unwrap_or_else(|| "All admitted iceberg compaction plans failed".to_owned()),
            )
        };
        let mut report = build_iceberg_task_report(task_id, self.sink_id, error_message);
        if is_drained {
            report.status =
                subscribe_iceberg_compaction_event_request::report_task::Status::Drained as i32;
        }
        populate_pk_index_report_fields(&mut report, self.pk_index_results);
        report
    }
}

/// Flattens the per-plan pk-index coordinated results into the `ReportTask` payload fields.
fn populate_pk_index_report_fields(
    report: &mut IcebergTaskReport,
    pk_index_results: Vec<PkIndexCompactionResult>,
) {
    if pk_index_results.is_empty() {
        return;
    }

    let read_snapshot_id = pk_index_results[0].read_snapshot_id;
    let schema_id = pk_index_results[0].schema_id;
    let partition_spec_id = pk_index_results[0].partition_spec_id;
    // The planner builds all plans of a task from one branch snapshot, so they must agree.
    // Reject inconsistencies in release builds as well: using the first plan's metadata to encode
    // files produced against another snapshot or spec would create an invalid report payload.
    if !pk_index_results.iter().all(|result| {
        result.read_snapshot_id == read_snapshot_id
            && result.schema_id == schema_id
            && result.partition_spec_id == partition_spec_id
    }) {
        return fail_pk_index_report(
            report,
            "pk_index_result.metadata",
            "coordinated compaction plans must share one read snapshot, schema, and partition spec",
        );
    }

    let mut output_files: Vec<SerializedDataFile> = Vec::new();
    let mut input_file_paths: Vec<String> = Vec::new();
    for pk_index_result in pk_index_results {
        output_files.extend(pk_index_result.output_files);
        input_file_paths.extend(pk_index_result.input_file_paths);
    }

    // This task is pk-index coordinated (that's why we're populating these fields at all), so
    // meta's sink coordinator relies on this payload to perform the actual iceberg commit.
    // Reporting Success without it would make meta silently treat the rewrite as done while
    // dropping the output files entirely. Fail the report instead so meta retries the task.
    let output_files = match SinkMetadata::try_from(&IcebergCommitResult {
        schema_id,
        partition_spec_id,
        data_files: output_files,
    }) {
        Ok(metadata) => metadata,
        Err(e) => return fail_pk_index_report(report, "pk_index_result.output_files", e),
    };
    report.pk_index_result = Some(PbPkIndexCompactionResult {
        output_files: Some(output_files),
        input_file_paths,
        read_snapshot_id,
    });
}

/// Marks `report` as failed after a pk-index payload field failed validation or serialization, so
/// meta retries the task instead of silently dropping the compaction output.
fn fail_pk_index_report(
    report: &mut IcebergTaskReport,
    field_name: &str,
    error: impl std::fmt::Display,
) {
    tracing::warn!(
        %error,
        task_id = %report.task_id,
        sink_id = report.sink_id,
        "Failed to build {field_name}; failing pk-index compaction report"
    );
    report.pk_index_result = None;
    report.status = subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32;
    report.error_message = Some(format!(
        "invalid pk-index compaction report payload ({field_name}): {}",
        error
    ));
}

pub(crate) fn build_iceberg_task_report(
    task_id: IcebergCompactionTaskId,
    sink_id: u32,
    error_message: Option<String>,
) -> IcebergTaskReport {
    subscribe_iceberg_compaction_event_request::ReportTask {
        task_id,
        sink_id,
        status: if error_message.is_some() {
            subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32
        } else {
            subscribe_iceberg_compaction_event_request::report_task::Status::Success as i32
        },
        error_message,
        pk_index_result: None,
    }
}

pub(crate) fn build_drained_iceberg_task_report(
    task_id: IcebergCompactionTaskId,
    sink_id: u32,
) -> IcebergTaskReport {
    let mut report = build_iceberg_task_report(task_id, sink_id, None);
    report.status = subscribe_iceberg_compaction_event_request::report_task::Status::Drained as i32;
    report
}

pub(crate) fn send_iceberg_task_report(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    report_event: IcebergTaskReport,
) -> Result<(), IcebergTaskReport> {
    if let Err(e) = request_sender.send(SubscribeIcebergCompactionEventRequest {
        event: Some(
            subscribe_iceberg_compaction_event_request::Event::ReportTask(report_event.clone()),
        ),
        create_at: SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_millis() as u64,
    }) {
        tracing::warn!(
            iceberg_component = "compaction_worker",
            iceberg_operation = "report_task",
            error = %e.as_report(),
            task_id = %report_event.task_id,
            sink_id = report_event.sink_id,
            "iceberg_compaction_task_report_send_failed",
        );
        return Err(report_event);
    }

    Ok(())
}

pub(crate) fn send_or_buffer_iceberg_task_report(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    pending_task_reports: &mut VecDeque<IcebergTaskReport>,
    report: IcebergTaskReport,
) -> ReportSendResult {
    if let Err(report) = send_iceberg_task_report(request_sender, report) {
        pending_task_reports.push_back(report);
        return ReportSendResult::RestartStream;
    }
    ReportSendResult::Sent
}

pub(crate) fn flush_pending_iceberg_task_reports(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    pending_task_reports: &mut VecDeque<IcebergTaskReport>,
) -> ReportSendResult {
    while let Some(report_event) = pending_task_reports.pop_front() {
        if let Err(report_event) = send_iceberg_task_report(request_sender, report_event) {
            pending_task_reports.push_front(report_event);
            return ReportSendResult::RestartStream;
        }
    }
    ReportSendResult::Sent
}

#[cfg(test)]
mod tests {
    use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request;

    use super::*;

    #[test]
    fn test_send_iceberg_task_report_returns_payload_on_send_failure() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);

        let report = build_iceberg_task_report(7.into(), 9, Some("send failure".to_owned()));
        let failed_report = send_iceberg_task_report(&tx, report.clone()).unwrap_err();

        assert_eq!(failed_report.task_id, report.task_id);
        assert_eq!(failed_report.sink_id, report.sink_id);
        assert_eq!(failed_report.error_message, report.error_message);
    }

    #[test]
    fn test_build_iceberg_task_result_partial_enqueue_is_success_if_admitted_plan_succeeds() {
        let mut tracker = IcebergTaskTracker::new(9, 1, false);
        tracker.record_completion(None, None);

        let report = tracker.into_report(7.into());

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Success as i32
        );
        assert!(report.error_message.is_none());
    }

    #[test]
    fn test_fully_admitted_bounded_round_reports_drained_after_success() {
        let mut tracker = IcebergTaskTracker::new(9, 2, true);
        tracker.record_completion(None, None);
        tracker.record_completion(None, None);

        let report = tracker.into_report(7.into());

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Drained as i32
        );
        assert!(report.error_message.is_none());
    }

    #[test]
    fn test_fully_admitted_bounded_round_with_partial_failure_reports_success() {
        let mut tracker = IcebergTaskTracker::new(9, 2, true);
        tracker.record_completion(None, None);
        tracker.record_completion(Some("failure".to_owned()), None);

        let report = tracker.into_report(7.into());

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Success as i32
        );
        assert!(report.error_message.is_none());
    }

    #[test]
    fn test_bounded_empty_planning_is_reported_as_drained() {
        let report = build_drained_iceberg_task_report(7.into(), 9);

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Drained as i32
        );
        assert!(report.error_message.is_none());
    }

    #[test]
    fn test_into_report_populates_pk_index_fields_when_pk_index_result_present() {
        let mut tracker = IcebergTaskTracker::new(9, 2, false);
        tracker.record_completion(
            None,
            Some(PkIndexCompactionResult {
                output_files: vec![],
                schema_id: 1,
                partition_spec_id: 2,
                input_file_paths: vec![],
                read_snapshot_id: 42,
            }),
        );
        tracker.record_completion(
            None,
            Some(PkIndexCompactionResult {
                output_files: vec![],
                schema_id: 1,
                partition_spec_id: 2,
                input_file_paths: vec![],
                read_snapshot_id: 42,
            }),
        );

        let report = tracker.into_report(7.into());

        let result = report.pk_index_result.unwrap();
        assert!(result.output_files.is_some());
        assert!(result.input_file_paths.is_empty());
        assert_eq!(result.read_snapshot_id, 42);
    }

    #[test]
    fn test_into_report_rejects_mismatched_pk_index_plan_metadata() {
        for (read_snapshot_id, schema_id, partition_spec_id) in [(43, 1, 2), (42, 3, 2), (42, 1, 4)]
        {
            let mut tracker = IcebergTaskTracker::new(9, 2, false);
            tracker.record_completion(
                None,
                Some(PkIndexCompactionResult {
                    output_files: vec![],
                    schema_id: 1,
                    partition_spec_id: 2,
                    input_file_paths: vec![],
                    read_snapshot_id: 42,
                }),
            );
            tracker.record_completion(
                None,
                Some(PkIndexCompactionResult {
                    output_files: vec![],
                    schema_id,
                    partition_spec_id,
                    input_file_paths: vec![],
                    read_snapshot_id,
                }),
            );

            let report = tracker.into_report(7.into());

            assert_eq!(
                report.status,
                subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32
            );
            assert!(report.pk_index_result.is_none());
        }
    }

    #[test]
    fn test_into_report_leaves_pk_index_fields_none_for_non_pk_index_task() {
        let mut tracker = IcebergTaskTracker::new(9, 1, false);
        tracker.record_completion(None, None);

        let report = tracker.into_report(7.into());

        assert!(report.pk_index_result.is_none());
    }

    #[test]
    fn test_build_iceberg_task_result_fails_if_all_admitted_plans_fail() {
        let mut tracker = IcebergTaskTracker::new(9, 2, false);
        tracker.record_completion(Some("first failure".to_owned()), None);
        tracker.record_completion(Some("second failure".to_owned()), None);

        let report = tracker.into_report(7.into());

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32
        );
        assert_eq!(report.error_message.as_deref(), Some("first failure"));
    }
}
