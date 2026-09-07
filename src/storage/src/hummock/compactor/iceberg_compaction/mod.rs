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

pub use self::iceberg_compactor_runner::create_task_execution;
#[cfg(madsim)]
pub use self::report::set_simulated_pk_index_compaction_result;
#[cfg(madsim)]
pub(crate) use self::report::simulated_pk_index_compaction_result;
pub(crate) use self::report::{
    IcebergPlanCompletion, IcebergTaskReport, IcebergTaskTracker, PkIndexCompactionResult,
    ReportSendResult, build_drained_iceberg_task_report, build_iceberg_task_report,
    build_pk_index_compaction_result, flush_pending_iceberg_task_reports,
    send_or_buffer_iceberg_task_report,
};
use crate::hummock::compactor::iceberg_compaction::iceberg_compactor_runner::IcebergCompactionPlanRunner;

pub(crate) mod iceberg_compactor_runner;
pub(crate) mod memory;
pub(crate) mod report;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use risingwave_pb::id::IcebergCompactionTaskId;
use tokio::sync::Notify;

use crate::monitor::CompactorMetrics;

/// Unique key combining `(task_id, plan_index)` since one task can have multiple plans.
pub(crate) type TaskKey = (IcebergCompactionTaskId, usize);

/// Task metadata for queue operations.
#[derive(Debug, Clone)]
pub struct IcebergTaskMeta {
    pub task_id: IcebergCompactionTaskId,
    pub plan_index: usize,
    /// Must be in range `1..=max_parallelism`
    pub required_parallelism: u32,
    /// Estimated heap peak used for admission control.
    /// Must be in range `1..=total_memory_budget_bytes`.
    pub memory_reservation_bytes: usize,
}

#[derive(Debug)]
pub struct PoppedIcebergTask {
    pub meta: IcebergTaskMeta,
    pub runner: Option<IcebergCompactionPlanRunner>,
}

impl IcebergTaskMeta {
    fn key(&self) -> TaskKey {
        (self.task_id, self.plan_index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IcebergTaskResources {
    required_parallelism: u32,
    memory_reservation_bytes: usize,
}

/// Internal storage for the task queue.
struct IcebergTaskQueueInner {
    /// FIFO queue of waiting task metadata
    deque: VecDeque<IcebergTaskMeta>,
    /// Maps `(task_id, plan_index)` to its reserved resources for tracking
    resource_map: HashMap<TaskKey, IcebergTaskResources>,
    /// Sum of `required_parallelism` for all waiting tasks
    waiting_parallelism_sum: u32,
    /// Sum of `required_parallelism` for all running tasks
    running_parallelism_sum: u32,
    /// Sum of estimated heap peak reservations for all running tasks
    running_memory_reservation_bytes: usize,
    /// Optional runner payloads indexed by `(task_id, plan_index)`
    runners: HashMap<TaskKey, IcebergCompactionPlanRunner>,
}

/// FIFO task queue with parallelism- and memory-based scheduling for Iceberg compaction.
///
/// Tasks execute in submission order when sufficient parallelism is available.
/// The queue tracks waiting and running tasks to prevent over-commitment of running resources.
///
/// Constraints:
/// - Each task requires `1..=max_parallelism` units
/// - Each task reserves `1..=total_memory_budget_bytes` bytes while running
/// - Total waiting parallelism cannot exceed `pending_parallelism_budget`
/// - Total running parallelism cannot exceed `max_parallelism`
/// - Total running memory cannot exceed `total_memory_budget_bytes`
/// - Tasks block until enough parallelism and memory are available
///
/// Note: The queue rejects duplicate task keys but does not reorder or merge tasks.
/// Higher-level task management remains Meta's responsibility.
pub struct IcebergTaskQueue {
    inner: IcebergTaskQueueInner,
    /// Maximum concurrent parallelism for running tasks
    max_parallelism: u32,
    /// Maximum total parallelism for waiting tasks (backpressure limit)
    pending_parallelism_budget: u32,
    /// Maximum total memory reserved by running tasks
    total_memory_budget_bytes: usize,
    /// Notification for event-driven scheduling
    schedule_notify: Arc<Notify>,
    metrics: Arc<CompactorMetrics>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Added,
    /// Would exceed `pending_parallelism_budget`
    RejectedCapacity,
    /// The task's parallelism or estimated memory exceeds the worker capacity.
    RejectedTooLarge,
    /// `required_parallelism` == 0
    RejectedInvalidParallelism,
    /// Task with same `(task_id, plan_index)` already exists
    RejectedDuplicate,
}

impl IcebergTaskQueue {
    #[cfg(test)]
    pub fn new(
        max_parallelism: u32,
        pending_parallelism_budget: u32,
        total_memory_budget_bytes: usize,
    ) -> Self {
        Self::new_with_metrics(
            max_parallelism,
            pending_parallelism_budget,
            total_memory_budget_bytes,
            Arc::new(CompactorMetrics::unused()),
        )
    }

    pub(crate) fn new_with_metrics(
        max_parallelism: u32,
        pending_parallelism_budget: u32,
        total_memory_budget_bytes: usize,
        metrics: Arc<CompactorMetrics>,
    ) -> Self {
        assert!(max_parallelism > 0, "max_parallelism must be > 0");
        assert!(
            pending_parallelism_budget >= max_parallelism,
            "pending budget should allow at least one task"
        );
        assert!(
            total_memory_budget_bytes > 0,
            "total memory budget must be > 0"
        );
        metrics
            .iceberg_compaction_memory_budget_bytes
            .set(i64::try_from(total_memory_budget_bytes).unwrap_or(i64::MAX));
        metrics
            .iceberg_compaction_running_memory_reservation_bytes
            .set(0);
        Self {
            inner: IcebergTaskQueueInner {
                deque: VecDeque::new(),
                resource_map: HashMap::new(),
                waiting_parallelism_sum: 0,
                running_parallelism_sum: 0,
                running_memory_reservation_bytes: 0,
                runners: HashMap::new(),
            },
            max_parallelism,
            pending_parallelism_budget,
            total_memory_budget_bytes,
            schedule_notify: Arc::new(Notify::new()),
            metrics,
        }
    }

    /// Waits until there are tasks that can be scheduled.
    ///
    /// Returns `true` if there are schedulable tasks, `false` otherwise.
    /// Use this in a `tokio::select!` to wake up when tasks become schedulable.
    pub async fn wait_schedulable(&self) -> bool {
        // Check if we have tasks that can be scheduled right now
        if self.has_schedulable_tasks() {
            return true;
        }
        // Otherwise wait for notification
        self.schedule_notify.notified().await;
        self.has_schedulable_tasks()
    }

    fn has_schedulable_tasks(&self) -> bool {
        if let Some(front_task) = self.inner.deque.front() {
            let available_parallelism = self
                .max_parallelism
                .saturating_sub(self.inner.running_parallelism_sum);
            let available_memory = self
                .total_memory_budget_bytes
                .saturating_sub(self.inner.running_memory_reservation_bytes);
            available_parallelism >= front_task.required_parallelism
                && available_memory >= front_task.memory_reservation_bytes
        } else {
            false
        }
    }

    fn notify_schedulable(&self) {
        if self.has_schedulable_tasks() {
            self.schedule_notify.notify_one();
        }
    }

    pub fn running_parallelism_sum(&self) -> u32 {
        self.inner.running_parallelism_sum
    }

    pub fn waiting_parallelism_sum(&self) -> u32 {
        self.inner.waiting_parallelism_sum
    }

    fn available_parallelism(&self) -> u32 {
        self.max_parallelism
            .saturating_sub(self.inner.running_parallelism_sum)
    }

    fn available_memory_reservation_bytes(&self) -> usize {
        self.total_memory_budget_bytes
            .saturating_sub(self.inner.running_memory_reservation_bytes)
    }

    fn update_running_memory_metric(&self) {
        self.metrics
            .iceberg_compaction_running_memory_reservation_bytes
            .set(i64::try_from(self.inner.running_memory_reservation_bytes).unwrap_or(i64::MAX));
    }

    /// Push a task into the queue.
    ///
    /// The task is validated and added to the end of the FIFO queue if constraints are met.
    pub fn push(
        &mut self,
        meta: IcebergTaskMeta,
        runner: Option<IcebergCompactionPlanRunner>,
    ) -> PushResult {
        if meta.required_parallelism == 0 {
            return PushResult::RejectedInvalidParallelism;
        }
        if meta.required_parallelism > self.max_parallelism
            || meta.memory_reservation_bytes > self.total_memory_budget_bytes
        {
            return PushResult::RejectedTooLarge;
        }
        let key = meta.key();

        // Reject duplicate keys to prevent inconsistent state between resource_map and deque.
        if self.inner.resource_map.contains_key(&key) {
            return PushResult::RejectedDuplicate;
        }

        let Some(new_parallelism_total) = self
            .inner
            .waiting_parallelism_sum
            .checked_add(meta.required_parallelism)
        else {
            return PushResult::RejectedCapacity;
        };
        if new_parallelism_total > self.pending_parallelism_budget {
            return PushResult::RejectedCapacity;
        }
        self.inner.resource_map.insert(
            key,
            IcebergTaskResources {
                required_parallelism: meta.required_parallelism,
                memory_reservation_bytes: meta.memory_reservation_bytes,
            },
        );
        self.inner.waiting_parallelism_sum = new_parallelism_total;

        if let Some(r) = runner {
            self.inner.runners.insert(key, r);
        }

        self.inner.deque.push_back(meta);

        self.notify_schedulable();
        PushResult::Added
    }

    /// Pop the next task if sufficient parallelism and memory are available.
    ///
    /// Returns `None` if the queue is empty or the front task cannot fit
    /// within the available running resource budgets.
    pub fn pop(&mut self) -> Option<PoppedIcebergTask> {
        let front = self.inner.deque.front()?;
        if front.required_parallelism > self.available_parallelism() {
            return None;
        }
        if front.memory_reservation_bytes > self.available_memory_reservation_bytes() {
            return None;
        }

        let meta = self.inner.deque.pop_front()?;
        self.inner.waiting_parallelism_sum = self
            .inner
            .waiting_parallelism_sum
            .saturating_sub(meta.required_parallelism);
        self.inner.running_parallelism_sum = self
            .inner
            .running_parallelism_sum
            .saturating_add(meta.required_parallelism);
        self.inner.running_memory_reservation_bytes = self
            .inner
            .running_memory_reservation_bytes
            .checked_add(meta.memory_reservation_bytes)
            .expect("running memory reservation sum overflowed");

        let key = meta.key();
        self.update_running_memory_metric();
        let runner = self.inner.runners.remove(&key);
        Some(PoppedIcebergTask { meta, runner })
    }

    /// Mark a task as finished, freeing its parallelism and memory for other tasks.
    ///
    /// Returns `true` if the task was found and removed, `false` otherwise.
    pub fn finish_running(&mut self, task_key: TaskKey) -> bool {
        let Some(resources) = self.inner.resource_map.remove(&task_key) else {
            tracing::warn!(
                task_id = %task_key.0,
                plan_index = task_key.1,
                "finish_running called for unknown task key, possible bug: double-finish or invalid key"
            );
            return false;
        };
        self.inner.running_parallelism_sum = self
            .inner
            .running_parallelism_sum
            .saturating_sub(resources.required_parallelism);
        self.inner.running_memory_reservation_bytes = self
            .inner
            .running_memory_reservation_bytes
            .checked_sub(resources.memory_reservation_bytes)
            .expect("running memory reservation bookkeeping underflowed");
        self.inner.runners.remove(&task_key);
        self.update_running_memory_metric();
        self.notify_schedulable();
        true
    }

    /// Cancel all waiting plans belonging to the given task.
    ///
    /// Returns the number of waiting plans removed from the queue.
    pub fn cancel_waiting_task(&mut self, task_id: IcebergCompactionTaskId) -> usize {
        let mut retained = VecDeque::with_capacity(self.inner.deque.len());
        let mut cancelled_parallelism = 0;
        let mut cancelled_count = 0;

        while let Some(meta) = self.inner.deque.pop_front() {
            if meta.task_id == task_id {
                cancelled_parallelism += meta.required_parallelism;
                cancelled_count += 1;
                self.inner.resource_map.remove(&meta.key());
                self.inner.runners.remove(&meta.key());
            } else {
                retained.push_back(meta);
            }
        }

        self.inner.deque = retained;
        self.inner.waiting_parallelism_sum = self
            .inner
            .waiting_parallelism_sum
            .saturating_sub(cancelled_parallelism);

        if cancelled_count > 0 {
            self.notify_schedulable();
        }

        cancelled_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_meta(id: u64, plan_index: usize, p: u32) -> IcebergTaskMeta {
        IcebergTaskMeta {
            task_id: id.into(),
            plan_index,
            required_parallelism: p,
            memory_reservation_bytes: 1,
        }
    }

    #[test]
    fn test_basic_push_pop() {
        let mut q = IcebergTaskQueue::new(8, 32, usize::MAX);
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 4);

        let popped = q.pop().expect("should pop");
        assert_eq!(popped.meta.task_id.as_raw_id(), 1);
        assert_eq!(q.waiting_parallelism_sum(), 0);
        assert_eq!(q.running_parallelism_sum(), 4);

        assert!(q.finish_running((1.into(), 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_fifo_ordering() {
        let mut q = IcebergTaskQueue::new(8, 32, usize::MAX);
        assert_eq!(q.push(mk_meta(1, 0, 2), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 2), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(3, 0, 2), None), PushResult::Added);

        assert_eq!(q.pop().unwrap().meta.task_id.as_raw_id(), 1);
        assert_eq!(q.pop().unwrap().meta.task_id.as_raw_id(), 2);
        assert_eq!(q.pop().unwrap().meta.task_id.as_raw_id(), 3);
    }

    #[test]
    fn test_capacity_reject() {
        let mut q = IcebergTaskQueue::new(4, 6, usize::MAX);
        assert_eq!(q.push(mk_meta(1, 0, 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 3), None), PushResult::Added); // sum=6
        assert_eq!(q.push(mk_meta(3, 0, 1), None), PushResult::RejectedCapacity); // would exceed
    }

    #[test]
    fn test_invalid_parallelism() {
        let mut q = IcebergTaskQueue::new(4, 10, usize::MAX);
        assert_eq!(
            q.push(mk_meta(1, 0, 0), None),
            PushResult::RejectedInvalidParallelism
        );
        assert_eq!(q.push(mk_meta(2, 0, 5), None), PushResult::RejectedTooLarge); // > max
    }

    #[test]
    fn test_memory_estimate_exceeding_budget_is_rejected() {
        let mut q = IcebergTaskQueue::new(4, 10, 100);
        let mut meta = mk_meta(1, 0, 1);
        meta.memory_reservation_bytes = 101;

        assert_eq!(q.push(meta, None), PushResult::RejectedTooLarge);
    }

    #[test]
    fn test_duplicate_key_rejected() {
        let mut q = IcebergTaskQueue::new(8, 32, usize::MAX);
        assert_eq!(q.push(mk_meta(1, 0, 3), None), PushResult::Added);
        // Same (task_id, plan_index) should be rejected
        assert_eq!(
            q.push(mk_meta(1, 0, 5), None),
            PushResult::RejectedDuplicate
        );
        // Parallelism sum should not have changed
        assert_eq!(q.waiting_parallelism_sum(), 3);

        // Different plan_index is allowed
        assert_eq!(q.push(mk_meta(1, 1, 2), None), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 5);

        // After pop and finish, the key can be reused
        let p = q.pop().unwrap();
        assert_eq!(p.meta.task_id.as_raw_id(), 1);
        assert_eq!(p.meta.plan_index, 0);
        q.finish_running((1.into(), 0));

        // Now the same key can be pushed again
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
    }

    #[test]
    fn test_pop_insufficient_parallelism() {
        let mut q = IcebergTaskQueue::new(8, 32, usize::MAX);
        assert_eq!(q.push(mk_meta(1, 0, 6), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 4), None), PushResult::Added);

        let p1 = q.pop().unwrap();
        assert_eq!(p1.meta.task_id.as_raw_id(), 1);
        // Not enough remaining parallelism (only 2 left)
        assert!(q.pop().is_none());

        // Finish first, then second becomes schedulable
        assert!(q.finish_running((1.into(), 0)));
        let p2 = q.pop().unwrap();
        assert_eq!(p2.meta.task_id.as_raw_id(), 2);
    }

    #[test]
    fn test_finish_nonexistent_task() {
        let mut q = IcebergTaskQueue::new(4, 16, usize::MAX);
        assert!(!q.finish_running((999.into(), 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_double_finish() {
        let mut q = IcebergTaskQueue::new(8, 32, usize::MAX);
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
        q.pop().unwrap();
        assert_eq!(q.running_parallelism_sum(), 4);

        // First finish succeeds
        assert!(q.finish_running((1.into(), 0)));
        assert_eq!(q.running_parallelism_sum(), 0);

        // Second finish on same key returns false (triggers warn log)
        assert!(!q.finish_running((1.into(), 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_max_parallelism_boundary() {
        // pending_budget == max_parallelism: minimal valid configuration
        let mut q = IcebergTaskQueue::new(4, 4, usize::MAX);

        // Task with required_parallelism == max_parallelism should be accepted
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
        // Budget exhausted
        assert_eq!(q.push(mk_meta(2, 0, 1), None), PushResult::RejectedCapacity);

        // Can pop and run at full parallelism
        let p = q.pop().unwrap();
        assert_eq!(p.meta.required_parallelism, 4);
        assert_eq!(q.running_parallelism_sum(), 4);

        // No room for any new running task
        assert_eq!(q.push(mk_meta(3, 0, 1), None), PushResult::Added);
        assert!(q.pop().is_none());
    }

    #[test]
    fn test_same_task_id_multiple_plans() {
        let mut q = IcebergTaskQueue::new(10, 30, usize::MAX);
        let task_id = 1u64;

        // Same task_id with different plan_index are independent
        assert_eq!(q.push(mk_meta(task_id, 0, 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 1, 4), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 2, 2), None), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 9);

        // Pop all
        for i in 0..3 {
            let p = q.pop().unwrap();
            assert_eq!(p.meta.task_id.as_raw_id(), task_id);
            assert_eq!(p.meta.plan_index, i);
        }
        assert_eq!(q.running_parallelism_sum(), 9);

        // Finish out of order
        assert!(q.finish_running((task_id.into(), 1)));
        assert_eq!(q.running_parallelism_sum(), 5);
        assert!(q.finish_running((task_id.into(), 0)));
        assert!(q.finish_running((task_id.into(), 2)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_cancel_waiting_task_only_removes_waiting_plans() {
        let mut q = IcebergTaskQueue::new(10, 30, usize::MAX);
        let task_id = 1u64;

        assert_eq!(q.push(mk_meta(task_id, 0, 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 1, 4), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 2), None), PushResult::Added);

        let popped = q.pop().unwrap();
        assert_eq!(popped.meta.task_id.as_raw_id(), task_id);
        assert_eq!(popped.meta.plan_index, 0);
        assert_eq!(q.running_parallelism_sum(), 3);
        assert_eq!(q.waiting_parallelism_sum(), 6);

        assert_eq!(q.cancel_waiting_task(task_id.into()), 1);
        assert_eq!(q.running_parallelism_sum(), 3);
        assert_eq!(q.waiting_parallelism_sum(), 2);

        assert!(q.finish_running((task_id.into(), 0)));
        let next = q.pop().unwrap();
        assert_eq!(next.meta.task_id.as_raw_id(), 2);
        assert_eq!(next.meta.plan_index, 0);
    }

    #[test]
    fn test_empty_queue_behavior() {
        let mut q = IcebergTaskQueue::new(8, 32, usize::MAX);
        assert!(q.pop().is_none());
        assert!(!q.finish_running((1.into(), 0)));
        assert_eq!(q.waiting_parallelism_sum(), 0);
        assert_eq!(q.running_parallelism_sum(), 0);
    }
    #[test]
    fn waiting_memory_does_not_count_against_running_budget() {
        let mut q = IcebergTaskQueue::new(8, 32, 100);
        let meta = |id, memory_reservation_bytes| IcebergTaskMeta {
            task_id: IcebergCompactionTaskId::new(id),
            plan_index: 0,
            required_parallelism: 1,
            memory_reservation_bytes,
        };
        assert_eq!(q.push(meta(1, 80), None), PushResult::Added);
        q.pop().unwrap();

        assert_eq!(q.push(meta(2, 60), None), PushResult::Added);
        assert!(q.pop().is_none());

        assert!(q.finish_running((1.into(), 0)));
        assert_eq!(q.pop().unwrap().meta.task_id.as_raw_id(), 2);
    }

    #[test]
    fn memory_head_of_line_blocking_preserves_fifo() {
        let mut q = IcebergTaskQueue::new(8, 32, 100);
        let meta = |id, memory_reservation_bytes| IcebergTaskMeta {
            task_id: IcebergCompactionTaskId::new(id),
            plan_index: 0,
            required_parallelism: 1,
            memory_reservation_bytes,
        };
        assert_eq!(q.push(meta(1, 60), None), PushResult::Added);
        q.pop().unwrap();
        assert_eq!(q.push(meta(2, 50), None), PushResult::Added);
        assert_eq!(q.push(meta(3, 40), None), PushResult::Added);

        assert!(q.pop().is_none());
        assert!(q.finish_running((1.into(), 0)));
        assert_eq!(q.pop().unwrap().meta.task_id.as_raw_id(), 2);
        assert_eq!(q.pop().unwrap().meta.task_id.as_raw_id(), 3);
    }
}
