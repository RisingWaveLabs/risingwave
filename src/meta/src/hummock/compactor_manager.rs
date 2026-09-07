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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use fail::fail_point;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compact::statistics_compact_task;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockContextId};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    CancelCompactTask, CompactTaskProgress, SubscribeCompactionEventResponse,
};
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
use risingwave_pb::iceberg_compaction::{
    CancelCompactTask as IcebergCancelCompactTask, SubscribeIcebergCompactionEventResponse,
};
use risingwave_pb::id::IcebergCompactionTaskId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::MetaResult;
use crate::hummock::model::ext::compaction_task_model_to_assignment;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModelError;

pub type CompactorManagerRef = Arc<CompactorManager>;
pub type IcebergCompactorManagerRef = Arc<IcebergCompactorManager>;

pub const TASK_RUN_TOO_LONG: &str = "running too long";
pub const TASK_NOT_FOUND: &str = "task not found";
pub const TASK_NORMAL: &str = "task is normal, please wait some time";

type CompactorSubscribeStreamSender = UnboundedSender<MetaResult<SubscribeCompactionEventResponse>>;
type CompactorSubscribeStreamReceiver =
    UnboundedReceiver<MetaResult<SubscribeCompactionEventResponse>>;

type IcebergCompactorSubscribeStreamSender =
    UnboundedSender<MetaResult<SubscribeIcebergCompactionEventResponse>>;
type IcebergCompactorSubscribeStreamReceiver =
    UnboundedReceiver<MetaResult<SubscribeIcebergCompactionEventResponse>>;

type CompactorSubscribeResponseEvent = ResponseEvent;

type IcebergCompactorSubscribeResponseEvent = IcebergResponseEvent;

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
pub struct Compactor {
    context_id: HummockContextId,
    sender: CompactorSubscribeStreamSender,
}

pub struct IcebergCompactor {
    context_id: HummockContextId,
    sender: IcebergCompactorSubscribeStreamSender,
}

struct TaskHeartbeat {
    task: CompactTask,
    num_ssts_sealed: u32,
    num_ssts_uploaded: u32,
    num_progress_key: u64,
    num_pending_read_io: u64,
    num_pending_write_io: u64,
    create_time: Instant,
    expire_at: u64,

    update_at: u64,
}

impl Compactor {
    pub fn new(context_id: HummockContextId, sender: CompactorSubscribeStreamSender) -> Self {
        Self { context_id, sender }
    }

    pub fn send_event(&self, event: CompactorSubscribeResponseEvent) -> MetaResult<()> {
        fail_point!("compaction_send_task_fail", |_| Err(anyhow::anyhow!(
            "compaction_send_task_fail"
        )
        .into()));

        self.sender
            .send(Ok(SubscribeCompactionEventResponse {
                create_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_millis() as u64,
                event: Some(event),
            }))
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }

    pub fn cancel_task(&self, task_id: u64) -> MetaResult<()> {
        self.send_event(ResponseEvent::CancelCompactTask(CancelCompactTask {
            context_id: self.context_id,
            task_id,
        }))
    }

    pub fn cancel_tasks(&self, task_ids: &Vec<u64>) -> MetaResult<()> {
        for task_id in task_ids {
            self.cancel_task(*task_id)?;
        }
        Ok(())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }
}

pub trait CompactorManagerTrait {
    fn add_compactor(&self, context_id: HummockContextId) -> CompactorSubscribeStreamReceiver;
    fn remove_compactor(&self, context_id: HummockContextId);
    fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>>;
    fn next_compactor(&self) -> Option<Arc<Compactor>>;
    fn compactor_num(&self) -> usize;
}

/// `CompactorManagerInner` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`.
///
/// A compact task can be in one of these states:
/// 1. Success: an assigned task is reported as success via `CompactStatus::report_compact_task`.
///    It's the final state.
/// 2. Failed: an Failed task is reported as success via `CompactStatus::report_compact_task`.
///    It's the final state.
/// 3. Cancelled: a task is reported as cancelled via `CompactStatus::report_compact_task`. It's
///    the final state.
pub struct CompactorManagerInner {
    pub task_expired_seconds: u64,
    pub heartbeat_expired_seconds: u64,
    task_heartbeats: HashMap<HummockCompactionTaskId, TaskHeartbeat>,

    /// The outer lock is a `RwLock`, so we should still be able to modify each compactor
    pub compactor_map: HashMap<HummockContextId, Arc<Compactor>>,
}

impl CompactorManagerInner {
    pub async fn with_meta(env: MetaSrvEnv) -> MetaResult<Self> {
        use risingwave_meta_model::compaction_task;
        use sea_orm::EntityTrait;
        // Retrieve the existing task assignments from metastore.
        let task_assignment: Vec<_> = compaction_task::Entity::find()
            .all(&env.meta_store_ref().conn)
            .await
            .map_err(MetadataModelError::from)?
            .into_iter()
            .map(compaction_task_model_to_assignment)
            .collect();
        let mut manager = Self {
            task_expired_seconds: env.opts.compaction_task_max_progress_interval_secs,
            heartbeat_expired_seconds: env.opts.compaction_task_max_heartbeat_interval_secs,
            task_heartbeats: Default::default(),
            compactor_map: Default::default(),
        };
        // Initialize heartbeat for existing tasks.
        task_assignment.into_iter().for_each(|assignment| {
            manager.initiate_task_heartbeat(assignment.compact_task);
        });
        Ok(manager)
    }

    /// Only used for unit test.
    pub fn for_test() -> Self {
        Self {
            task_expired_seconds: 1,
            heartbeat_expired_seconds: 1,
            task_heartbeats: Default::default(),
            compactor_map: Default::default(),
        }
    }

    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        use rand::Rng;
        if self.compactor_map.is_empty() {
            return None;
        }

        let rand_index = rand::rng().random_range(0..self.compactor_map.len());
        let compactor = self.compactor_map.values().nth(rand_index).unwrap().clone();

        Some(compactor)
    }

    /// Retrieve a receiver of tasks for the compactor identified by `context_id`. The sender should
    /// be obtained by calling one of the compactor getters.
    ///
    ///  If `add_compactor` is called with the same `context_id` more than once, the only cause
    /// would be compactor re-subscription, as `context_id` is a monotonically increasing
    /// sequence.
    pub fn add_compactor(
        &mut self,
        context_id: HummockContextId,
    ) -> CompactorSubscribeStreamReceiver {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        self.compactor_map
            .insert(context_id, Arc::new(Compactor::new(context_id, tx)));

        tracing::info!(context_id = %context_id, "Added compactor session");

        rx
    }

    /// Used when meta exiting to support graceful shutdown.
    pub fn abort_all_compactors(&mut self) {
        while let Some(compactor) = self.next_compactor() {
            self.remove_compactor(compactor.context_id);
        }
    }

    pub fn remove_compactor(&mut self, context_id: HummockContextId) {
        if self.compactor_map.remove(&context_id).is_some() {
            tracing::info!(context_id = %context_id, "Removed compactor session")
        };
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.compactor_map.get(&context_id).cloned()
    }

    pub fn check_tasks_status(
        &self,
        tasks: &[HummockCompactionTaskId],
        slow_task_duration: Duration,
    ) -> HashMap<HummockCompactionTaskId, (Duration, &'static str)> {
        let tasks_ids: HashSet<u64> = HashSet::from_iter(tasks.to_vec());
        let mut ret = HashMap::default();
        for TaskHeartbeat {
            task, create_time, ..
        } in self.task_heartbeats.values()
        {
            if !tasks_ids.contains(&task.task_id) {
                continue;
            }
            let pending_time = create_time.elapsed();
            if pending_time > slow_task_duration {
                ret.insert(task.task_id, (pending_time, TASK_RUN_TOO_LONG));
            } else {
                ret.insert(task.task_id, (pending_time, TASK_NORMAL));
            }
        }

        for task_id in tasks {
            if !ret.contains_key(task_id) {
                ret.insert(*task_id, (Duration::from_secs(0), TASK_NOT_FOUND));
            }
        }
        ret
    }

    pub fn get_heartbeat_expired_tasks(&self) -> Vec<CompactTask> {
        let heartbeat_expired_ts: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs()
            - self.heartbeat_expired_seconds;
        Self::get_heartbeat_expired_tasks_impl(&self.task_heartbeats, heartbeat_expired_ts)
    }

    fn get_heartbeat_expired_tasks_impl(
        task_heartbeats: &HashMap<HummockCompactionTaskId, TaskHeartbeat>,
        heartbeat_expired_ts: u64,
    ) -> Vec<CompactTask> {
        let mut cancellable_tasks = vec![];
        const MAX_TASK_DURATION_SEC: u64 = 2700;

        for TaskHeartbeat {
            expire_at,
            task,
            create_time,
            num_ssts_sealed,
            num_ssts_uploaded,
            num_progress_key,
            num_pending_read_io,
            num_pending_write_io,
            update_at,
        } in task_heartbeats.values()
        {
            if *update_at < heartbeat_expired_ts {
                cancellable_tasks.push(task.clone());
            }

            let task_duration_too_long = create_time.elapsed().as_secs() > MAX_TASK_DURATION_SEC;
            if task_duration_too_long {
                let compact_task_statistics = statistics_compact_task(task);
                tracing::info!(
                    "CompactionGroupId {} Task {} duration too long create_time {:?} expire_at {:?} num_ssts_sealed {} num_ssts_uploaded {} num_progress_key {} \
                        pending_read_io_count {} pending_write_io_count {} target_level {} \
                        base_level {} target_sub_level_id {} task_type {} compact_task_statistics {:?}",
                    task.compaction_group_id,
                    task.task_id,
                    create_time,
                    expire_at,
                    num_ssts_sealed,
                    num_ssts_uploaded,
                    num_progress_key,
                    num_pending_read_io,
                    num_pending_write_io,
                    task.target_level,
                    task.base_level,
                    task.target_sub_level_id,
                    task.task_type.as_str_name(),
                    compact_task_statistics
                );
            }
        }
        cancellable_tasks
    }

    pub fn initiate_task_heartbeat(&mut self, task: CompactTask) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        self.task_heartbeats.insert(
            task.task_id,
            TaskHeartbeat {
                task,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                num_progress_key: 0,
                num_pending_read_io: 0,
                num_pending_write_io: 0,
                create_time: Instant::now(),
                expire_at: now + self.task_expired_seconds,
                update_at: now,
            },
        );
    }

    pub fn remove_task_heartbeat(&mut self, task_id: u64) {
        self.task_heartbeats.remove(&task_id).unwrap();
    }

    pub fn update_task_heartbeats(
        &mut self,
        progress_list: &Vec<CompactTaskProgress>,
    ) -> Vec<CompactTask> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut cancel_tasks = vec![];
        for progress in progress_list {
            if let Some(task_ref) = self.task_heartbeats.get_mut(&progress.task_id) {
                task_ref.update_at = now;

                if task_ref.num_ssts_sealed < progress.num_ssts_sealed
                    || task_ref.num_ssts_uploaded < progress.num_ssts_uploaded
                    || task_ref.num_progress_key < progress.num_progress_key
                {
                    // Refresh the expired of the task as it is showing progress.
                    task_ref.expire_at = now + self.task_expired_seconds;
                    task_ref.num_ssts_sealed = progress.num_ssts_sealed;
                    task_ref.num_ssts_uploaded = progress.num_ssts_uploaded;
                    task_ref.num_progress_key = progress.num_progress_key;
                }
                task_ref.num_pending_read_io = progress.num_pending_read_io;
                task_ref.num_pending_write_io = progress.num_pending_write_io;

                // timeout check
                if task_ref.expire_at < now {
                    // cancel
                    cancel_tasks.push(task_ref.task.clone())
                }
            }
        }

        cancel_tasks
    }

    pub fn compactor_num(&self) -> usize {
        self.compactor_map.len()
    }

    pub fn get_progress(&self) -> Vec<CompactTaskProgress> {
        self.task_heartbeats
            .values()
            .map(|hb| CompactTaskProgress {
                task_id: hb.task.task_id,
                num_ssts_sealed: hb.num_ssts_sealed,
                num_ssts_uploaded: hb.num_ssts_uploaded,
                num_progress_key: hb.num_progress_key,
                num_pending_read_io: hb.num_pending_read_io,
                num_pending_write_io: hb.num_pending_write_io,
                compaction_group_id: Some(hb.task.compaction_group_id),
            })
            .collect()
    }
}

pub struct CompactorManager {
    inner: Arc<RwLock<CompactorManagerInner>>,
}

impl CompactorManager {
    pub async fn with_meta(env: MetaSrvEnv) -> MetaResult<Self> {
        let inner = CompactorManagerInner::with_meta(env).await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    /// Only used for unit test.
    pub fn for_test() -> Self {
        let inner = CompactorManagerInner::for_test();
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        self.inner.read().next_compactor()
    }

    pub fn add_compactor(&self, context_id: HummockContextId) -> CompactorSubscribeStreamReceiver {
        self.inner.write().add_compactor(context_id)
    }

    pub fn abort_all_compactors(&self) {
        self.inner.write().abort_all_compactors();
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        self.inner.write().remove_compactor(context_id)
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.inner.read().get_compactor(context_id)
    }

    pub fn check_tasks_status(
        &self,
        tasks: &[HummockCompactionTaskId],
        slow_task_duration: Duration,
    ) -> HashMap<HummockCompactionTaskId, (Duration, &'static str)> {
        self.inner
            .read()
            .check_tasks_status(tasks, slow_task_duration)
    }

    pub fn get_heartbeat_expired_tasks(&self) -> Vec<CompactTask> {
        self.inner.read().get_heartbeat_expired_tasks()
    }

    pub fn initiate_task_heartbeat(&self, task: CompactTask) {
        self.inner.write().initiate_task_heartbeat(task);
    }

    pub fn remove_task_heartbeat(&self, task_id: u64) {
        self.inner.write().remove_task_heartbeat(task_id);
    }

    pub fn update_task_heartbeats(
        &self,
        progress_list: &Vec<CompactTaskProgress>,
    ) -> Vec<CompactTask> {
        self.inner.write().update_task_heartbeats(progress_list)
    }

    pub fn compactor_num(&self) -> usize {
        self.inner.read().compactor_num()
    }

    pub fn get_progress(&self) -> Vec<CompactTaskProgress> {
        self.inner.read().get_progress()
    }
}

impl IcebergCompactor {
    pub fn new(
        context_id: HummockContextId,
        sender: IcebergCompactorSubscribeStreamSender,
    ) -> Self {
        Self { context_id, sender }
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn send_event(&self, event: IcebergCompactorSubscribeResponseEvent) -> MetaResult<()> {
        fail_point!("iceberg_compaction_send_task_fail", |_| Err(
            anyhow::anyhow!("iceberg_compaction_send_task_fail").into()
        ));

        self.sender
            .send(Ok(SubscribeIcebergCompactionEventResponse {
                create_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_millis() as u64,
                event: Some(event),
            }))
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }

    pub fn cancel_task(&self, task_id: IcebergCompactionTaskId) -> MetaResult<()> {
        self.send_event(IcebergResponseEvent::CancelCompactTask(
            IcebergCancelCompactTask { task_id },
        ))
    }
}

pub struct IcebergCompactorManagerInner {
    pub compactor_map: HashMap<HummockContextId, Arc<IcebergCompactor>>,
}

pub struct IcebergCompactorManager {
    inner: Arc<RwLock<IcebergCompactorManagerInner>>,
}

impl IcebergCompactorManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(IcebergCompactorManagerInner {
                compactor_map: HashMap::new(),
            })),
        }
    }

    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
    ) -> IcebergCompactorSubscribeStreamReceiver {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.inner
            .write()
            .compactor_map
            .insert(context_id, Arc::new(IcebergCompactor::new(context_id, tx)));
        tracing::info!(context_id = %context_id, "Added iceberg compactor session");
        rx
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        if self
            .inner
            .write()
            .compactor_map
            .remove(&context_id)
            .is_some()
        {
            tracing::info!(context_id = %context_id, "Removed iceberg compactor session");
        }
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<IcebergCompactor>> {
        self.inner.read().compactor_map.get(&context_id).cloned()
    }

    pub fn next_compactor(&self) -> Option<Arc<IcebergCompactor>> {
        use rand::Rng;
        let compactor_map = &self.inner.read().compactor_map;
        if compactor_map.is_empty() {
            return None;
        }
        let rand_index = rand::rng().random_range(0..compactor_map.len());
        compactor_map.values().nth(rand_index).cloned()
    }

    pub fn compactor_num(&self) -> usize {
        self.inner.read().compactor_map.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::CompactTaskProgress;
    use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
    use risingwave_rpc_client::HummockMetaClient;

    use crate::hummock::compaction::selector::default_compaction_selector;
    use crate::hummock::test_utils::{
        add_ssts, register_table_ids_to_compaction_group, setup_compute_env,
    };
    use crate::hummock::{CompactorManager, IcebergCompactorManager, MockHummockMetaClient};

    #[tokio::test]
    async fn test_compactor_manager() {
        // Initialize metastore with task assignment.
        let (env, context_id) = {
            let (env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
            let context_id = worker_id as _;
            let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(
                MockHummockMetaClient::new(hummock_manager.clone(), context_id),
            );
            let compactor_manager = hummock_manager.compactor_manager.clone();
            register_table_ids_to_compaction_group(
                hummock_manager.as_ref(),
                &[1],
                StaticCompactionGroupId::StateDefault,
            )
            .await;
            let _sst_infos =
                add_ssts(1, hummock_manager.as_ref(), hummock_meta_client.clone()).await;
            let _receiver = compactor_manager.add_compactor(context_id);
            hummock_manager
                .get_compact_task(
                    StaticCompactionGroupId::StateDefault,
                    &mut *default_compaction_selector(),
                )
                .await
                .unwrap()
                .unwrap();
            (env, context_id)
        };

        // Restart. Set task_expired_seconds to 0 only to speed up test.
        let compactor_manager = CompactorManager::with_meta(env).await.unwrap();
        // Because task assignment exists.
        // Because compactor gRPC is not established yet.
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());

        // Ensure task is expired.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let expired = compactor_manager.get_heartbeat_expired_tasks();
        assert_eq!(expired.len(), 1);

        // Mimic no-op compaction heartbeat
        assert_eq!(compactor_manager.get_heartbeat_expired_tasks().len(), 1);

        // Mimic compaction heartbeat with invalid task id
        compactor_manager.update_task_heartbeats(&vec![CompactTaskProgress {
            task_id: expired[0].task_id + 1,
            num_ssts_sealed: 1,
            num_ssts_uploaded: 1,
            num_progress_key: 100,
            ..Default::default()
        }]);
        assert_eq!(compactor_manager.get_heartbeat_expired_tasks().len(), 1);

        // Mimic effective compaction heartbeat
        compactor_manager.update_task_heartbeats(&vec![CompactTaskProgress {
            task_id: expired[0].task_id,
            num_ssts_sealed: 1,
            num_ssts_uploaded: 1,
            num_progress_key: 100,
            ..Default::default()
        }]);
        assert_eq!(compactor_manager.get_heartbeat_expired_tasks().len(), 0);

        // Test add
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
        compactor_manager.add_compactor(context_id);
        assert_eq!(compactor_manager.compactor_num(), 1);
        assert_eq!(
            compactor_manager
                .get_compactor(context_id)
                .unwrap()
                .context_id(),
            context_id
        );
        // Test remove
        compactor_manager.remove_compactor(context_id);
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
    }

    #[test]
    fn test_iceberg_compactor_manager() {
        // Test Add and Remove Iceberg Compactor
        let iceberg_context_id = 1000.into();
        let iceberg_compactor_manager = IcebergCompactorManager::new();
        assert_eq!(iceberg_compactor_manager.compactor_num(), 0);
        assert!(
            iceberg_compactor_manager
                .get_compactor(iceberg_context_id)
                .is_none()
        );
        let mut receiver = iceberg_compactor_manager.add_compactor(iceberg_context_id);
        assert_eq!(iceberg_compactor_manager.compactor_num(), 1);
        let compactor = iceberg_compactor_manager
            .get_compactor(iceberg_context_id)
            .unwrap();
        assert_eq!(compactor.context_id(), iceberg_context_id);

        compactor.cancel_task(42.into()).unwrap();
        let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
        match event {
            IcebergResponseEvent::CancelCompactTask(cancel_task) => {
                assert_eq!(cancel_task.task_id, 42);
            }
            other => panic!("unexpected iceberg compactor event: {other:?}"),
        }

        // Test remove
        iceberg_compactor_manager.remove_compactor(iceberg_context_id);
        assert_eq!(iceberg_compactor_manager.compactor_num(), 0);
        assert!(
            iceberg_compactor_manager
                .get_compactor(iceberg_context_id)
                .is_none()
        );
    }
}
