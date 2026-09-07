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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use risingwave_common::id::JobId;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_meta_model::ObjectId;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{
    MetaSnapshot, PbObject, PbObjectGroup, SubscribeResponse, SubscribeType,
};
use thiserror_ext::AsReport;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedSender};
use tonic::Status;

use crate::controller::SqlMetaStore;
use crate::manager::WorkerKey;
use crate::manager::notification_version::NotificationVersionGenerator;
use crate::model::FragmentId;

pub type MessageStatus = Status;
pub type Notification = Result<SubscribeResponse, Status>;
pub type NotificationManagerRef = Arc<NotificationManager>;
pub type NotificationVersion = u64;
/// NOTE(kwannoel): This is just ignored, used in background DDL
pub const IGNORED_NOTIFICATION_VERSION: u64 = 0;

#[derive(Clone, Debug)]
pub enum LocalNotification {
    WorkerNodeDeleted(WorkerNode),
    WorkerNodeActivated(WorkerNode),
    SystemParamsChange(SystemParamsReader),
    BatchParallelismChange,
    ServingFragmentMappingsUpsert(Vec<FragmentId>),
    ServingFragmentMappingsDelete(Vec<FragmentId>),
    SourceDropped(ObjectId),
    StreamingJobBackfillFinished(JobId),
}

#[derive(Debug)]
struct Target {
    subscribe_type: SubscribeType,
    // `None` indicates sending to all subscribers of `subscribe_type`.
    worker_key: Option<WorkerKey>,
}

impl From<SubscribeType> for Target {
    fn from(value: SubscribeType) -> Self {
        Self {
            subscribe_type: value,
            worker_key: None,
        }
    }
}

#[derive(Debug)]
struct Task {
    target: Target,
    operation: Operation,
    info: Info,
    version: Option<NotificationVersion>,
}

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager {
    core: Arc<parking_lot::Mutex<NotificationManagerCore>>,
    /// Sender used to add a notification into the waiting queue.
    task_tx: UnboundedSender<Task>,
    /// The current notification version generator.
    version_generator: Mutex<NotificationVersionGenerator>,
}

impl NotificationManager {
    pub async fn new(meta_store_impl: SqlMetaStore) -> Self {
        // notification waiting queue.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();
        let core = Arc::new(parking_lot::Mutex::new(NotificationManagerCore::new()));
        let core_clone = core.clone();
        let version_generator = NotificationVersionGenerator::new(meta_store_impl)
            .await
            .unwrap();

        tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                let response = SubscribeResponse {
                    status: None,
                    operation: task.operation as i32,
                    info: Some(task.info),
                    version: task.version.unwrap_or_default(),
                };
                core.lock().notify(task.target, response);
            }
        });

        Self {
            core: core_clone,
            task_tx,
            version_generator: Mutex::new(version_generator),
        }
    }

    pub fn abort_all(&self) {
        let mut guard = self.core.lock();
        *guard = NotificationManagerCore::new();
        guard.exiting = true;
    }

    #[inline(always)]
    fn notify(
        &self,
        target: Target,
        operation: Operation,
        info: Info,
        version: Option<NotificationVersion>,
    ) {
        let task = Task {
            target,
            operation,
            info,
            version,
        };
        self.task_tx.send(task).unwrap();
    }

    /// Add a notification to the waiting queue and increase notification version.
    async fn notify_with_version(
        &self,
        target: Target,
        operation: Operation,
        info: Info,
    ) -> NotificationVersion {
        let mut version_guard = self.version_generator.lock().await;
        version_guard.increase_version().await;
        let version = version_guard.current_version();
        self.notify(target, operation, info, Some(version));
        version
    }

    /// Add a notification to the waiting queue and return immediately
    #[inline(always)]
    fn notify_without_version(&self, target: Target, operation: Operation, info: Info) {
        self.notify(target, operation, info, None);
    }

    pub fn notify_snapshot(
        &self,
        worker_key: WorkerKey,
        subscribe_type: SubscribeType,
        meta_snapshot: MetaSnapshot,
    ) {
        self.notify_without_version(
            Target {
                subscribe_type,
                worker_key: Some(worker_key),
            },
            Operation::Snapshot,
            Info::Snapshot(meta_snapshot),
        )
    }

    pub fn notify_all_without_version(&self, operation: Operation, info: Info) {
        for subscribe_type in [
            SubscribeType::Frontend,
            SubscribeType::Hummock,
            SubscribeType::Compactor,
            SubscribeType::Compute,
        ] {
            self.notify_without_version(subscribe_type.into(), operation, info.clone());
        }
    }

    pub async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Frontend.into(), operation, info)
            .await
    }

    pub async fn notify_frontend_object_info(
        &self,
        operation: Operation,
        object_info: PbObjectInfo,
    ) -> NotificationVersion {
        self.notify_with_version(
            SubscribeType::Frontend.into(),
            operation,
            Info::ObjectGroup(PbObjectGroup {
                objects: vec![PbObject {
                    object_info: object_info.into(),
                }],
                dependencies: vec![],
            }),
        )
        .await
    }

    pub async fn notify_hummock(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Hummock.into(), operation, info)
            .await
    }

    pub(crate) async fn notify_hummock_targeted_update(
        &self,
        worker_key: WorkerKey,
        info: Info,
    ) -> NotificationVersion {
        self.notify_with_version(
            Target {
                subscribe_type: SubscribeType::Hummock,
                worker_key: Some(worker_key),
            },
            Operation::Update,
            info,
        )
        .await
    }

    pub async fn notify_compactor(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Compactor.into(), operation, info)
            .await
    }

    pub fn notify_compute_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Compute.into(), operation, info)
    }

    pub fn notify_frontend_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Frontend.into(), operation, info)
    }

    pub fn notify_hummock_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Hummock.into(), operation, info)
    }

    pub fn notify_compactor_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Compactor.into(), operation, info)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn notify_hummock_with_version(
        &self,
        operation: Operation,
        info: Info,
        version: Option<NotificationVersion>,
    ) {
        self.notify(SubscribeType::Hummock.into(), operation, info, version)
    }

    pub fn notify_local_subscribers(&self, notification: LocalNotification) {
        let mut core_guard = self.core.lock();
        core_guard.local_senders.retain(|sender| {
            if let Err(err) = sender.send(notification.clone()) {
                tracing::warn!(error = %err.as_report(), "Failed to notify local subscriber");
                return false;
            }
            true
        });
    }

    /// Tell `NotificationManagerCore` to delete sender.
    pub fn delete_sender(&self, worker_type: WorkerType, worker_key: WorkerKey) {
        let mut core_guard = self.core.lock();
        // TODO: we may avoid passing the worker_type and remove the `worker_key` in all sender
        // holders anyway
        match worker_type {
            WorkerType::Frontend => core_guard.frontend_senders.remove(&worker_key),
            WorkerType::ComputeNode | WorkerType::RiseCtl => {
                core_guard.hummock_senders.remove(&worker_key)
            }
            WorkerType::Compactor => core_guard.compactor_senders.remove(&worker_key),
            _ => unreachable!(),
        };
    }

    /// Tell `NotificationManagerCore` to insert sender by `worker_type`.
    pub fn insert_sender(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) {
        let mut core_guard = self.core.lock();
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
            return;
        }
        let senders = core_guard.senders_of(subscribe_type);

        senders.insert(worker_key, sender);
    }

    pub fn insert_local_sender(&self, sender: UnboundedSender<LocalNotification>) {
        let mut core_guard = self.core.lock();
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
            return;
        }
        core_guard.local_senders.push(sender);
    }

    #[cfg(test)]
    pub fn clear_local_sender(&self) {
        self.core.lock().local_senders.clear();
    }

    pub async fn current_version(&self) -> NotificationVersion {
        let version_guard = self.version_generator.lock().await;
        version_guard.current_version()
    }
}

type SenderMap = HashMap<WorkerKey, UnboundedSender<Notification>>;

struct NotificationManagerCore {
    /// The notification sender to frontends.
    frontend_senders: SenderMap,
    /// The notification sender to nodes that subscribes the hummock.
    hummock_senders: SenderMap,
    /// The notification sender to compactor nodes.
    compactor_senders: SenderMap,
    /// The notification sender to compute nodes.
    compute_senders: SenderMap,
    /// The notification sender to local subscribers.
    local_senders: Vec<UnboundedSender<LocalNotification>>,
    exiting: bool,
}

impl NotificationManagerCore {
    fn new() -> Self {
        Self {
            frontend_senders: HashMap::new(),
            hummock_senders: HashMap::new(),
            compactor_senders: HashMap::new(),
            compute_senders: HashMap::new(),
            local_senders: vec![],
            exiting: false,
        }
    }

    fn notify(&mut self, target: Target, response: SubscribeResponse) {
        macro_rules! warn_send_failure {
            ($subscribe_type:expr, $worker_key:expr, $err:expr) => {
                tracing::warn!(
                    "Failed to notify {:?} {:?}: {}",
                    $subscribe_type,
                    $worker_key,
                    $err
                );
            };
        }

        let senders = self.senders_of(target.subscribe_type);

        if let Some(worker_key) = target.worker_key {
            match senders.entry(worker_key.clone()) {
                Entry::Occupied(entry) => {
                    let _ = entry.get().send(Ok(response)).inspect_err(|err| {
                        warn_send_failure!(target.subscribe_type, &worker_key, err.as_report());
                        entry.remove_entry();
                    });
                }
                Entry::Vacant(_) => {
                    tracing::warn!("Failed to find notification sender of {:?}", worker_key)
                }
            }
        } else {
            senders.retain(|worker_key, sender| {
                sender
                    .send(Ok(response.clone()))
                    .inspect_err(|err| {
                        warn_send_failure!(target.subscribe_type, &worker_key, err.as_report());
                    })
                    .is_ok()
            });
        }
    }

    fn senders_of(&mut self, subscribe_type: SubscribeType) -> &mut SenderMap {
        match subscribe_type {
            SubscribeType::Frontend => &mut self.frontend_senders,
            SubscribeType::Hummock => &mut self.hummock_senders,
            SubscribeType::Compactor => &mut self.compactor_senders,
            SubscribeType::Compute => &mut self.compute_senders,
            SubscribeType::Unspecified => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::id::JobId;
    use risingwave_pb::common::HostAddress;

    use super::*;
    use crate::manager::WorkerKey;

    #[tokio::test]
    async fn test_multiple_subscribers_one_worker() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key1 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let worker_key2 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 2,
        });
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key1.clone(), tx1);
        mgr.insert_sender(SubscribeType::Frontend, worker_key1.clone(), tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key2, tx3);
        mgr.notify_snapshot(
            worker_key1.clone(),
            SubscribeType::Hummock,
            MetaSnapshot::default(),
        );
        assert!(rx1.recv().await.is_some());
        assert!(rx2.try_recv().is_err());
        assert!(rx3.try_recv().is_err());

        mgr.notify_frontend(Operation::Add, Info::Database(Default::default()))
            .await;
        assert!(rx1.try_recv().is_err());
        assert!(rx2.recv().await.is_some());
        assert!(rx3.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_notify_hummock_targeted_update() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key1 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let worker_key2 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 2,
        });
        let (hummock_tx1, mut hummock_rx1) = mpsc::unbounded_channel();
        let (hummock_tx2, mut hummock_rx2) = mpsc::unbounded_channel();
        let (frontend_tx1, mut frontend_rx1) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key1.clone(), hummock_tx1);
        mgr.insert_sender(SubscribeType::Hummock, worker_key2.clone(), hummock_tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key1.clone(), frontend_tx1);

        mgr.notify_hummock_targeted_update(worker_key1, Info::Database(Default::default()))
            .await;

        assert!(hummock_rx1.recv().await.is_some());
        assert!(hummock_rx2.try_recv().is_err());
        assert!(frontend_rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_local_notification_backfill_finished() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        mgr.insert_local_sender(tx);

        let job_id = JobId::new(42);
        mgr.notify_local_subscribers(LocalNotification::StreamingJobBackfillFinished(job_id));

        match rx.recv().await.expect("should receive notification") {
            LocalNotification::StreamingJobBackfillFinished(received) => {
                assert_eq!(received, job_id);
            }
            other => panic!("unexpected notification: {other:?}"),
        }
    }
}
