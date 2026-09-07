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
use std::sync::Arc;

use risingwave_common::id::WorkerId;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::{Channel, NotificationClient, ObserverError};
use risingwave_meta::controller::cluster::ClusterControllerRef;
use risingwave_meta::hummock::{HummockManager, HummockManagerRef};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_pb::backup_service::MetaBackupManifestId;
use risingwave_pb::hummock::WriteLimits;
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, SubscribeType};
use tokio::sync::mpsc::UnboundedReceiver;

pub struct MockNotificationClient {
    addr: HostAddr,
    notification_manager: NotificationManagerRef,
    hummock_manager: HummockManagerRef,
}

impl MockNotificationClient {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef,
        hummock_manager: HummockManagerRef,
    ) -> Self {
        Self {
            addr,
            notification_manager,
            hummock_manager,
        }
    }
}

#[async_trait::async_trait]
impl NotificationClient for MockNotificationClient {
    type Channel = TestChannel<SubscribeResponse>;

    async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> Result<Self::Channel, ObserverError> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let worker_key = WorkerKey(self.addr.to_protobuf());
        self.notification_manager
            .insert_sender(subscribe_type, worker_key.clone(), tx.clone());

        let hummock_version = self.hummock_manager.get_current_version().await;
        let meta_snapshot = MetaSnapshot {
            hummock_version: Some(hummock_version.into()),
            version: Some(Default::default()),
            meta_backup_manifest_id: Some(MetaBackupManifestId { id: 0 }),
            hummock_write_limits: Some(WriteLimits {
                write_limits: HashMap::new(),
            }),
            cluster_resource: Some(Default::default()),
            ..Default::default()
        };

        self.notification_manager.notify_snapshot(tx, meta_snapshot);

        Ok(TestChannel(rx))
    }
}

pub async fn get_notification_client_for_test(
    env: MetaSrvEnv,
    hummock_manager_ref: Arc<HummockManager>,
    cluster_controller_ref: ClusterControllerRef,
    worker_id: WorkerId,
) -> MockNotificationClient {
    let worker_node = cluster_controller_ref
        .get_worker_by_id(worker_id)
        .await
        .unwrap()
        .unwrap();

    MockNotificationClient::new(
        worker_node.get_host().unwrap().into(),
        env.notification_manager_ref(),
        hummock_manager_ref,
    )
}

pub struct TestChannel<T>(UnboundedReceiver<Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for TestChannel<T> {
    type Item = T;

    async fn message(&mut self) -> Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}
