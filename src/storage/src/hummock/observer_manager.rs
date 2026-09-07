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

use risingwave_common::config::Role;
use risingwave_common::license::LicenseManager;
use risingwave_common_service::ObserverState;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_trace::TraceSpan;
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{PbTableRefillRuntimeConfig, SubscribeResponse};
use tokio::sync::mpsc::UnboundedSender;

use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::backup_reader::BackupReaderRef;
use crate::hummock::event_handler::{HummockObserverEvent, HummockVersionUpdate};
use crate::hummock::write_limiter::WriteLimiterRef;

pub struct HummockObserverNode {
    role: Role,
    compaction_catalog_manager: CompactionCatalogManagerRef,
    backup_reader: BackupReaderRef,
    write_limiter: WriteLimiterRef,
    observer_event_sender: UnboundedSender<HummockObserverEvent>,
    version: u64,
}

impl ObserverState for HummockObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Hummock
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let _span: risingwave_hummock_trace::MayTraceSpan =
            TraceSpan::new_meta_message_span(resp.clone());

        match info.to_owned() {
            Info::ObjectGroup(object_group) => {
                for object in object_group.objects {
                    match object.object_info.unwrap() {
                        PbObjectInfo::Table(table_catalog) => {
                            self.handle_catalog_notification(resp.operation(), table_catalog);
                        }
                        info => panic!("invalid notification info: {info}"),
                    };
                }
                assert!(
                    resp.version > self.version,
                    "resp version={:?}, current version={:?}",
                    resp.version,
                    self.version
                );
                self.version = resp.version;
            }
            Info::HummockVersionDeltas(hummock_version_deltas) => {
                let _ = self
                    .observer_event_sender
                    .send(HummockObserverEvent::VersionUpdate(
                        HummockVersionUpdate::VersionDeltas(
                            hummock_version_deltas
                                .version_deltas
                                .iter()
                                .map(HummockVersionDelta::from_rpc_protobuf)
                                .collect(),
                        ),
                    ))
                    .inspect_err(|e| {
                        tracing::error!(event = ?e.0, "unable to send version delta");
                    });
            }

            Info::MetaBackupManifestId(id) => {
                self.backup_reader.try_refresh_manifest(id.id);
            }

            Info::HummockWriteLimits(write_limits) => {
                self.write_limiter
                    .update_write_limits(write_limits.write_limits);
            }

            Info::ClusterResource(resource) => {
                LicenseManager::get().update_cluster_resource(resource);
            }
            Info::TableRefillRuntimeConfig(config) => {
                self.handle_table_refill_runtime_config(resp.operation(), config);
            }
            info => {
                panic!("invalid notification info: {info}");
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
        let _span: risingwave_hummock_trace::MayTraceSpan =
            TraceSpan::new_meta_message_span(resp.clone());

        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };

        self.handle_catalog_snapshot(snapshot.tables);
        self.backup_reader.try_refresh_manifest(
            snapshot
                .meta_backup_manifest_id
                .expect("should get meta backup manifest id")
                .id,
        );
        self.write_limiter.update_write_limits(
            snapshot
                .hummock_write_limits
                .expect("should get hummock_write_limits")
                .write_limits,
        );
        let _ = self
            .observer_event_sender
            .send(HummockObserverEvent::VersionUpdate(
                HummockVersionUpdate::PinnedVersion(Box::new(HummockVersion::from_rpc_protobuf(
                    &snapshot
                        .hummock_version
                        .expect("should get hummock version"),
                ))),
            ))
            .inspect_err(|e| {
                tracing::error!(event = ?e.0, "unable to send full version");
            });
        let snapshot_version = snapshot.version.unwrap();
        self.version = snapshot_version.catalog_version;
        LicenseManager::get().update_cluster_resource(snapshot.cluster_resource.unwrap());

        self.handle_table_refill_runtime_config(
            Operation::Snapshot,
            snapshot.table_refill_runtime_config.unwrap_or_default(),
        );
    }
}

impl HummockObserverNode {
    pub fn new(
        role: Role,
        compaction_catalog_manager: CompactionCatalogManagerRef,
        backup_reader: BackupReaderRef,
        observer_event_sender: UnboundedSender<HummockObserverEvent>,
        write_limiter: WriteLimiterRef,
    ) -> Self {
        Self {
            role,
            compaction_catalog_manager,
            backup_reader,
            observer_event_sender,
            version: 0,
            write_limiter,
        }
    }

    fn handle_catalog_snapshot(&mut self, tables: Vec<Table>) {
        self.compaction_catalog_manager
            .sync(tables.into_iter().map(|t| (t.id, t)).collect());
    }

    fn handle_table_refill_runtime_config(
        &self,
        operation: Operation,
        mut config: PbTableRefillRuntimeConfig,
    ) {
        if self.role.for_serving()
            && !self.role.for_streaming()
            && let Some(policies) = &mut config.table_cache_refill_policies
        {
            policies.internal_table_policies.clear();
        }

        tracing::debug!(
            ?operation,
            ?config,
            "receive table refill runtime config updates"
        );

        let _ = self
            .observer_event_sender
            .send(HummockObserverEvent::TableRefillRuntimeConfig(
                operation, config,
            ))
            .inspect_err(|e| {
                tracing::error!(
                    event = ?e.0,
                    "unable to send table refill runtime config"
                );
            });
    }

    fn handle_catalog_notification(&mut self, operation: Operation, table_catalog: Table) {
        match operation {
            Operation::Add | Operation::Update => {
                self.compaction_catalog_manager
                    .update(table_catalog.id, table_catalog);
            }

            Operation::Delete => {
                self.compaction_catalog_manager.remove(table_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::config::Role;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common_service::ObserverState;
    use risingwave_pb::backup_service::MetaBackupManifestId;
    use risingwave_pb::hummock::{PbHummockVersion, WriteLimits};
    use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
    use risingwave_pb::meta::serving_table_vnode_mappings::PbServingTableVnodeMapping;
    use risingwave_pb::meta::subscribe_response::{Info, Operation};
    use risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy;
    use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;
    use risingwave_pb::meta::{
        MetaSnapshot, PbServingTableVnodeMappings, PbTableRefillRuntimeConfig, SubscribeResponse,
        TableCacheRefillPolicies,
    };
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

    use super::HummockObserverNode;
    use crate::compaction_catalog_manager::CompactionCatalogManager;
    use crate::hummock::backup_reader::BackupReader;
    use crate::hummock::event_handler::{HummockObserverEvent, HummockVersionUpdate};
    use crate::hummock::write_limiter::WriteLimiter;

    fn serving_vnodes() -> Bitmap {
        Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1, 3])
    }

    fn runtime_config(table_id: u32, version: u64) -> PbTableRefillRuntimeConfig {
        PbTableRefillRuntimeConfig {
            table_cache_refill_policies: Some(TableCacheRefillPolicies {
                table_policies: vec![PbTableCacheRefillPolicy {
                    table_id,
                    policy: PbCacheRefillPolicy::Serving as i32,
                }],
                internal_table_policies: vec![PbTableCacheRefillPolicy {
                    table_id: table_id + 1,
                    policy: PbCacheRefillPolicy::Both as i32,
                }],
            }),
            serving_table_vnode_mappings: Some(PbServingTableVnodeMappings {
                mappings: vec![PbServingTableVnodeMapping {
                    table_id,
                    bitmap: Some(serving_vnodes().to_protobuf()),
                }],
            }),
            version,
        }
    }

    fn runtime_snapshot(catalog_version: u64, table_id: u32, version: u64) -> SubscribeResponse {
        SubscribeResponse {
            info: Some(Info::Snapshot(MetaSnapshot {
                hummock_version: Some(PbHummockVersion::default()),
                version: Some(SnapshotVersion {
                    catalog_version,
                    ..Default::default()
                }),
                meta_backup_manifest_id: Some(MetaBackupManifestId { id: 0 }),
                hummock_write_limits: Some(WriteLimits {
                    write_limits: HashMap::new(),
                }),
                cluster_resource: Some(Default::default()),
                table_refill_runtime_config: Some(runtime_config(table_id, version)),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    async fn recv_runtime_config(
        receiver: &mut UnboundedReceiver<HummockObserverEvent>,
    ) -> (Operation, PbTableRefillRuntimeConfig) {
        let HummockObserverEvent::TableRefillRuntimeConfig(operation, config) =
            receiver.recv().await.unwrap()
        else {
            panic!("expect table refill runtime config");
        };
        (operation, config)
    }

    #[tokio::test]
    async fn test_resubscribe_snapshot_refreshes_table_refill_runtime_config() {
        let (observer_event_tx, mut observer_event_rx) = unbounded_channel();
        let mut observer = HummockObserverNode::new(
            Role::Streaming,
            Arc::new(CompactionCatalogManager::default()),
            BackupReader::unused().await,
            observer_event_tx,
            WriteLimiter::unused(),
        );

        for (catalog_version, table_id, config_version) in [(1, 233, 10), (2, 333, 20)] {
            observer.handle_initialization_notification(runtime_snapshot(
                catalog_version,
                table_id,
                config_version,
            ));
            assert!(matches!(
                observer_event_rx.recv().await.unwrap(),
                HummockObserverEvent::VersionUpdate(HummockVersionUpdate::PinnedVersion(_))
            ));
            let (operation, config) = recv_runtime_config(&mut observer_event_rx).await;
            assert_eq!(operation, Operation::Snapshot);
            assert_eq!(config.version, config_version);
            let policies = config.table_cache_refill_policies.unwrap();
            assert_eq!(policies.table_policies[0].table_id, table_id);
            assert_eq!(policies.internal_table_policies[0].table_id, table_id + 1);
            let mapping = &config.serving_table_vnode_mappings.unwrap().mappings[0];
            assert_eq!(mapping.table_id, table_id);
            assert_eq!(
                Bitmap::from(mapping.bitmap.clone().unwrap()),
                serving_vnodes()
            );
        }
    }

    #[tokio::test]
    async fn test_table_refill_policies_are_scoped_at_notification_boundary() {
        let table_id = 233;
        let internal_table_id = 234;
        let config = runtime_config(table_id, 42);
        let expected_serving_mappings = config.serving_table_vnode_mappings.clone();

        for (role, expects_internal_policy) in [
            (Role::Serving, false),
            (Role::Streaming, true),
            (Role::Both, true),
            (Role::None, true),
        ] {
            let (observer_event_tx, mut observer_event_rx) = unbounded_channel();
            let observer = HummockObserverNode::new(
                role,
                Arc::new(CompactionCatalogManager::default()),
                BackupReader::unused().await,
                observer_event_tx,
                WriteLimiter::unused(),
            );

            observer.handle_table_refill_runtime_config(Operation::Update, config.clone());
            let (operation, config) = recv_runtime_config(&mut observer_event_rx).await;
            assert_eq!(operation, Operation::Update);
            assert_eq!(config.version, 42);
            assert_eq!(
                config.serving_table_vnode_mappings,
                expected_serving_mappings
            );
            let policies = config.table_cache_refill_policies.unwrap();
            assert_eq!(policies.table_policies[0].table_id, table_id);
            assert_eq!(
                policies.internal_table_policies.first().map(|p| p.table_id),
                expects_internal_policy.then_some(internal_table_id)
            );
        }
    }
}
