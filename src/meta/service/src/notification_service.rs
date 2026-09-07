// Copyright 2023 RisingWave Labs
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

use anyhow::{Context, anyhow};
use risingwave_common::secret::{LocalSecretManager, SecretEncryption};
use risingwave_hummock_sdk::FrontendHummockVersion;
use risingwave_meta::MetaResult;
use risingwave_meta::controller::catalog::Catalog;
use risingwave_meta::controller::fragment::FragmentServingInfo;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::{FragmentId, WorkerId};
use risingwave_pb::backup_service::MetaBackupManifestId;
use risingwave_pb::catalog::{Secret, Table};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::{ClusterResource, WorkerNode, WorkerType};
use risingwave_pb::hummock::WriteLimits;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, GetSessionParamsResponse, MetaSnapshot, PbTableCacheRefillPolicies,
    SubscribeRequest, SubscribeType,
};
use risingwave_pb::user::UserInfo;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::{MetaSrvEnv, Notification, NotificationVersion, WorkerKey};
use crate::serving::ServingVnodeMappingRef;
use crate::table_refill::build_hummock_table_refill_runtime_config;

pub struct NotificationServiceImpl {
    env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    serving_vnode_mapping: ServingVnodeMappingRef,
}

impl NotificationServiceImpl {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        backup_manager: BackupManagerRef,
        serving_vnode_mapping: ServingVnodeMappingRef,
    ) -> MetaResult<Self> {
        let service = Self {
            env,
            metadata_manager,
            hummock_manager,
            backup_manager,
            serving_vnode_mapping,
        };
        let (secrets, _catalog_version) = service.get_decrypted_secret_snapshot().await?;
        LocalSecretManager::global().init_secrets(secrets);
        Ok(service)
    }

    async fn get_catalog_snapshot(
        &self,
    ) -> MetaResult<(Catalog, Vec<UserInfo>, NotificationVersion)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let (
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
        ) = catalog_guard.snapshot().await?;
        let notification_version = self.env.notification_manager().current_version().await;
        Ok((
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
            notification_version,
        ))
    }

    /// Get decrypted secret snapshot
    async fn get_decrypted_secret_snapshot(
        &self,
    ) -> MetaResult<(Vec<Secret>, NotificationVersion)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let secrets = catalog_guard.list_secrets().await?;
        let notification_version = self.env.notification_manager().current_version().await;

        let decrypted_secrets = self.decrypt_secrets(secrets)?;

        Ok((decrypted_secrets, notification_version))
    }

    fn decrypt_secrets(&self, secrets: Vec<Secret>) -> MetaResult<Vec<Secret>> {
        // Skip getting `secret_store_private_key` if there is no secret
        if secrets.is_empty() {
            return Ok(vec![]);
        }
        let secret_store_private_key = self
            .env
            .opts
            .secret_store_private_key
            .clone()
            .ok_or_else(|| anyhow!("secret_store_private_key is not configured"))?;
        let mut decrypted_secrets = Vec::with_capacity(secrets.len());
        for mut secret in secrets {
            let encrypted_secret = SecretEncryption::deserialize(secret.get_value())
                .context(format!("failed to deserialize secret {}", secret.name))?;
            let decrypted_secret = encrypted_secret
                .decrypt(secret_store_private_key.as_slice())
                .context(format!("failed to decrypt secret {}", secret.name))?;
            secret.value = decrypted_secret;
            decrypted_secrets.push(secret);
        }
        Ok(decrypted_secrets)
    }

    async fn get_worker_slot_mapping_snapshot(
        &self,
    ) -> MetaResult<(Vec<FragmentWorkerSlotMapping>, NotificationVersion)> {
        let mappings = self
            .metadata_manager
            .catalog_controller
            .get_worker_slot_mappings();

        let notification_version = self.env.notification_manager().current_version().await;
        Ok((mappings, notification_version))
    }

    fn get_serving_vnode_mappings(&self) -> Vec<FragmentWorkerSlotMapping> {
        self.serving_vnode_mapping
            .all()
            .iter()
            .map(|(&fragment_id, mapping)| FragmentWorkerSlotMapping {
                fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect()
    }

    async fn get_worker_node_snapshot(&self) -> MetaResult<(Vec<WorkerNode>, NotificationVersion)> {
        let cluster_guard = self
            .metadata_manager
            .cluster_controller
            .get_inner_read_guard()
            .await;
        let compute_nodes = cluster_guard
            .list_workers(Some(WorkerType::ComputeNode.into()), Some(Running.into()))
            .await?;
        let frontends = cluster_guard
            .list_workers(Some(WorkerType::Frontend.into()), Some(Running.into()))
            .await?;
        let worker_nodes = compute_nodes.into_iter().chain(frontends).collect();
        let notification_version = self.env.notification_manager().current_version().await;
        Ok((worker_nodes, notification_version))
    }

    async fn get_tables_snapshot(&self) -> MetaResult<(Vec<Table>, NotificationVersion)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let notification_version = self.env.notification_manager().current_version().await;
        Ok((tables, notification_version))
    }

    async fn get_hummock_catalog_snapshot(
        &self,
    ) -> MetaResult<(
        Vec<Table>,
        PbTableCacheRefillPolicies,
        HashMap<FragmentId, FragmentServingInfo>,
        NotificationVersion,
    )> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let table_cache_refill_policies =
            catalog_guard.table_cache_refill_policies_snapshot().await?;
        let fragment_serving_infos = catalog_guard.fragment_serving_infos().await?;
        let notification_version = self.env.notification_manager().current_version().await;
        Ok((
            tables,
            table_cache_refill_policies,
            fragment_serving_infos,
            notification_version,
        ))
    }

    /// Get the total resource of the cluster.
    async fn get_cluster_resource(&self) -> MetaResult<ClusterResource> {
        self.metadata_manager
            .cluster_controller
            .cluster_resource()
            .await
    }

    async fn compactor_subscribe(&self) -> MetaResult<MetaSnapshot> {
        let (tables, catalog_version) = self.get_tables_snapshot().await?;
        let cluster_resource = self.get_cluster_resource().await?;

        Ok(MetaSnapshot {
            tables,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }

    async fn frontend_subscribe(&self) -> MetaResult<MetaSnapshot> {
        let (
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
            catalog_version,
        ) = self.get_catalog_snapshot().await?;
        let object_dependencies = self
            .metadata_manager
            .catalog_controller
            .list_created_object_dependencies()
            .await?;

        // Use the plain text secret value for frontend. The secret value will be masked in frontend handle.
        let decrypted_secrets = self.decrypt_secrets(secrets)?;

        let (streaming_worker_slot_mappings, streaming_worker_slot_mapping_version) =
            self.get_worker_slot_mapping_snapshot().await?;

        let streaming_job_count = self.metadata_manager.count_streaming_job().await?;
        if streaming_job_count > 0 && streaming_worker_slot_mappings.is_empty() {
            tracing::warn!(
                streaming_job_count,
                "frontend subscribe returns empty streaming_worker_slot_mappings while streaming jobs exist; meta may still be recovering"
            );
        }

        let serving_worker_slot_mappings = self.get_serving_vnode_mappings();

        let (nodes, worker_node_version) = self.get_worker_node_snapshot().await?;

        let hummock_version = self
            .hummock_manager
            .on_current_version(|version| {
                FrontendHummockVersion::from_version(version).to_protobuf()
            })
            .await;

        let session_params = self
            .env
            .session_params_manager_impl_ref()
            .get_params()
            .await;

        let session_params = Some(GetSessionParamsResponse {
            params: serde_json::to_string(&session_params)
                .context("failed to encode session params")?,
        });

        let cluster_resource = self.get_cluster_resource().await?;

        Ok(MetaSnapshot {
            databases,
            schemas,
            sources,
            sinks,
            tables,
            indexes,
            views,
            subscriptions,
            functions,
            connections,
            secrets: decrypted_secrets,
            users,
            nodes,
            hummock_version: Some(hummock_version),
            version: Some(SnapshotVersion {
                catalog_version,
                worker_node_version,
                streaming_worker_slot_mapping_version,
            }),
            serving_worker_slot_mappings,
            streaming_worker_slot_mappings,
            session_params,
            object_dependencies,
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }

    async fn hummock_subscribe(&self, worker_id: WorkerId) -> MetaResult<MetaSnapshot> {
        let (tables, table_cache_refill_policies, fragment_serving_infos, catalog_version) =
            self.get_hummock_catalog_snapshot().await?;
        let hummock_version = self
            .hummock_manager
            .on_current_version(|version| version.into())
            .await;
        let hummock_write_limits = self.hummock_manager.write_limits().await;
        let meta_backup_manifest_id = self.backup_manager.manifest().await.manifest_id;
        let cluster_resource = self.get_cluster_resource().await?;
        let table_refill_runtime_config = build_hummock_table_refill_runtime_config(
            &self.serving_vnode_mapping,
            worker_id,
            table_cache_refill_policies,
            &fragment_serving_infos,
            catalog_version,
        );

        Ok(MetaSnapshot {
            tables,
            hummock_version: Some(hummock_version),
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            meta_backup_manifest_id: Some(MetaBackupManifestId {
                id: meta_backup_manifest_id,
            }),
            hummock_write_limits: Some(WriteLimits {
                write_limits: hummock_write_limits,
            }),
            cluster_resource: Some(cluster_resource),
            table_refill_runtime_config: Some(table_refill_runtime_config),
            ..Default::default()
        })
    }

    async fn compute_subscribe(&self) -> MetaResult<MetaSnapshot> {
        let (secrets, catalog_version) = self.get_decrypted_secret_snapshot().await?;
        let cluster_resource = self.get_cluster_resource().await?;

        Ok(MetaSnapshot {
            secrets,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl NotificationService for NotificationServiceImpl {
    type SubscribeStream = UnboundedReceiverStream<Notification>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let host_address = req.get_host()?.clone();
        let subscribe_type = req.get_subscribe_type()?;

        let worker_key = WorkerKey(host_address);

        let (tx, rx) = mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_sender(subscribe_type, worker_key, tx.clone());

        let meta_snapshot = match subscribe_type {
            SubscribeType::Compactor => self.compactor_subscribe().await?,
            SubscribeType::Frontend => self.frontend_subscribe().await?,
            SubscribeType::Hummock => {
                self.hummock_manager
                    .pin_version(req.get_worker_id())
                    .await?;
                self.hummock_subscribe(req.get_worker_id()).await?
            }
            SubscribeType::Compute => self.compute_subscribe().await?,
            SubscribeType::Unspecified => unreachable!(),
        };

        self.env
            .notification_manager()
            .notify_snapshot(tx, meta_snapshot);

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}
