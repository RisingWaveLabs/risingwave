// Copyright 2024 RisingWave Labs
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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rand::seq::IndexedRandom;
use risingwave_common::bail;
use risingwave_common::hash::{WorkerSlotId, WorkerSlotMapping};
use risingwave_common::id::{FragmentId, WorkerId};
use risingwave_common::util::version::current_rw_version;
use risingwave_common::vnode_mapping::vnode_placement::place_vnode;
use risingwave_pb::common::{WorkerNode, WorkerType};

use crate::error::{BatchError, Result};

/// `WorkerNodeManager` manages live worker nodes and table vnode mapping information.
pub struct WorkerNodeManager {
    inner: RwLock<WorkerNodeManagerInner>,
    /// Temporarily make worker invisible from serving cluster.
    worker_node_mask: Arc<RwLock<HashSet<WorkerId>>>,
}

struct WorkerNodeManagerInner {
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    /// fragment vnode mapping info for streaming
    streaming_fragment_vnode_mapping: Option<HashMap<FragmentId, WorkerSlotMapping>>,
    /// fragment vnode mapping info for serving
    serving_fragment_vnode_mapping: HashMap<FragmentId, WorkerSlotMapping>,
}

pub type WorkerNodeManagerRef = Arc<WorkerNodeManager>;

impl Default for WorkerNodeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerNodeManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(WorkerNodeManagerInner {
                worker_nodes: Default::default(),
                streaming_fragment_vnode_mapping: None,
                serving_fragment_vnode_mapping: Default::default(),
            }),
            worker_node_mask: Arc::new(Default::default()),
        }
    }

    /// Used in tests.
    pub fn mock(worker_nodes: Vec<WorkerNode>) -> Self {
        let worker_nodes = worker_nodes.into_iter().map(|w| (w.id, w)).collect();
        let inner = RwLock::new(WorkerNodeManagerInner {
            worker_nodes,
            streaming_fragment_vnode_mapping: None,
            serving_fragment_vnode_mapping: HashMap::new(),
        });
        Self {
            inner,
            worker_node_mask: Arc::new(Default::default()),
        }
    }

    pub fn list_compute_nodes(&self) -> Vec<WorkerNode> {
        self.inner
            .read()
            .unwrap()
            .worker_nodes
            .values()
            .filter(|w| w.r#type() == WorkerType::ComputeNode)
            .cloned()
            .collect()
    }

    pub fn list_frontend_nodes(&self) -> Vec<WorkerNode> {
        self.inner
            .read()
            .unwrap()
            .worker_nodes
            .values()
            .filter(|w| w.r#type() == WorkerType::Frontend)
            .cloned()
            .collect()
    }

    fn list_serving_worker_nodes(&self) -> Vec<WorkerNode> {
        self.list_compute_nodes()
            .into_iter()
            .filter(|w| w.property.as_ref().is_some_and(|p| p.is_serving))
            .collect()
    }

    fn list_streaming_worker_nodes(&self) -> Vec<WorkerNode> {
        self.list_compute_nodes()
            .into_iter()
            .filter(|w| w.property.as_ref().is_some_and(|p| p.is_streaming))
            .collect()
    }

    pub fn add_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        write_guard.worker_nodes.insert(node.id, node);
    }

    pub fn remove_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        write_guard.worker_nodes.remove(&node.id);
    }

    pub fn refresh(
        &self,
        nodes: Vec<WorkerNode>,
        streaming_mapping: HashMap<FragmentId, WorkerSlotMapping>,
        serving_mapping: HashMap<FragmentId, WorkerSlotMapping>,
    ) {
        let mut write_guard = self.inner.write().unwrap();
        tracing::debug!("Refresh worker nodes {:?}.", nodes);
        tracing::debug!(
            "Refresh streaming vnode mapping for fragments {:?}.",
            streaming_mapping.keys()
        );
        tracing::debug!(
            "Refresh serving vnode mapping for fragments {:?}.",
            serving_mapping.keys()
        );
        write_guard.worker_nodes = nodes.into_iter().map(|w| (w.id, w)).collect();
        write_guard.streaming_fragment_vnode_mapping = Some(streaming_mapping);
        write_guard.serving_fragment_vnode_mapping = serving_mapping;
    }

    /// If worker slot ids is empty, the scheduler may fail to schedule any task and stuck at
    /// schedule next stage. If we do not return error in this case, needs more complex control
    /// logic above. Report in this function makes the schedule root fail reason more clear.
    pub fn get_workers_by_worker_slot_ids(
        &self,
        worker_slot_ids: &[WorkerSlotId],
    ) -> Result<Vec<WorkerNode>> {
        if worker_slot_ids.is_empty() {
            return Err(BatchError::EmptyWorkerNodes);
        }
        let guard = self.inner.read().unwrap();
        let mut workers = Vec::with_capacity(worker_slot_ids.len());
        for worker_slot_id in worker_slot_ids {
            match guard.worker_nodes.get(&worker_slot_id.worker_id()) {
                Some(worker) => workers.push((*worker).clone()),
                None => bail!(
                    "No worker node found for worker slot id: {}",
                    worker_slot_id
                ),
            }
        }

        Ok(workers)
    }

    pub fn get_streaming_fragment_mapping(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<WorkerSlotMapping> {
        let guard = self.inner.read().unwrap();

        let Some(streaming_mapping) = guard.streaming_fragment_vnode_mapping.as_ref() else {
            return Err(BatchError::StreamingVnodeMappingNotInitialized);
        };

        streaming_mapping
            .get(fragment_id)
            .cloned()
            .ok_or_else(|| BatchError::StreamingVnodeMappingNotFound(*fragment_id))
    }

    pub fn insert_streaming_fragment_mapping(
        &self,
        fragment_id: FragmentId,
        vnode_mapping: WorkerSlotMapping,
    ) {
        let mut guard = self.inner.write().unwrap();
        let mapping = guard
            .streaming_fragment_vnode_mapping
            .get_or_insert_with(HashMap::new);
        if mapping.try_insert(fragment_id, vnode_mapping).is_err() {
            tracing::info!(
                "Previous batch vnode mapping not found for fragment {fragment_id}, maybe offline scaling with background ddl"
            );
        }
    }

    pub fn update_streaming_fragment_mapping(
        &self,
        fragment_id: FragmentId,
        vnode_mapping: WorkerSlotMapping,
    ) {
        let mut guard = self.inner.write().unwrap();
        let mapping = guard
            .streaming_fragment_vnode_mapping
            .get_or_insert_with(HashMap::new);
        if mapping.insert(fragment_id, vnode_mapping).is_none() {
            tracing::info!(
                "Previous vnode mapping not found for fragment {fragment_id}, maybe offline scaling with background ddl"
            );
        }
    }

    pub fn remove_streaming_fragment_mapping(&self, fragment_id: &FragmentId) {
        let mut guard = self.inner.write().unwrap();

        let res = guard
            .streaming_fragment_vnode_mapping
            .as_mut()
            .and_then(|mapping| mapping.remove(fragment_id));
        match &res {
            Some(_) => {}
            None if fragment_id.is_placeholder() => {
                // Do nothing for placeholder fragment.
            }
            None => {
                tracing::warn!(%fragment_id, "Streaming vnode mapping not found");
            }
        };
    }

    /// Returns fragment's vnode mapping for serving.
    fn serving_fragment_mapping(&self, fragment_id: FragmentId) -> Result<WorkerSlotMapping> {
        self.inner
            .read()
            .unwrap()
            .get_serving_fragment_mapping(fragment_id)
            .ok_or_else(|| BatchError::ServingVnodeMappingNotFound(fragment_id))
    }

    pub fn set_serving_fragment_mapping(&self, mappings: HashMap<FragmentId, WorkerSlotMapping>) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Set serving vnode mapping for fragments {:?}",
            mappings.keys()
        );
        guard.serving_fragment_vnode_mapping = mappings;
    }

    pub fn upsert_serving_fragment_mapping(
        &self,
        mappings: HashMap<FragmentId, WorkerSlotMapping>,
    ) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Upsert serving vnode mapping for fragments {:?}",
            mappings.keys()
        );
        for (fragment_id, mapping) in mappings {
            guard
                .serving_fragment_vnode_mapping
                .insert(fragment_id, mapping);
        }
    }

    pub fn remove_serving_fragment_mapping(&self, fragment_ids: &[FragmentId]) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Delete serving vnode mapping for fragments {:?}",
            fragment_ids
        );
        for fragment_id in fragment_ids {
            guard.serving_fragment_vnode_mapping.remove(fragment_id);
        }
    }

    #[cfg(test)]
    fn worker_node_mask(&self) -> std::sync::RwLockReadGuard<'_, HashSet<WorkerId>> {
        self.worker_node_mask.read().unwrap()
    }

    fn worker_node_mask_snapshot(&self) -> HashSet<WorkerId> {
        self.worker_node_mask.read().unwrap().clone()
    }

    pub fn mask_worker_node(&self, worker_node_id: WorkerId, duration: Duration) {
        tracing::info!(
            "Mask worker node {} for {:?} temporarily",
            worker_node_id,
            duration
        );
        let mut worker_node_mask = self.worker_node_mask.write().unwrap();
        if worker_node_mask.contains(&worker_node_id) {
            return;
        }
        worker_node_mask.insert(worker_node_id);
        let worker_node_mask_ref = self.worker_node_mask.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            worker_node_mask_ref
                .write()
                .unwrap()
                .remove(&worker_node_id);
        });
    }

    pub fn worker_node(&self, worker_id: WorkerId) -> Option<WorkerNode> {
        self.inner.read().unwrap().worker_node(worker_id)
    }
}

impl WorkerNodeManagerInner {
    fn get_serving_fragment_mapping(&self, fragment_id: FragmentId) -> Option<WorkerSlotMapping> {
        self.serving_fragment_vnode_mapping
            .get(&fragment_id)
            .cloned()
    }

    fn worker_node(&self, worker_id: WorkerId) -> Option<WorkerNode> {
        self.worker_nodes.get(&worker_id).cloned()
    }
}

/// Selects workers for query according to `enable_barrier_read`
#[derive(Clone)]
pub struct WorkerNodeSelector {
    pub manager: WorkerNodeManagerRef,
    enable_barrier_read: bool,
}

impl WorkerNodeSelector {
    pub fn new(manager: WorkerNodeManagerRef, enable_barrier_read: bool) -> Self {
        Self {
            manager,
            enable_barrier_read,
        }
    }

    pub fn worker_node_count(&self) -> usize {
        if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes().len()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
                .len()
        }
    }

    pub fn schedule_unit_count(&self) -> usize {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
        };
        worker_nodes
            .iter()
            .map(|node| node.compute_node_parallelism())
            .sum()
    }

    pub fn fragment_mapping(
        &self,
        fragment_id: FragmentId,
        batch_parallelism: usize,
    ) -> Result<WorkerSlotMapping> {
        if self.enable_barrier_read {
            self.manager.get_streaming_fragment_mapping(&fragment_id)
        } else {
            let mapping = (self.manager.serving_fragment_mapping(fragment_id)).or_else(|_| {
                tracing::warn!(
                    %fragment_id,
                    "Serving fragment mapping not found, fall back to streaming one."
                );
                self.manager.get_streaming_fragment_mapping(&fragment_id)
            })?;
            let workers = self.apply_worker_node_mask(self.manager.list_serving_worker_nodes());

            // Filter out unavailable workers.
            if workers.is_empty() {
                Err(BatchError::EmptyWorkerNodes)
            } else {
                let worker_ids = workers
                    .iter()
                    .map(|worker| worker.id)
                    .collect::<HashSet<_>>();
                let mapping_uses_only_available_workers = mapping
                    .iter_unique()
                    .all(|slot| worker_ids.contains(&slot.worker_id()));
                if mapping_uses_only_available_workers {
                    Ok(mapping)
                } else {
                    // If it's a singleton, set max_parallelism=1 for place_vnode.
                    // Otherwise, cap re-placement by the query's effective batch parallelism.
                    let max_parallelism =
                        mapping.to_single().map(|_| 1).or(Some(batch_parallelism));
                    let masked_mapping =
                        place_vnode(Some(&mapping), &workers, max_parallelism, mapping.len())
                            .ok_or_else(|| BatchError::EmptyWorkerNodes)?;
                    Ok(masked_mapping)
                }
            }
        }
    }

    pub fn next_random_worker(&self) -> Result<WorkerNode> {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
        };
        worker_nodes
            .choose(&mut rand::rng())
            .ok_or_else(|| BatchError::EmptyWorkerNodes)
            .map(|w| (*w).clone())
    }

    fn is_current_version_worker(worker: &WorkerNode, current_rw_version: &str) -> bool {
        worker
            .resource
            .as_ref()
            .is_some_and(|resource| resource.rw_version == current_rw_version)
    }

    fn apply_worker_node_mask(&self, origin: Vec<WorkerNode>) -> Vec<WorkerNode> {
        let current_rw_version = current_rw_version();
        let workers_with_current_version = origin
            .into_iter()
            .filter(|worker| Self::is_current_version_worker(worker, &current_rw_version))
            .collect::<Vec<_>>();
        let worker_node_mask = self.manager.worker_node_mask_snapshot();
        Self::apply_worker_node_mask_inner(workers_with_current_version, &worker_node_mask)
    }

    fn apply_worker_node_mask_inner(
        origin: Vec<WorkerNode>,
        worker_node_mask: &HashSet<WorkerId>,
    ) -> Vec<WorkerNode> {
        if origin.iter().all(|w| worker_node_mask.contains(&w.id)) {
            return origin;
        }
        origin
            .into_iter()
            .filter(|w| !worker_node_mask.contains(&w.id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_common::RW_VERSION;
    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::worker_node;
    use risingwave_pb::common::worker_node::Property;

    use super::*;

    #[test]
    fn test_worker_node_manager() {
        let manager = WorkerNodeManager::mock(vec![]);
        assert_eq!(manager.list_serving_worker_nodes().len(), 0);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 0);
        assert_eq!(manager.list_compute_nodes(), vec![]);

        let worker_nodes = vec![
            WorkerNode {
                id: 1.into(),
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1234").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                property: Some(Property {
                    is_serving: true,
                    is_streaming: true,
                    ..Default::default()
                }),
                transactional_id: Some(1),
                ..Default::default()
            },
            WorkerNode {
                id: 2.into(),
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1235").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                property: Some(Property {
                    is_serving: true,
                    is_streaming: false,
                    ..Default::default()
                }),
                transactional_id: Some(2),
                ..Default::default()
            },
        ];
        worker_nodes
            .iter()
            .for_each(|w| manager.add_worker_node(w.clone()));
        assert_eq!(manager.list_serving_worker_nodes().len(), 2);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 1);
        assert_eq!(
            manager
                .list_compute_nodes()
                .into_iter()
                .sorted_by_key(|w| w.id)
                .collect_vec(),
            worker_nodes
        );

        manager.remove_worker_node(worker_nodes[0].clone());
        assert_eq!(manager.list_serving_worker_nodes().len(), 1);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 0);
        assert_eq!(
            manager
                .list_compute_nodes()
                .into_iter()
                .sorted_by_key(|w| w.id)
                .collect_vec(),
            worker_nodes.as_slice()[1..].to_vec()
        );
    }

    fn serving_worker(id: u32, rw_version: Option<&str>) -> WorkerNode {
        WorkerNode {
            id: id.into(),
            r#type: WorkerType::ComputeNode as i32,
            host: Some(
                HostAddr::try_from(format!("127.0.0.1:{}", 1234 + id).as_str())
                    .unwrap()
                    .to_protobuf(),
            ),
            state: worker_node::State::Running as i32,
            property: Some(Property {
                is_serving: true,
                parallelism: 1,
                ..Default::default()
            }),
            transactional_id: Some(id),
            resource: rw_version.map(|version| worker_node::Resource {
                rw_version: version.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn fragment_mapping_worker_ids(mapping: &WorkerSlotMapping) -> Vec<WorkerId> {
        mapping
            .iter_unique()
            .map(|worker_slot_id| worker_slot_id.worker_id())
            .collect_vec()
    }

    fn worker_slot_mapping(worker_ids: impl IntoIterator<Item = u32>) -> WorkerSlotMapping {
        WorkerSlotMapping::new_uniform(
            worker_ids
                .into_iter()
                .map(|worker_id| WorkerSlotId::new(worker_id.into(), 0))
                .collect_vec()
                .into_iter(),
            4,
        )
    }

    #[test]
    fn test_fragment_mapping_masks_serving_workers_with_version_mismatch() {
        let manager = Arc::new(WorkerNodeManager::mock(vec![
            serving_worker(1, Some(RW_VERSION)),
            serving_worker(2, Some("different-version")),
        ]));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        manager
            .set_serving_fragment_mapping(HashMap::from([(0.into(), worker_slot_mapping([1, 2]))]));

        let mapping = selector.fragment_mapping(0.into(), 4).unwrap();

        assert_eq!(
            fragment_mapping_worker_ids(&mapping),
            vec![WorkerId::new(1)]
        );
    }

    #[test]
    fn test_fragment_mapping_keeps_serving_workers_with_current_version() {
        let manager = Arc::new(WorkerNodeManager::mock(vec![
            serving_worker(1, Some(RW_VERSION)),
            serving_worker(2, Some(RW_VERSION)),
        ]));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        manager
            .set_serving_fragment_mapping(HashMap::from([(0.into(), worker_slot_mapping([1, 2]))]));

        let mapping = selector.fragment_mapping(0.into(), 4).unwrap();

        assert_eq!(
            fragment_mapping_worker_ids(&mapping),
            vec![WorkerId::new(1), WorkerId::new(2)]
        );
    }

    #[test]
    fn test_fragment_mapping_masks_serving_workers_without_current_version() {
        let manager = Arc::new(WorkerNodeManager::mock(vec![
            serving_worker(1, Some(RW_VERSION)),
            serving_worker(2, None),
            serving_worker(3, Some("")),
        ]));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        manager.set_serving_fragment_mapping(HashMap::from([(
            0.into(),
            worker_slot_mapping([1, 2, 3]),
        )]));

        let mapping = selector.fragment_mapping(0.into(), 4).unwrap();

        assert_eq!(
            fragment_mapping_worker_ids(&mapping),
            vec![WorkerId::new(1)]
        );
    }

    #[test]
    fn test_fragment_mapping_errors_when_all_serving_workers_have_version_mismatch() {
        let manager = Arc::new(WorkerNodeManager::mock(vec![
            serving_worker(1, Some("different-version")),
            serving_worker(2, None),
            serving_worker(3, Some("")),
        ]));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        manager.set_serving_fragment_mapping(HashMap::from([(
            0.into(),
            worker_slot_mapping([1, 2, 3]),
        )]));

        assert!(matches!(
            selector.fragment_mapping(0.into(), 4),
            Err(BatchError::EmptyWorkerNodes)
        ));
    }

    #[tokio::test]
    async fn test_fragment_mapping_temporary_mask_fallback_keeps_version_filter() {
        let manager = Arc::new(WorkerNodeManager::mock(vec![
            serving_worker(1, Some(RW_VERSION)),
            serving_worker(2, Some("different-version")),
        ]));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        manager
            .set_serving_fragment_mapping(HashMap::from([(0.into(), worker_slot_mapping([1, 2]))]));
        manager.mask_worker_node(1.into(), Duration::from_secs(60));

        let mapping = selector.fragment_mapping(0.into(), 4).unwrap();

        assert_eq!(
            fragment_mapping_worker_ids(&mapping),
            vec![WorkerId::new(1)]
        );
    }

    #[tokio::test]
    async fn test_fragment_mapping_version_mask_does_not_clear_temporary_mask() {
        let manager = Arc::new(WorkerNodeManager::mock(vec![
            serving_worker(1, Some(RW_VERSION)),
            serving_worker(2, Some(RW_VERSION)),
            serving_worker(3, Some("different-version")),
        ]));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        manager.set_serving_fragment_mapping(HashMap::from([(
            0.into(),
            worker_slot_mapping([1, 2, 3]),
        )]));
        manager.mask_worker_node(2.into(), Duration::from_secs(60));

        let mapping = selector.fragment_mapping(0.into(), 4).unwrap();

        assert_eq!(
            fragment_mapping_worker_ids(&mapping),
            vec![WorkerId::new(1)]
        );
        assert!(manager.worker_node_mask().contains(&WorkerId::new(2)));
    }

    #[test]
    fn test_fragment_mapping_respects_batch_parallelism_with_masked_workers() {
        let worker_nodes = vec![
            WorkerNode {
                id: 1.into(),
                r#type: WorkerType::ComputeNode as i32,
                state: worker_node::State::Running as i32,
                property: Some(Property {
                    parallelism: 8,
                    is_serving: true,
                    ..Default::default()
                }),
                resource: Some(worker_node::Resource {
                    rw_version: RW_VERSION.to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            WorkerNode {
                id: 2.into(),
                r#type: WorkerType::ComputeNode as i32,
                state: worker_node::State::Running as i32,
                property: Some(Property {
                    parallelism: 8,
                    is_serving: true,
                    ..Default::default()
                }),
                resource: Some(worker_node::Resource {
                    rw_version: RW_VERSION.to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        let manager = Arc::new(WorkerNodeManager::mock(worker_nodes));
        let selector = WorkerNodeSelector::new(manager.clone(), false);
        let fragment_id = 1.into();
        let worker_slot_ids = (0..8)
            .map(|slot| WorkerSlotId::new(1.into(), slot))
            .chain((0..8).map(|slot| WorkerSlotId::new(2.into(), slot)))
            .collect_vec();
        let mapping = WorkerSlotMapping::build_from_ids(&worker_slot_ids, 32);
        manager.set_serving_fragment_mapping([(fragment_id, mapping)].into_iter().collect());

        manager.worker_node_mask.write().unwrap().insert(1.into());

        let masked_mapping = selector.fragment_mapping(fragment_id, 3).unwrap();
        assert_eq!(masked_mapping.iter_unique().count(), 3);
        assert!(
            masked_mapping
                .iter_unique()
                .all(|worker_slot_id| worker_slot_id.worker_id() == WorkerId::new(2))
        );
    }
}
