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

use std::collections::HashMap;

use risingwave_meta_model::{FragmentId, WorkerId};
use risingwave_pb::meta::{PbTableCacheRefillPolicies, PbTableRefillRuntimeConfig};

use crate::controller::fragment::FragmentServingInfo;
use crate::manager::NotificationVersion;
use crate::serving::{ServingVnodeMappingRef, to_pb_serving_table_vnode_mappings};

pub fn build_hummock_table_refill_runtime_config(
    serving_vnode_mapping: &ServingVnodeMappingRef,
    worker_id: WorkerId,
    table_cache_refill_policies: PbTableCacheRefillPolicies,
    fragment_serving_infos: &HashMap<FragmentId, FragmentServingInfo>,
    version: NotificationVersion,
) -> PbTableRefillRuntimeConfig {
    let table_vnode_mapping = serving_vnode_mapping
        .table_vnode_mappings_by_worker([worker_id], fragment_serving_infos)
        .remove(&worker_id)
        .expect("requested worker must have a table vnode mapping");

    PbTableRefillRuntimeConfig {
        table_cache_refill_policies: Some(table_cache_refill_policies),
        serving_table_vnode_mappings: Some(to_pb_serving_table_vnode_mappings(
            &table_vnode_mapping,
        )),
        version,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::hash::{VirtualNode, WorkerSlotId};
    use risingwave_meta_model::TableId;
    use risingwave_pb::common::{WorkerNode, WorkerType, worker_node};
    use risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy;
    use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;

    use super::*;
    use crate::serving::ServingVnodeMapping;

    fn serving_worker(id: u32) -> WorkerNode {
        WorkerNode {
            id: id.into(),
            r#type: WorkerType::ComputeNode as i32,
            property: Some(worker_node::Property {
                is_serving: true,
                parallelism: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_build_hummock_table_refill_runtime_config_snapshot() {
        let worker1 = serving_worker(1);
        let worker2 = serving_worker(2);
        let fragment_id = FragmentId::new(233);
        let result_table_id = TableId::new(234);
        let internal_table_id = TableId::new(235);
        let fragment_serving_infos = HashMap::from([(
            fragment_id,
            FragmentServingInfo {
                result_table_id: Some(result_table_id),
                distribution_type: FragmentDistributionType::Hash,
                vnode_count: VirtualNode::COUNT_FOR_TEST,
            },
        )]);
        let serving_vnode_mapping = Arc::new(ServingVnodeMapping::default());
        let (fragment_mappings, failed) = serving_vnode_mapping.upsert(
            &fragment_serving_infos,
            &[worker1.clone(), worker2],
            None,
        );
        assert!(failed.is_empty());
        let expected_bitmap =
            fragment_mappings[&fragment_id].to_bitmaps()[&WorkerSlotId::new(worker1.id, 0)].clone();

        let policies = PbTableCacheRefillPolicies {
            table_policies: vec![PbTableCacheRefillPolicy {
                table_id: result_table_id.as_raw_id(),
                policy: PbCacheRefillPolicy::Serving as i32,
            }],
            internal_table_policies: vec![PbTableCacheRefillPolicy {
                table_id: internal_table_id.as_raw_id(),
                policy: PbCacheRefillPolicy::Streaming as i32,
            }],
        };
        let config = build_hummock_table_refill_runtime_config(
            &serving_vnode_mapping,
            worker1.id,
            policies.clone(),
            &fragment_serving_infos,
            42,
        );

        assert_eq!(config.version, 42);
        assert_eq!(config.table_cache_refill_policies, Some(policies));
        let mappings = config.serving_table_vnode_mappings.unwrap().mappings;
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].table_id, result_table_id.as_raw_id());
        assert_eq!(
            Bitmap::from(mappings[0].bitmap.clone().unwrap()),
            expected_bitmap
        );
    }
}
