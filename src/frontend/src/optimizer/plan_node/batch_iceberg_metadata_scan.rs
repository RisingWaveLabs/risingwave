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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_connector::sink::iceberg::IcebergMetadataTableType;
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;
use risingwave_pb::batch_plan::IcebergMetadataScanNode;
use risingwave_pb::batch_plan::iceberg_metadata_scan_node::{MetadataType, TimeTravel};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, ToBatchPb, ToDistributedBatch, ToLocalBatch,
    generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchIcebergMetadataScan {
    pub base: PlanBase<Batch>,
    pub core: generic::IcebergMetadataScan,
}

impl BatchIcebergMetadataScan {
    pub fn new(core: generic::IcebergMetadataScan) -> Self {
        let base = PlanBase::new_batch_with_core(&core, Distribution::Single, Order::any());
        Self { base, core }
    }

    fn clone_with_dist(&self) -> Self {
        Self {
            base: self.base.clone_with_new_distribution(Distribution::Single),
            core: self.core.clone(),
        }
    }
}

impl_plan_tree_node_for_leaf! { Batch, BatchIcebergMetadataScan }

impl Distill for BatchIcebergMetadataScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![
            (
                "metadata_type",
                Pretty::debug(&self.core.metadata_type.suffix()),
            ),
            ("columns", column_names_pretty(self.schema())),
        ];
        childless_record("BatchIcebergMetadataScan", fields)
    }
}

impl ToLocalBatch for BatchIcebergMetadataScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchIcebergMetadataScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchIcebergMetadataScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let metadata_type = match self.core.metadata_type {
            IcebergMetadataTableType::Snapshots => MetadataType::Snapshots,
            IcebergMetadataTableType::Manifests => MetadataType::Manifests,
            IcebergMetadataTableType::Files => MetadataType::Files,
        };
        let time_travel =
            self.core
                .time_travel_info
                .as_ref()
                .map(|time_travel_info| match time_travel_info {
                    IcebergTimeTravelInfo::Version(snapshot_id) => {
                        TimeTravel::SnapshotId(*snapshot_id)
                    }
                    IcebergTimeTravelInfo::TimestampMs(timestamp_ms) => {
                        TimeTravel::TimestampMs(*timestamp_ms)
                    }
                });

        NodeBody::IcebergMetadataScan(IcebergMetadataScanNode {
            with_properties: self.core.properties.clone(),
            secret_refs: self.core.secret_refs.clone(),
            metadata_type: metadata_type as i32,
            time_travel,
        })
    }
}

impl ExprRewritable<Batch> for BatchIcebergMetadataScan {}

impl ExprVisitable for BatchIcebergMetadataScan {}
