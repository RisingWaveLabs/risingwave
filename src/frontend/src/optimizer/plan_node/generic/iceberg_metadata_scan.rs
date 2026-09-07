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

use std::collections::BTreeMap;

use educe::Educe;
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::iceberg::IcebergMetadataTableType;
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;
use risingwave_pb::secret::PbSecretRef;

use super::GenericPlanNode;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct IcebergMetadataScan {
    pub metadata_type: IcebergMetadataTableType,
    pub properties: BTreeMap<String, String>,
    pub secret_refs: BTreeMap<String, PbSecretRef>,
    pub time_travel_info: Option<IcebergTimeTravelInfo>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for IcebergMetadataScan {
    fn schema(&self) -> Schema {
        self.metadata_type.schema()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.metadata_type.schema().len())
    }
}
