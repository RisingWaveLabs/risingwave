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

use educe::Educe;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::Schema;

use super::{DistillUnit, GenericPlanNode};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

/// The convention-independent core of a plan node that retrieves channel delta statistics.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct GetChannelDeltaStats {
    pub schema: Schema,
    pub at_time: Option<u64>,
    pub time_offset: Option<u64>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GetChannelDeltaStats {
    pub fn new(
        ctx: OptimizerContextRef,
        schema: Schema,
        at_time: Option<u64>,
        time_offset: Option<u64>,
    ) -> Self {
        Self {
            schema,
            at_time,
            time_offset,
            ctx,
        }
    }
}

impl GenericPlanNode for GetChannelDeltaStats {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

impl DistillUnit for GetChannelDeltaStats {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(
            name,
            vec![
                ("at_time", Pretty::debug(&self.at_time)),
                ("time_offset", Pretty::debug(&self.time_offset)),
            ],
        )
    }
}
