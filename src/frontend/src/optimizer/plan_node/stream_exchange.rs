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

use std::assert_matches;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, PbDispatchOutputMapping,
};

use super::stream::prelude::*;
use super::utils::{Distill, childless_record, plan_node_name};
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef};
use crate::Explain as _;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::LOCAL_PHASE_VNODE_COLUMN_NAME;
use crate::optimizer::property::{
    Distribution, DistributionDisplay, MonotonicityMap, RequiredDist,
};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamExchange` imposes a particular distribution on its input
/// without changing its content.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamExchange {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    no_shuffle: bool,
}

impl StreamExchange {
    pub fn new(input: PlanRef, dist: Distribution) -> Self {
        assert_matches!(
            dist,
            Distribution::HashShard(_) | Distribution::Single | Distribution::Broadcast,
            "exchange cannot be used to enforce this distribution"
        );

        // For non-append-only distributed stream, check that distribution key is a subset of stream key
        // for input plan.
        //
        // Otherwise, the changes on the same stream key might already be on different parallelism, and
        // merging them with this exchange could break the correct ordering, leading to inconsistent
        // stream or data loss.
        //
        // An exception is two-phase aggregation or top-n, where we used the `rw_vnode(dist_key)` as the
        // group key (thus stream key). In this case, changes on the same stream key (then vnode value)
        // must be on the same parallelism based on our scheduling algorithm, thus it's safe to shuffle.
        if !input.append_only()
            && input.distribution() != &Distribution::Single
            && let Some(input_dist_key) = input.distribution().dist_column_indices_opt()
            && let Some(input_stream_key) = input.stream_key()
            && input_stream_key
                .iter()
                .all(|k| input.schema()[*k].name != LOCAL_PHASE_VNODE_COLUMN_NAME)
        {
            assert!(
                input_dist_key
                    .iter()
                    .all(|idx| input_stream_key.contains(idx)),
                "distribution key must be a subset of stream key before shuffle to a different distribution\n\
                 - dist_key: {input_dist_key:?}\n\
                 - stream_key: {input_stream_key:?}\n\
                 - schema: {}\n\
                 - plan:\n{}",
                input.schema().formatted_col_names(),
                input.explain_to_string()
            );
        }

        let columns_monotonicity = if input.distribution().satisfies(&RequiredDist::single()) {
            // If the input is a singleton, the monotonicity will be preserved during shuffle
            // since we use ordered channel/buffer when exchanging data.
            input.columns_monotonicity().clone()
        } else {
            MonotonicityMap::new()
        };
        assert!(!input.schema().is_empty());
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.stream_key().map(|v| v.to_vec()),
            input.functional_dependency().clone(),
            dist,
            input.stream_kind(), // stream kind property won't change
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            columns_monotonicity,
        );
        StreamExchange {
            base,
            input,
            no_shuffle: false,
        }
    }

    pub fn new_no_shuffle(input: PlanRef) -> Self {
        let ctx = input.ctx();
        let base = PlanBase::new_stream(
            ctx,
            input.schema().clone(),
            input.stream_key().map(|v| v.to_vec()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.stream_kind(), // stream kind property won't change
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );
        StreamExchange {
            base,
            input,
            no_shuffle: true,
        }
    }

    pub fn no_shuffle(&self) -> bool {
        self.no_shuffle
    }
}

impl Distill for StreamExchange {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let distribution_display = DistributionDisplay {
            distribution: self.base.distribution(),
            input_schema: self.input.schema(),
        };
        childless_record(
            plan_node_name!(
                "StreamExchange",
                { "no_shuffle", self.no_shuffle },
            ),
            vec![("dist", Pretty::display(&distribution_display))],
        )
    }
}

impl PlanTreeNodeUnary<Stream> for StreamExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        if self.no_shuffle {
            Self::new_no_shuffle(input)
        } else {
            Self::new(input, self.distribution().clone())
        }
    }
}
impl_plan_tree_node_for_unary! { Stream, StreamExchange}

impl StreamNode for StreamExchange {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        let output_mapping = PbDispatchOutputMapping::identical(self.schema().len()).into();

        NodeBody::Exchange(Box::new(ExchangeNode {
            strategy: if self.no_shuffle {
                Some(DispatchStrategy {
                    r#type: DispatcherType::NoShuffle as i32,
                    dist_key_indices: vec![],
                    output_mapping,
                })
            } else {
                Some(DispatchStrategy {
                    r#type: match &self.base.distribution() {
                        Distribution::HashShard(_) => DispatcherType::Hash,
                        Distribution::Single => DispatcherType::Simple,
                        Distribution::Broadcast => DispatcherType::Broadcast,
                        _ => panic!("Do not allow Any or AnyShard in serialization process"),
                    } as i32,
                    dist_key_indices: match &self.base.distribution() {
                        Distribution::HashShard(keys) => {
                            keys.iter().map(|num| *num as u32).collect()
                        }
                        _ => vec![],
                    },
                    output_mapping,
                })
            },
        }))
    }
}

impl ExprRewritable<Stream> for StreamExchange {}

impl ExprVisitable for StreamExchange {}
