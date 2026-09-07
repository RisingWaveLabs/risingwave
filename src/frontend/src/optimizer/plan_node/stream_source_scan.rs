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

use std::rc::Rc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::{NodeBody, PbNodeBody};

use super::stream::prelude::*;
use super::utils::TableCatalogBuilder;
use super::{PlanBase, PlanRef};
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{ExprRewritable, StreamNode, generic};
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{Explain, TableCatalog};

/// `StreamSourceScan` scans from a *shared source*. It forwards data from the upstream [`StreamSource`],
/// and also backfills data from the external source.
///
/// Unlike [`StreamSource`], which is a leaf node in the stream graph, `StreamSourceScan` is converted to `merge -> backfill`
///
/// [`StreamSource`]:super::StreamSource
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSourceScan {
    pub base: PlanBase<Stream>,
    core: generic::Source,
}

impl_plan_tree_node_for_leaf! { Stream, StreamSourceScan }

impl StreamSourceScan {
    pub const BACKFILL_PROGRESS_COLUMN_NAME: &str = "backfill_progress";
    pub const PARTITION_ID_COLUMN_NAME: &str = "partition_id";

    pub fn new(core: generic::Source) -> Self {
        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::SomeShard,
            core.stream_kind(),
            false,
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );

        Self { base, core }
    }

    fn get_columns(&self) -> Vec<&str> {
        self.core
            .column_catalog
            .iter()
            .map(|column| column.name())
            .collect()
    }

    pub fn source_catalog(&self) -> Rc<SourceCatalog> {
        self.core
            .catalog
            .clone()
            .expect("source scan should have source cataglog")
    }

    fn has_deferred_generated_stream_key(&self) -> bool {
        let source_catalog = self.source_catalog();
        let mut missing_pk_column = false;

        for pk_column_id in &source_catalog.pk_col_ids {
            if self
                .core
                .column_catalog
                .iter()
                .any(|column| column.column_id() == *pk_column_id)
            {
                continue;
            }

            missing_pk_column = true;
            if !source_catalog
                .columns
                .iter()
                .any(|column| column.column_id() == *pk_column_id && column.is_generated())
            {
                return false;
            }
        }

        missing_pk_column
    }

    /// The state is different from but similar to `StreamSource`.
    /// Refer to [`generic::Source::infer_internal_table_catalog`] for more details.
    pub fn infer_internal_table_catalog() -> TableCatalog {
        let mut builder = TableCatalogBuilder::default();

        let key = Field {
            data_type: DataType::Varchar,
            name: Self::PARTITION_ID_COLUMN_NAME.to_owned(),
        };
        let value = Field {
            data_type: DataType::Jsonb,
            name: Self::BACKFILL_PROGRESS_COLUMN_NAME.to_owned(),
        };

        let ordered_col_idx = builder.add_column(&key);
        builder.add_column(&value);
        builder.add_order_column(ordered_col_idx, OrderType::ascending());
        // Hacky: read prefix hint is 0, because we need to scan all data in the state table.
        builder.build(vec![], 0)
    }

    pub fn adhoc_to_stream_prost(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbStreamNode> {
        use risingwave_pb::stream_plan::*;

        let stream_key = match self.stream_key() {
            Some(stream_key) => stream_key.iter().map(|x| *x as u32).collect_vec(),
            None => {
                if !self.has_deferred_generated_stream_key() {
                    return Err(anyhow::anyhow!(
                        "source backfill may omit its stream key only when a primary-key column \
                         is generated and absent from the raw source output, sub plan: {}",
                        PlanRef::from(self.clone()).explain_to_string()
                    )
                    .into());
                }
                // Generated columns are evaluated by the project immediately above the source
                // backfill node, which restores the catalog-declared stream key.
                vec![]
            }
        };

        let source_catalog = self.source_catalog();
        let (with_properties, secret_refs) = source_catalog.with_properties.clone().into_parts();
        let backfill = SourceBackfillNode {
            upstream_source_id: source_catalog.id,
            source_name: source_catalog.name.clone(),
            state_table: Some(
                Self::infer_internal_table_catalog()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            info: Some(source_catalog.info.clone()),
            row_id_index: self.core.row_id_index.map(|index| index as _),
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            with_properties,
            rate_limit: self.base.ctx().overwrite_options().backfill_rate_limit,
            secret_refs,
        };

        let fields = self.schema().to_prost();
        // plan: merge -> backfill
        Ok(PbStreamNode {
            fields: fields.clone(),
            input: vec![
                // The merge node body will be filled by the `ActorBuilder` on the meta service.
                PbStreamNode {
                    node_body: Some(PbNodeBody::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields,
                    stream_key: vec![], // not used
                    ..Default::default()
                },
            ],
            node_body: Some(PbNodeBody::SourceBackfill(Box::new(backfill))),
            stream_key,
            operator_id: self.base.id().to_stream_node_operator_id(),
            identity: self.distill_to_string(),
            stream_kind: self.stream_kind().to_protobuf() as i32,
        })
    }
}

impl Distill for StreamSourceScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let columns = self
            .get_columns()
            .iter()
            .map(|ele| Pretty::from(ele.to_string()))
            .collect();
        let col = Pretty::Array(columns);
        childless_record("StreamSourceScan", vec![("columns", col)])
    }
}

impl ExprRewritable<Stream> for StreamSourceScan {}

impl ExprVisitable for StreamSourceScan {}

impl StreamNode for StreamSourceScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        unreachable!(
            "stream source scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead."
        )
    }
}
