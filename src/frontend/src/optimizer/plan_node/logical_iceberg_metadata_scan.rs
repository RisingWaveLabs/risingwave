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
use risingwave_common::bail;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    BatchIcebergMetadataScan, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    PlanBase, PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, LogicalProject, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIcebergMetadataScan {
    pub base: PlanBase<Logical>,
    pub core: generic::IcebergMetadataScan,
}

impl LogicalIcebergMetadataScan {
    pub fn new(core: generic::IcebergMetadataScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergMetadataScan }

impl Distill for LogicalIcebergMetadataScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![
            (
                "metadata_type",
                Pretty::debug(&self.core.metadata_type.suffix()),
            ),
            ("columns", column_names_pretty(self.schema())),
        ];
        childless_record("LogicalIcebergMetadataScan", fields)
    }
}

impl ColPrunable for LogicalIcebergMetadataScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().copied()).into()
    }
}

impl ExprRewritable<Logical> for LogicalIcebergMetadataScan {}

impl ExprVisitable for LogicalIcebergMetadataScan {}

impl PredicatePushdown for LogicalIcebergMetadataScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalIcebergMetadataScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        Ok(BatchIcebergMetadataScan::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalIcebergMetadataScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        bail!("Iceberg metadata relations are not supported in streaming queries")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail!("Iceberg metadata relations are not supported in streaming queries")
    }
}
