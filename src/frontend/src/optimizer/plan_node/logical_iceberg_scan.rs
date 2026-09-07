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

use std::hash::{Hash, Hasher};

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_connector::source::iceberg::IcebergFileScanTask;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, PlanBase, PredicatePushdown,
    ToBatch, ToStream, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, LogicalFilter, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries.
/// It represents a scan of Iceberg data files, with delete files handled via anti-joins
/// added on top of this scan.
///
/// The conversion flow is:
/// 1. `LogicalSource` (iceberg) -> `LogicalIcebergIntermediateScan`
/// 2. Predicate pushdown and column pruning on `LogicalIcebergIntermediateScan`
/// 3. `LogicalIcebergIntermediateScan` -> `LogicalIcebergScan`
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    pub task: IcebergFileScanTask,
}

impl Eq for LogicalIcebergScan {}

impl Hash for LogicalIcebergScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.base.hash(state);
        self.core.hash(state);
        self.iceberg_scan_type().hash(state);
    }
}

impl LogicalIcebergScan {
    pub fn new(core: generic::Source, task: IcebergFileScanTask) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalIcebergScan { base, core, task }
    }

    pub fn source_catalog(&self) -> Option<&SourceCatalog> {
        self.core.catalog.as_deref()
    }

    pub fn iceberg_scan_type(&self) -> IcebergScanType {
        self.task.scan_type()
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergScan}
impl Distill for LogicalIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
                (
                    "iceberg_scan_type",
                    Pretty::debug(&self.iceberg_scan_type()),
                ),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalIcebergScan", fields)
    }
}

impl ColPrunable for LogicalIcebergScan {
    fn prune_col(&self, _: &[usize], _: &mut ColumnPruningContext) -> PlanRef {
        // Column pruning should have been done in LogicalIcebergIntermediateScan.
        unreachable!()
    }
}

impl ExprRewritable<Logical> for LogicalIcebergScan {}

impl ExprVisitable for LogicalIcebergScan {}

impl PredicatePushdown for LogicalIcebergScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // Predicate pushdown should have been done in LogicalIcebergIntermediateScan.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalIcebergScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let plan = BatchIcebergScan::new(self.core.clone(), self.task.clone()).into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
