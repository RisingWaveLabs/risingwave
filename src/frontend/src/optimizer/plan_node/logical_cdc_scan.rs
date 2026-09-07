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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc};
use risingwave_connector::source::cdc::CdcScanOptions;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef, ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalPlanRef as PlanRef,
    LogicalProject, PlanBase, PredicatePushdown, StreamPlanRef, ToBatch, ToStream, generic,
};
use crate::catalog::ColumnId;
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamCdcTableScan,
    ToStreamContext,
};
use crate::optimizer::property::Order;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalCdcScan` reads rows of a table from an external upstream database
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalCdcScan {
    pub base: PlanBase<Logical>,
    core: generic::CdcScan,
}

impl From<generic::CdcScan> for LogicalCdcScan {
    fn from(core: generic::CdcScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl From<generic::CdcScan> for PlanRef {
    fn from(core: generic::CdcScan) -> Self {
        LogicalCdcScan::from(core).into()
    }
}

impl LogicalCdcScan {
    pub fn create(
        table_name: String, // explain-only
        cdc_table_desc: Rc<CdcTableDesc>,
        ctx: OptimizerContextRef,
        options: CdcScanOptions,
    ) -> Self {
        generic::CdcScan::new(
            table_name,
            (0..cdc_table_desc.columns.len()).collect(),
            cdc_table_desc,
            ctx,
            options,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn cdc_table_desc(&self) -> &CdcTableDesc {
        self.core.cdc_table_desc.as_ref()
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.core.output_column_ids()
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalCdcScan}

impl Distill for LogicalCdcScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        let key_is_columns = true;
        let key = if key_is_columns {
            "columns"
        } else {
            "output_columns"
        };
        vec.push((key, self.core.columns_pretty(verbose)));
        if !key_is_columns {
            vec.push((
                "required_columns",
                Pretty::Array(
                    self.output_col_idx()
                        .iter()
                        .map(|i| {
                            let col_name = &self.cdc_table_desc().columns[*i].name;
                            Pretty::from(if verbose {
                                format!("{}.{}", self.table_name(), col_name)
                            } else {
                                col_name.clone()
                            })
                        })
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalCdcScan", vec)
    }
}

impl ColPrunable for LogicalCdcScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // The CDC executor currently expects an identity projection over the complete external
        // table descriptor. Keep the scan intact and let a project above it prune the user-visible
        // columns. During stream rewrite, the project also keeps the stream key internally.
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().copied()).into()
    }
}

impl ExprRewritable<Logical> for LogicalCdcScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalCdcScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalCdcScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalCdcScan {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        unreachable!()
    }

    fn to_batch_with_order_required(&self, _required_order: &Order) -> Result<BatchPlanRef> {
        unreachable!()
    }
}

impl ToStream for LogicalCdcScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        Ok(StreamCdcTableScan::new(self.core.clone()).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use risingwave_common::catalog::{CdcTableDesc, ColumnDesc, ColumnId, TableId};
    use risingwave_common::id::SourceId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_connector::source::cdc::CdcScanOptions;

    use super::*;
    use crate::expr::InputRef;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::PlanTreeNodeUnary;

    #[test]
    fn test_predicate_pushdown_preserves_filter() {
        let desc = CdcTableDesc {
            table_id: TableId::new(1),
            source_id: SourceId::new(2),
            external_table_name: "mydb.orders".to_owned(),
            pk: vec![ColumnOrder::new(0, OrderType::ascending())],
            columns: vec![
                ColumnDesc::named("id", ColumnId::new(1), DataType::Int32),
                ColumnDesc::named("selected", ColumnId::new(2), DataType::Boolean),
            ],
            stream_key: vec![0],
            ..Default::default()
        };
        let scan: PlanRef = LogicalCdcScan::create(
            "orders".to_owned(),
            Rc::new(desc),
            OptimizerContext::mock(),
            CdcScanOptions {
                disable_backfill: true,
                ..Default::default()
            },
        )
        .into();
        let predicate = Condition::with_expr(InputRef::new(1, DataType::Boolean).into());
        let mut ctx = PredicatePushdownContext::new(scan.clone());

        let result = scan.predicate_pushdown(predicate.clone(), &mut ctx);

        let filter = result
            .as_logical_filter()
            .expect("the predicate must remain above the CDC scan");
        assert_eq!(filter.predicate(), &predicate);
        assert!(filter.input().as_logical_cdc_scan().is_some());
    }
}
