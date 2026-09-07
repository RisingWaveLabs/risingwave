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
use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_connector::source::iceberg::IcebergFileScanTask;
use risingwave_pb::batch_plan::IcebergScanNode;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, ToBatchPb, ToDistributedBatch, ToLocalBatch,
    generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq)]
pub struct BatchIcebergScan {
    pub base: PlanBase<Batch>,
    pub core: generic::Source,
    pub task: IcebergFileScanTask,
    limit: Option<u64>,
}

impl Eq for BatchIcebergScan {}

impl Hash for BatchIcebergScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.base.hash(state);
        self.core.hash(state);
        self.iceberg_scan_type().hash(state);
        self.limit.hash(state);
    }
}

impl BatchIcebergScan {
    pub fn new(core: generic::Source, task: IcebergFileScanTask) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            // Use `Single` by default, will be updated later with `clone_with_dist`.
            Distribution::Single,
            Order::any(),
        );

        Self {
            base,
            core,
            task,
            limit: None,
        }
    }

    pub fn clone_with_limit(&self, limit: Option<u64>) -> Self {
        Self {
            base: self.base.clone(),
            core: self.core.clone(),
            task: self.task.clone(),
            limit,
        }
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub fn iceberg_scan_type(&self) -> IcebergScanType {
        self.task.scan_type()
    }

    pub fn predicate(&self) -> Option<String> {
        let predicate = self.task.predicate()?;
        Some(predicate.to_string())
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_dist(&self) -> Self {
        let base = self
            .base
            .clone_with_new_distribution(Distribution::SomeShard);
        Self {
            base,
            core: self.core.clone(),
            task: self.task.clone(),
            limit: self.limit,
        }
    }

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }
}

impl_plan_tree_node_for_leaf! { Batch, BatchIcebergScan }

impl Distill for BatchIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().unwrap().name.clone());
        let mut fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
            (
                "iceberg_scan_type",
                Pretty::from(format!("{:?}", self.iceberg_scan_type())),
            ),
        ];
        if let Some(predicate) = self.predicate() {
            fields.push(("predicate", Pretty::from(predicate)));
        }
        if let Some(limit) = self.limit {
            fields.push(("limit", Pretty::debug(&limit)));
        }
        childless_record("BatchIcebergScan", fields)
    }
}

impl ToLocalBatch for BatchIcebergScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchIcebergScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchIcebergScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.source_catalog().unwrap();
        let (with_properties, secret_refs) = source_catalog.with_properties.clone().into_parts();
        NodeBody::IcebergScan(IcebergScanNode {
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            with_properties,
            split: vec![],
            secret_refs,
            iceberg_scan_type: self.iceberg_scan_type() as i32,
        })
    }
}

impl ExprRewritable<Batch> for BatchIcebergScan {}

impl ExprVisitable for BatchIcebergScan {}
