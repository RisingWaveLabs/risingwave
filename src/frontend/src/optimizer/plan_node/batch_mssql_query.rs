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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::MssqlQueryNode;
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

/// Batch plan node for the `mssql_query` table function. The actual
/// SQL Server round-trip is performed by `MssalQueryExecutor`; this node
/// only carries the connection parameters, the pre-discovered column
/// schema (from `describe_mssql_query`), and the user query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchMssqlQuery {
    pub base: PlanBase<Batch>,
    pub core: generic::MssqlQuery,
}

impl BatchMssqlQuery {
    /// Build a new `BatchMssalQuery` plan node. The plan runs on a
    /// single fragment (`Distribution::Single`) with no specific row
    /// order; the executor returns whatever order SQL Server produces.
    pub fn new(core: generic::MssqlQuery) -> Self {
        let base = PlanBase::new_batch_with_core(&core, Distribution::Single, Order::any());

        Self { base, core }
    }

    /// Names of the result columns as discovered at bind time. For
    /// unnamed columns like `SELECT COUNT(*) FROM t` these are the
    /// synthetic `column_N` names produced by `describe_mssql_query`.
    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    /// Return a clone of this node with `Distribution::Single`. Used
    /// by `ToLocalBatch` / `ToDistributedBatch` because the underlying
    /// `mssql_query` always runs on a single fragment.
    pub fn clone_with_dist(&self) -> Self {
        let base = self.base.clone_with_new_distribution(Distribution::Single);
        Self {
            base,
            core: self.core.clone(),
        }
    }
}

impl_plan_tree_node_for_leaf! { Batch, BatchMssqlQuery }

impl Distill for BatchMssqlQuery {
    /// Pretty-print the node as `BatchMssqlQuery { columns: [...] }` for
    /// `EXPLAIN` output.
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("BatchMssqlQuery", fields)
    }
}

impl ToLocalBatch for BatchMssqlQuery {
    /// `mssql_query` always runs on a single fragment; the local plan
    /// is a clone with the single-node distribution.
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchMssqlQuery {
    /// `mssql_query` always runs on a single fragment; the distributed
    /// plan is a clone with the single-node distribution.
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchMssqlQuery {
    /// Serialize the node into a `NodeBody::MssqlQuery` proto for the
    /// batch task. `encrypt` / `trust_cert` are emitted as the strings
    /// `"true"` / `"false"`, with the default `false` / `true` used when
    /// the binder did not set them (2-arg source-reference form).
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::MssqlQuery(MssqlQueryNode {
            columns: self
                .core
                .columns()
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            hostname: self.core.hostname.clone(),
            port: self.core.port.clone(),
            username: self.core.username.clone(),
            password: self.core.password.clone(),
            database: self.core.database.clone(),
            query: self.core.query.clone(),
            encrypt: self
                .core
                .encrypt
                .clone()
                .unwrap_or_else(|| "false".to_owned()),
            trust_cert: self
                .core
                .trust_cert
                .clone()
                .unwrap_or_else(|| "true".to_owned()),
        })
    }
}

impl ExprRewritable<Batch> for BatchMssqlQuery {}

impl ExprVisitable for BatchMssqlQuery {}
