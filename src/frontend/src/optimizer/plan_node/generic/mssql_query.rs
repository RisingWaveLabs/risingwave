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
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};

use super::GenericPlanNode;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

/// Generic (logical) plan node for the `mssql_query` table function.
/// Carries the connection parameters, the pre-discovered schema, and the
/// user query. This is the shared body used by both [`LogicalMssalQuery`]
/// and [`BatchMssalQuery`] via the convention-generic `GenericPlanNode` trait.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct MssqlQuery {
    /// Result schema discovered at bind time by `describe_mssql_query`.
    pub schema: Schema,
    pub hostname: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub query: String,
    /// SQL Server `encrypt` connection string option: `true` or `false`.
    /// `None` for the 2-arg source-reference form (the value is taken
    /// from the referenced `sqlserver-cdc` source instead).
    pub encrypt: Option<String>,
    /// SQL Server `TrustServerCertificate` connection string option:
    /// `true` or `false`. `None` for the 2-arg source-reference form.
    pub trust_cert: Option<String>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for MssqlQuery {
    /// Return the pre-discovered result schema.
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// `mssql_query` does not produce a stream key — the executor returns
    /// rows in whatever order SQL Server produces them.
    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    /// Return the optimizer context associated with this node.
    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    /// No non-trivial functional dependencies are known for an arbitrary
    /// user-provided query; report a fresh empty FD set.
    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

impl MssqlQuery {
    /// Build the [`ColumnDesc`] list consumed by `BatchMssalQuery::to_batch_prost_body`.
    /// Column ids are placeholder-based — the executor does not depend on them.
    pub fn columns(&self) -> Vec<ColumnDesc> {
        self.schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                ColumnDesc::named(f.name.clone(), ColumnId::new(i as i32), f.data_type.clone())
            })
            .collect()
    }
}
