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
use risingwave_common::bail;
use risingwave_common::catalog::Schema;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    BatchMssqlQuery, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    LogicalProject, PlanBase, PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::OptimizerContextRef;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// Logical plan node for the `mssql_query` table function. The user query
/// is run exactly once by the batch executor; streaming mode is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalMssqlQuery {
    pub base: PlanBase<Logical>,
    pub core: generic::MssqlQuery,
}

impl LogicalMssqlQuery {
    /// Build a `LogicalMssalQuery` from the pre-discovered schema, the
    /// connection parameters, and the optimizer context. The eight inline
    /// form fields are all required; the 2-arg source-reference form
    /// instead reuses connection parameters from the named source.
    pub fn new(
        ctx: OptimizerContextRef,
        schema: Schema,
        hostname: String,
        port: String,
        username: String,
        password: String,
        database: String,
        query: String,
        encrypt: Option<String>,
        trust_cert: Option<String>,
    ) -> Self {
        let core = generic::MssqlQuery {
            schema,
            hostname,
            port,
            username,
            password,
            database,
            query,
            encrypt,
            trust_cert,
            ctx,
        };

        let base = PlanBase::new_logical_with_core(&core);

        LogicalMssqlQuery { base, core }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalMssqlQuery}
impl Distill for LogicalMssqlQuery {
    /// Pretty-print the node as `LogicalMssalQuery { columns: [...] }` for
    /// `EXPLAIN` output.
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("LogicalMssqlQuery", fields)
    }
}

impl ColPrunable for LogicalMssqlQuery {
    /// Column pruning wraps the node in a `LogicalProject` that keeps only
    /// the requested columns. The executor always returns the full schema
    /// discovered at bind time; pruning happens in the project above.
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().cloned()).into()
    }
}

impl ExprRewritable<Logical> for LogicalMssqlQuery {}

impl ExprVisitable for LogicalMssqlQuery {}

impl PredicatePushdown for LogicalMssqlQuery {
    /// `mssql_query` does not support predicate pushdown — the user
    /// query is opaque to the optimizer. Wrap with `LogicalFilter` so
    /// the predicate is applied to the result of the query.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalMssqlQuery {
    /// Lower to a [`BatchMssalQuery`] plan node that the batch executor
    /// (`MssalQueryExecutor`) will consume.
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        Ok(BatchMssqlQuery::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalMssqlQuery {
    /// Reject streaming conversion with a clear error — `mssql_query` is
    /// batch-only. Streaming CDC ingestion should use the
    /// `sqlserver-cdc` source instead.
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        bail!("mssql_query function is not supported in streaming mode")
    }

    /// `mssql_query` is batch-only; refuse the streaming rewrite as well.
    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail!("mssql_query function is not supported in streaming mode")
    }
}
