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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalMssqlQuery, LogicalTableFunction};

/// Optimizer rule that rewrites a `LogicalTableFunction` of type
/// `MSSQL_QUERY` into a `LogicalMssalQuery` plan node. Registered in the
/// `TABLE_FUNCTION_TO_MSSQL_QUERY` stage of the logical optimization
/// pipeline (after the equivalent `postgres_query` and `mysql_query`
/// rules). Validates that exactly 8 arguments were supplied; the 2-arg
/// source-reference form is handled earlier by the binder, not here.
pub struct TableFunctionToMssqlQueryRule {}
impl Rule<Logical> for TableFunctionToMssqlQueryRule {
    /// Apply the rewrite: if the plan is a `LogicalTableFunction` of
    /// type `MSSQL_QUERY`, build a `LogicalMssalQuery` from its 8 inline
    /// arguments. Returns `None` for any other plan.
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function().function_type != TableFunctionType::MssqlQuery {
            return None;
        }
        assert!(!logical_table_function.with_ordinality());
        let table_function_return_type = logical_table_function.table_function().return_type();

        if let DataType::Struct(st) = table_function_return_type {
            let fields = st
                .iter()
                .map(|(name, data_type)| Field::with_name(data_type.clone(), name.to_owned()))
                .collect_vec();

            let schema = Schema::new(fields);

            // mssql_query has 8 args: hostname, port, username, password, database, query, encrypt, trust_cert
            assert_eq!(logical_table_function.table_function().args.len(), 8);
            let mut eval_args = vec![];
            for arg in &logical_table_function.table_function().args {
                assert_eq!(arg.return_type(), DataType::Varchar);
                let value = arg.try_fold_const().unwrap().unwrap();
                match value {
                    Some(ScalarImpl::Utf8(s)) => {
                        eval_args.push(s.to_string());
                    }
                    _ => {
                        unreachable!("must be a varchar")
                    }
                }
            }
            let hostname = eval_args[0].clone();
            let port = eval_args[1].clone();
            let username = eval_args[2].clone();
            let password = eval_args[3].clone();
            let database = eval_args[4].clone();
            let query = eval_args[5].clone();
            let encrypt = Some(eval_args[6].clone());
            let trust_cert = Some(eval_args[7].clone());

            Some(
                LogicalMssqlQuery::new(
                    logical_table_function.ctx(),
                    schema,
                    hostname,
                    port,
                    username,
                    password,
                    database,
                    query,
                    encrypt,
                    trust_cert,
                )
                .into(),
            )
        } else {
            unreachable!("TableFunction return type should be struct")
        }
    }
}

impl TableFunctionToMssqlQueryRule {
    /// Construct a boxed instance for registration in the
    /// `TABLE_FUNCTION_TO_MSSQL_QUERY` stage.
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToMssqlQueryRule {})
    }
}
