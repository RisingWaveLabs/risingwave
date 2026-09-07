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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalMySqlQuery, LogicalTableFunction};

/// Transform a special `TableFunction` (with `MYSQL_QUERY` table function type) into a `LogicalMySqlQuery`
pub struct TableFunctionToMySqlQueryRule {}
impl Rule<Logical> for TableFunctionToMySqlQueryRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function().function_type != TableFunctionType::MysqlQuery {
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

            assert_eq!(logical_table_function.table_function().args.len(), 6);
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

            Some(
                LogicalMySqlQuery::new(
                    logical_table_function.ctx(),
                    schema,
                    hostname,
                    port,
                    username,
                    password,
                    database,
                    query,
                )
                .into(),
            )
        } else {
            unreachable!("TableFunction return type should be struct")
        }
    }
}

impl TableFunctionToMySqlQueryRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToMySqlQueryRule {})
    }
}
