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

use itertools::Itertools;
use risingwave_common::types::DataType;

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalProject, LogicalProjectSet, LogicalTableFunction, LogicalValues, PlanTreeNodeUnary,
};

/// Transform a `TableFunction` (used in FROM clause) into a `ProjectSet` so that it can be unnested later if it contains `CorrelatedInputRef`.
///
/// Before:
///
/// ```text
///             LogicalTableFunction
/// ```
///
/// After:
///
///
/// ```text
///             LogicalProject (type alignment)
///                   |
///            LogicalProjectSet
///                   |
///             LogicalValues
/// ```
pub struct TableFunctionToProjectSetRule {}
impl Rule<Logical> for TableFunctionToProjectSetRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        let table_function =
            ExprImpl::TableFunction(logical_table_function.table_function().clone().into());
        let table_function_return_type = table_function.return_type();
        let logical_values = LogicalValues::create_empty_scalar(logical_table_function.base.ctx());
        let logical_project_set = LogicalProjectSet::create(logical_values, vec![table_function]);
        // We need a project to align schema type because
        // 1. `LogicalProjectSet` has a hidden column `projected_row_id` (0-th col)
        // 2. When the function returns a struct type, TableFunction will return flatten it into multiple columns, while ProjectSet still returns a single column.
        let table_function_col_idx = 1;
        let logical_project = if let DataType::Struct(st) = table_function_return_type.clone() {
            let exprs = st
                .types()
                .enumerate()
                .map(|(i, data_type)| {
                    let field_access = FunctionCall::new_unchecked(
                        ExprType::Field,
                        vec![
                            InputRef::new(
                                table_function_col_idx,
                                table_function_return_type.clone(),
                            )
                            .into(),
                            ExprImpl::literal_int(i as i32),
                        ],
                        data_type.clone(),
                    );
                    ExprImpl::FunctionCall(field_access.into())
                })
                .collect_vec();
            LogicalProject::new(logical_project_set, exprs)
        } else {
            LogicalProject::with_out_col_idx(
                logical_project_set,
                std::iter::once(table_function_col_idx),
            )
        };

        if logical_table_function.with_ordinality() {
            let projected_row_id = InputRef::new(0, DataType::Int64).into();
            let ordinality = FunctionCall::new(
                ExprType::Add,
                vec![projected_row_id, ExprImpl::literal_bigint(1)],
            )
            .unwrap() // i64 + i64 is ok
            .into();
            let mut exprs = logical_project.exprs().clone();
            exprs.push(ordinality);
            let logical_project = LogicalProject::new(logical_project.input(), exprs);
            Some(logical_project.into())
        } else {
            Some(logical_project.into())
        }
    }
}

impl TableFunctionToProjectSetRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToProjectSetRule {})
    }
}
