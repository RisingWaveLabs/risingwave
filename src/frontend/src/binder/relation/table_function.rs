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

use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{Field, RW_INTERNAL_TABLE_FUNCTION_NAME, Schema};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Function, FunctionArg, FunctionArgList, ObjectName, TableAlias};

use super::watermark::is_watermark_func;
use super::{Binder, Relation, Result, WindowTableFunctionKind};
use crate::binder::bind_context::Clause;
use crate::error::ErrorCode;
use crate::expr::{Expr, ExprImpl};

impl Binder {
    /// Binds a table function AST, which is a function call in a relation position.
    ///
    /// Besides [`crate::expr::TableFunction`] expr, it can also be other things like window table
    /// functions, or scalar functions.
    ///
    /// `with_ordinality` is only supported for the `TableFunction` case now.
    pub(super) fn bind_table_function(
        &mut self,
        name: &ObjectName,
        alias: Option<&TableAlias>,
        args: &[FunctionArg],
        with_ordinality: bool,
    ) -> Result<Relation> {
        let func_name = &name.0[0].real_value();
        // internal/system table functions
        {
            if func_name.eq_ignore_ascii_case(RW_INTERNAL_TABLE_FUNCTION_NAME) {
                if with_ordinality {
                    bail_not_implemented!(
                        "WITH ORDINALITY for internal/system table function {}",
                        func_name
                    );
                }
                return self.bind_internal_table(args, alias);
            }
        }
        // window table functions (tumble/hop)
        if let Ok(kind) = WindowTableFunctionKind::from_str(func_name) {
            if with_ordinality {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "WITH ORDINALITY for window table function {}",
                    func_name
                ))
                .into());
            }
            return Ok(Relation::WindowTableFunction(Box::new(
                self.bind_window_table_function(alias, kind, args)?,
            )));
        }
        // gap_fill
        if func_name.eq_ignore_ascii_case("gap_fill") {
            if with_ordinality {
                return Err(ErrorCode::InvalidInputSyntax(
                    "WITH ORDINALITY for gap_fill".to_owned(),
                )
                .into());
            }
            return Ok(Relation::GapFill(Box::new(
                self.bind_gap_fill(alias, args)?,
            )));
        }
        // watermark
        if is_watermark_func(func_name) {
            if with_ordinality {
                return Err(ErrorCode::InvalidInputSyntax(
                    "WITH ORDINALITY for watermark".to_owned(),
                )
                .into());
            }
            return Ok(Relation::Watermark(Box::new(
                self.bind_watermark(alias, args)?,
            )));
        };

        self.push_context();
        let mut clause = Some(Clause::From);
        std::mem::swap(&mut self.context.clause, &mut clause);
        let func = self.bind_function(&Function {
            scalar_as_agg: false,
            name: name.clone(),
            arg_list: FunctionArgList::args_only(args.to_vec()),
            over: None,
            filter: None,
            within_group: None,
        });
        self.context.clause = clause;
        self.pop_context()?;
        let func = func?;

        if let ExprImpl::TableFunction(func) = &func
            && func.args.iter().any(|arg| arg.has_subquery())
        {
            // Same error reports as DuckDB.
            return Err(ErrorCode::InvalidInputSyntax(
                    format!("Only table-in-out functions can have subquery parameters. The table function has subquery parameters is {}", func.name()),
                )
                    .into());
        }

        // bool indicates if the field is hidden
        let mut columns = if let DataType::Struct(s) = func.return_type() {
            // If the table function returns a struct, it will be flattened into multiple columns.
            let schema = Schema::from(&s);
            schema.fields.into_iter().map(|f| (false, f)).collect_vec()
        } else {
            // If there is an table alias (and it doesn't return a struct),
            // we should use the alias as the table function's
            // column name. If column aliases are also provided, they
            // are handled in bind_table_to_context.
            //
            // Note: named return value should take precedence over table alias.
            // But we don't support it yet.
            // e.g.,
            // ```sql
            // > create function foo(ret out int) language sql as 'select 1';
            // > select t.ret from foo() as t;
            // ```
            let col_name = if let Some(alias) = &alias {
                alias.name.real_value()
            } else {
                func_name.clone()
            };
            vec![(false, Field::with_name(func.return_type(), col_name))]
        };
        if with_ordinality {
            columns.push((false, Field::with_name(DataType::Int64, "ordinality")));
        }

        self.bind_table_to_context(columns, func_name.clone(), None, alias)?;

        Ok(Relation::TableFunction {
            expr: func,
            with_ordinality,
        })
    }
}
