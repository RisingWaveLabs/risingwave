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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::acl::AclMode;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::INFORMATION_SCHEMA_SCHEMA_NAME;
use risingwave_common::license::Feature;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, MapType, StructType};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::AggType;
use risingwave_expr::window_function::WindowFuncKind;
use risingwave_sqlparser::ast::{
    self, Expr as AstExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgList, Ident,
    OrderByExpr, SecretRefAsType, Statement, Window,
};
use risingwave_sqlparser::parser::Parser;

use crate::binder::Binder;
use crate::binder::bind_context::Clause;
use crate::catalog::OwnedByUserCatalog;
use crate::catalog::function_catalog::FunctionCatalog;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{
    Expr, ExprImpl, ExprType, FunctionCall, FunctionCallWithLambda, InputRef, TableFunction,
    TableFunctionType, UserDefinedFunction, expr_impl_to_string_fn,
};
use crate::handler::privilege::ObjectCheckItem;

mod aggregate;
mod builtin_scalar;
mod window;

// Defines system functions that without args, ref: https://www.postgresql.org/docs/current/functions-info.html
const SYS_FUNCTION_WITHOUT_ARGS: &[&str] = &[
    "session_user",
    "user",
    "current_user",
    "current_role",
    "current_catalog",
    "current_schema",
    "current_timestamp",
];

const INLINE_QUERY_ARG_LEN: usize = 6;
const CDC_SOURCE_QUERY_ARG_LEN: usize = 2;

pub(super) fn is_sys_function_without_args(ident: &Ident) -> bool {
    SYS_FUNCTION_WITHOUT_ARGS
        .iter()
        .any(|e| ident.real_value().as_str() == *e && ident.quote_style().is_none())
}

macro_rules! reject_syntax {
    ($pred:expr, $msg:expr) => {
        if $pred {
            return Err(ErrorCode::InvalidInputSyntax($msg.to_string()).into());
        }
    };

    ($pred:expr, $fmt:expr, $($arg:tt)*) => {
        if $pred {
            return Err(ErrorCode::InvalidInputSyntax(
                format!($fmt, $($arg)*)
            ).into());
        }
    };
}

impl Binder {
    fn bind_postgres_or_mysql_query_args(
        &self,
        schema_name: Option<&str>,
        args: Vec<ExprImpl>,
        expected_connector_name: &str,
    ) -> Result<Vec<ExprImpl>> {
        match args.len() {
            INLINE_QUERY_ARG_LEN => {
                let mut cast_args = Vec::with_capacity(INLINE_QUERY_ARG_LEN);
                for arg in args {
                    cast_args.push(arg.cast_implicit(&DataType::Varchar)?);
                }
                Ok(cast_args)
            }
            CDC_SOURCE_QUERY_ARG_LEN => {
                let source_name = expr_impl_to_string_fn(&args[0])?;
                let source_catalog = self
                    .catalog
                    .get_source_by_name(
                        &self.db_name,
                        self.bind_schema_path(schema_name),
                        &source_name,
                    )?
                    .0;

                self.check_privilege(
                    ObjectCheckItem::new(
                        source_catalog.owner,
                        AclMode::Select,
                        source_catalog.name.clone(),
                        source_catalog.id,
                    ),
                    source_catalog.database_id,
                )?;

                if !source_catalog
                    .connector_name()
                    .eq_ignore_ascii_case(expected_connector_name)
                {
                    return Err(ErrorCode::BindError(format!(
                        "TVF function only accepts `mysql-cdc` and `postgres-cdc` source. Expected: {}, but got: {}",
                        expected_connector_name,
                        source_catalog.connector_name()
                    ))
                    .into());
                }

                let (props, secret_refs) = source_catalog.with_properties.clone().into_parts();
                let secret_resolved =
                    LocalSecretManager::global().fill_secrets(props, secret_refs)?;

                let mut args_vec = vec![
                    ExprImpl::literal_varchar(secret_resolved["hostname"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["port"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["username"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["password"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["database.name"].clone()),
                    args[1].clone().cast_implicit(&DataType::Varchar)?,
                ];

                if expected_connector_name.eq_ignore_ascii_case("postgres-cdc") {
                    args_vec.push(ExprImpl::literal_varchar(
                        secret_resolved.get("ssl.mode").cloned().unwrap_or_default(),
                    ));
                    args_vec.push(ExprImpl::literal_varchar(
                        secret_resolved
                            .get("ssl.root.cert")
                            .cloned()
                            .unwrap_or_default(),
                    ));
                }

                Ok(args_vec)
            }
            _ => Err(ErrorCode::BindError("postgres_query function and mysql_query function accept either 2 arguments: (cdc_source_name varchar, query varchar) or 6 arguments: (hostname varchar, port varchar, username varchar, password varchar, database_name varchar, query varchar)".to_owned()).into()),
        }
    }

    pub(in crate::binder) fn bind_function(
        &mut self,
        Function {
            scalar_as_agg,
            name,
            arg_list,
            within_group,
            filter,
            over,
        }: &Function,
    ) -> Result<ExprImpl> {
        let (schema_name, func_name) = match name.0.as_slice() {
            [name] => (None, name.real_value()),
            [schema, name] => {
                let schema_name = schema.real_value();
                let func_name = if schema_name == INFORMATION_SCHEMA_SCHEMA_NAME {
                    // definition of information_schema: https://github.com/postgres/postgres/blob/e0b2eed047df9045664da6f724cb42c10f8b12f0/src/backend/catalog/information_schema.sql
                    //
                    // FIXME: handle schema correctly, so that the functions are hidden if the schema is not in the search path.
                    let function_name = name.real_value();
                    if function_name != "_pg_expandarray" {
                        bail_not_implemented!(
                            issue = 12422,
                            "Unsupported function name under schema: {}",
                            schema_name
                        );
                    }
                    function_name
                } else {
                    name.real_value()
                };
                (Some(schema_name), func_name)
            }
            [database, schema, name] => {
                // Support database.schema.function qualified names when database matches current database
                let database_name = database.real_value();
                if database_name != self.db_name {
                    return Err(ErrorCode::BindError(format!(
                        "Cross-database function call is not supported: {}",
                        name
                    ))
                    .into());
                }
                let schema_name = schema.real_value();
                let func_name = name.real_value();
                (Some(schema_name), func_name)
            }
            _ => bail_not_implemented!(issue = 112, "qualified function {}", name),
        };

        // FIXME: This is a hack to support [Bytebase queries](https://github.com/TennyZhuang/bytebase/blob/4a26f7c62b80e86e58ad2f77063138dc2f420623/backend/plugin/db/pg/sync.go#L549).
        // Bytebase widely used the pattern like `obj_description(format('%s.%s',
        // quote_ident(idx.schemaname), quote_ident(idx.indexname))::regclass) AS comment` to
        // retrieve object comment, however we don't support casting a non-literal expression to
        // regclass. We just hack the `obj_description` and `col_description` here, to disable it to
        // bind its arguments.
        if func_name == "obj_description" || func_name == "col_description" {
            return Ok(ExprImpl::literal_varchar("".to_owned()));
        }

        // special binding logic for `array_transform` and `map_filter`
        if func_name == "array_transform" || func_name == "map_filter" {
            return self.validate_and_bind_special_function_params(
                &func_name,
                *scalar_as_agg,
                arg_list,
                within_group.as_deref(),
                filter.as_deref(),
                over.as_ref(),
            );
        }

        // Bind function arguments. Secret references are bound as ExprImpl::SecretRef first,
        // and then restricted to UDF calls below.
        let has_secret_ref_arg = arg_list.args.iter().any(|arg| {
            matches!(
                arg,
                FunctionArg::Unnamed(FunctionArgExpr::SecretRef(_))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::SecretRef(_),
                        ..
                    }
            )
        });

        // Return a clear usage error before secret lookup when there is no UDF candidate by name.
        // This avoids leaking "not found" for secret refs used in clearly invalid contexts.
        if has_secret_ref_arg {
            let has_udf_candidate = self
                .catalog
                .get_functions_by_name(
                    &self.db_name,
                    self.bind_schema_path(schema_name.as_deref()),
                    &func_name,
                )
                .map(|(funcs, _)| !funcs.is_empty())
                .unwrap_or(false);
            if !has_udf_candidate {
                return Err(ErrorCode::InvalidInputSyntax(
                    "secret reference is only allowed in user-defined function arguments"
                        .to_owned(),
                )
                .into());
            }
        }

        let bind_arg = if func_name.eq_ignore_ascii_case("jsonb_agg") {
            Self::bind_jsonb_agg_arg
        } else {
            Self::bind_function_arg
        };
        let mut args: Vec<ExprImpl> = arg_list
            .args
            .iter()
            .map(|arg| bind_arg(self, arg))
            .flatten_ok()
            .try_collect()?;

        let mut referred_udfs = HashSet::new();
        let mut is_udf_call = false;

        let wrapped_agg_type = if *scalar_as_agg {
            // Let's firstly try to apply the `AGGREGATE:` prefix.
            // We will reject functions that are not able to be wrapped as aggregate function.
            let mut array_args = args
                .iter()
                .enumerate()
                .map(|(i, expr)| InputRef::new(i, DataType::list(expr.return_type())).into())
                .collect_vec();
            let schema_path = self.bind_schema_path(schema_name.as_deref());
            let scalar_func_expr = if let Ok((func, _)) = self.catalog.get_function_by_name_inputs(
                &self.db_name,
                schema_path,
                &func_name,
                &mut array_args,
            ) {
                // record the dependency upon the UDF
                referred_udfs.insert(func.id);
                is_udf_call = true;
                self.check_privilege(
                    ObjectCheckItem::new(func.owner, AclMode::Execute, func.name.clone(), func.id),
                    self.database_id,
                )?;

                if !func.kind.is_scalar() {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "expect a scalar function after `AGGREGATE:`".to_owned(),
                    )
                    .into());
                }

                if func.language == "sql" {
                    self.bind_sql_udf(func.clone(), array_args)?
                } else {
                    UserDefinedFunction::new(func.clone(), array_args).into()
                }
            } else {
                self.bind_builtin_scalar_function(&func_name, array_args, arg_list.variadic)?
            };

            // `AggType::WrapScalar` requires the inner expression to be convertible to `ExprNode`
            // and directly executable. If there's any subquery in the expression, this will fail.
            let expr_node = match scalar_func_expr.try_to_expr_proto() {
                Ok(expr_node) => expr_node,
                Err(e) => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "function {func_name} cannot be used after `AGGREGATE:`: {e}",
                    ))
                    .into());
                }
            };

            // now this is either an aggregate/window function call
            Some(AggType::WrapScalar(expr_node))
        } else {
            None
        };

        let schema_path = self.bind_schema_path(schema_name.as_deref());
        let udf = if wrapped_agg_type.is_none()
            && let Ok((func, _)) = self.catalog.get_function_by_name_inputs(
                &self.db_name,
                schema_path,
                &func_name,
                &mut args,
            ) {
            // record the dependency upon the UDF
            referred_udfs.insert(func.id);
            is_udf_call = true;
            self.check_privilege(
                ObjectCheckItem::new(func.owner, AclMode::Execute, func.name.clone(), func.id),
                self.database_id,
            )?;
            Some(func.clone())
        } else {
            None
        };

        if has_secret_ref_arg && !is_udf_call {
            return Err(ErrorCode::InvalidInputSyntax(
                "secret reference is only allowed in user-defined function arguments".to_owned(),
            )
            .into());
        }

        self.included_udfs.extend(referred_udfs);

        let agg_type = if wrapped_agg_type.is_some() {
            wrapped_agg_type
        } else if let Some(ref udf) = udf
            && udf.kind.is_aggregate()
        {
            assert_ne!(udf.language, "sql", "SQL UDAF is not supported yet");
            Some(AggType::UserDefined(udf.as_ref().into()))
        } else {
            AggType::from_str(&func_name).ok()
        };

        // try to bind it as a window function call
        if let Some(over) = over {
            reject_syntax!(
                arg_list.distinct,
                "`DISTINCT` is not allowed in window function call"
            );
            reject_syntax!(
                arg_list.variadic,
                "`VARIADIC` is not allowed in window function call"
            );
            reject_syntax!(
                !arg_list.order_by.is_empty(),
                "`ORDER BY` is not allowed in window function call argument list"
            );
            reject_syntax!(
                within_group.is_some(),
                "`WITHIN GROUP` is not allowed in window function call"
            );

            let kind = if let Some(agg_type) = agg_type {
                // aggregate as window function
                WindowFuncKind::Aggregate(agg_type)
            } else if let Ok(kind) = WindowFuncKind::from_str(&func_name) {
                kind
            } else {
                bail_not_implemented!(issue = 8961, "Unrecognized window function: {}", func_name);
            };
            return self.bind_window_function(
                kind,
                args,
                arg_list.ignore_nulls,
                filter.as_deref(),
                over,
            );
        }

        // now it's an aggregate/scalar/table function call
        reject_syntax!(
            arg_list.ignore_nulls,
            "`IGNORE NULLS` is not allowed in aggregate/scalar/table function call"
        );

        // try to bind it as an aggregate function call
        if let Some(agg_type) = agg_type {
            reject_syntax!(
                arg_list.variadic,
                "`VARIADIC` is not allowed in aggregate function call"
            );
            return self.bind_aggregate_function(
                agg_type,
                arg_list.distinct,
                args,
                &arg_list.order_by,
                within_group.as_deref(),
                filter.as_deref(),
            );
        }

        // now it's a scalar/table function call
        reject_syntax!(
            arg_list.distinct,
            "`DISTINCT` is not allowed in scalar/table function call"
        );
        reject_syntax!(
            !arg_list.order_by.is_empty(),
            "`ORDER BY` is not allowed in scalar/table function call"
        );
        reject_syntax!(
            within_group.is_some(),
            "`WITHIN GROUP` is not allowed in scalar/table function call"
        );
        reject_syntax!(
            filter.is_some(),
            "`FILTER` is not allowed in scalar/table function call"
        );

        // try to bind it as a table function call
        {
            // `file_scan` table function
            if func_name.eq_ignore_ascii_case("file_scan") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_file_scan(args)?.into());
            }
            // `postgres_query` table function
            if func_name.eq("postgres_query") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                let args = self
                    .bind_postgres_or_mysql_query_args(schema_name.as_deref(), args, "postgres-cdc")
                    .context("postgres_query error")?;
                return Ok(TableFunction::new_postgres_query(args)
                    .context("postgres_query error")?
                    .into());
            }
            // `mysql_query` table function
            if func_name.eq("mysql_query") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                let args = self
                    .bind_postgres_or_mysql_query_args(schema_name.as_deref(), args, "mysql-cdc")
                    .context("mysql_query error")?;
                return Ok(TableFunction::new_mysql_query(args)
                    .context("mysql_query error")?
                    .into());
            }
            // `mssql_query` table function (enterprise feature)
            if func_name.eq("mssql_query") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;

                // Enterprise feature gating
                Feature::MssqlQuery.check_available()?;

                return Ok(TableFunction::new_mssql_query(
                    &self.catalog,
                    &self.db_name,
                    self.bind_schema_path(schema_name.as_deref()),
                    args,
                )
                .context("mssql_query error")?
                .into());
            }
            // `internal_backfill_progress` table function
            if func_name.eq("internal_backfill_progress") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_internal_backfill_progress().into());
            }
            // `internal_source_backfill_progress` table function
            if func_name.eq("internal_source_backfill_progress") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_internal_source_backfill_progress().into());
            }
            // `internal_get_channel_delta_stats` table function
            if func_name.eq("internal_get_channel_delta_stats") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;

                return Ok(TableFunction::new_internal_get_channel_delta_stats(args).into());
            }
            // UDTF
            if let Some(ref udf) = udf
                && udf.kind.is_table()
            {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                if udf.language == "sql" {
                    return self.bind_sql_udf(udf.clone(), args);
                }
                return Ok(TableFunction::new_user_defined(udf.clone(), args).into());
            }
            // builtin table function
            if let Ok(function_type) = TableFunctionType::from_str(&func_name) {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new(function_type, args)?.into());
            }
        }

        // try to bind it as a scalar function call
        if let Some(ref udf) = udf {
            assert!(udf.kind.is_scalar());
            reject_syntax!(
                arg_list.variadic,
                "`VARIADIC` is not allowed in user-defined function call"
            );
            if udf.language == "sql" {
                return self.bind_sql_udf(udf.clone(), args);
            }
            return Ok(UserDefinedFunction::new(udf.clone(), args).into());
        }

        self.bind_builtin_scalar_function(&func_name, args, arg_list.variadic)
    }

    fn validate_and_bind_special_function_params(
        &mut self,
        func_name: &str,
        scalar_as_agg: bool,
        arg_list: &FunctionArgList,
        within_group: Option<&OrderByExpr>,
        filter: Option<&risingwave_sqlparser::ast::Expr>,
        over: Option<&Window>,
    ) -> Result<ExprImpl> {
        assert!(["array_transform", "map_filter"].contains(&func_name));

        reject_syntax!(
            scalar_as_agg,
            "`AGGREGATE:` prefix is not allowed for `{}`",
            func_name
        );
        reject_syntax!(
            !arg_list.is_args_only(),
            "keywords like `DISTINCT`, `ORDER BY` are not allowed in `{}` argument list",
            func_name
        );
        reject_syntax!(
            within_group.is_some(),
            "`WITHIN GROUP` is not allowed in `{}` call",
            func_name
        );
        reject_syntax!(
            filter.is_some(),
            "`FILTER` is not allowed in `{}` call",
            func_name
        );
        reject_syntax!(
            over.is_some(),
            "`OVER` is not allowed in `{}` call",
            func_name
        );
        if func_name == "array_transform" {
            self.bind_array_transform(&arg_list.args)
        } else {
            self.bind_map_filter(&arg_list.args)
        }
    }

    fn bind_array_transform(&mut self, args: &[FunctionArg]) -> Result<ExprImpl> {
        let [array, lambda] = args else {
            return Err(ErrorCode::BindError(format!(
                "`array_transform` expect two inputs `array` and `lambda`, but {} were given",
                args.len()
            ))
            .into());
        };

        let bound_array = self.bind_function_arg(array)?;
        let [bound_array] = <[ExprImpl; 1]>::try_from(bound_array).map_err(|bound_array| -> RwError {
            ErrorCode::BindError(format!("The `array` argument for `array_transform` should be bound to one argument, but {} were got", bound_array.len()))
                .into()
        })?;

        let inner_ty = match bound_array.return_type() {
            DataType::List(ty) => ty.into_elem(),
            real_type => return Err(ErrorCode::BindError(format!(
                "The `array` argument for `array_transform` should be an array, but {} were got",
                real_type
            ))
            .into()),
        };

        let ast::FunctionArgExpr::Expr(ast::Expr::LambdaFunction {
            args: lambda_args,
            body: lambda_body,
        }) = lambda.get_expr()
        else {
            return Err(ErrorCode::BindError(
                "The `lambda` argument for `array_transform` should be a lambda function"
                    .to_owned(),
            )
            .into());
        };

        let [lambda_arg] = <[Ident; 1]>::try_from(lambda_args).map_err(|args| -> RwError {
            ErrorCode::BindError(format!(
                "The `lambda` argument for `array_transform` should be a lambda function with one argument, but {} were given",
                args.len()
            ))
            .into()
        })?;

        let bound_lambda = self.bind_unary_lambda_function(inner_ty, lambda_arg, *lambda_body)?;

        let lambda_ret_type = bound_lambda.return_type();
        let transform_ret_type = DataType::list(lambda_ret_type);

        Ok(ExprImpl::FunctionCallWithLambda(Box::new(
            FunctionCallWithLambda::new_unchecked(
                ExprType::ArrayTransform,
                vec![bound_array],
                bound_lambda,
                transform_ret_type,
            ),
        )))
    }

    fn bind_unary_lambda_function(
        &mut self,
        input_ty: DataType,
        arg: Ident,
        body: ast::Expr,
    ) -> Result<ExprImpl> {
        let lambda_args = HashMap::from([(arg.real_value(), (0usize, input_ty))]);
        let orig_lambda_args = self.context.lambda_args.replace(lambda_args);
        let body = self.bind_expr_inner(&body)?;
        self.context.lambda_args = orig_lambda_args;

        Ok(body)
    }

    fn bind_map_filter(&mut self, args: &[FunctionArg]) -> Result<ExprImpl> {
        let [input, lambda] = args else {
            return Err(ErrorCode::BindError(format!(
                "`map_filter` requires two arguments (input_map and lambda), got {}",
                args.len()
            ))
            .into());
        };

        let bound_input = self.bind_function_arg(input)?;
        let [bound_input] = <[ExprImpl; 1]>::try_from(bound_input).map_err(|e| {
            ErrorCode::BindError(format!(
                "Input argument should resolve to single expression, got {}",
                e.len()
            ))
        })?;

        let (key_type, value_type) = match bound_input.return_type() {
            DataType::Map(map_type) => (map_type.key().clone(), map_type.value().clone()),
            t => {
                return Err(
                    ErrorCode::BindError(format!("Input must be Map type, got {}", t)).into(),
                );
            }
        };

        let ast::FunctionArgExpr::Expr(ast::Expr::LambdaFunction {
            args: lambda_args,
            body: lambda_body,
        }) = lambda.get_expr()
        else {
            return Err(ErrorCode::BindError(
                "Second argument must be a lambda function".to_owned(),
            )
            .into());
        };

        let [key_arg, value_arg] = <[Ident; 2]>::try_from(lambda_args).map_err(|args| {
            ErrorCode::BindError(format!(
                "Lambda must have exactly two parameters (key, value), got {}",
                args.len()
            ))
        })?;

        let bound_lambda = self.bind_binary_lambda_function(
            key_arg,
            key_type.clone(),
            value_arg,
            value_type.clone(),
            *lambda_body,
        )?;

        let lambda_ret_type = bound_lambda.return_type();
        if lambda_ret_type != DataType::Boolean {
            return Err(ErrorCode::BindError(format!(
                "Lambda must return Boolean type, got {}",
                lambda_ret_type
            ))
            .into());
        }

        let map_type = MapType::from_kv(key_type, value_type);
        let return_type = DataType::Map(map_type);

        Ok(ExprImpl::FunctionCallWithLambda(Box::new(
            FunctionCallWithLambda::new_unchecked(
                ExprType::MapFilter,
                vec![bound_input],
                bound_lambda,
                return_type,
            ),
        )))
    }

    fn bind_binary_lambda_function(
        &mut self,
        first_arg: Ident,
        first_ty: DataType,
        second_arg: Ident,
        second_ty: DataType,
        body: ast::Expr,
    ) -> Result<ExprImpl> {
        let lambda_args = HashMap::from([
            (first_arg.real_value(), (0usize, first_ty)),
            (second_arg.real_value(), (1usize, second_ty)),
        ]);

        let orig_ctx = self.context.lambda_args.replace(lambda_args);
        let bound_body = self.bind_expr_inner(&body)?;
        self.context.lambda_args = orig_ctx;

        Ok(bound_body)
    }

    fn ensure_table_function_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::JoinOn
                | Clause::Where
                | Clause::Having
                | Clause::Filter
                | Clause::Values
                | Clause::Insert
                | Clause::GeneratedColumn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "table functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
                Clause::GroupBy | Clause::From => {}
            }
        }
        Ok(())
    }

    /// A common utility function to extract sql udf expression out from the input `ast`.
    pub(crate) fn extract_udf_expr(ast: Vec<Statement>) -> Result<AstExpr> {
        if ast.len() != 1 {
            return Err(ErrorCode::InvalidInputSyntax(
                "the query for sql udf should contain only one statement".to_owned(),
            )
            .into());
        }

        // Extract the expression out
        let Statement::Query(query) = ast.into_iter().next().unwrap() else {
            return Err(ErrorCode::InvalidInputSyntax(
                "invalid function definition, please recheck the syntax".to_owned(),
            )
            .into());
        };

        if let Some(expr) = query.as_single_select_item() {
            // Inline SQL UDF.
            Ok(expr.clone())
        } else {
            // Subquery SQL UDF.
            Ok(AstExpr::Subquery(query))
        }
    }

    pub fn bind_sql_udf_inner(
        &mut self,
        body: &str,
        arg_names: &[String],
        args: Vec<ExprImpl>,
    ) -> Result<ExprImpl> {
        // This represents the current user defined function is `language sql`
        let ast = Parser::parse_sql(body)?;

        // Stash the current arguments.
        // For subquery SQL UDF, as we always push a new context, there should be no arguments to stash.
        // For inline SQL UDF, we need to stash the arguments in case of nesting.
        let stashed_arguments = self.context.sql_udf_arguments.take();

        // The actual inline logic for sql udf.
        let mut arguments = HashMap::new();
        for (i, arg) in args.into_iter().enumerate() {
            if arg_names[i].is_empty() {
                // unnamed argument, use `$1`, `$2` as the name
                arguments.insert(format!("${}", i + 1), arg);
            } else {
                // named argument
                arguments.insert(arg_names[i].clone(), arg);
            }
        }
        self.context.sql_udf_arguments = Some(arguments);

        let Ok(expr) = Self::extract_udf_expr(ast) else {
            return Err(ErrorCode::InvalidInputSyntax(
                "failed to parse the input query and extract the udf expression, \
                please recheck the syntax"
                    .to_owned(),
            )
            .into());
        };

        let bind_result = self.bind_expr(&expr);
        // Restore arguments information for subsequent binding.
        self.context.sql_udf_arguments = stashed_arguments;

        bind_result
    }

    fn bind_sql_udf(
        &mut self,
        func: Arc<FunctionCatalog>,
        args: Vec<ExprImpl>,
    ) -> Result<ExprImpl> {
        let Some(body) = &func.body else {
            return Err(
                ErrorCode::InvalidInputSyntax("`body` must exist for sql udf".to_owned()).into(),
            );
        };

        self.bind_sql_udf_inner(body, &func.arg_names, args)
    }

    pub(in crate::binder) fn bind_function_expr_arg(
        &mut self,
        arg_expr: &FunctionArgExpr,
    ) -> Result<Vec<ExprImpl>> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Ok(vec![self.bind_expr_inner(expr)?]),
            FunctionArgExpr::QualifiedWildcard(_, _)
            | FunctionArgExpr::ExprQualifiedWildcard(_, _) => Err(ErrorCode::InvalidInputSyntax(
                format!("unexpected wildcard {}", arg_expr),
            )
            .into()),
            FunctionArgExpr::Wildcard(None) => Ok(vec![]),
            FunctionArgExpr::Wildcard(Some(_)) => unreachable!(),
            FunctionArgExpr::SecretRef(secret_ref_value) => {
                let (schema_name, secret_name) = Binder::resolve_schema_qualified_name(
                    &self.db_name,
                    &secret_ref_value.secret_name,
                )?;
                let schema_path = self.bind_schema_path(schema_name.as_deref());
                let (secret_catalog, _) =
                    self.catalog
                        .get_secret_by_name(&self.db_name, schema_path, &secret_name)?;

                self.check_privilege(
                    ObjectCheckItem::new(
                        secret_catalog.owner(),
                        AclMode::Usage,
                        secret_catalog.name.clone(),
                        secret_catalog.id,
                    ),
                    self.database_id,
                )?;

                self.included_secrets.insert(secret_catalog.id);

                let ref_as = match secret_ref_value.ref_as {
                    SecretRefAsType::Text => risingwave_pb::secret::secret_ref::RefAsType::Text,
                    SecretRefAsType::File => risingwave_pb::secret::secret_ref::RefAsType::File,
                };

                Ok(vec![
                    crate::expr::SecretRef {
                        secret_id: secret_catalog.id,
                        ref_as,
                        secret_name: secret_catalog.name.clone(),
                    }
                    .into(),
                ])
            }
        }
    }

    pub(in crate::binder) fn bind_function_arg(
        &mut self,
        arg: &FunctionArg,
    ) -> Result<Vec<ExprImpl>> {
        match arg {
            FunctionArg::Unnamed(expr) => self.bind_function_expr_arg(expr),
            FunctionArg::Named { .. } => Err(ErrorCode::InvalidInputSyntax(
                "named function arguments are not supported yet".to_owned(),
            )
            .into()),
        }
    }

    fn bind_jsonb_agg_arg(&mut self, arg: &FunctionArg) -> Result<Vec<ExprImpl>> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(prefix, except)) => {
                let (schema_name, table_name) =
                    Binder::resolve_schema_qualified_name(&self.db_name, prefix)?;
                let except_indices = self.generate_except_indices(except.as_deref())?;
                let (begin, end) = self
                    .context
                    .resolve_relation_range(&table_name, &schema_name)?;
                let (exprs, names) = Self::iter_bound_columns(
                    self.context.columns[begin..end]
                        .iter()
                        .filter(|c| !c.is_hidden && !except_indices.contains(&c.index)),
                );
                self.wrap_wildcard_exprs_as_named_row(exprs, names)
            }
            FunctionArg::Unnamed(FunctionArgExpr::ExprQualifiedWildcard(expr, prefix)) => {
                let (exprs, names) = self.bind_wildcard_field_column(expr, prefix)?;
                self.wrap_wildcard_exprs_as_named_row(exprs, names)
            }
            _ => self.bind_function_arg(arg),
        }
    }

    fn wrap_wildcard_exprs_as_named_row(
        &self,
        exprs: Vec<ExprImpl>,
        names: Vec<Option<String>>,
    ) -> Result<Vec<ExprImpl>> {
        let return_type =
            DataType::Struct(StructType::new(
                names.into_iter().zip_eq_fast(exprs.iter()).enumerate().map(
                    |(idx, (name, expr))| {
                        (
                            name.unwrap_or_else(|| format!("f{}", idx + 1)),
                            expr.return_type(),
                        )
                    },
                ),
            ));
        Ok(vec![
            FunctionCall::new_unchecked(ExprType::Row, exprs, return_type).into(),
        ])
    }
}
