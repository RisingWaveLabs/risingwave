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

use either::Either;
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::StructType;
use risingwave_pb::catalog::PbFunction;
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};

use super::*;
use crate::expr::{Expr, Literal};
use crate::{Binder, bind_data_type};

pub async fn handle_create_sql_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    if_not_exists: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    returns: Option<CreateFunctionReturns>,
    params: CreateFunctionBody,
) -> Result<RwPgResponse> {
    if or_replace {
        bail_not_implemented!("CREATE OR REPLACE FUNCTION");
    }

    if temporary {
        bail_not_implemented!("CREATE TEMPORARY FUNCTION");
    }

    let language = "sql".to_owned();

    // Just a basic sanity check for `language`
    if !matches!(params.language, Some(lang) if lang.real_value().to_lowercase() == "sql") {
        return Err(ErrorCode::InvalidParameterValue(
            "`language` for sql udf must be `sql`".to_owned(),
        )
        .into());
    }

    // SQL udf function supports both single quote (i.e., as 'select $1 + $2')
    // and double dollar (i.e., as $$select $1 + $2$$) for as clause
    let body = match &params.as_ {
        Some(FunctionDefinition::SingleQuotedDef(s)) => s.clone(),
        Some(FunctionDefinition::DoubleDollarDef(s)) => s.clone(),
        Some(FunctionDefinition::Identifier(_)) => {
            return Err(ErrorCode::InvalidParameterValue("expect quoted string".to_owned()).into());
        }
        None => {
            if params.return_.is_none() {
                return Err(ErrorCode::InvalidParameterValue(
                    "AS or RETURN must be specified".to_owned(),
                )
                .into());
            }
            // Otherwise this is a return expression
            // Note: this is a current work around, and we are assuming return sql udf
            // will NOT involve complex syntax, so just reuse the logic for select definition
            format!("select {}", params.return_.unwrap())
        }
    };

    // Sanity check for link, this must be none with sql udf function
    if let Some(CreateFunctionUsing::Link(_)) = params.using {
        return Err(ErrorCode::InvalidParameterValue(
            "USING must NOT be specified with sql udf function".to_owned(),
        )
        .into());
    };

    // Get return type for the current sql udf function
    let return_type;
    let kind = match returns {
        Some(CreateFunctionReturns::Value(data_type)) => {
            return_type = bind_data_type(&data_type)?;
            Kind::Scalar(ScalarFunction {})
        }
        Some(CreateFunctionReturns::Table(columns)) => {
            if columns.len() == 1 {
                // return type is the original type for single column
                return_type = bind_data_type(&columns[0].data_type)?;
            } else {
                // return type is a struct for multiple columns
                let fields = columns
                    .iter()
                    .map(|c| Ok((c.name.real_value(), bind_data_type(&c.data_type)?)))
                    .collect::<Result<Vec<_>>>()?;
                return_type = StructType::new(fields).into();
            }
            Kind::Table(TableFunction {})
        }
        None => {
            return Err(ErrorCode::InvalidParameterValue(
                "return type must be specified".to_owned(),
            )
            .into());
        }
    };

    let mut arg_names = vec![];
    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
        arg_names.push(arg.name.map_or("".to_owned(), |n| n.real_value()));
        arg_types.push(bind_data_type(&arg.data_type)?);
    }

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = &session.database();
    let (schema_name, function_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    // check if function exists
    if let Either::Right(resp) = session.check_function_name_duplicated(
        StatementType::CREATE_FUNCTION,
        name,
        &arg_types,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    // Try bind the function call with mock arguments.
    // Note that the parsing here is just basic syntax / semantic check, the result will NOT be stored
    // e.g., The provided function body contains invalid syntax, return type mismatch, ..., etc.
    {
        let mut binder = Binder::new_for_system(session);
        let args = arg_types
            .iter()
            .map(|ty| Literal::new(None, ty.clone()).into() /* NULL */)
            .collect();

        let expr = binder.bind_sql_udf_inner(&body, &arg_names, args)?;

        // Check if the return type mismatches
        if expr.return_type() != return_type {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "return type mismatch detected\nexpected: [{}]\nactual: [{}]\nplease adjust your function definition accordingly",
                return_type,
                expr.return_type()
            ))
            .into());
        }
    }

    // Create the actual function, will be stored in function catalog
    let function = PbFunction {
        id: FunctionId::placeholder(),
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_names,
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        runtime: None,
        name_in_runtime: None, // None for SQL UDF
        body: Some(body),
        compressed_binary: None,
        link: None,
        owner: session.user_id(),
        always_retry_on_network_error: false,
        unsafe_skip_materializing_exprs: false,
        is_async: None,
        is_batched: None,
        created_at_epoch: None,
        created_at_cluster_version: None,
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}
