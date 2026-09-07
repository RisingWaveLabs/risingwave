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

use anyhow::Context;
use either::Either;
use risingwave_common::catalog::FunctionId;
use risingwave_expr::sig::{CreateOptions, UdfKind};
use risingwave_pb::catalog::Function;
use risingwave_pb::catalog::function::{AggregateFunction, Kind};
use risingwave_sqlparser::ast::DataType as AstDataType;

use super::*;
use crate::handler::create_function::reject_variant_in_udf_signature;
use crate::{Binder, bind_data_type};

pub async fn handle_create_aggregate(
    handler_args: HandlerArgs,
    or_replace: bool,
    if_not_exists: bool,
    name: ObjectName,
    args: Vec<OperateFunctionArg>,
    returns: AstDataType,
    params: CreateFunctionBody,
) -> Result<RwPgResponse> {
    if or_replace {
        bail_not_implemented!("CREATE OR REPLACE AGGREGATE");
    }

    let udf_config = handler_args.session.env().udf_config();

    // e.g., `language [ python / java / ...etc]`
    let language = match params.language {
        Some(lang) => {
            let lang = lang.real_value().to_lowercase();
            match &*lang {
                "python" if udf_config.enable_embedded_python_udf => lang,
                "javascript" if udf_config.enable_embedded_javascript_udf => lang,
                "python" | "javascript" => {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "{} UDF is not enabled in configuration",
                        lang
                    ))
                    .into());
                }
                _ => {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "language {} is not supported",
                        lang
                    ))
                    .into());
                }
            }
        }
        None => return Err(ErrorCode::InvalidParameterValue("no language".into()).into()),
    };

    let runtime = match params.runtime {
        Some(_) => {
            return Err(ErrorCode::InvalidParameterValue(
                "runtime selection is currently not supported".to_owned(),
            )
            .into());
        }
        None => None,
    };

    let return_type = bind_data_type(&returns)?;

    let mut arg_names = vec![];
    let mut arg_types = vec![];
    for arg in args {
        arg_names.push(arg.name.map_or("".to_owned(), |n| n.real_value()));
        arg_types.push(bind_data_type(&arg.data_type)?);
    }

    reject_variant_in_udf_signature(&return_type, &arg_types, "aggregate function")?;

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = &session.database();
    let (schema_name, function_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    // check if the function exists in the catalog
    if let Either::Right(resp) = session.check_function_name_duplicated(
        StatementType::CREATE_FUNCTION,
        name,
        &arg_types,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let link = match &params.using {
        Some(CreateFunctionUsing::Link(l)) => Some(l.as_str()),
        _ => None,
    };
    let base64_decoded = match &params.using {
        Some(CreateFunctionUsing::Base64(encoded)) => {
            use base64::prelude::{BASE64_STANDARD, Engine};
            let bytes = BASE64_STANDARD
                .decode(encoded)
                .context("invalid base64 encoding")?;
            Some(bytes)
        }
        _ => None,
    };

    let create_fn = risingwave_expr::sig::find_udf_impl(&language, None, link)?.create_fn;
    let output = create_fn(CreateOptions {
        kind: UdfKind::Aggregate,
        name: &function_name,
        arg_names: &arg_names,
        arg_types: &arg_types,
        return_type: &return_type,
        as_: params.as_.as_ref().map(|s| s.as_str()),
        using_link: link,
        using_base64_decoded: base64_decoded.as_deref(),
    })?;

    let function = Function {
        id: FunctionId::placeholder(),
        schema_id,
        database_id,
        name: function_name,
        kind: Some(Kind::Aggregate(AggregateFunction {})),
        arg_names,
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        runtime,
        name_in_runtime: Some(output.name_in_runtime),
        link: link.map(|s| s.to_owned()),
        body: output.body,
        compressed_binary: output.compressed_binary,
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

    Ok(PgResponse::empty_result(StatementType::CREATE_AGGREGATE))
}
