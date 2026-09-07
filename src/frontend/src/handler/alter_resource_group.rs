// Copyright 2025 RisingWave Labs
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

use pgwire::pg_response::StatementType;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue, SetVariableValueSingle, Value};

use super::alter_utils::resolve_streaming_job_id_for_alter;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};

pub async fn handle_alter_resource_group(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    resource_group: Option<SetVariableValue>,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    risingwave_common::license::Feature::ResourceGroup.check_available()?;

    let job_id =
        resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "resource group")?;

    let resource_group = resource_group
        .map(resolve_resource_group)
        .transpose()?
        .flatten();

    let mut builder = RwPgResponse::builder(stmt_type);

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_resource_group(job_id, resource_group, deferred)
        .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_owned());
    }

    Ok(builder.into())
}

// Resolve the resource group from the given SetVariableValue.
pub(crate) fn resolve_resource_group(resource_group: SetVariableValue) -> Result<Option<String>> {
    Ok(match resource_group {
        SetVariableValue::Single(SetVariableValueSingle::Ident(ident)) => Some(ident.real_value()),
        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::SingleQuotedString(v)))
            if v.as_str().eq_ignore_ascii_case(DEFAULT_RESOURCE_GROUP) =>
        {
            None
        }
        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::SingleQuotedString(v))) => {
            Some(v)
        }
        SetVariableValue::Default => None,
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(
                "target resource group must be a valid string or default".to_owned(),
            )
            .into());
        }
    })
}
