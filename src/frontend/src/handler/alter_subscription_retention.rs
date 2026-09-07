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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_sqlparser::ast::{
    Ident, ObjectName, SqlOption, SqlOptionValue, Statement, Value, WithProperties,
};
use risingwave_sqlparser::parser::Parser;

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::util::convert_interval_to_u64_seconds;

fn retention_value_to_string(retention: &Value) -> Result<String> {
    match retention {
        Value::SingleQuotedString(value) => Ok(value.clone()),
        Value::CstyleEscapedString(value) => Ok(value.value.clone()),
        Value::Number(value) => Ok(value.clone()),
        _ => Err(ErrorCode::InvalidParameterValue(
            "ALTER SUBSCRIPTION SET RETENTION only supports string or numeric literals".to_owned(),
        )
        .into()),
    }
}

fn update_retention_in_with_properties(with_properties: &mut WithProperties, retention: Value) {
    if let Some(option) = with_properties
        .0
        .iter_mut()
        .find(|option| option.name.real_value().eq_ignore_ascii_case("retention"))
    {
        option.value = SqlOptionValue::Value(retention);
        return;
    }

    with_properties.0.push(SqlOption {
        name: ObjectName(vec![Ident::new_unchecked("retention")]),
        value: SqlOptionValue::Value(retention),
    });
}

fn update_subscription_definition(definition: &str, retention: Value) -> Result<String> {
    let mut statements = Parser::parse_sql(definition)?;
    let statement = match statements.as_mut_slice() {
        [statement] => statement,
        _ => {
            return Err(ErrorCode::InternalError(
                "Subscription definition should contain a single statement.".to_owned(),
            )
            .into());
        }
    };

    match statement {
        Statement::CreateSubscription { stmt } => {
            update_retention_in_with_properties(&mut stmt.with_properties, retention);
        }
        _ => {
            return Err(ErrorCode::InternalError(
                "Unexpected statement in the subscription definition.".to_owned(),
            )
            .into());
        }
    }

    Ok(statement.to_string())
}

pub async fn handle_alter_subscription_retention(
    handler_args: HandlerArgs,
    subscription_name: ObjectName,
    retention: Value,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_subscription_name) =
        Binder::resolve_schema_qualified_name(db_name, &subscription_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let subscription = {
        let reader = session.env().catalog_reader().read_guard();
        let (subscription, schema_name) =
            reader.get_subscription_by_name(db_name, schema_path, &real_subscription_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**subscription)?;
        subscription.clone()
    };

    let retention_value = retention_value_to_string(&retention)?;
    let retention_seconds = convert_interval_to_u64_seconds(&retention_value)?;
    let new_definition = update_subscription_definition(&subscription.definition, retention)?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_subscription_retention(subscription.id, retention_seconds, new_definition)
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SUBSCRIPTION))
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::Value;

    use super::update_subscription_definition;

    #[test]
    fn test_update_subscription_definition() {
        let definition = "CREATE SUBSCRIPTION sub FROM mv WITH (retention = '1D')";
        let updated =
            update_subscription_definition(definition, Value::SingleQuotedString("2H".to_owned()))
                .unwrap();
        assert_eq!(
            updated,
            "CREATE SUBSCRIPTION sub FROM mv WITH (retention = '2H')"
        );
    }
}
