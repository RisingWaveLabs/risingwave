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

use std::sync::Arc;

use bytes::Bytes;
use pgwire::pg_response::StatementType;
use pgwire::types::Format;
use risingwave_sqlparser::ast::{
    DataType as AstDataType, Expr, Ident, Statement, UnaryOperator, Value,
};

use crate::bind_data_type;
use crate::error::{ErrorCode, Result};
use crate::handler::extended_handle::{self, Portal, PrepareStatement};
use crate::handler::{HandlerArgs, RwPgResponse, query};
use crate::session::SessionImpl;

pub async fn handle_prepare(
    session: Arc<SessionImpl>,
    name: Ident,
    data_types: Vec<AstDataType>,
    statement: Box<Statement>,
) -> Result<RwPgResponse> {
    let statement = *statement;
    ensure_supported_prepare_statement(&statement)?;

    let specified_param_types = data_types
        .iter()
        .map(bind_data_type)
        .map(|data_type| data_type.map(Some))
        .collect::<Result<Vec<_>>>()?;
    let prepare_statement =
        extended_handle::handle_parse(session.clone(), statement, specified_param_types).await?;
    if !matches!(prepare_statement, PrepareStatement::Prepared(_)) {
        return Err(ErrorCode::NotSupported(
            "SQL PREPARE only supports query, INSERT, UPDATE, and DELETE statements".into(),
            "Use the extended query protocol for other prepared statement forms.".into(),
        )
        .into());
    }

    session.create_simple_query_prepared_statement(name.real_value(), prepare_statement)?;

    Ok(RwPgResponse::builder(StatementType::PREPARE).into())
}

pub fn handle_deallocate(
    session: Arc<SessionImpl>,
    name: Option<Ident>,
    _prepare: bool,
) -> Result<RwPgResponse> {
    if let Some(name) = name {
        session.drop_simple_query_prepared_statement(&name.real_value());
    } else {
        session.drop_all_simple_query_prepared_statements();
    }

    Ok(RwPgResponse::builder(StatementType::DEALLOCATE).into())
}

pub async fn handle_execute(
    session: Arc<SessionImpl>,
    name: Ident,
    parameters: Vec<Expr>,
    result_formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let name = name.real_value();
    let prepare_statement = session.get_simple_query_prepared_statement(&name)?;
    let expected_parameter_count = match &prepare_statement {
        PrepareStatement::Prepared(prepared) => prepared.bound_result.param_types.len(),
        PrepareStatement::Empty | PrepareStatement::PureStatement(_) => {
            return Err(ErrorCode::NotSupported(
                "SQL EXECUTE only supports prepared query, INSERT, UPDATE, and DELETE statements"
                    .into(),
                "Pure prepared statements are not supported in simple query mode.".into(),
            )
            .into());
        }
    };
    let params = convert_execute_parameters(&parameters, expected_parameter_count)?;
    let portal = extended_handle::handle_bind(
        prepare_statement,
        params,
        vec![Format::Text],
        result_formats,
    )?;

    let Portal::Portal(portal) = portal else {
        return Err(ErrorCode::NotSupported(
            "SQL EXECUTE only supports prepared query, INSERT, UPDATE, and DELETE statements"
                .into(),
            "Pure prepared statements are not supported in simple query mode.".into(),
        )
        .into());
    };

    let sql = Arc::from(portal.statement.to_string());
    let handler_args = HandlerArgs::new(session, &portal.statement, sql)?;
    query::handle_execute(handler_args, portal).await
}

fn ensure_supported_prepare_statement(statement: &Statement) -> Result<()> {
    if matches!(
        statement,
        Statement::Query(_)
            | Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
    ) {
        Ok(())
    } else {
        Err(ErrorCode::NotSupported(
            "SQL PREPARE only supports query, INSERT, UPDATE, and DELETE statements".into(),
            "Other prepared statement forms are not supported in simple query mode.".into(),
        )
        .into())
    }
}

fn convert_execute_parameters(
    parameters: &[Expr],
    expected_parameter_count: usize,
) -> Result<Vec<Option<Bytes>>> {
    if parameters.len() != expected_parameter_count {
        return Err(ErrorCode::BindError(format!(
            "wrong number of parameters for prepared statement: expected {}, got {}",
            expected_parameter_count,
            parameters.len()
        ))
        .into());
    }

    parameters
        .iter()
        .map(execute_parameter_to_text)
        .collect::<Result<Vec<_>>>()
}

fn execute_parameter_to_text(expr: &Expr) -> Result<Option<Bytes>> {
    Ok(match expr {
        Expr::Nested(inner) => return execute_parameter_to_text(inner),
        Expr::Value(Value::Null) => None,
        Expr::Value(Value::Number(value)) | Expr::Value(Value::SingleQuotedString(value)) => {
            Some(Bytes::from(value.clone()))
        }
        Expr::Value(Value::Boolean(value)) => {
            Some(Bytes::from(if *value { "true" } else { "false" }))
        }
        Expr::TypedString { value, .. } => Some(Bytes::from(value.clone())),
        Expr::UnaryOp { op, expr } => match (op, expr.as_ref()) {
            (UnaryOperator::Plus, Expr::Value(Value::Number(value))) => {
                Some(Bytes::from(value.clone()))
            }
            (UnaryOperator::Minus, Expr::Value(Value::Number(value))) => {
                Some(Bytes::from(format!("-{}", value)))
            }
            _ => return unsupported_execute_parameter(expr),
        },
        _ => return unsupported_execute_parameter(expr),
    })
}

fn unsupported_execute_parameter<T>(expr: &Expr) -> Result<T> {
    Err(ErrorCode::BindError(format!(
        "unsupported EXECUTE parameter expression: {}",
        expr
    ))
    .into())
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::{DataType, Expr, UnaryOperator, Value};

    use super::convert_execute_parameters;

    #[test]
    fn convert_execute_parameters_accepts_supported_literals() {
        let params = convert_execute_parameters(
            &[
                Expr::Value(Value::Number("42".into())),
                Expr::Value(Value::SingleQuotedString("rw".into())),
                Expr::Value(Value::Boolean(true)),
                Expr::Value(Value::Null),
                Expr::TypedString {
                    data_type: DataType::Date,
                    value: "2026-06-29".into(),
                },
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(Value::Number("7".into()))),
                },
            ],
            6,
        )
        .unwrap();

        assert_eq!(params[0].as_ref().unwrap().as_ref(), b"42");
        assert_eq!(params[1].as_ref().unwrap().as_ref(), b"rw");
        assert_eq!(params[2].as_ref().unwrap().as_ref(), b"true");
        assert!(params[3].is_none());
        assert_eq!(params[4].as_ref().unwrap().as_ref(), b"2026-06-29");
        assert_eq!(params[5].as_ref().unwrap().as_ref(), b"-7");
    }

    #[test]
    fn convert_execute_parameters_rejects_wrong_arity_and_expressions() {
        let wrong_arity = convert_execute_parameters(&[Expr::Value(Value::Number("1".into()))], 2)
            .unwrap_err()
            .to_string();
        assert!(wrong_arity.contains("wrong number of parameters"));

        let unsupported = convert_execute_parameters(
            &[Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Number("1".into()))),
                op: risingwave_sqlparser::ast::BinaryOperator::Plus,
                right: Box::new(Expr::Value(Value::Number("2".into()))),
            }],
            1,
        )
        .unwrap_err()
        .to_string();
        assert!(unsupported.contains("unsupported EXECUTE parameter expression"));
    }
}
