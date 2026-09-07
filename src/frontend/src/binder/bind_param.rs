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

use bytes::Bytes;
use pgwire::types::{Format, FormatIterator};
use risingwave_common::bail;
use risingwave_common::error::BoxedError;
use risingwave_common::types::{Datum, ScalarImpl};

use super::BoundStatement;
use super::statement::RewriteExprsRecursive;
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr, ExprImpl, ExprRewriter, Literal, default_rewrite_expr};

/// Rewrites parameter expressions to literals.
pub(crate) struct ParamRewriter {
    pub(crate) params: Vec<Option<Bytes>>,
    pub(crate) parsed_params: Vec<Datum>,
    pub(crate) param_formats: Vec<Format>,
    pub(crate) error: Option<BoxedError>,
}

impl ParamRewriter {
    pub(crate) fn new(param_formats: Vec<Format>, params: Vec<Option<Bytes>>) -> Self {
        Self {
            parsed_params: vec![None; params.len()],
            params,
            param_formats,
            error: None,
        }
    }
}

impl ExprRewriter for ParamRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        default_rewrite_expr(self, expr)
    }

    fn rewrite_subquery(&mut self, mut subquery: crate::expr::Subquery) -> ExprImpl {
        subquery.query.rewrite_exprs_recursive(self);
        subquery.into()
    }

    fn rewrite_parameter(&mut self, parameter: crate::expr::Parameter) -> ExprImpl {
        let data_type = parameter.return_type();

        // Postgresql parameter index is 1-based. e.g. $1,$2,$3
        // But we store it in 0-based vector. So we need to minus 1.
        let parameter_index = (parameter.index - 1) as usize;

        fn cstr_to_str(b: &[u8]) -> std::result::Result<&str, BoxedError> {
            let without_null = if b.last() == Some(&0) {
                &b[..b.len() - 1]
            } else {
                b
            };
            Ok(std::str::from_utf8(without_null)?)
        }

        let datum: Datum = if let Some(val_bytes) = self.params[parameter_index].clone() {
            let res = match self.param_formats[parameter_index] {
                Format::Text => {
                    cstr_to_str(&val_bytes).and_then(|str| ScalarImpl::from_text(str, &data_type))
                }
                Format::Binary => ScalarImpl::from_binary(&val_bytes, &data_type),
            };
            match res {
                Ok(datum) => Some(datum),
                Err(e) => {
                    self.error = Some(e);
                    return parameter.into();
                }
            }
        } else {
            None
        };

        self.parsed_params[parameter_index].clone_from(&datum);
        Literal::new(datum, data_type).into()
    }
}

impl BoundStatement {
    pub fn bind_parameter(
        mut self,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
    ) -> Result<(BoundStatement, Vec<Datum>)> {
        let mut rewriter = ParamRewriter::new(
            FormatIterator::new(&param_formats, params.len())
                .map_err(ErrorCode::BindError)?
                .collect(),
            params,
        );

        self.rewrite_exprs_recursive(&mut rewriter);

        if let Some(err) = rewriter.error {
            bail!(err);
        }

        Ok((self, rewriter.parsed_params))
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pgwire::types::Format;
    use risingwave_common::types::DataType;
    use risingwave_sqlparser::test_utils::parse_sql_statements;

    use crate::binder::BoundStatement;
    use crate::binder::test_utils::{mock_binder, mock_binder_with_param_types};

    fn create_expect_bound(sql: &str) -> BoundStatement {
        let mut binder = mock_binder();
        let stmt = parse_sql_statements(sql).unwrap().remove(0);
        binder.bind(stmt).unwrap()
    }

    fn create_actual_bound(
        sql: &str,
        param_types: Vec<Option<DataType>>,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
    ) -> BoundStatement {
        let mut binder = mock_binder_with_param_types(param_types);
        let stmt = parse_sql_statements(sql).unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        bound.bind_parameter(params, param_formats).unwrap().0
    }

    fn expect_actual_eq(expect: BoundStatement, actual: BoundStatement) {
        // Use debug format to compare. May modify in future.
        assert_eq!(format!("{:?}", expect), format!("{:?}", actual));
    }

    #[tokio::test]
    async fn basic_select() {
        expect_actual_eq(
            create_expect_bound("select 1::int4"),
            create_actual_bound(
                "select $1::int4",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn basic_value() {
        expect_actual_eq(
            create_expect_bound("values(1::int4)"),
            create_actual_bound(
                "values($1::int4)",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn default_type() {
        expect_actual_eq(
            create_expect_bound("select '1'"),
            create_actual_bound(
                "select $1",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn in_list_parameters_stay_flat_after_binding() {
        expect_actual_eq(
            create_expect_bound("select 1::int4 in (2::int4, 3::int4)"),
            create_actual_bound(
                "select 1::int4 in ($1::int4, $2::int4)",
                vec![],
                vec![Some("2".into()), Some("3".into())],
                vec![Format::Text, Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn large_in_list_parameters_do_not_recurse() {
        let param_count = 4096;
        let params = (1..=param_count)
            .map(|index| format!("${index}::int4"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("select 0::int4 in ({params})");

        let mut binder = mock_binder();
        let stmt = parse_sql_statements(&sql).unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        bound
            .bind_parameter(
                vec![Some("1".into()); param_count],
                vec![Format::Text; param_count],
            )
            .unwrap();
    }

    #[tokio::test]
    async fn cast_after_specific() {
        expect_actual_eq(
            create_expect_bound("select 1::varchar"),
            create_actual_bound(
                "select $1::varchar",
                vec![Some(DataType::Int32)],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn infer_case() {
        expect_actual_eq(
            create_expect_bound("select 1,1::INT4"),
            create_actual_bound(
                "select $1,$1::INT4",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn subquery() {
        expect_actual_eq(
            create_expect_bound("select (select '1')"),
            create_actual_bound(
                "select (select $1)",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }
}
