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

use std::cmp::min;
use std::str::from_utf8;

use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, Literal};
use crate::optimizer::plan_node::{ExprRewritable, LogicalFilter};

/// `RewriteLikeExprRule` rewrites simple like expression, so that it can benefit from index selection.
/// col like 'ABC' => col = 'ABC'
/// col like 'ABC%' => col >= 'ABC' and col < 'ABD'
/// col like 'ABC%E' => col >= 'ABC' and col < 'ABD' and col like 'ABC%E'
pub struct RewriteLikeExprRule {}
impl Rule<Logical> for RewriteLikeExprRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        if filter.predicate().conjunctions.iter().any(|expr| {
            let mut has_like = HasSimpleLikeExprVisitor { has: false };
            has_like.visit_expr(expr);
            has_like.has
        }) {
            let mut rewriter = LikeExprRewriter {};
            Some(filter.rewrite_exprs(&mut rewriter))
        } else {
            None
        }
    }
}

struct HasSimpleLikeExprVisitor {
    has: bool,
}

impl ExprVisitor for HasSimpleLikeExprVisitor {
    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        // Simple like expression is a binary operation, e.g. col like 'ABC%'
        // While col like 'ABC%' ESCAPE '!' is a complex like expression.
        if func_call.func_type() == ExprType::Like
            && func_call.inputs().len() == 2
            && let (_, ExprImpl::InputRef(_), ExprImpl::Literal(_)) =
                func_call.clone().decompose_as_binary()
        {
            self.has = true;
        } else {
            func_call
                .inputs()
                .iter()
                .for_each(|expr| self.visit_expr(expr));
        }
    }
}

struct LikeExprRewriter {}

impl LikeExprRewriter {
    fn cal_index_and_unescape(bytes: &[u8]) -> (Option<usize>, Option<usize>, Vec<u8>) {
        // The pattern without escape character.
        let mut unescaped_bytes = vec![];
        // The idx of `_` in the `unescaped_bytes`.
        let mut char_wildcard_idx: Option<usize> = None;
        // The idx of `%` in the `unescaped_bytes`.
        let mut str_wildcard_idx: Option<usize> = None;

        let mut in_escape = false;
        const ESCAPE: u8 = b'\\';
        for &c in bytes {
            if !in_escape && c == ESCAPE {
                in_escape = true;
                continue;
            }
            if in_escape {
                in_escape = false;
                unescaped_bytes.push(c);
                continue;
            }
            unescaped_bytes.push(c);
            if c == b'_' {
                char_wildcard_idx.get_or_insert(unescaped_bytes.len() - 1);
            } else if c == b'%' {
                str_wildcard_idx.get_or_insert(unescaped_bytes.len() - 1);
            }
        }
        // Note: we only support `\\` as the escape character now, and it can't be positioned at the
        // end of string, which will be banned by parser.
        assert!(!in_escape);
        (char_wildcard_idx, str_wildcard_idx, unescaped_bytes)
    }
}

impl ExprRewriter for LikeExprRewriter {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs: Vec<ExprImpl> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        let func_call = FunctionCall::new_unchecked(func_type, inputs, ret.clone());

        if func_call.func_type() != ExprType::Like || func_call.inputs().len() != 2 {
            return func_call.into();
        }

        let (_, ExprImpl::InputRef(x), ExprImpl::Literal(y)) =
            func_call.clone().decompose_as_binary()
        else {
            return func_call.into();
        };

        if y.return_type() != DataType::Varchar {
            return func_call.into();
        }

        let data = y.get_data();
        let Some(ScalarImpl::Utf8(data)) = data else {
            return func_call.into();
        };

        let bytes = data.as_bytes();
        let len = bytes.len();

        let (char_wildcard_idx, str_wildcard_idx, unescaped_bytes) =
            Self::cal_index_and_unescape(bytes);

        let idx = match (char_wildcard_idx, str_wildcard_idx) {
            (Some(a), Some(b)) => min(a, b),
            (Some(idx), None) => idx,
            (None, Some(idx)) => idx,
            (None, None) => {
                let Ok(unescaped_y) = String::from_utf8(unescaped_bytes) else {
                    // FIXME: We should definitely treat the argument as UTF-8 string instead of bytes, but currently, we just fallback here.
                    return func_call.into();
                };
                let inputs = vec![
                    ExprImpl::InputRef(x),
                    ExprImpl::literal_varchar(unescaped_y),
                ];
                let func_call = FunctionCall::new_unchecked(ExprType::Equal, inputs, ret);
                return func_call.into();
            }
        };

        if idx == 0 {
            return func_call.into();
        }

        let (low, high) = {
            let low = unescaped_bytes[0..idx].to_owned();
            if low[idx - 1] == 255 {
                return func_call.into();
            }
            let mut high = low.clone();
            high[idx - 1] += 1;
            match (from_utf8(&low), from_utf8(&high)) {
                (Ok(low), Ok(high)) => (low.to_owned(), high.to_owned()),
                _ => {
                    return func_call.into();
                }
            }
        };

        let between = FunctionCall::new_unchecked(
            ExprType::And,
            vec![
                FunctionCall::new_unchecked(
                    ExprType::GreaterThanOrEqual,
                    vec![
                        ExprImpl::InputRef(x.clone()),
                        ExprImpl::Literal(
                            Literal::new(Some(ScalarImpl::Utf8(low.into())), DataType::Varchar)
                                .into(),
                        ),
                    ],
                    DataType::Boolean,
                )
                .into(),
                FunctionCall::new_unchecked(
                    ExprType::LessThan,
                    vec![
                        ExprImpl::InputRef(x),
                        ExprImpl::Literal(
                            Literal::new(Some(ScalarImpl::Utf8(high.into())), DataType::Varchar)
                                .into(),
                        ),
                    ],
                    DataType::Boolean,
                )
                .into(),
            ],
            DataType::Boolean,
        );

        if idx == len - 1 {
            between.into()
        } else {
            FunctionCall::new_unchecked(
                ExprType::And,
                vec![between.into(), func_call.into()],
                DataType::Boolean,
            )
            .into()
        }
    }
}

impl RewriteLikeExprRule {
    pub fn create() -> BoxedRule {
        Box::new(RewriteLikeExprRule {})
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_cal_index_and_unescape() {
        #[expect(clippy::type_complexity, reason = "in testcase")]
        let testcases: [(&str, (Option<usize>, Option<usize>, &str)); 7] = [
            ("testname", (None, None, "testname")),
            ("test_name", (Some(4), None, "test_name")),
            ("test_name_2", (Some(4), None, "test_name_2")),
            ("test%name", (None, Some(4), "test%name")),
            (r"test\_name", (None, None, "test_name")),
            (r"test\_name_2", (Some(9), None, "test_name_2")),
            (r"test\\_name_2", (Some(5), None, r"test\_name_2")),
        ];

        for (pattern, (c, s, ub)) in testcases {
            let input = pattern.as_bytes();
            let (char_wildcard_idx, str_wildcard_idx, unescaped_bytes) =
                super::LikeExprRewriter::cal_index_and_unescape(input);
            assert_eq!(char_wildcard_idx, c);
            assert_eq!(str_wildcard_idx, s);
            assert_eq!(&String::from_utf8(unescaped_bytes).unwrap(), ub);
        }
    }
}
