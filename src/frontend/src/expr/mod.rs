// Copyright 2022 RisingWave Labs
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

use enum_as_inner::EnumAsInner;
use fixedbitset::FixedBitSet;
use futures::FutureExt;
use paste::paste;
use risingwave_common::array::ListValue;
use risingwave_common::types::{
    DataType, Datum, JsonbVal, MapType, Scalar, ScalarImpl, ToOwnedDatum,
};
use risingwave_expr::aggregate::PbAggKind;
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::{ExprNode, ProjectSetSelectItem};
use user_defined_function::UserDefinedFunctionDisplay;

use crate::error::{ErrorCode, Result as RwResult};
use crate::session::current;

mod agg_call;
mod correlated_input_ref;
mod function_call;
mod function_call_with_lambda;
mod input_ref;
mod literal;
mod now;
mod parameter;
mod pure;
mod subquery;
mod table_function;
mod user_defined_function;
mod window_function;

mod order_by_expr;
pub use order_by_expr::{OrderBy, OrderByExpr};

mod expr_mutator;
mod expr_rewriter;
mod expr_visitor;
pub mod function_impl;
mod secret_ref;
mod session_timezone;
mod type_inference;
mod utils;

pub use agg_call::AggCall;
pub use correlated_input_ref::{CorrelatedId, CorrelatedInputRef, Depth, InputRefDepthRewriter};
pub use expr_mutator::ExprMutator;
pub use expr_rewriter::{ExprRewriter, default_rewrite_expr};
pub use expr_visitor::{ExprVisitor, default_visit_expr};
pub use function_call::{FunctionCall, FunctionCallDisplay, is_row_function};
pub use function_call_with_lambda::FunctionCallWithLambda;
pub use input_ref::{InputRef, InputRefDisplay, input_ref_to_column_indices};
pub use literal::Literal;
pub use now::{InlineNowProcTime, Now, NowProcTimeFinder};
pub use parameter::Parameter;
pub use pure::*;
pub use risingwave_pb::expr::expr_node::Type as ExprType;
pub use secret_ref::SecretRef;
pub use session_timezone::{SessionTimezone, TimestamptzExprFinder};
pub use subquery::{Subquery, SubqueryKind};
pub(crate) use table_function::expr_impl_to_string_fn;
pub use table_function::{TableFunction, TableFunctionType};
pub use type_inference::*;
pub use user_defined_function::UserDefinedFunction;
pub use utils::*;
pub use window_function::WindowFunction;

pub(crate) const EXPR_DEPTH_THRESHOLD: usize = 30;
pub(crate) const EXPR_TOO_DEEP_NOTICE: &str = "Some expression is too complicated. \
Consider simplifying or splitting the query if you encounter any issues.";

pub(crate) fn reject_impure(expr: impl Into<ExprImpl>, context: &str) -> RwResult<()> {
    if let Some(impure_expr_desc) = impure_expr_desc(&expr.into()) {
        let msg = format!(
            "using an impure expression ({impure_expr_desc}) in {context} \
             on a retract stream may lead to inconsistent results"
        );
        if current::config()
            .is_some_and(|c| c.read().streaming_unsafe_allow_unmaterialized_impure_expr())
        {
            current::notice_to_user(msg);
        } else {
            return Err(ErrorCode::NotSupported(
                msg,
                "rewrite the query to extract the impure expression into the select list, \
                 or set `streaming_unsafe_allow_unmaterialized_impure_expr` to allow \
                 the behavior at the risk of inconsistent results or panics during execution"
                    .into(),
            )
            .into());
        }
    }
    Ok(())
}

/// the trait of bound expressions
pub trait Expr: Into<ExprImpl> {
    /// Get the return type of the expr
    fn return_type(&self) -> DataType;

    /// Try to serialize the expression, returning an error if it's impossible.
    fn try_to_expr_proto(&self) -> Result<ExprNode, String>;

    /// Serialize the expression. Panic if it's impossible.
    fn to_expr_proto(&self) -> ExprNode {
        self.try_to_expr_proto()
            .expect("failed to serialize expression to protobuf")
    }

    /// Serialize the expression. Returns an error if this will result in an impure expression on a
    /// retract stream, which may lead to inconsistent results.
    fn to_expr_proto_checked_pure(
        &self,
        retract: bool,
        context: &str,
    ) -> crate::error::Result<ExprNode>
    where
        Self: Clone,
    {
        if retract {
            reject_impure(self.clone(), context)?;
        }
        self.try_to_expr_proto()
            .map_err(|e| ErrorCode::InternalError(e).into())
    }
}

macro_rules! impl_expr_impl {
    ($($t:ident,)*) => {
        #[derive(Clone, Eq, PartialEq, Hash, EnumAsInner)]
        pub enum ExprImpl {
            $($t(Box<$t>),)*
        }

        impl ExprImpl {
            pub fn variant_name(&self) -> &'static str {
                match self {
                    $(ExprImpl::$t(_) => stringify!($t),)*
                }
            }
        }

        $(
        impl From<$t> for ExprImpl {
            fn from(o: $t) -> ExprImpl {
                ExprImpl::$t(Box::new(o))
            }
        })*

        impl Expr for ExprImpl {
            fn return_type(&self) -> DataType {
                match self {
                    $(ExprImpl::$t(expr) => expr.return_type(),)*
                }
            }

            fn try_to_expr_proto(&self) -> Result<ExprNode, String> {
                match self {
                    $(ExprImpl::$t(expr) => expr.try_to_expr_proto(),)*
                }
            }
        }
    };
}

impl_expr_impl!(
    // BoundColumnRef, might be used in binder.
    CorrelatedInputRef,
    InputRef,
    Literal,
    FunctionCall,
    FunctionCallWithLambda,
    AggCall,
    Subquery,
    TableFunction,
    WindowFunction,
    UserDefinedFunction,
    Parameter,
    Now,
    SecretRef,
);

impl ExprImpl {
    /// A literal int value.
    #[inline(always)]
    pub fn literal_int(v: i32) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Int32).into()
    }

    /// A literal bigint value
    #[inline(always)]
    pub fn literal_bigint(v: i64) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Int64).into()
    }

    /// A literal float64 value.
    #[inline(always)]
    pub fn literal_f64(v: f64) -> Self {
        Literal::new(Some(v.into()), DataType::Float64).into()
    }

    /// A literal boolean value.
    #[inline(always)]
    pub fn literal_bool(v: bool) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Boolean).into()
    }

    /// A literal varchar value.
    #[inline(always)]
    pub fn literal_varchar(v: String) -> Self {
        Literal::new(Some(v.into()), DataType::Varchar).into()
    }

    /// A literal null value.
    #[inline(always)]
    pub fn literal_null(element_type: DataType) -> Self {
        Literal::new(None, element_type).into()
    }

    /// A literal jsonb value.
    #[inline(always)]
    pub fn literal_jsonb(v: JsonbVal) -> Self {
        Literal::new(Some(v.into()), DataType::Jsonb).into()
    }

    /// A literal list value.
    #[inline(always)]
    pub fn literal_list(v: ListValue, element_type: DataType) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::list(element_type)).into()
    }

    /// Takes the expression, leaving a literal null of the same type in its place.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::literal_null(self.return_type()))
    }

    /// A `count(*)` aggregate function.
    #[inline(always)]
    pub fn count_star() -> Self {
        AggCall::new(
            PbAggKind::Count.into(),
            vec![],
            false,
            OrderBy::any(),
            Condition::true_cond(),
            vec![],
        )
        .unwrap()
        .into()
    }

    /// Create a new expression by merging the given expressions by `And`.
    ///
    /// If `exprs` is empty, return a literal `true`.
    pub fn and(exprs: impl IntoIterator<Item = ExprImpl>) -> Self {
        merge_expr_by_logical(exprs, ExprType::And, ExprImpl::literal_bool(true))
    }

    /// Create a new expression by merging the given expressions by `Or`.
    ///
    /// If `exprs` is empty, return a literal `false`.
    pub fn or(exprs: impl IntoIterator<Item = ExprImpl>) -> Self {
        merge_expr_by_logical(exprs, ExprType::Or, ExprImpl::literal_bool(false))
    }

    /// Collect all `InputRef`s' indexes in the expression.
    ///
    /// # Panics
    /// Panics if `input_ref >= input_col_num`.
    pub fn collect_input_refs(&self, input_col_num: usize) -> FixedBitSet {
        collect_input_refs(input_col_num, [self])
    }

    /// Check if the expression has no side effects and output is deterministic
    pub fn is_pure(&self) -> bool {
        is_pure(self)
    }

    pub fn is_impure(&self) -> bool {
        is_impure(self)
    }

    /// Count `Now`s in the expression.
    pub fn count_nows(&self) -> usize {
        let mut visitor = CountNow::default();
        visitor.visit_expr(self);
        visitor.count()
    }

    /// Check whether self is literal NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.get_data().is_none())
    }

    /// Check whether self is a literal NULL or literal string.
    pub fn is_untyped(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.is_untyped())
            || matches!(self, ExprImpl::Parameter(parameter) if !parameter.has_infer())
    }

    /// Shorthand to create cast expr to `target` type in implicit context.
    pub fn cast_implicit(mut self, target: &DataType) -> Result<ExprImpl, CastError> {
        FunctionCall::cast_mut(&mut self, target, CastContext::Implicit)?;
        Ok(self)
    }

    /// Shorthand to create cast expr to `target` type in assign context.
    pub fn cast_assign(mut self, target: &DataType) -> Result<ExprImpl, CastError> {
        FunctionCall::cast_mut(&mut self, target, CastContext::Assign)?;
        Ok(self)
    }

    /// Shorthand to create cast expr to `target` type in explicit context.
    pub fn cast_explicit(mut self, target: &DataType) -> Result<ExprImpl, CastError> {
        FunctionCall::cast_mut(&mut self, target, CastContext::Explicit)?;
        Ok(self)
    }

    /// Shorthand to inplace cast expr to `target` type in implicit context.
    pub fn cast_implicit_mut(&mut self, target: &DataType) -> Result<(), CastError> {
        FunctionCall::cast_mut(self, target, CastContext::Implicit)
    }

    /// Shorthand to inplace cast expr to `target` type in explicit context.
    pub fn cast_explicit_mut(&mut self, target: &DataType) -> Result<(), CastError> {
        FunctionCall::cast_mut(self, target, CastContext::Explicit)
    }

    /// Casting to Regclass type means getting the oid of expr.
    /// See <https://www.postgresql.org/docs/current/datatype-oid.html>
    pub fn cast_to_regclass(self) -> Result<ExprImpl, CastError> {
        match self.return_type() {
            DataType::Varchar => Ok(ExprImpl::FunctionCall(Box::new(
                FunctionCall::new_unchecked(ExprType::CastRegclass, vec![self], DataType::Int32),
            ))),
            DataType::Int32 => Ok(self),
            dt if dt.is_int() => Ok(self.cast_explicit(&DataType::Int32)?),
            _ => bail_cast_error!("unsupported input type"),
        }
    }

    /// Shorthand to inplace cast expr to `regclass` type.
    pub fn cast_to_regclass_mut(&mut self) -> Result<(), CastError> {
        let owned = std::mem::replace(self, ExprImpl::literal_bool(false));
        *self = owned.cast_to_regclass()?;
        Ok(())
    }

    /// Ensure the return type of this expression is an array of some type.
    pub fn ensure_array_type(&self) -> Result<(), ErrorCode> {
        if self.is_untyped() {
            return Err(ErrorCode::BindError(
                "could not determine polymorphic type because input has type unknown".into(),
            ));
        }
        match self.return_type() {
            DataType::List(_) => Ok(()),
            t => Err(ErrorCode::BindError(format!("expects array but got {t}"))),
        }
    }

    /// Ensure the return type of this expression is a map of some type.
    pub fn try_into_map_type(&self) -> Result<MapType, ErrorCode> {
        if self.is_untyped() {
            return Err(ErrorCode::BindError(
                "could not determine polymorphic type because input has type unknown".into(),
            ));
        }
        match self.return_type() {
            DataType::Map(m) => Ok(m),
            t => Err(ErrorCode::BindError(format!("expects map but got {t}"))),
        }
    }

    /// Shorthand to enforce implicit cast to boolean
    pub fn enforce_bool_clause(self, clause: &str) -> RwResult<ExprImpl> {
        if self.is_untyped() {
            let inner = self.cast_implicit(&DataType::Boolean)?;
            return Ok(inner);
        }
        let return_type = self.return_type();
        if return_type != DataType::Boolean {
            bail!(
                "argument of {} must be boolean, not type {:?}",
                clause,
                return_type
            )
        }
        Ok(self)
    }

    /// Create "cast" expr to string (`varchar`) type. This is different from a real cast, as
    /// boolean is converted to a single char rather than full word.
    ///
    /// Choose between `cast_output` and `cast_{assign,explicit}(Varchar)` based on `PostgreSQL`'s
    /// behavior on bools. For example, `concat(':', true)` is `:t` but `':' || true` is `:true`.
    /// All other types have the same behavior when formatting to output and casting to string.
    ///
    /// References in `PostgreSQL`:
    /// * [cast](https://github.com/postgres/postgres/blob/a3ff08e0b08dbfeb777ccfa8f13ebaa95d064c04/src/include/catalog/pg_cast.dat#L437-L444)
    /// * [impl](https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/backend/utils/adt/bool.c#L204-L209)
    pub fn cast_output(self) -> RwResult<ExprImpl> {
        if self.return_type() == DataType::Boolean {
            return Ok(FunctionCall::new(ExprType::BoolOut, vec![self])?.into());
        }
        // Use normal cast for other types. Both `assign` and `explicit` can pass the castability
        // check and there is no difference.
        self.cast_assign(&DataType::Varchar)
            .map_err(|err| err.into())
    }

    /// Evaluate the expression on the given input.
    ///
    /// TODO: This is a naive implementation. We should avoid proto ser/de.
    /// Tracking issue: <https://github.com/risingwavelabs/risingwave/issues/3479>
    pub async fn eval_row(&self, input: &OwnedRow) -> RwResult<Datum> {
        let backend_expr = build_from_prost(&self.to_expr_proto())?;
        Ok(backend_expr.eval_row(input).await?)
    }

    /// Try to evaluate an expression if it's a constant expression by `ExprImpl::is_const`.
    ///
    /// Returns...
    /// - `None` if it's not a constant expression,
    /// - `Some(Ok(_))` if constant evaluation succeeds,
    /// - `Some(Err(_))` if there's an error while evaluating a constant expression.
    pub fn try_fold_const(&self) -> Option<RwResult<Datum>> {
        if self.is_const() {
            self.eval_row(&OwnedRow::empty())
                .now_or_never()
                .expect("constant expression should not be async")
                .into()
        } else {
            None
        }
    }

    /// Similar to `ExprImpl::try_fold_const`, but panics if the expression is not constant.
    pub fn fold_const(&self) -> RwResult<Datum> {
        self.try_fold_const().expect("expression is not constant")
    }
}

/// Implement helper functions which recursively checks whether an variant is included in the
/// expression. e.g., `has_subquery(&self) -> bool`
///
/// It will not traverse inside subqueries.
macro_rules! impl_has_variant {
    ( $($variant:ty),* ) => {
        paste! {
            impl ExprImpl {
                $(
                    pub fn [<has_ $variant:snake>](&self) -> bool {
                        struct Has { has: bool }

                        impl ExprVisitor for Has {
                            fn [<visit_ $variant:snake>](&mut self, _: &$variant) {
                                self.has = true;
                            }
                        }

                        let mut visitor = Has { has: false };
                        visitor.visit_expr(self);
                        visitor.has
                    }
                )*
            }
        }
    };
}

impl_has_variant! {InputRef, Literal, FunctionCall, FunctionCallWithLambda, AggCall, Subquery, TableFunction, WindowFunction, UserDefinedFunction, Now}

/// Inequality condition between two input columns with clearer semantics.
/// Represents: `left_col <op> right_col` where op is one of `<`, `<=`, `>`, `>=`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InequalityInputPair {
    /// Index of the left side column (from left input).
    pub left_idx: usize,
    /// Index of the right side column (from right input, NOT offset by `left_cols_num`).
    pub right_idx: usize,
    /// Comparison operator: `left_col <op> right_col`.
    pub op: ExprType,
}

impl InequalityInputPair {
    pub fn new(left_idx: usize, right_idx: usize, op: ExprType) -> Self {
        debug_assert!(matches!(
            op,
            ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual
        ));
        Self {
            left_idx,
            right_idx,
            op,
        }
    }

    /// Returns true if the left side has larger values based on the operator.
    /// For `>` and `>=`, left side is larger.
    /// State cleanup applies to the side with larger values.
    pub fn left_side_is_larger(&self) -> bool {
        matches!(
            self.op,
            ExprType::GreaterThan | ExprType::GreaterThanOrEqual
        )
    }
}

impl ExprImpl {
    /// This function is not meant to be called. In most cases you would want
    /// [`ExprImpl::has_correlated_input_ref_by_depth`].
    ///
    /// When an expr contains a [`CorrelatedInputRef`] with lower depth, the whole expr is still
    /// considered to be uncorrelated, and can be checked with [`ExprImpl::has_subquery`] as well.
    /// See examples on [`crate::binder::BoundQuery::is_correlated_by_depth`] for details.
    ///
    /// This is a placeholder to trigger a compiler error when a trivial implementation checking for
    /// enum variant is generated by accident. It cannot be called either because you cannot pass
    /// `Infallible` to it.
    pub fn has_correlated_input_ref(&self, _: std::convert::Infallible) -> bool {
        unreachable!()
    }

    /// Used to check whether the expression has [`CorrelatedInputRef`].
    ///
    /// This is the core logic that supports [`crate::binder::BoundQuery::is_correlated_by_depth`]. Check the
    /// doc of it for examples of `depth` being equal, less or greater.
    // We need to traverse inside subqueries.
    pub fn has_correlated_input_ref_by_depth(&self, depth: Depth) -> bool {
        struct Has {
            depth: usize,
            has: bool,
        }

        impl ExprVisitor for Has {
            fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
                if correlated_input_ref.depth() == self.depth {
                    self.has = true;
                }
            }

            fn visit_subquery(&mut self, subquery: &Subquery) {
                self.has |= subquery.is_correlated_by_depth(self.depth);
            }
        }

        let mut visitor = Has { depth, has: false };
        visitor.visit_expr(self);
        visitor.has
    }

    pub fn has_correlated_input_ref_by_correlated_id(&self, correlated_id: CorrelatedId) -> bool {
        struct Has {
            correlated_id: CorrelatedId,
            has: bool,
        }

        impl ExprVisitor for Has {
            fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
                if correlated_input_ref.correlated_id() == self.correlated_id {
                    self.has = true;
                }
            }

            fn visit_subquery(&mut self, subquery: &Subquery) {
                self.has |= subquery.is_correlated_by_correlated_id(self.correlated_id);
            }
        }

        let mut visitor = Has {
            correlated_id,
            has: false,
        };
        visitor.visit_expr(self);
        visitor.has
    }

    /// Collect `CorrelatedInputRef`s in `ExprImpl` by relative `depth`, return their indices, and
    /// assign absolute `correlated_id` for them.
    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        struct Collector {
            depth: Depth,
            correlated_indices: Vec<usize>,
            correlated_id: CorrelatedId,
        }

        impl ExprMutator for Collector {
            fn visit_correlated_input_ref(
                &mut self,
                correlated_input_ref: &mut CorrelatedInputRef,
            ) {
                if correlated_input_ref.depth() == self.depth {
                    self.correlated_indices.push(correlated_input_ref.index());
                    correlated_input_ref.set_correlated_id(self.correlated_id);
                }
            }

            fn visit_subquery(&mut self, subquery: &mut Subquery) {
                self.correlated_indices.extend(
                    subquery.collect_correlated_indices_by_depth_and_assign_id(
                        self.depth,
                        self.correlated_id,
                    ),
                );
            }
        }

        let mut collector = Collector {
            depth,
            correlated_indices: vec![],
            correlated_id,
        };
        collector.visit_expr(self);
        collector.correlated_indices.sort();
        collector.correlated_indices.dedup();
        collector.correlated_indices
    }

    pub fn only_literal_and_func(&self) -> bool {
        {
            struct HasOthers {
                has_others: bool,
            }

            impl ExprVisitor for HasOthers {
                fn visit_expr(&mut self, expr: &ExprImpl) {
                    match expr {
                        ExprImpl::CorrelatedInputRef(_)
                        | ExprImpl::InputRef(_)
                        | ExprImpl::AggCall(_)
                        | ExprImpl::Subquery(_)
                        | ExprImpl::TableFunction(_)
                        | ExprImpl::WindowFunction(_)
                        | ExprImpl::UserDefinedFunction(_)
                        | ExprImpl::Parameter(_)
                        | ExprImpl::Now(_)
                        | ExprImpl::SecretRef(_) => self.has_others = true,
                        ExprImpl::Literal(_inner) => {}
                        ExprImpl::FunctionCall(inner) => {
                            if !self.is_short_circuit(inner) {
                                // only if the current `func_call` is *not* a short-circuit
                                // expression, e.g., true or (...) | false and (...),
                                // shall we proceed to visit it.
                                self.visit_function_call(inner)
                            }
                        }
                        ExprImpl::FunctionCallWithLambda(inner) => {
                            self.visit_function_call_with_lambda(inner)
                        }
                    }
                }
            }

            impl HasOthers {
                fn is_short_circuit(&self, func_call: &FunctionCall) -> bool {
                    /// evaluate the first parameter of `Or` or `And` function call
                    fn eval_first(e: &ExprImpl, expect: bool) -> bool {
                        if let ExprImpl::Literal(l) = e {
                            *l.get_data() == Some(ScalarImpl::Bool(expect))
                        } else {
                            false
                        }
                    }

                    match func_call.func_type {
                        ExprType::Or => eval_first(&func_call.inputs()[0], true),
                        ExprType::And => eval_first(&func_call.inputs()[0], false),
                        _ => false,
                    }
                }
            }

            let mut visitor = HasOthers { has_others: false };
            visitor.visit_expr(self);
            !visitor.has_others
        }
    }

    /// Checks whether this is a constant expr that can be evaluated over a dummy chunk.
    ///
    /// The expression tree should only consist of literals and **pure** function calls.
    pub fn is_const(&self) -> bool {
        self.only_literal_and_func() && self.is_pure()
    }

    /// Returns the `InputRefs` of an Equality predicate if it matches
    /// ordered by the canonical ordering (lower, higher), else returns None
    pub fn as_eq_cond(&self) -> Option<(InputRef, InputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::Equal
            && let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) =
                function_call.clone().decompose_as_binary()
        {
            if x.index() < y.index() {
                Some((*x, *y))
            } else {
                Some((*y, *x))
            }
        } else {
            None
        }
    }

    pub fn as_is_not_distinct_from_cond(&self) -> Option<(InputRef, InputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::IsNotDistinctFrom
            && let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) =
                function_call.clone().decompose_as_binary()
        {
            if x.index() < y.index() {
                Some((*x, *y))
            } else {
                Some((*y, *x))
            }
        } else {
            None
        }
    }

    pub fn reverse_comparison(comparison: ExprType) -> ExprType {
        match comparison {
            ExprType::LessThan => ExprType::GreaterThan,
            ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
            ExprType::GreaterThan => ExprType::LessThan,
            ExprType::GreaterThanOrEqual => ExprType::LessThanOrEqual,
            ExprType::Equal | ExprType::IsNotDistinctFrom => comparison,
            _ => unreachable!(),
        }
    }

    pub fn as_comparison_cond(&self) -> Option<(InputRef, ExprType, InputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.func_type() {
                ty @ (ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let (_, op1, op2) = function_call.clone().decompose_as_binary();
                    if let (ExprImpl::InputRef(x), ExprImpl::InputRef(y)) = (op1, op2) {
                        if x.index < y.index {
                            Some((*x, ty, *y))
                        } else {
                            Some((*y, Self::reverse_comparison(ty), *x))
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    /// Accepts expressions of the form `input_expr cmp now_expr` or `now_expr cmp input_expr`,
    /// where `input_expr` contains an `InputRef` and contains no `now()`, and `now_expr`
    /// contains a `now()` but no `InputRef`.
    ///
    /// Canonicalizes to the first ordering and returns `(input_expr, cmp, now_expr)`
    pub fn as_now_comparison_cond(&self) -> Option<(ExprImpl, ExprType, ExprImpl)> {
        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.func_type() {
                ty @ (ExprType::Equal
                | ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let (_, op1, op2) = function_call.clone().decompose_as_binary();
                    if !op1.has_now()
                        && op1.has_input_ref()
                        && op2.has_now()
                        && !op2.has_input_ref()
                    {
                        Some((op1, ty, op2))
                    } else if op1.has_now()
                        && !op1.has_input_ref()
                        && !op2.has_now()
                        && op2.has_input_ref()
                    {
                        Some((op2, Self::reverse_comparison(ty), op1))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    /// Returns the `InputRef` and offset of a predicate if it matches
    /// the form `InputRef [+- const_expr]`, else returns None.
    ///
    /// Deprecated: Only used by `as_input_comparison_cond`.
    #[expect(dead_code)]
    fn as_input_offset(&self) -> Option<(usize, Option<(ExprType, ExprImpl)>)> {
        match self {
            ExprImpl::InputRef(input_ref) => Some((input_ref.index(), None)),
            ExprImpl::FunctionCall(function_call) => {
                let expr_type = function_call.func_type();
                match expr_type {
                    ExprType::Add | ExprType::Subtract => {
                        let (_, lhs, rhs) = function_call.clone().decompose_as_binary();
                        if let ExprImpl::InputRef(input_ref) = &lhs
                            && rhs.is_const()
                        {
                            // Currently we will return `None` for non-literal because the result of the expression might be '1 day'. However, there will definitely exist false positives such as '1 second + 1 second'.
                            // We will treat the expression as an input offset when rhs is `null`.
                            if rhs.return_type() == DataType::Interval
                                && rhs.as_literal().is_none_or(|literal| {
                                    literal.get_data().as_ref().is_some_and(|scalar| {
                                        let interval = scalar.as_interval();
                                        interval.months() != 0 || interval.days() != 0
                                    })
                                })
                            {
                                None
                            } else {
                                Some((input_ref.index(), Some((expr_type, rhs))))
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub fn as_eq_const(&self) -> Option<(InputRef, ExprImpl)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::Equal
        {
            match function_call.clone().decompose_as_binary() {
                (_, ExprImpl::InputRef(x), y) if y.is_const() => Some((*x, y)),
                (_, x, ExprImpl::InputRef(y)) if x.is_const() => Some((*y, x)),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_eq_correlated_input_ref(&self) -> Option<(InputRef, CorrelatedInputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::Equal
        {
            match function_call.clone().decompose_as_binary() {
                (_, ExprImpl::InputRef(x), ExprImpl::CorrelatedInputRef(y)) => Some((*x, *y)),
                (_, ExprImpl::CorrelatedInputRef(x), ExprImpl::InputRef(y)) => Some((*y, *x)),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_is_null(&self) -> Option<InputRef> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::IsNull
        {
            match function_call.clone().decompose_as_unary() {
                (_, ExprImpl::InputRef(x)) => Some(*x),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_comparison_const(&self) -> Option<(InputRef, ExprType, ExprImpl)> {
        fn reverse_comparison(comparison: ExprType) -> ExprType {
            match comparison {
                ExprType::LessThan => ExprType::GreaterThan,
                ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
                ExprType::GreaterThan => ExprType::LessThan,
                ExprType::GreaterThanOrEqual => ExprType::LessThanOrEqual,
                _ => unreachable!(),
            }
        }

        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.func_type() {
                ty @ (ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let (_, op1, op2) = function_call.clone().decompose_as_binary();
                    match (op1, op2) {
                        (ExprImpl::InputRef(x), y) if y.is_const() => Some((*x, ty, y)),
                        (x, ExprImpl::InputRef(y)) if x.is_const() => {
                            Some((*y, reverse_comparison(ty), x))
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_in_const_list(&self) -> Option<(InputRef, Vec<ExprImpl>)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::In
        {
            let (input, list) = function_call.inputs().split_first()?;
            let input_ref = match input {
                ExprImpl::InputRef(i) => i.as_ref().clone(),
                _ => return None,
            };
            if !list.iter().all(ExprImpl::is_const) {
                return None;
            }

            Some((input_ref, list.to_vec()))
        } else {
            None
        }
    }

    pub fn as_some_eq_const_list(&self) -> Option<(InputRef, Vec<ExprImpl>)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::Some
        {
            let (_, inner) = function_call.clone().decompose_as_unary();
            let ExprImpl::FunctionCall(inner_call) = inner else {
                return None;
            };
            if inner_call.func_type() != ExprType::Equal {
                return None;
            }

            let (_, left, right) = inner_call.decompose_as_binary();
            let (input_ref, list_expr) = match (left, right) {
                (ExprImpl::InputRef(input_ref), list_expr) if list_expr.is_const() => {
                    (input_ref.as_ref().clone(), list_expr)
                }
                (list_expr, ExprImpl::InputRef(input_ref)) if list_expr.is_const() => {
                    (input_ref.as_ref().clone(), list_expr)
                }
                _ => return None,
            };

            let literal = list_expr.as_literal()?;
            let DataType::List(list_type) = literal.return_type() else {
                return None;
            };
            let list = literal
                .get_data()
                .as_ref()?
                .as_list()
                .iter()
                .map(|datum| {
                    ExprImpl::Literal(Box::new(Literal::new(
                        datum.to_owned_datum(),
                        list_type.elem().clone(),
                    )))
                })
                .collect();

            Some((input_ref, list))
        } else {
            None
        }
    }

    pub fn as_or_disjunctions(&self) -> Option<Vec<ExprImpl>> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.func_type() == ExprType::Or
        {
            Some(to_disjunctions(self.clone()))
        } else {
            None
        }
    }

    pub fn to_project_set_select_item_proto(&self) -> ProjectSetSelectItem {
        use risingwave_pb::expr::project_set_select_item::SelectItem::*;

        ProjectSetSelectItem {
            select_item: Some(match self {
                ExprImpl::TableFunction(tf) => TableFunction(tf.to_protobuf()),
                expr => Expr(expr.to_expr_proto()),
            }),
        }
    }

    /// Serialize the expression. Returns an error if this will result in an impure expression on a
    /// retract stream, which may lead to inconsistent results.
    pub fn to_project_set_select_item_proto_checked_pure(
        &self,
        retract: bool,
    ) -> crate::error::Result<ProjectSetSelectItem> {
        use risingwave_pb::expr::project_set_select_item::SelectItem::*;

        Ok(ProjectSetSelectItem {
            select_item: Some(match self {
                ExprImpl::TableFunction(tf) => TableFunction(tf.to_protobuf_checked_pure(retract)?),
                expr => Expr(expr.to_expr_proto_checked_pure(retract, "SELECT list")?),
            }),
        })
    }

    pub fn from_expr_proto(proto: &ExprNode) -> RwResult<Self> {
        let rex_node = proto.get_rex_node()?;
        let ret_type = proto.get_return_type()?.into();

        Ok(match rex_node {
            RexNode::InputRef(column_index) => Self::InputRef(Box::new(InputRef::from_expr_proto(
                *column_index as _,
                ret_type,
            )?)),
            RexNode::Constant(_) => Self::Literal(Box::new(Literal::from_expr_proto(proto)?)),
            RexNode::Udf(udf) => Self::UserDefinedFunction(Box::new(
                UserDefinedFunction::from_expr_proto(udf, ret_type)?,
            )),
            RexNode::FuncCall(function_call) => {
                Self::FunctionCall(Box::new(FunctionCall::from_expr_proto(
                    function_call,
                    proto.get_function_type()?, // only interpret if it's a function call
                    ret_type,
                )?))
            }
            RexNode::Now(_) => Self::Now(Box::new(Now {})),
            RexNode::SecretRef(sr) => Self::SecretRef(Box::new(SecretRef {
                secret_id: sr.secret_id.into(),
                ref_as: risingwave_pb::secret::secret_ref::RefAsType::try_from(sr.ref_as)
                    .unwrap_or(risingwave_pb::secret::secret_ref::RefAsType::Text),
                secret_name: format!("<secret:{}>", sr.secret_id),
            })),
        })
    }
}

impl From<Condition> for ExprImpl {
    fn from(c: Condition) -> Self {
        ExprImpl::and(c.conjunctions)
    }
}

/// A custom Debug implementation that is more concise and suitable to use with
/// [`std::fmt::Formatter::debug_list`] in plan nodes. If the verbose output is preferred, it is
/// still available via `{:#?}`.
impl std::fmt::Debug for ExprImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return match self {
                Self::InputRef(arg0) => f.debug_tuple("InputRef").field(arg0).finish(),
                Self::Literal(arg0) => f.debug_tuple("Literal").field(arg0).finish(),
                Self::FunctionCall(arg0) => f.debug_tuple("FunctionCall").field(arg0).finish(),
                Self::FunctionCallWithLambda(arg0) => {
                    f.debug_tuple("FunctionCallWithLambda").field(arg0).finish()
                }
                Self::AggCall(arg0) => f.debug_tuple("AggCall").field(arg0).finish(),
                Self::Subquery(arg0) => f.debug_tuple("Subquery").field(arg0).finish(),
                Self::CorrelatedInputRef(arg0) => {
                    f.debug_tuple("CorrelatedInputRef").field(arg0).finish()
                }
                Self::TableFunction(arg0) => f.debug_tuple("TableFunction").field(arg0).finish(),
                Self::WindowFunction(arg0) => f.debug_tuple("WindowFunction").field(arg0).finish(),
                Self::UserDefinedFunction(arg0) => {
                    f.debug_tuple("UserDefinedFunction").field(arg0).finish()
                }
                Self::Parameter(arg0) => f.debug_tuple("Parameter").field(arg0).finish(),
                Self::Now(_) => f.debug_tuple("Now").finish(),
                Self::SecretRef(arg0) => f.debug_tuple("SecretRef").field(arg0).finish(),
            };
        }
        match self {
            Self::InputRef(x) => write!(f, "{:?}", x),
            Self::Literal(x) => write!(f, "{:?}", x),
            Self::FunctionCall(x) => write!(f, "{:?}", x),
            Self::FunctionCallWithLambda(x) => write!(f, "{:?}", x),
            Self::AggCall(x) => write!(f, "{:?}", x),
            Self::Subquery(x) => write!(f, "{:?}", x),
            Self::CorrelatedInputRef(x) => write!(f, "{:?}", x),
            Self::TableFunction(x) => write!(f, "{:?}", x),
            Self::WindowFunction(x) => write!(f, "{:?}", x),
            Self::UserDefinedFunction(x) => write!(f, "{:?}", x),
            Self::Parameter(x) => write!(f, "{:?}", x),
            Self::Now(x) => write!(f, "{:?}", x),
            Self::SecretRef(x) => write!(f, "Secret({})", x.secret_name),
        }
    }
}

pub struct ExprDisplay<'a> {
    pub expr: &'a ExprImpl,
    pub input_schema: &'a Schema,
}

impl std::fmt::Debug for ExprDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let that = self.expr;
        match that {
            ExprImpl::InputRef(x) => write!(
                f,
                "{:?}",
                InputRefDisplay {
                    input_ref: x,
                    input_schema: self.input_schema
                }
            ),
            ExprImpl::Literal(x) => write!(f, "{:?}", x),
            ExprImpl::FunctionCall(x) => write!(
                f,
                "{:?}",
                FunctionCallDisplay {
                    function_call: x,
                    input_schema: self.input_schema
                }
            ),
            ExprImpl::FunctionCallWithLambda(x) => write!(
                f,
                "{:?}",
                FunctionCallDisplay {
                    function_call: &x.to_full_function_call(),
                    input_schema: self.input_schema
                }
            ),
            ExprImpl::AggCall(x) => write!(f, "{:?}", x),
            ExprImpl::Subquery(x) => write!(f, "{:?}", x),
            ExprImpl::CorrelatedInputRef(x) => write!(f, "{:?}", x),
            ExprImpl::TableFunction(x) => {
                // TODO: TableFunctionCallVerboseDisplay
                write!(f, "{:?}", x)
            }
            ExprImpl::WindowFunction(x) => {
                // TODO: WindowFunctionCallVerboseDisplay
                write!(f, "{:?}", x)
            }
            ExprImpl::UserDefinedFunction(x) => {
                write!(
                    f,
                    "{:?}",
                    UserDefinedFunctionDisplay {
                        func_call: x,
                        input_schema: self.input_schema
                    }
                )
            }
            ExprImpl::Parameter(x) => write!(f, "{:?}", x),
            ExprImpl::Now(x) => write!(f, "{:?}", x),
            ExprImpl::SecretRef(x) => write!(f, "Secret({})", x.secret_name),
        }
    }
}

impl std::fmt::Display for ExprDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (self as &dyn std::fmt::Debug).fmt(f)
    }
}

#[cfg(test)]
/// Asserts that the expression is an [`InputRef`] with the given index.
macro_rules! assert_eq_input_ref {
    ($e:expr, $index:expr) => {
        match $e {
            ExprImpl::InputRef(i) => assert_eq!(i.index(), $index),
            _ => assert!(false, "Expected input ref, found {:?}", $e),
        }
    };
}

#[cfg(test)]
pub(crate) use assert_eq_input_ref;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::row::OwnedRow;

use crate::utils::Condition;

#[cfg(test)]
mod tests {
    use risingwave_pb::secret::secret_ref::RefAsType;

    use super::*;

    #[test]
    fn test_expr_debug_alternate() {
        let mut e = InputRef::new(1, DataType::Boolean).into();
        e = FunctionCall::new(ExprType::Not, vec![e]).unwrap().into();
        let s = format!("{:#?}", e);
        assert!(s.contains("return_type: Boolean"))
    }

    #[test]
    fn test_secret_ref_display_contains_name() {
        let expr = ExprImpl::SecretRef(Box::new(SecretRef {
            secret_id: 42.into(),
            ref_as: RefAsType::Text,
            secret_name: "test_secret".to_owned(),
        }));

        assert_eq!(format!("{expr:?}"), "Secret(test_secret)");
        assert_eq!(
            format!(
                "{:?}",
                ExprDisplay {
                    expr: &expr,
                    input_schema: Schema::empty(),
                }
            ),
            "Secret(test_secret)"
        );
        assert!(format!("{expr:#?}").contains("test_secret"));
    }
}
