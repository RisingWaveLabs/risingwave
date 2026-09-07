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

//! Conversion functions from RisingWave function calls to DataFusion expressions.

use std::sync::Arc;

use datafusion::functions::{core, datetime, math, string, unicode};
use datafusion::functions_nested::{
    array_has, cardinality, concat, extract, flatten, length, make_array, min_max, position,
    remove, replace, reverse, sort, string as nested_string,
};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    self, BinaryExpr, Case, Cast, Operator, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::Expr as DFExpr;
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType as RwDataType;
use risingwave_expr::expr::build_from_prost;

use super::data_type_ext::RwDataTypeDataFusionExt;
use super::rw_scalar_function::RwScalarFunction;
use crate::datafusion::{CastExecutor, ColumnTrait, convert_expr};
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprType, FunctionCall};

/// Converts a RisingWave [`FunctionCall`] into a DataFusion expression.
///
/// This function tries multiple conversion strategies in order:
/// 1. Unary functions (NOT, IS NULL, etc.)
/// 2. Binary functions (arithmetic, comparison, logical)
/// 3. Case expressions
/// 4. Cast expressions
/// 5. Field access (struct fields)
/// 6. Row construction
/// 7. Trivial DataFusion functions (math, string, datetime)
/// 8. Fallback to RisingWave expression wrapper
pub fn convert_function_call(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> RwResult<DFExpr> {
    macro_rules! try_rules {
        ( $($func:ident),* ) => {
            $(
                if let Some(df_expr) = $func(func_call, input_columns) {
                    return Ok(df_expr);
                }
            )*
        };
    }
    try_rules!(
        convert_unary_func,
        convert_binary_func,
        convert_case_func,
        convert_cast_func,
        convert_trivial_datafusion_func,
        convert_field_func,
        convert_row_func,
        convert_in_list_func,
        convert_list_ops_func,
        fallback_rw_expr_builder
    );

    bail_not_implemented!(
        "DataFusionPlanConverter: unsupported function call expression {:?}",
        func_call
    );
}

fn convert_unary_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.inputs().len() != 1 {
        return None;
    }

    macro_rules! match_unary_expr {
        ($arg:expr, $(($func_type:ident, $df_expr_variant:ident)),*) => {
            match $arg {
                $(
                ExprType::$func_type => {
                    let arg = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
                    Some(DFExpr::$df_expr_variant(Box::new(arg)))
                }
                )*
                _ => None
            }
        };
    }
    match_unary_expr!(
        func_call.func_type(),
        (Not, Not),
        (IsNotNull, IsNotNull),
        (IsNull, IsNull),
        (IsTrue, IsTrue),
        (IsFalse, IsFalse),
        (Neg, Negative)
    )
}

fn convert_binary_op(expr_type: ExprType) -> Option<Operator> {
    macro_rules! match_binary_op {
        ( $(($rw_type:ident, $df_type:ident)),* ) => {
            match expr_type {
                $(
                    ExprType::$rw_type => Some(Operator::$df_type),
                )*
                _ => None
            }
        };
    }
    match_binary_op!(
        (Equal, Eq),
        (NotEqual, NotEq),
        (LessThan, Lt),
        (LessThanOrEqual, LtEq),
        (GreaterThan, Gt),
        (GreaterThanOrEqual, GtEq),
        (Add, Plus),
        (Subtract, Minus),
        (Multiply, Multiply),
        (Divide, Divide),
        (Modulus, Modulo),
        (And, And),
        (Or, Or),
        (IsDistinctFrom, IsDistinctFrom),
        // datafusion's regex misses some features, waiting https://github.com/apache/datafusion/issues/18778
        // (RegexpMatch, RegexMatch),
        (Like, LikeMatch),
        (ILike, ILikeMatch),
        (BitwiseAnd, BitwiseAnd),
        (BitwiseOr, BitwiseOr),
        (BitwiseXor, BitwiseXor),
        (BitwiseShiftRight, BitwiseShiftRight),
        (BitwiseShiftLeft, BitwiseShiftLeft)
    )
}

fn convert_binary_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.inputs().len() != 2 {
        return None;
    }
    if !func_call
        .inputs()
        .iter()
        .all(|input| input.return_type().is_datafusion_native())
    {
        return None;
    }

    let op = convert_binary_op(func_call.func_type())?;
    let left = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
    let right = convert_expr(&func_call.inputs()[1], input_columns).ok()?;
    Some(DFExpr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn convert_case_func(func_call: &FunctionCall, input_columns: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.func_type() != ExprType::Case {
        return None;
    }

    let mut args = func_call
        .inputs()
        .iter()
        .map(|arg| {
            let expr = convert_expr(arg, input_columns).ok()?;
            Some(Box::new(expr))
        })
        .collect::<Option<Vec<_>>>()?;

    let else_expr = if args.len() % 2 == 1 {
        Some(args.pop().unwrap())
    } else {
        None
    };
    let when_then_expr = args.into_iter().tuples().collect();
    Some(DFExpr::Case(Case {
        expr: None,
        when_then_expr,
        else_expr,
    }))
}

fn convert_cast_func(func_call: &FunctionCall, input_columns: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.inputs().len() != 1 {
        return None;
    }
    if func_call.func_type() != ExprType::Cast {
        return None;
    }

    let rw_from_type = func_call.inputs()[0].return_type();
    let rw_to_type = func_call.return_type();
    if !can_cast_by_datafusion(&rw_from_type, &rw_to_type) {
        return None;
    }

    let arg = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
    let target_type = IcebergArrowConvert
        .to_arrow_field("", &func_call.return_type())
        .ok()?
        .data_type()
        .clone();
    Some(DFExpr::Cast(Cast {
        expr: Box::new(arg),
        data_type: target_type,
    }))
}

// TODO: verify more cast safety cases
// case 1: both from and to are datafusion native types
fn can_cast_by_datafusion(from: &RwDataType, to: &RwDataType) -> bool {
    from.is_datafusion_native() && to.is_datafusion_native()
}

/// Converts struct field access expression.
///
/// RW uses index-based access `field(struct, int4)`, while DF uses name-based access
/// `get_field(struct, field_name)`. We look up the field name from the struct type at the given index.
fn convert_field_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.func_type() != ExprType::Field {
        return None;
    }
    if func_call.inputs().len() != 2 {
        return None;
    }

    let struct_expr = &func_call.inputs()[0];
    let index_expr = &func_call.inputs()[1];
    let struct_type = match struct_expr.return_type() {
        RwDataType::Struct(s) => s,
        _ => return None,
    };
    if !struct_type.types().all(|ty| ty.is_datafusion_native()) {
        return None;
    }

    let index = match index_expr {
        crate::expr::ExprImpl::Literal(lit) => *lit.get_data().as_ref()?.as_int32() as usize,
        _ => return None,
    };
    let field_name = struct_type.names().nth(index)?;

    let df_struct_expr = convert_expr(struct_expr, input_columns).ok()?;
    let args = vec![
        df_struct_expr,
        DFExpr::Literal(
            datafusion_common::ScalarValue::Utf8(Some(field_name.to_owned())),
            None,
        ),
    ];

    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: core::get_field(),
        args,
    }))
}

/// Converts row construction expression.
///
/// RW uses field names `f1`, `f2`, etc., while DF's `struct()` uses `c0`, `c1`, etc.
/// We use DF's `named_struct()` to preserve the RW field naming convention.
fn convert_row_func(func_call: &FunctionCall, input_columns: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.func_type() != ExprType::Row {
        return None;
    }
    if func_call.inputs().is_empty() {
        return None;
    }

    if !func_call
        .inputs()
        .iter()
        .all(|input| input.return_type().is_datafusion_native())
    {
        return None;
    }
    if !func_call.return_type().is_datafusion_native() {
        return None;
    }

    // Build named_struct args: [name1, expr1, name2, expr2, ...]
    // RW uses f1, f2, f3, ... as field names for unnamed structs
    let mut args = Vec::with_capacity(func_call.inputs().len() * 2);
    for (i, input) in func_call.inputs().iter().enumerate() {
        // Field name: f1, f2, f3, ... (1-indexed to match RW convention)
        args.push(DFExpr::Literal(
            datafusion_common::ScalarValue::Utf8(Some(format!("f{}", i + 1))),
            None,
        ));
        args.push(convert_expr(input, input_columns).ok()?);
    }

    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: core::named_struct(),
        args,
    }))
}

fn convert_in_list_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.func_type() != ExprType::In {
        return None;
    }
    if func_call.inputs().len() < 2 {
        return None;
    }

    let left = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
    let list = func_call.inputs()[1..]
        .iter()
        .map(|input| convert_expr(input, input_columns).ok())
        .collect::<Option<Vec<_>>>()?;

    Some(logical_expr::in_list(left, list, false))
}

/// Array functions that only depend on list structure (length, cardinality), not
/// element type. These can be mapped to DataFusion even when the list element type is not
/// datafusion-native (e.g. list of jsonb), as long as the list can be represented in Arrow.
fn convert_list_ops_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    let udf_impl: Arc<ScalarUDF> = match func_call.func_type() {
        ExprType::ArrayLength => length::array_length_udf(),
        ExprType::Cardinality => cardinality::cardinality_udf(),
        _ => return None,
    };

    let inputs = func_call.inputs();
    if inputs.is_empty() {
        return None;
    }
    if !matches!(inputs[0].return_type(), RwDataType::List(_)) {
        return None;
    }
    if !func_call.return_type().is_datafusion_native() {
        return None;
    }

    let args = func_call
        .inputs()
        .iter()
        .map(|input| convert_expr(input, input_columns).ok())
        .collect::<Option<Vec<_>>>()?;

    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: udf_impl,
        args,
    }))
}

fn pow_type_mismatch(base: &RwDataType, exp: &RwDataType) -> bool {
    matches!(
        base,
        RwDataType::Float32 | RwDataType::Float64 | RwDataType::Decimal
    ) || matches!(
        exp,
        RwDataType::Float32 | RwDataType::Float64 | RwDataType::Decimal
    )
}

fn array_to_string_type_mismatch(ty: &RwDataType) -> bool {
    match ty {
        RwDataType::List(list) => matches!(
            list.elem(),
            &RwDataType::Float32 | &RwDataType::Float64 | &RwDataType::Decimal
        ),
        _ => false,
    }
}

fn convert_trivial_datafusion_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    let func_type = func_call.func_type();
    let inputs = func_call.inputs();
    let return_type = func_call.return_type();

    if !inputs
        .iter()
        .all(|input| input.return_type().is_datafusion_native())
    {
        return None;
    }
    if !return_type.is_datafusion_native() {
        return None;
    }

    let udf_impl: Arc<ScalarUDF> = match func_type {
        // Math functions
        //
        // Note: `round` is intentionally NOT mapped because RisingWave uses banker's rounding
        // (round_ties_even) while DataFusion uses standard rounding (round ties away from zero).
        // Example: round(2.5) -> RW: 2.0, DF: 3.0
        //
        // Edge case differences for error handling (RW returns error, DF returns NaN/inf):
        // - sqrt: negative input -> RW: error, DF: NaN
        // - ln/log10: input <= 0 -> RW: error, DF: NaN/-inf
        // - exp: extreme values -> RW: error, DF: inf/0
        ExprType::Abs => math::abs(),
        ExprType::Ceil => math::ceil(),
        ExprType::Floor => math::floor(),
        ExprType::Trunc => math::trunc(),
        // - pow: DataFusion doesn't support power which exp is not integer. And pow(decimal, integer) is easy to overflow because it will keep the precision, e.x. pow(2::decimal(38, 10), 4) will calculate (2e10 ^ 4) / (1e10 ^ 4) and 2e10 ^ 4 = 1.6e41 has exceed the max precision of decimal(38, 10).
        ExprType::Pow if !pow_type_mismatch(&inputs[0].return_type(), &inputs[1].return_type()) => {
            math::power()
        }
        ExprType::Exp => math::exp(),
        ExprType::Sin => math::sin(),
        ExprType::Cos => math::cos(),
        ExprType::Tan => math::tan(),
        ExprType::Cot => math::cot(),
        ExprType::Asin => math::asin(),
        ExprType::Acos => math::acos(),
        ExprType::Atan => math::atan(),
        ExprType::Atan2 => math::atan2(),
        ExprType::Sqrt => math::sqrt(),
        ExprType::Degrees => math::degrees(),
        ExprType::Radians => math::radians(),
        ExprType::Sinh => math::sinh(),
        ExprType::Cosh => math::cosh(),
        ExprType::Tanh => math::tanh(),
        ExprType::Asinh => math::asinh(),
        ExprType::Acosh => math::acosh(),
        ExprType::Atanh => math::atanh(),
        ExprType::Ln => math::ln(),
        ExprType::Log10 => math::log10(),
        // Date/time functions
        //
        // Note: The following functions are NOT mapped due to incompatibilities:
        // - date_part/extract: Return type differs (RW: Decimal/Float64 with subsecond precision,
        //   DF: Int32 for most fields). Example: second -> RW: 57.123456, DF: 57
        // - make_time: Signature differs (RW: float8 for seconds with nanosecond precision,
        //   DF: int32 for seconds only)
        // - to_timestamp/from_unixtime: Different signatures and timezone handling
        // - to_char/to_date: Format string syntax differs - RW uses PostgreSQL patterns (YYYY-MM-DD),
        //   DF uses Chrono patterns (%Y-%m-%d). Same format string produces different results.
        // - date_trunc: RW supports 'decade', 'century', 'millennium' granularities, DF doesn't. And RW deals with timezone differently.
        // - date_bin: Iceberg doesn't support interval type.
        //
        // Edge case differences (acceptable):
        // - make_date: BC year handling differs (RW adjusts negative years, DF doesn't)
        ExprType::MakeDate => datetime::make_date(),
        // String functions
        //
        // Note: The following functions are NOT mapped due to incompatibilities:
        // - upper/lower: RW uses ASCII-only conversion (to_ascii_uppercase/lowercase),
        //   DF uses full Unicode (to_uppercase/lowercase). Example: 'Ångström' -> RW: 'ÅNGSTRÖM', DF: 'ÅNGSTRÖM' with proper Unicode
        // - initcap: RW uses whitespace as word boundary, DF uses non-alphanumeric characters
        //
        // Edge case differences (acceptable):
        // - chr: RW returns empty for invalid code points, DF returns '\u{0000}' for 0
        ExprType::Ascii => string::ascii(),
        ExprType::CharLength => unicode::character_length(),
        ExprType::Chr => string::chr(),
        ExprType::Repeat => string::repeat(),
        ExprType::Replace => string::replace(),
        ExprType::Reverse => unicode::reverse(),
        ExprType::Translate => unicode::translate(),
        ExprType::Trim => string::btrim(),
        ExprType::Ltrim => string::ltrim(),
        ExprType::Rtrim => string::rtrim(),
        ExprType::StartsWith => string::starts_with(),
        ExprType::SplitPart => string::split_part(),
        ExprType::Position => unicode::strpos(),
        ExprType::Lpad => unicode::lpad(),
        ExprType::Rpad => unicode::rpad(),
        ExprType::Substr => unicode::substr(),
        ExprType::Left => unicode::left(),
        ExprType::Right => unicode::right(),
        // Array functions
        //
        // Note: The following functions are NOT mapped due to incompatibilities:
        // - Row — struct construction; handled by convert_row_func.
        // - FormatType — RW catalog/type name helper; no DF equivalent.
        // - TrimArray — RW trim_array(array, n) returns first length-n elements; DF has array_resize with different signature/semantics.
        // - ArrayTransform — lambda-based; DF has no direct equivalent in scalar UDF form.
        // - ArraySum — DF nested_expressions has no array_sum.
        // - ArrayDims — RW array_dims returns text, DF array_dims returns int32.
        // - ArrayDistinct — DF will make extra sort for distinct, which is different from RW.
        ExprType::Array => make_array::make_array_udf(),
        ExprType::ArrayAccess => extract::array_element_udf(),
        ExprType::ArrayRangeAccess => extract::array_slice_udf(),
        ExprType::ArrayCat => concat::array_concat_udf(),
        ExprType::ArrayAppend => concat::array_append_udf(),
        ExprType::ArrayPrepend => concat::array_prepend_udf(),
        ExprType::ArrayRemove => remove::array_remove_udf(),
        ExprType::ArrayPositions => position::array_positions_udf(),
        ExprType::StringToArray => nested_string::string_to_array_udf(),
        ExprType::ArrayPosition => position::array_position_udf(),
        ExprType::ArrayReplace => replace::array_replace_all_udf(),
        ExprType::ArrayMin => min_max::array_min_udf(),
        ExprType::ArrayMax => min_max::array_max_udf(),
        ExprType::ArraySort => sort::array_sort_udf(),
        ExprType::ArrayContains => array_has::array_has_all_udf(),
        ExprType::ArrayContained => array_has::array_has_all_udf(),
        ExprType::ArrayFlatten => flatten::flatten_udf(),
        ExprType::ArrayReverse => reverse::array_reverse_udf(),
        // ArrayToString: do not map when array element is float/decimal — RW uses ToText, DF uses
        // Arrow formatting; display can differ (precision, exponent).
        ExprType::ArrayToString if !array_to_string_type_mismatch(&inputs[0].return_type()) => {
            nested_string::array_to_string_udf()
        }
        // Misc functions
        ExprType::Coalesce => core::coalesce(),
        _ => return None,
    };
    let mut args = func_call
        .inputs()
        .iter()
        .map(|input| convert_expr(input, input_columns).ok())
        .collect::<Option<Vec<_>>>()?;

    // ArrayContained(a, b) = a <@ b means "a contained in b" = array_has_all(b, a); swap args.
    if func_call.func_type() == ExprType::ArrayContained && args.len() == 2 {
        args.swap(0, 1);
    }
    // ArraySort(array) = array_sort(array, ASC, NULLS LAST); datafusion default is ASC, NULLS FIRST.
    if func_call.func_type() == ExprType::ArraySort && args.len() == 1 {
        args.push(DFExpr::Literal(
            datafusion_common::ScalarValue::Utf8(Some("ASC".to_owned())),
            None,
        ));
        args.push(DFExpr::Literal(
            datafusion_common::ScalarValue::Utf8(Some("NULLS LAST".to_owned())),
            None,
        ));
    }

    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: udf_impl,
        args,
    }))
}

fn fallback_rw_expr_builder(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    // A variant result would cross the RW -> Arrow boundary at runtime, which is unsupported;
    // refuse so the query falls back to the RisingWave engine at plan time.
    if func_call.return_type().contains_variant() {
        return None;
    }

    tracing::warn!(
        "Falling back to RwScalarFunction for function call, it may have performance impact: {:?}",
        func_call
    );

    let cast = CastExecutor::from_iter(
        (0..input_columns.len()).map(|i| input_columns.df_data_type(i)),
        (0..input_columns.len()).map(|i| input_columns.rw_data_type(i)),
    )
    .ok()?;
    let boxed_expr = build_from_prost(&func_call.try_to_expr_proto().ok()?).ok()?;

    let column_name = (0..input_columns.len())
        .map(|i| input_columns.column(i).flat_name())
        .collect::<Vec<_>>();

    let volatility = match func_call.is_pure() {
        true => Volatility::Immutable,
        false => Volatility::Volatile,
    };
    let func = RwScalarFunction {
        name: format!("RisingWave Scalar Function ({:?})", func_call),
        column_name,
        cast,
        expr: boxed_expr,
        signature: Signature {
            type_signature: TypeSignature::Any(input_columns.len()),
            volatility,
            parameter_names: None,
        },
    };
    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: Arc::new(ScalarUDF::new_from_impl(func)),
        args: (0..input_columns.len())
            .map(|i| DFExpr::Column(input_columns.column(i)))
            .collect(),
    }))
}
