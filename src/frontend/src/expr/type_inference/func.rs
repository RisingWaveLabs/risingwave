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

use itertools::Itertools as _;
use num_integer::Integer as _;
use risingwave_common::bail_no_function;
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::{DataType, StructType};
use risingwave_common::util::iter_util::ZipEqFast;
pub use risingwave_expr::sig::*;
use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
use risingwave_pb::expr::table_function::PbType as PbTableFuncType;

use super::{CastContext, align_types, cast_ok_base};
use crate::error::{ErrorCode, Result};
use crate::expr::type_inference::cast::align_array_and_element;
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall, cast_ok, is_row_function};

/// Infers the return type of a function. Returns `Err` if the function with specified data types
/// is not supported on backend.
///
/// It also mutates the `inputs` by adding necessary casts.
#[tracing::instrument(level = "trace", skip(sig_map))]
pub fn infer_type_with_sigmap(
    func_name: FuncName,
    inputs: &mut [ExprImpl],
    sig_map: &FunctionRegistry,
) -> Result<DataType> {
    // special cases
    match &func_name {
        FuncName::Scalar(func_type) => {
            if let Some(res) = infer_type_for_special(*func_type, inputs).transpose() {
                return res;
            }
        }
        FuncName::Table(func_type) => {
            if let Some(res) = infer_type_for_special_table_function(*func_type, inputs).transpose()
            {
                return res;
            }
        }
        FuncName::Aggregate(agg_kind) if *agg_kind == PbAggKind::Grouping => {
            return Ok(DataType::Int32);
        }
        _ => {}
    }

    let actuals = inputs
        .iter()
        .map(|e| match e.is_untyped() {
            true => None,
            false => Some(e.return_type()),
        })
        .collect_vec();
    let sig = infer_type_name(sig_map, func_name, &actuals)?;
    tracing::trace!(?actuals, ?sig, "infer_type_name");

    // add implicit casts to inputs
    for (expr, t) in inputs.iter_mut().zip_eq_fast(&sig.inputs_type) {
        if expr.is_untyped() || !t.matches(&expr.return_type()) {
            if let SigDataType::Exact(t) = t {
                expr.cast_implicit_mut(t)?;
            } else {
                return Err(ErrorCode::BindError(format!(
                    "Cannot implicitly cast '{expr:?}' to polymorphic type {t:?}",
                ))
                .into());
            }
        }
    }

    let input_types = inputs.iter().map(|expr| expr.return_type()).collect_vec();
    let return_type = (sig.type_infer)(&input_types)?;
    tracing::trace!(?input_types, ?return_type, "finished type inference");
    Ok(return_type)
}

pub fn infer_type(func_name: FuncName, inputs: &mut [ExprImpl]) -> Result<DataType> {
    infer_type_with_sigmap(func_name, inputs, &FUNCTION_REGISTRY)
}

pub fn infer_some_all(
    mut func_types: Vec<ExprType>,
    inputs: &mut Vec<ExprImpl>,
) -> Result<DataType> {
    let element_type = if inputs[1].is_untyped() {
        None
    } else if let DataType::List(list) = inputs[1].return_type() {
        Some(list.into_elem())
    } else {
        return Err(ErrorCode::BindError(
            "op SOME/ANY/ALL (array) requires array on right side".to_owned(),
        )
        .into());
    };

    let final_type = func_types.pop().unwrap();
    let actuals = vec![
        (!inputs[0].is_untyped()).then_some(inputs[0].return_type()),
        element_type.clone(),
    ];
    let sig = infer_type_name(&FUNCTION_REGISTRY, final_type.into(), &actuals)?;
    if sig.ret_type != DataType::Boolean.into() {
        return Err(ErrorCode::BindError(format!(
            "op SOME/ANY/ALL (array) requires operator to yield boolean, but got {}",
            sig.ret_type
        ))
        .into());
    }
    if !sig.inputs_type[0].matches(&inputs[0].return_type()) {
        let SigDataType::Exact(t) = &sig.inputs_type[0] else {
            return Err(ErrorCode::BindError(
                "array of array/struct on right are not supported yet".into(),
            )
            .into());
        };
        inputs[0].cast_implicit_mut(t)?;
    }
    if !matches!(&element_type, Some(e) if sig.inputs_type[1].matches(e)) {
        let SigDataType::Exact(t) = &sig.inputs_type[1] else {
            return Err(
                ErrorCode::BindError("array/struct on left are not supported yet".into()).into(),
            );
        };
        inputs[1].cast_implicit_mut(&DataType::list(t.clone()))?;
    }

    let inputs_owned = std::mem::take(inputs);
    let mut func_call =
        FunctionCall::new_unchecked(final_type, inputs_owned, DataType::Boolean).into();
    while let Some(func_type) = func_types.pop() {
        func_call = FunctionCall::new(func_type, vec![func_call])?.into();
    }
    let return_type = func_call.return_type();
    *inputs = vec![func_call];
    Ok(return_type)
}

macro_rules! ensure_arity {
    ($func:expr, $lower:literal <= | $inputs:ident | <= $upper:literal) => {
        if !($lower <= $inputs.len() && $inputs.len() <= $upper) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes {} to {} arguments ({} given)",
                $func,
                $lower,
                $upper,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:expr, $lower:literal <= | $inputs:ident |) => {
        if !($lower <= $inputs.len()) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes at least {} arguments ({} given)",
                $func,
                $lower,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:expr, | $inputs:ident | == $num:literal) => {
        if !($inputs.len() == $num) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes {} arguments ({} given)",
                $func,
                $num,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:expr, | $inputs:ident | <= $upper:literal) => {
        if !($inputs.len() <= $upper) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes at most {} arguments ({} given)",
                $func,
                $upper,
                $inputs.len(),
            ))
            .into());
        }
    };
}

/// An intermediate representation of struct type when resolving the type to cast.
#[derive(Debug)]
pub enum NestedType {
    /// A type that can should inferred (will never be struct).
    Infer(DataType),
    /// A concrete data type.
    Type(DataType),
    /// A struct type.
    Struct(Vec<NestedType>),
}

/// Convert struct type to a nested type
fn extract_struct_nested_type(ty: &StructType) -> Result<NestedType> {
    let fields = ty
        .types()
        .map(|f| match f {
            DataType::Struct(s) => extract_struct_nested_type(s),
            _ => Ok(NestedType::Type(f.clone())),
        })
        .try_collect()?;
    Ok(NestedType::Struct(fields))
}

/// Decompose expression into a nested type to be inferred.
fn extract_expr_nested_type(expr: &ExprImpl) -> Result<NestedType> {
    if expr.is_untyped() {
        Ok(NestedType::Infer(expr.return_type()))
    } else if is_row_function(expr) {
        // For row function, recursively get the type requirement of each field.
        let func = expr.as_function_call().unwrap();
        let ret = func
            .inputs()
            .iter()
            .map(extract_expr_nested_type)
            .collect::<Result<Vec<_>>>()?;
        Ok(NestedType::Struct(ret))
    } else {
        match expr.return_type() {
            DataType::Struct(ty) => extract_struct_nested_type(&ty),
            ty => Ok(NestedType::Type(ty)),
        }
    }
}

/// Handle struct comparisons for [`infer_type_for_special`].
fn infer_struct_cast_target_type(
    func_type: ExprType,
    lexpr: NestedType,
    rexpr: NestedType,
) -> Result<(bool, bool, DataType)> {
    match (lexpr, rexpr) {
        (NestedType::Struct(lty), NestedType::Struct(rty)) => {
            // If both sides are structs, resolve the final data type recursively.
            if lty.len() != rty.len() {
                return Err(ErrorCode::BindError(format!(
                    "cannot infer type because of different number of fields: left={:?} right={:?}",
                    lty, rty
                ))
                .into());
            }
            let mut tys = vec![];
            let mut lcasts = false;
            let mut rcasts = false;
            tys.reserve(lty.len());
            for (lf, rf) in lty.into_iter().zip_eq_fast(rty) {
                let (lcast, rcast, ty) = infer_struct_cast_target_type(func_type, lf, rf)?;
                lcasts |= lcast;
                rcasts |= rcast;
                tys.push(("".to_owned(), ty)); // TODO(chi): generate field name
            }
            Ok((lcasts, rcasts, DataType::Struct(StructType::new(tys))))
        }
        (l, r @ NestedType::Struct(_)) | (l @ NestedType::Struct(_), r) => {
            // If only one side is nested type, these two types can never be casted.
            Err(ErrorCode::BindError(format!(
                "cannot infer type because unmatched types: left={:?} right={:?}",
                l, r
            ))
            .into())
        }
        (NestedType::Type(l), NestedType::Type(r)) => {
            // If both sides are concrete types, try cast in either direction.
            if l == r {
                Ok((false, false, r))
            } else if cast_ok(&l, &r, CastContext::Implicit) {
                Ok((true, false, r))
            } else if cast_ok(&r, &l, CastContext::Implicit) {
                Ok((false, true, l))
            } else {
                Err(
                    ErrorCode::BindError(format!("cannot cast {} to {} or {} to {}", l, r, r, l))
                        .into(),
                )
            }
        }
        (NestedType::Type(ty), NestedType::Infer(ity)) => {
            // If one side is *unknown*, cast to another type.
            Ok((false, ity != ty, ty))
        }
        (NestedType::Infer(ity), NestedType::Type(ty)) => {
            // If one side is *unknown*, cast to another type.
            Ok((ity != ty, false, ty))
        }
        (NestedType::Infer(l), NestedType::Infer(r)) => {
            // Both sides are *unknown*, using the sig_map to infer the return type.
            let actuals = vec![None, None];
            let sig = infer_type_name(&FUNCTION_REGISTRY, func_type.into(), &actuals)?;
            Ok((
                sig.ret_type != l.into(),
                sig.ret_type != r.into(),
                sig.ret_type.as_exact().clone(),
            ))
        }
    }
}

/// Special exprs that cannot be handled by [`infer_type_name`] and [`FunctionRegistry`] are handled here.
/// These include variadic functions, list and struct type, as well as non-implicit cast.
///
/// We should aim for enhancing the general inferring framework and reduce the special cases here.
///
/// Returns:
/// * `Err` when we are sure this expr is invalid
/// * `Ok(Some(t))` when the return type should be `t` under these special rules
/// * `Ok(None)` when no special rule matches and it should try general rules later
fn infer_type_for_special(
    func_type: ExprType,
    inputs: &mut [ExprImpl],
) -> Result<Option<DataType>> {
    match func_type {
        ExprType::Case => {
            let len = inputs.len();
            align_types(inputs.iter_mut().enumerate().filter_map(|(i, e)| {
                // `Case` organize `inputs` as (cond, res) pairs with a possible `else` res at
                // the end. So we align exprs at odd indices as well as the last one when length
                // is odd.
                match i.is_odd() || len.is_odd() && i == len - 1 {
                    true => Some(e),
                    false => None,
                }
            }))
            .map(Some)
            .map_err(Into::into)
        }
        ExprType::ConstantLookup => {
            let len = inputs.len();
            align_types(inputs.iter_mut().enumerate().filter_map(|(i, e)| {
                // This optimized `ConstantLookup` organize `inputs` as
                // [dummy_expression] (cond, res) [else / fallback]? pairs.
                // So we align exprs at even indices as well as the last one
                // when length is odd.
                match i != 0 && i.is_even() || i == len - 1 {
                    true => Some(e),
                    false => None,
                }
            }))
            .map(Some)
            .map_err(Into::into)
        }
        ExprType::In => {
            align_types(inputs.iter_mut())?;
            Ok(Some(DataType::Boolean))
        }
        ExprType::Coalesce => {
            ensure_arity!("coalesce", 1 <= | inputs |);
            align_types(inputs.iter_mut()).map(Some).map_err(Into::into)
        }
        ExprType::Concat => {
            ensure_arity!("concat", 1 <= | inputs |);
            Ok(Some(DataType::Varchar))
        }
        ExprType::ConcatWs => {
            ensure_arity!("concat_ws", 2 <= | inputs |);
            // 0-th arg must be string
            inputs[0].cast_implicit_mut(&DataType::Varchar)?;
            for input in inputs.iter_mut().skip(1) {
                // subsequent can be any type, using the output format
                let owned = input.take();
                *input = owned.cast_output()?;
            }
            Ok(Some(DataType::Varchar))
        }
        ExprType::ConcatOp => {
            for input in inputs {
                input.cast_explicit_mut(&DataType::Varchar)?;
            }
            Ok(Some(DataType::Varchar))
        }
        ExprType::Format => {
            ensure_arity!("format", 1 <= | inputs |);
            // 0-th arg must be string
            inputs[0].cast_implicit_mut(&DataType::Varchar)?;
            for input in inputs.iter_mut().skip(1) {
                // subsequent can be any type, using the output format
                let owned = input.take();
                *input = owned.cast_output()?;
            }
            Ok(Some(DataType::Varchar))
        }
        ExprType::Equal
        | ExprType::NotEqual
        | ExprType::LessThan
        | ExprType::LessThanOrEqual
        | ExprType::GreaterThan
        | ExprType::GreaterThanOrEqual
        | ExprType::IsDistinctFrom
        | ExprType::IsNotDistinctFrom => {
            ensure_arity!("cmp", | inputs | == 2);
            match (inputs[0].is_untyped(), inputs[1].is_untyped()) {
                // `'a' = null` handled by general rules later
                (true, true) => return Ok(None),
                // `null = array[1]` where null should have same type as right side
                // `null = 1` can use the general rule, but return `Ok(None)` here is less readable
                (true, false) => {
                    let t = inputs[1].return_type();
                    inputs[0].cast_implicit_mut(&t)?;
                    return Ok(Some(DataType::Boolean));
                }
                (false, true) => {
                    let t = inputs[0].return_type();
                    inputs[1].cast_implicit_mut(&t)?;
                    return Ok(Some(DataType::Boolean));
                }
                // Types of both sides are known. Continue.
                (false, false) => {}
            }
            let ok = match (inputs[0].return_type(), inputs[1].return_type()) {
                // Cast each field of the struct to their corresponding field in the other struct.
                (DataType::Struct(_), DataType::Struct(_)) => {
                    let (lcast, rcast, ret) = infer_struct_cast_target_type(
                        func_type,
                        extract_expr_nested_type(&inputs[0])?,
                        extract_expr_nested_type(&inputs[1])?,
                    )?;
                    if lcast {
                        inputs[0].cast_implicit_mut(&ret)?;
                    }
                    if rcast {
                        inputs[1].cast_implicit_mut(&ret)?;
                    }
                    true
                }
                // Unlike auto-cast in struct, PostgreSQL disallows `int[] = bigint[]` for array.
                // They have to match exactly.
                (l @ DataType::List { .. }, r @ DataType::List { .. }) => l == r,
                // use general rule unless `struct = struct` or `array = array`
                _ => return Ok(None),
            };
            if ok {
                Ok(Some(DataType::Boolean))
            } else {
                Err(ErrorCode::BindError(format!(
                    "cannot compare {} and {}",
                    inputs[0].return_type(),
                    inputs[1].return_type()
                ))
                .into())
            }
        }
        ExprType::ArrayCat => {
            ensure_arity!("array_cat", | inputs | == 2);
            let left_type = (!inputs[0].is_untyped()).then(|| inputs[0].return_type());
            let right_type = (!inputs[1].is_untyped()).then(|| inputs[1].return_type());
            let return_type = match (left_type, right_type) {
                (None, t @ None)
                | (None, t @ Some(DataType::List(_)))
                | (t @ Some(DataType::List(_)), None) => {
                    // when neither type is available, default to `varchar[]`
                    // when one side is unknown and other side is list, use that list type
                    let t = t.unwrap_or_else(|| DataType::Varchar.list());
                    for input in &mut *inputs {
                        input.cast_implicit_mut(&t)?;
                    }
                    Some(t)
                }
                (Some(DataType::List(_)), Some(DataType::List(_))) => {
                    align_types(inputs.iter_mut())
                        .or_else(|_| align_array_and_element(0, &[1], inputs))
                        .or_else(|_| align_array_and_element(1, &[0], inputs))
                        .ok()
                }
                // else either side is a known non-list type
                _ => None,
            };
            Ok(Some(return_type.ok_or_else(|| {
                ErrorCode::BindError(format!(
                    "Cannot concatenate {} and {}",
                    inputs[0].return_type(),
                    inputs[1].return_type()
                ))
            })?))
        }
        ExprType::ArrayAppend => {
            ensure_arity!("array_append", | inputs | == 2);
            let common_type = align_array_and_element(0, &[1], inputs);
            match common_type {
                Ok(casted) => Ok(Some(casted)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot append {} to {}",
                    inputs[1].return_type(),
                    inputs[0].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayPrepend => {
            ensure_arity!("array_prepend", | inputs | == 2);
            let common_type = align_array_and_element(1, &[0], inputs);
            match common_type {
                Ok(casted) => Ok(Some(casted)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot prepend {} to {}",
                    inputs[0].return_type(),
                    inputs[1].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayRemove => {
            ensure_arity!("array_remove", | inputs | == 2);
            let common_type = align_array_and_element(0, &[1], inputs);
            match common_type {
                Ok(casted) => Ok(Some(casted)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot remove {} from {}",
                    inputs[1].return_type(),
                    inputs[0].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayReplace => {
            ensure_arity!("array_replace", | inputs | == 3);
            let common_type = align_array_and_element(0, &[1, 2], inputs);
            match common_type {
                Ok(casted) => Ok(Some(casted)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot replace {} with {} in {}",
                    inputs[1].return_type(),
                    inputs[2].return_type(),
                    inputs[0].return_type(),
                ))
                .into()),
            }
        }
        ExprType::ArrayPosition => {
            ensure_arity!("array_position", 2 <= | inputs | <= 3);
            if let Some(start) = inputs.get_mut(2) {
                start.cast_implicit_mut(&DataType::Int32)?;
            }
            let common_type = align_array_and_element(0, &[1], inputs);
            match common_type {
                Ok(_) => Ok(Some(DataType::Int32)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot get position of {} in {}",
                    inputs[1].return_type(),
                    inputs[0].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayPositions => {
            ensure_arity!("array_positions", | inputs | == 2);
            let common_type = align_array_and_element(0, &[1], inputs);
            match common_type {
                Ok(_) => Ok(Some(DataType::Int32.list())),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot get position of {} in {}",
                    inputs[1].return_type(),
                    inputs[0].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayDims => {
            ensure_arity!("array_dims", | inputs | == 1);
            inputs[0].ensure_array_type()?;

            if let DataType::List(list) = inputs[0].return_type()
                && let DataType::List(_) = list.elem()
            {
                return Err(ErrorCode::BindError(
                    "array_dims for dimensions greater than 1 not supported".into(),
                )
                .into());
            }
            Ok(Some(DataType::Varchar))
        }
        ExprType::ArrayContains | ExprType::ArrayContained | ExprType::ArrayOverlaps => {
            ensure_arity!("array_contains/array_contained/array_overlaps", | inputs | == 2);
            let left_type = (!inputs[0].is_untyped()).then(|| inputs[0].return_type());
            let right_type = (!inputs[1].is_untyped()).then(|| inputs[1].return_type());
            match (left_type, right_type) {
                (None, Some(DataType::List(_))) | (Some(DataType::List(_)), None) => {
                    align_types(inputs.iter_mut())?;
                    Ok(Some(DataType::Boolean))
                }
                (Some(DataType::List(left)), Some(DataType::List(right))) => {
                    // cannot directly cast, find unnest type and judge if they are same type
                    let left = left.elem().unnest_list();
                    let right = right.elem().unnest_list();
                    if left.equals_datatype(right) {
                        Ok(Some(DataType::Boolean))
                    } else {
                        Err(ErrorCode::BindError(format!(
                            "Cannot compare array predicate unnested type {} to unnested type {}",
                            left, right
                        ))
                        .into())
                    }
                }
                // any other condition cannot determine polymorphic type
                _ => Ok(None),
            }
        }
        ExprType::ArraySort => {
            ensure_arity!("array_sort", 1 <= | inputs | <= 3);
            inputs[0].ensure_array_type()?;

            for (index, param) in &[(1, "descending"), (2, "nulls_first")] {
                if let Some(input) = inputs.get_mut(*index) {
                    input.cast_implicit_mut(&DataType::Boolean).map_err(|_| {
                        ErrorCode::BindError(format!(
                            "Cannot cast {} to boolean for parameter {}",
                            input.return_type(),
                            param
                        ))
                    })?;
                }
            }

            Ok(Some(inputs[0].return_type()))
        }
        ExprType::MapAccess => {
            ensure_arity!("map_access", | inputs | == 2);
            let map_type = inputs[0].try_into_map_type()?;
            // We do not align the map's key type with the input type here, but cast the latter to the former instead.
            // e.g., for {1:'a'}[1.0], if we align them, we will get "numeric" as the key type, which violates the map type's restriction.
            match inputs[1].cast_implicit_mut(map_type.key()) {
                Ok(()) => Ok(Some(map_type.value().clone())),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot access {} in {}",
                    inputs[1].return_type(),
                    inputs[0].return_type(),
                ))
                .into()),
            }
        }
        ExprType::MapContains => {
            ensure_arity!("map_contains", | inputs | == 2);
            let map_type = inputs[0].try_into_map_type()?;
            match inputs[1].cast_implicit_mut(map_type.key()) {
                Ok(()) => Ok(Some(DataType::Boolean)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot check if {} exists in {}",
                    inputs[1].return_type(),
                    inputs[0].return_type(),
                ))
                .into()),
            }
        }
        ExprType::MapCat => {
            ensure_arity!("map_cat", | inputs | == 2);
            Ok(Some(align_types(inputs.iter_mut())?))
        }
        ExprType::MapInsert => {
            ensure_arity!("map_insert", | inputs | == 3);
            let map_type = inputs[0].try_into_map_type()?;
            let rk = inputs[1].cast_implicit_mut(map_type.key());
            let rv = inputs[2].cast_implicit_mut(map_type.value());
            match (rk, rv) {
                (Ok(()), Ok(())) => Ok(Some(map_type.into())),
                _ => Err(ErrorCode::BindError(format!(
                    "Cannot insert ({},{}) to {}",
                    inputs[1].return_type(),
                    inputs[2].return_type(),
                    inputs[0].return_type(),
                ))
                .into()),
            }
        }
        ExprType::MapDelete => {
            ensure_arity!("map_delete", | inputs | == 2);
            let map_type = inputs[0].try_into_map_type()?;
            let rk = inputs[1].cast_implicit_mut(map_type.key());
            match rk {
                Ok(()) => Ok(Some(map_type.into())),
                _ => Err(ErrorCode::BindError(format!(
                    "Cannot delete {} from {}",
                    inputs[1].return_type(),
                    inputs[0].return_type(),
                ))
                .into()),
            }
        }
        ExprType::L2Distance
        | ExprType::CosineDistance
        | ExprType::L1Distance
        | ExprType::InnerProduct
        | ExprType::Add
        | ExprType::Subtract
        | ExprType::Multiply => {
            let Some(vec_arg_first) = inputs
                .iter()
                .find(|e| !e.is_untyped() && matches!(e.return_type(), DataType::Vector(_)))
            else {
                // no vector type, use general rules later
                return Ok(None);
            };
            ensure_arity!(func_type.as_str_name().to_ascii_lowercase(), | inputs | == 2);
            let vec_type = vec_arg_first.return_type();
            if inputs[0].is_untyped() {
                inputs[0].cast_implicit_mut(&vec_type)?;
            }
            if inputs[1].is_untyped() {
                inputs[1].cast_implicit_mut(&vec_type)?;
            }
            let ret = match func_type {
                ExprType::L2Distance
                | ExprType::CosineDistance
                | ExprType::L1Distance
                | ExprType::InnerProduct => DataType::Float64,
                ExprType::Add | ExprType::Subtract | ExprType::Multiply => vec_type,
                _ => unreachable!(),
            };
            match (inputs[0].return_type(), inputs[1].return_type()) {
                (DataType::Vector(l), DataType::Vector(r)) => match l == r {
                    true => Ok(Some(ret)),
                    false => Err(ErrorCode::BindError(format!(
                        "different vector dimensions {l} and {r}"
                    ))
                    .into()),
                },
                _ => Ok(None), // would be rejected by general rules later
            }
        }
        ExprType::VecConcat => {
            use risingwave_common::types::ScalarImpl;
            ensure_arity!("vec_concat", | inputs | == 2);
            for input in inputs.iter_mut() {
                if !input.is_untyped() {
                    continue;
                }
                let ExprImpl::Literal(lit) = input else {
                    // use general rules, which would reject with proper error message
                    return Ok(None);
                };
                let Some(ScalarImpl::Utf8(lit)) = lit.get_data() else {
                    // use general rules, which would reject with proper error message
                    return Ok(None);
                };
                let guessed = lit.matches(',').count() + 1;
                input.cast_explicit_mut(&DataType::Vector(guessed))?;
            }
            if let (DataType::Vector(l), DataType::Vector(r)) =
                (inputs[0].return_type(), inputs[1].return_type())
            {
                let s = l
                    .checked_add(r)
                    .and_then(|s| (s <= DataType::VEC_MAX_SIZE).then_some(s))
                    .ok_or_else(|| {
                        ErrorCode::BindError(format!(
                            "vector cannot have more than {} dimensions: {l} + {r}",
                            DataType::VEC_MAX_SIZE
                        ))
                    })?;
                Ok(Some(DataType::Vector(s)))
            } else {
                // use general rules, which would reject with proper error message
                Ok(None)
            }
        }
        // internal use only
        ExprType::Vnode => Ok(Some(VirtualNode::RW_TYPE)),
        // user-facing `rw_vnode`
        ExprType::VnodeUser => {
            ensure_arity!("rw_vnode", 2 <= | inputs |);
            inputs[0].cast_explicit_mut(&DataType::Int32)?; // vnode count
            Ok(Some(VirtualNode::RW_TYPE))
        }
        ExprType::Greatest | ExprType::Least => {
            ensure_arity!("greatest/least", 1 <= | inputs |);
            Ok(Some(align_types(inputs.iter_mut())?))
        }
        ExprType::JsonbBuildArray => Ok(Some(DataType::Jsonb)),
        ExprType::JsonbBuildObject => {
            if !inputs.len().is_multiple_of(2) {
                return Err(ErrorCode::BindError(
                    "argument list must have even number of elements".into(),
                )
                .into());
            }
            Ok(Some(DataType::Jsonb))
        }
        ExprType::JsonbExtractPath => {
            ensure_arity!("jsonb_extract_path", 2 <= | inputs |);
            inputs[0].cast_implicit_mut(&DataType::Jsonb)?;
            for input in inputs.iter_mut().skip(1) {
                input.cast_implicit_mut(&DataType::Varchar)?;
            }
            Ok(Some(DataType::Jsonb))
        }
        ExprType::JsonbExtractPathText => {
            ensure_arity!("jsonb_extract_path_text", 2 <= | inputs |);
            inputs[0].cast_implicit_mut(&DataType::Jsonb)?;
            for input in inputs.iter_mut().skip(1) {
                input.cast_implicit_mut(&DataType::Varchar)?;
            }
            Ok(Some(DataType::Varchar))
        }
        _ => Ok(None),
    }
}

fn infer_type_for_special_table_function(
    func_type: PbTableFuncType,
    inputs: &mut [ExprImpl],
) -> Result<Option<DataType>> {
    match func_type {
        PbTableFuncType::GenerateSeries => {
            if inputs.len() < 3 || !inputs[1].is_now() {
                // let signature map handle this
                return Ok(None);
            }
            // Now we are inferring type for `generate_series(start, now(), step)`, which will
            // be further handled by `GenerateSeriesWithNowRule`.
            if inputs[0].is_untyped() {
                inputs[0].cast_implicit_mut(&DataType::Timestamptz)?;
            }
            if inputs[2].is_untyped() {
                inputs[2].cast_implicit_mut(&DataType::Interval)?;
            }
            match (
                inputs[0].return_type(),
                inputs[1].return_type(),
                inputs[2].return_type(),
            ) {
                (DataType::Timestamptz, DataType::Timestamptz, DataType::Interval) => {
                    // This is to allow `generate_series('2024-06-20 00:00:00'::timestamptz, now(), interval '1 day')`,
                    // which in streaming mode will be further converted to `StreamNow`.
                    Ok(Some(DataType::Timestamptz))
                }
                // let signature map handle the rest
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// From all available functions in `sig_map`, find and return the best matching `FuncSign` for the
/// provided `func_name` and `inputs`. This not only support exact function signature match, but can
/// also match `substr(varchar, smallint)` or even `substr(varchar, unknown)` to `substr(varchar,
/// int)`.
///
/// This corresponds to the `PostgreSQL` rules on operators and functions here:
/// * <https://www.postgresql.org/docs/current/typeconv-oper.html>
/// * <https://www.postgresql.org/docs/current/typeconv-func.html>
///
/// To summarize,
/// 1. Find all functions with matching `func_name` and argument count.
/// 2. For binary operator with unknown on exactly one side, try to find an exact match assuming
///    both sides are same type.
/// 3. Rank candidates based on most matching positions. This covers Rule 2, 4a, 4c and 4d in
///    `PostgreSQL`. See [`top_matches`] for details.
/// 4. Attempt to narrow down candidates by selecting type categories for unknowns. This covers Rule
///    4e in `PostgreSQL`. See [`narrow_category`] for details.
/// 5. Attempt to narrow down candidates by assuming all arguments are same type. This covers Rule
///    4f in `PostgreSQL`. See [`narrow_same_type`] for details.
pub fn infer_type_name<'a>(
    sig_map: &'a FunctionRegistry,
    func_name: FuncName,
    inputs: &[Option<DataType>],
) -> Result<&'a FuncSign> {
    let candidates = sig_map.get_with_arg_nums(func_name.clone(), inputs.len());

    // Binary operators have a special `unknown` handling rule for exact match. We do not
    // distinguish operators from functions as of now.
    if inputs.len() == 2 {
        let t = match (&inputs[0], &inputs[1]) {
            (None, t) => Ok(t),
            (t, None) => Ok(t),
            (Some(_), Some(_)) => Err(()),
        };
        if let Ok(Some(t)) = t {
            let exact = candidates
                .iter()
                .find(|sig| sig.inputs_type[0].matches(t) && sig.inputs_type[1].matches(t));
            if let Some(sig) = exact {
                return Ok(sig);
            }
        }
    }

    let mut candidates = top_matches(&candidates, inputs);

    // show function in error message
    let sig = || {
        format!(
            "{}({})",
            func_name,
            inputs.iter().map(TypeDisplay).format(", ")
        )
    };

    if candidates.is_empty() {
        // TODO: when type mismatches, show what are supported signatures for the
        // function with the given name.
        bail_no_function!("{}", sig());
    }

    // After this line `candidates` will never be empty, as the narrow rules will retain original
    // list when no desirable candidates can be selected.

    candidates = narrow_category(candidates, inputs);

    candidates = narrow_same_type(candidates, inputs);

    match &candidates[..] {
        [] => unreachable!(),
        [sig] => Ok(*sig),
        _ => Err(ErrorCode::BindError(format!(
            "function {} is not unique\nHINT:  Could not choose a best candidate function. You might need to add explicit type casts.",
            sig(),
        ))
        .into()),
    }
}

/// Checks if `t` is a preferred type in any type category, as defined by `PostgreSQL`:
/// <https://www.postgresql.org/docs/current/catalog-pg-type.html>.
fn is_preferred(t: &SigDataType) -> bool {
    use DataType as T;
    matches!(
        t,
        SigDataType::Exact(T::Float64 | T::Boolean | T::Varchar | T::Timestamptz | T::Interval)
    )
}

/// Checks if `source` can be implicitly casted to `target`. Generally we do not consider it being a
/// valid cast when they are of the same type, because it may deserve special treatment. This is
/// also the behavior of underlying [`cast_ok_base`].
///
/// Sometimes it is more convenient to include equality when checking whether a formal parameter can
/// accept an actual argument. So we introduced `eq_ok` to control this behavior.
fn implicit_ok(source: &DataType, target: &SigDataType, eq_ok: bool) -> bool {
    eq_ok && target.matches(source)
        || target.is_exact() && cast_ok_base(source, target.as_exact(), CastContext::Implicit)
}

/// Find the top `candidates` that match `inputs` on most non-null positions. This covers Rule 2,
/// 4a, 4c and 4d in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// * Rule 2 & 4c: Keep candidates that have most exact type matches. Exact match on all positions
///   is just a special case.
/// * Rule 4d: Break ties by selecting those that accept preferred types at most positions.
/// * Rule 4a: If the input cannot implicit cast to expected type at any position, this candidate is
///   discarded.
///
/// Corresponding implementation in `PostgreSQL`:
/// * Rule 2 on operators: `OpernameGetOprid()` in `namespace.c` [14.0/86a4dc1][rule 2 oper src]
///   * Note that unknown-handling logic of `binary_oper_exact()` in `parse_oper.c` is in
///     [`infer_type_name`].
/// * Rule 2 on functions: Line 1427 - Line 1439 of `func_get_detail()` in `parse_func.c`
///   [14.0/86a4dc1][rule 2 func src]
/// * Rule 4a: `func_match_argtypes()` in `parse_func.c` [14.0/86a4dc1][rule 4a src]
/// * Rule 4c: Line 1062 - Line 1104 of `func_select_candidate()` in `parse_func.c`
///   [14.0/86a4dc1][rule 4c src]
/// * Rule 4d: Line 1106 - Line 1153 of `func_select_candidate()` in `parse_func.c`
///   [14.0/86a4dc1][rule 4d src]
///
/// [rule 2 oper src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/catalog/namespace.c#L1516-L1611
/// [rule 2 func src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1427-L1439
/// [rule 4a src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L907-L947
/// [rule 4c src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1062-L1104
/// [rule 4d src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1106-L1153
fn top_matches<'a>(candidates: &[&'a FuncSign], inputs: &[Option<DataType>]) -> Vec<&'a FuncSign> {
    let mut best_exact = 0;
    let mut best_preferred = 0;
    let mut best_candidates = Vec::new();

    for sig in candidates {
        let mut n_exact = 0;
        let mut n_preferred = 0;
        let mut castable = true;
        for (formal, actual) in sig.inputs_type.iter().zip_eq_fast(inputs) {
            let Some(actual) = actual else { continue };
            if formal.matches(actual) {
                n_exact += 1;
            } else if !implicit_ok(actual, formal, false) {
                castable = false;
                break;
            }
            if is_preferred(formal) {
                n_preferred += 1;
            }
        }
        if !castable {
            continue;
        }
        if n_exact > best_exact || n_exact == best_exact && n_preferred > best_preferred {
            best_exact = n_exact;
            best_preferred = n_preferred;
            best_candidates.clear();
        }
        if n_exact == best_exact && n_preferred == best_preferred {
            best_candidates.push(*sig);
        }
    }
    best_candidates
}

/// Attempt to narrow down candidates by selecting type categories for unknowns. This covers Rule 4e
/// in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// This is done in 2 phases:
/// * First, check each unknown position individually and try to find a type category for it.
///   * If any candidate accept varchar, varchar is selected;
///   * otherwise, if all candidate accept the same category, select this category.
///     * Also record whether any candidate accept the preferred type within this category.
/// * When all unknown positions are assigned their type categories, discard a candidate if at any
///   position
///   * it does not agree with the type category assignment;
///   * the assigned type category contains a preferred type but the candidate is not preferred.
///
/// If the first phase fails or the second phase gives an empty set, this attempt preserves original
/// list untouched.
///
/// Corresponding implementation in `PostgreSQL`:
/// * Line 1164 - Line 1298 of `func_select_candidate()` in `parse_func.c` [14.0/86a4dc1][rule 4e
///   src]
///
/// [rule 4e src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1164-L1298
fn narrow_category<'a>(
    candidates: Vec<&'a FuncSign>,
    inputs: &[Option<DataType>],
) -> Vec<&'a FuncSign> {
    const BIASED_TYPE: SigDataType = SigDataType::Exact(DataType::Varchar);
    let Ok(categories) = inputs
        .iter()
        .enumerate()
        .map(|(i, actual)| {
            // This closure returns
            // * Err(()) when a category cannot be selected
            // * Ok(None) when actual argument is non-null and can skip selection
            // * Ok(Some(t)) when the selected category is `t`
            //
            // Here `t` is actually just one type within that selected category, rather than the
            // category itself. It is selected to be the [`super::least_restrictive`] over all
            // candidates. This makes sure that `t` is the preferred type if any candidate accept
            // it.
            if actual.is_some() {
                return Ok(None);
            }
            let mut category = Ok(&candidates[0].inputs_type[i]);
            for sig in &candidates[1..] {
                let formal = &sig.inputs_type[i];
                if formal == &BIASED_TYPE || category == Ok(&BIASED_TYPE) {
                    category = Ok(&BIASED_TYPE);
                    break;
                }
                // formal != BIASED_TYPE && category.is_err():
                // - Category conflict err can only be solved by a later varchar. Skip this
                //   candidate.
                let Ok(selected) = &category else { continue };
                // least_restrictive or mark temporary conflict err
                if formal.is_exact() && implicit_ok(formal.as_exact(), selected, true) {
                    // noop
                } else if selected.is_exact() && implicit_ok(selected.as_exact(), formal, false) {
                    category = Ok(formal);
                } else {
                    category = Err(());
                }
            }
            category.map(Some)
        })
        .try_collect::<_, Vec<_>, _>()
    else {
        // First phase failed.
        return candidates;
    };
    let cands_temp = candidates
        .iter()
        .filter(|sig| {
            sig.inputs_type
                .iter()
                .zip_eq_fast(&categories)
                .all(|(formal, category)| {
                    // category.is_none() means the actual argument is non-null and skipped category
                    // selection.
                    let Some(selected) = category else {
                        return true;
                    };
                    formal == *selected
                        || !is_preferred(selected)
                            && formal.is_exact()
                            && implicit_ok(formal.as_exact(), selected, false)
                })
        })
        .copied()
        .collect_vec();
    match cands_temp.is_empty() {
        true => candidates,
        false => cands_temp,
    }
}

/// Attempt to narrow down candidates by assuming all arguments are same type. This covers Rule 4f
/// in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// If all non-null arguments are same type, assume all unknown arguments are of this type as well.
/// Discard a candidate if its parameter type cannot be casted from this type.
///
/// If the condition is not met or the result is empty, this attempt preserves original list
/// untouched.
///
/// Note this rule cannot replace special treatment given to binary operators in [Rule 2], because
/// this runs after varchar-biased Rule 4e ([`narrow_category`]), and has no preference between
/// exact-match and castable-match.
///
/// Corresponding implementation in `PostgreSQL`:
/// * Line 1300 - Line 1355 of `func_select_candidate()` in `parse_func.c` [14.0/86a4dc1][rule 4f
///   src]
///
/// [rule 4f src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1300-L1355
/// [Rule 2]: https://www.postgresql.org/docs/current/typeconv-oper.html#:~:text=then%20assume%20it%20is%20the%20same%20type%20as%20the%20other%20argument%20for%20this%20check
fn narrow_same_type<'a>(
    candidates: Vec<&'a FuncSign>,
    inputs: &[Option<DataType>],
) -> Vec<&'a FuncSign> {
    let Ok(Some(same_type)) = inputs.iter().try_fold(None, |acc, cur| match (acc, cur) {
        (None, t) => Ok(t.as_ref()),
        (t, None) => Ok(t),
        (Some(l), Some(r)) if l == r => Ok(Some(l)),
        _ => Err(()),
    }) else {
        return candidates;
    };
    let cands_temp = candidates
        .iter()
        .filter(|sig| {
            sig.inputs_type
                .iter()
                .all(|formal| implicit_ok(same_type, formal, true))
        })
        .copied()
        .collect_vec();
    match cands_temp.is_empty() {
        true => candidates,
        false => cands_temp,
    }
}

struct TypeDisplay<'a>(&'a Option<DataType>);
impl std::fmt::Display for TypeDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(t) => t.fmt(f),
            None => write!(f, "unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::iproduct;

    use super::*;

    fn infer_type_v0(func_type: ExprType, inputs_type: Vec<DataType>) -> Result<DataType> {
        let mut inputs = inputs_type
            .into_iter()
            .map(|t| {
                crate::expr::Literal::new(
                    Some(match t {
                        DataType::Boolean => true.into(),
                        DataType::Int16 => 1i16.into(),
                        DataType::Int32 => 1i32.into(),
                        DataType::Int64 => 1i64.into(),
                        DataType::Float32 => 1f32.into(),
                        DataType::Float64 => 1f64.into(),
                        DataType::Decimal => risingwave_common::types::Decimal::NaN.into(),
                        _ => unimplemented!(),
                    }),
                    t,
                )
                .into()
            })
            .collect_vec();
        infer_type(func_type.into(), &mut inputs)
    }

    fn test_simple_infer_type(
        func_type: ExprType,
        inputs_type: Vec<DataType>,
        expected_type_name: DataType,
    ) {
        let ret = infer_type_v0(func_type, inputs_type).unwrap();
        assert_eq!(ret, expected_type_name);
    }

    fn test_infer_type_not_exist(func_type: ExprType, inputs_type: Vec<DataType>) {
        let ret = infer_type_v0(func_type, inputs_type);
        assert!(ret.is_err());
    }

    #[test]
    fn test_arithmetics() {
        use DataType::*;
        let atm_exprs = vec![
            ExprType::Add,
            ExprType::Subtract,
            ExprType::Multiply,
            ExprType::Divide,
        ];
        let num_promote_table = vec![
            (Int16, Int16, Int16),
            (Int16, Int32, Int32),
            (Int16, Int64, Int64),
            (Int16, Decimal, Decimal),
            (Int16, Float32, Float64),
            (Int16, Float64, Float64),
            (Int32, Int16, Int32),
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int32, Decimal, Decimal),
            (Int32, Float32, Float64),
            (Int32, Float64, Float64),
            (Int64, Int16, Int64),
            (Int64, Int32, Int64),
            (Int64, Int64, Int64),
            (Int64, Decimal, Decimal),
            (Int64, Float32, Float64),
            (Int64, Float64, Float64),
            (Decimal, Int16, Decimal),
            (Decimal, Int32, Decimal),
            (Decimal, Int64, Decimal),
            (Decimal, Decimal, Decimal),
            (Decimal, Float32, Float64),
            (Decimal, Float64, Float64),
            (Float32, Int16, Float64),
            (Float32, Int32, Float64),
            (Float32, Int64, Float64),
            (Float32, Decimal, Float64),
            (Float32, Float32, Float32),
            (Float32, Float64, Float64),
            (Float64, Int16, Float64),
            (Float64, Int32, Float64),
            (Float64, Int64, Float64),
            (Float64, Decimal, Float64),
            (Float64, Float32, Float64),
            (Float64, Float64, Float64),
        ];
        for (expr, (t1, t2, tr)) in iproduct!(atm_exprs, num_promote_table) {
            test_simple_infer_type(expr, vec![t1, t2], tr);
        }
    }

    #[test]
    fn test_bitwise() {
        use DataType::*;
        let bitwise_exprs = vec![
            ExprType::BitwiseAnd,
            ExprType::BitwiseOr,
            ExprType::BitwiseXor,
        ];
        let num_promote_table = vec![
            (Int16, Int16, Int16),
            (Int16, Int32, Int32),
            (Int16, Int64, Int64),
            (Int32, Int16, Int32),
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int64, Int16, Int64),
            (Int64, Int32, Int64),
            (Int64, Int64, Int64),
        ];
        for (expr, (t1, t2, tr)) in iproduct!(bitwise_exprs, num_promote_table) {
            test_simple_infer_type(expr, vec![t1, t2], tr);
        }

        for (expr, (t1, t2, tr)) in iproduct!(
            vec![ExprType::BitwiseShiftLeft, ExprType::BitwiseShiftRight,],
            vec![
                (Int16, Int16, Int16),
                (Int32, Int16, Int32),
                (Int64, Int16, Int64),
                (Int16, Int32, Int16),
                (Int64, Int32, Int64),
                (Int32, Int32, Int32),
            ]
        ) {
            test_simple_infer_type(expr, vec![t1, t2], tr);
        }
    }
    #[test]
    fn test_bool_num_not_exist() {
        let exprs = vec![
            ExprType::Add,
            ExprType::Subtract,
            ExprType::Multiply,
            ExprType::Divide,
            ExprType::Modulus,
            ExprType::Equal,
            ExprType::NotEqual,
            ExprType::LessThan,
            ExprType::LessThanOrEqual,
            ExprType::GreaterThan,
            ExprType::GreaterThanOrEqual,
            ExprType::And,
            ExprType::Or,
            ExprType::Not,
        ];
        let num_types = vec![
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
        ];

        for (expr, num_t) in iproduct!(exprs, num_types) {
            test_infer_type_not_exist(expr, vec![num_t, DataType::Boolean]);
        }
    }

    #[test]
    fn test_match_implicit() {
        use DataType as T;
        let testcases = [
            (
                "Binary special rule prefers arguments of same type.",
                &[
                    &[T::Int32, T::Int32][..],
                    &[T::Int32, T::Varchar],
                    &[T::Int32, T::Float64],
                ][..],
                &[Some(T::Int32), None][..],
                Ok(&[T::Int32, T::Int32][..]),
            ),
            (
                "Without binary special rule, Rule 4e selects varchar.",
                &[
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Int32, T::Int32, T::Varchar],
                    &[T::Int32, T::Int32, T::Float64],
                ],
                &[Some(T::Int32), Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Varchar]),
            ),
            (
                "Without binary special rule, Rule 4e selects preferred type.",
                &[
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Int32, T::Int32, T::Float64],
                ],
                &[Some(T::Int32), Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Float64]),
            ),
            (
                "Without binary special rule, Rule 4f treats exact-match and cast-match equally.",
                &[
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Int32, T::Int32, T::Float32],
                ],
                &[Some(T::Int32), Some(T::Int32), None],
                Err("not unique"),
            ),
            (
                "`top_matches` ranks by exact count then preferred count",
                &[
                    &[T::Float64, T::Float64, T::Float64, T::Timestamptz], /* 0 exact 3 preferred */
                    &[T::Float64, T::Int32, T::Float32, T::Timestamp],     // 1 exact 1 preferred
                    &[T::Float32, T::Float32, T::Int32, T::Timestamptz],   // 1 exact 0 preferred
                    &[T::Int32, T::Float64, T::Float32, T::Timestamptz],   // 1 exact 1 preferred
                    &[T::Int32, T::Int16, T::Int32, T::Timestamptz], // 2 exact 1 non-castable
                    &[T::Int32, T::Float64, T::Float32, T::Date],    // 1 exact 1 preferred
                ],
                &[Some(T::Int32), Some(T::Int32), Some(T::Int32), None],
                Ok(&[T::Int32, T::Float64, T::Float32, T::Timestamptz]),
            ),
            (
                "Rule 4e fails and Rule 4f unique.",
                &[
                    &[T::Int32, T::Int32, T::Time],
                    &[T::Int32, T::Int32, T::Int32],
                ],
                &[None, Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Int32]),
            ),
            (
                "Rule 4e empty and Rule 4f unique.",
                &[
                    &[T::Int32, T::Int32, T::Varchar],
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Varchar, T::Int32, T::Int32],
                ],
                &[None, Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Int32]),
            ),
            (
                "Rule 4e varchar resolves prior category conflict.",
                &[
                    &[T::Int32, T::Int32, T::Float32],
                    &[T::Time, T::Int32, T::Int32],
                    &[T::Varchar, T::Int32, T::Int32],
                ],
                &[None, Some(T::Int32), None],
                Ok(&[T::Varchar, T::Int32, T::Int32]),
            ),
            (
                "Rule 4f fails.",
                &[
                    &[T::Float32, T::Float32, T::Float32, T::Float32],
                    &[T::Decimal, T::Decimal, T::Int64, T::Decimal],
                ],
                &[Some(T::Int16), Some(T::Int32), None, Some(T::Int64)],
                Err("not unique"),
            ),
            (
                "Rule 4f all unknown.",
                &[
                    &[T::Float32, T::Float32, T::Float32, T::Float32],
                    &[T::Decimal, T::Decimal, T::Int64, T::Decimal],
                ],
                &[None, None, None, None],
                Err("not unique"),
            ),
        ];
        for (desc, candidates, inputs, expected) in testcases {
            let mut sig_map = FunctionRegistry::default();
            for formals in candidates {
                sig_map.insert(FuncSign {
                    // func_name does not affect the overload resolution logic
                    name: ExprType::Add.into(),
                    inputs_type: formals.iter().map(|t| t.clone().into()).collect(),
                    variadic: false,
                    // ret_type does not affect the overload resolution logic
                    ret_type: T::Int32.into(),
                    build: FuncBuilder::Scalar(|_, _| unreachable!()),
                    type_infer: |_| unreachable!(),
                    deprecated: false,
                });
            }
            let result = infer_type_name(&sig_map, ExprType::Add.into(), inputs);
            match (expected, result) {
                (Ok(expected), Ok(found)) => {
                    if !found.match_args(expected) {
                        panic!("case `{}` expect {:?} != found {:?}", desc, expected, found)
                    }
                }
                (Ok(_), Err(err)) => panic!("case `{}` unexpected error: {:?}", desc, err),
                (Err(_), Ok(f)) => panic!(
                    "case `{}` expect error but found: {:?}",
                    desc, f.inputs_type
                ),
                (Err(expected), Err(err)) => assert!(
                    err.to_string().contains(expected),
                    "case `{}` expect err `{}` != {:?}",
                    desc,
                    expected,
                    err
                ),
            }
        }
    }
}
