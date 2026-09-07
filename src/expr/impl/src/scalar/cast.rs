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

use std::str::FromStr;
use std::sync::Arc;

use futures_util::FutureExt;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, ListRef, ListValue, StructRef, StructValue, VectorVal};
use risingwave_common::cast;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, F64, Int256, JsonbRef, JsonbVal, MapRef, MapValue, ScalarRef as _, Serial,
    Timestamptz, ToText, VariantRef, VariantVal,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::row_id::row_id_to_unix_millis;
use risingwave_expr::expr::{Context, InputRefExpression, SyncExpressionBoxExt, build_func};
use risingwave_expr::{ExprError, Result, function};
use risingwave_pb::expr::expr_node::PbType;
use thiserror_ext::AsReport;

#[function("cast(varchar) -> *int")]
#[function("cast(varchar) -> decimal")]
#[function("cast(varchar) -> *float")]
#[function("cast(varchar) -> int256")]
#[function("cast(varchar) -> date")]
#[function("cast(varchar) -> time")]
#[function("cast(varchar) -> timestamp")]
#[function("cast(varchar) -> interval")]
#[function("cast(varchar) -> jsonb")]
#[function("cast(varchar) -> variant")]
pub fn str_parse<T>(elem: &str, ctx: &Context) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    elem.trim().parse().map_err(|err: <T as FromStr>::Err| {
        ExprError::Parse(format!("{} {}", ctx.return_type, err).into())
    })
}

// TODO: introduce `FromBinary` and support all types
#[function("pgwire_recv(bytea) -> int8")]
pub fn pgwire_recv(elem: &[u8]) -> Result<i64> {
    let fixed_length =
        <[u8; 8]>::try_from(elem).map_err(|e| ExprError::Parse(e.to_report_string().into()))?;
    Ok(i64::from_be_bytes(fixed_length))
}

#[function("cast(int2) -> int256")]
#[function("cast(int4) -> int256")]
#[function("cast(int8) -> int256")]
pub fn to_int256<T: TryInto<Int256>>(elem: T) -> Result<Int256> {
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange("int256"))
}

#[function("cast(jsonb) -> boolean")]
pub fn jsonb_to_bool(v: JsonbRef<'_>) -> Result<Option<bool>> {
    if v.is_jsonb_null() {
        Ok(None)
    } else {
        v.as_bool()
            .map(Some)
            .map_err(|e| ExprError::Parse(e.into()))
    }
}

/// Note that PostgreSQL casts JSON numbers from arbitrary precision `numeric` but we use `f64`.
/// This is less powerful but still meets RFC 8259 interoperability.
#[function("cast(jsonb) -> int2")]
#[function("cast(jsonb) -> int4")]
#[function("cast(jsonb) -> int8")]
#[function("cast(jsonb) -> decimal")]
#[function("cast(jsonb) -> float4")]
#[function("cast(jsonb) -> float8")]
pub fn jsonb_to_number<T: TryFrom<F64>>(v: JsonbRef<'_>) -> Result<Option<T>> {
    if v.is_jsonb_null() {
        Ok(None)
    } else {
        v.as_number()
            .map_err(|e| ExprError::Parse(e.into()))?
            .try_into()
            .map(Some)
            .map_err(|_| ExprError::NumericOutOfRange)
    }
}

#[function("cast(int4) -> int2")]
#[function("cast(int8) -> int2")]
#[function("cast(int8) -> int4")]
#[function("cast(int8) -> serial")]
#[function("cast(serial) -> int8")]
#[function("cast(float4) -> int2")]
#[function("cast(float8) -> int2")]
#[function("cast(float4) -> int4")]
#[function("cast(float8) -> int4")]
#[function("cast(float4) -> int8")]
#[function("cast(float8) -> int8")]
#[function("cast(float8) -> float4")]
#[function("cast(decimal) -> int2")]
#[function("cast(decimal) -> int4")]
#[function("cast(decimal) -> int8")]
#[function("cast(decimal) -> float4")]
#[function("cast(decimal) -> float8")]
#[function("cast(float4) -> decimal")]
#[function("cast(float8) -> decimal")]
pub fn try_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
{
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange(std::any::type_name::<T2>()))
}

#[function("cast(boolean) -> int4")]
#[function("cast(int2) -> int4")]
#[function("cast(int2) -> int8")]
#[function("cast(int2) -> float4")]
#[function("cast(int2) -> float8")]
#[function("cast(int2) -> decimal")]
#[function("cast(int4) -> int8")]
#[function("cast(int4) -> float4")]
#[function("cast(int4) -> float8")]
#[function("cast(int4) -> decimal")]
#[function("cast(int8) -> float4")]
#[function("cast(int8) -> float8")]
#[function("cast(int8) -> decimal")]
#[function("cast(float4) -> float8")]
#[function("cast(date) -> timestamp")]
#[function("cast(time) -> interval")]
#[function("cast(timestamp) -> date")]
#[function("cast(timestamp) -> time")]
#[function("cast(interval) -> time")]
#[function("cast(varchar) -> varchar")]
#[function("cast(int256) -> float8")]
pub fn cast<T1, T2>(elem: T1) -> T2
where
    T1: Into<T2>,
{
    elem.into()
}

#[function("cast(jsonb) -> variant")]
pub fn jsonb_to_variant(elem: JsonbRef<'_>) -> Result<VariantVal> {
    VariantVal::from_jsonb(elem).map_err(|e| ExprError::Parse(e.to_report_string().into()))
}

#[function("cast(variant) -> jsonb")]
pub fn variant_to_jsonb(elem: VariantRef<'_>) -> Result<JsonbVal> {
    elem.to_jsonb()
        .map_err(|e| ExprError::Parse(e.to_report_string().into()))
}

/// Extract the timestamp from row id.
#[function("cast(serial) -> timestamptz")]
pub fn serial_to_timestamptz(elem: Serial) -> Result<Timestamptz> {
    let unix_ms = row_id_to_unix_millis(elem.as_row_id()).ok_or(ExprError::NumericOutOfRange)?;
    Timestamptz::from_millis(unix_ms).ok_or(ExprError::NumericOutOfRange)
}

#[function("cast(varchar) -> boolean")]
pub fn str_to_bool(input: &str) -> Result<bool> {
    cast::str_to_bool(input).map_err(|err| ExprError::Parse(err.into()))
}

#[function("cast(int4) -> boolean")]
pub fn int_to_bool(input: i32) -> bool {
    input != 0
}

/// For most of the types, cast them to varchar is the same as their pgwire "TEXT" format.
/// So we use `ToText` to cast type to varchar.
#[function("cast(*int) -> varchar")]
#[function("cast(decimal) -> varchar")]
#[function("cast(*float) -> varchar")]
#[function("cast(int256) -> varchar")]
#[function("cast(time) -> varchar")]
#[function("cast(date) -> varchar")]
#[function("cast(interval) -> varchar")]
#[function("cast(timestamp) -> varchar")]
#[function("cast(jsonb) -> varchar")]
#[function("cast(variant) -> varchar")]
#[function("cast(bytea) -> varchar")]
#[function("cast(anyarray) -> varchar")]
#[function("cast(vector) -> varchar")]
pub fn general_to_text(elem: impl ToText, mut writer: &mut impl std::fmt::Write) {
    elem.write(&mut writer).unwrap();
}

// TODO: use `ToBinary` and support all types
#[function("pgwire_send(int8) -> bytea")]
fn pgwire_send(elem: i64, writer: &mut impl std::io::Write) {
    writer.write_all(&elem.to_be_bytes()).unwrap();
}

#[function("cast(boolean) -> varchar")]
pub fn bool_to_varchar(input: bool, writer: &mut impl std::fmt::Write) {
    writer
        .write_str(if input { "true" } else { "false" })
        .unwrap();
}

/// `bool_out` is different from `cast(boolean) -> varchar` to produce a single char. `PostgreSQL`
/// uses different variants of bool-to-string in different situations.
#[function("bool_out(boolean) -> varchar")]
pub fn bool_out(input: bool, writer: &mut impl std::fmt::Write) {
    writer.write_str(if input { "t" } else { "f" }).unwrap();
}

#[function("cast(varchar) -> bytea")]
pub fn str_to_bytea(elem: &str, writer: &mut impl std::io::Write) -> Result<()> {
    cast::str_to_bytea(elem, writer).map_err(|err| ExprError::Parse(err.into()))
}

#[function("cast(varchar) -> anyarray", type_infer = "unreachable")]
fn str_to_list(input: &str, ctx: &Context) -> Result<ListValue> {
    ListValue::from_str(input, &ctx.return_type).map_err(|err| ExprError::Parse(err.into()))
}

#[function("cast(varchar) -> vector", type_infer = "unreachable")]
fn str_to_vector(input: &str, ctx: &Context) -> Result<VectorVal> {
    let DataType::Vector(size) = &ctx.return_type else {
        unreachable!()
    };
    VectorVal::from_text(input, *size).map_err(|err| ExprError::Parse(err.into()))
}

/// Cast array with `source_elem_type` into array with `target_elem_type` by casting each element.
#[function("cast(anyarray) -> anyarray", type_infer = "unreachable")]
fn list_cast(input: ListRef<'_>, ctx: &Context) -> Result<ListValue> {
    let cast = build_func(
        PbType::Cast,
        ctx.return_type.as_list_elem().clone(),
        vec![InputRefExpression::new(ctx.arg_types[0].as_list_elem().clone(), 0).boxed()],
    )
    .unwrap();
    let items = Arc::new(input.to_owned_scalar().into_array());
    let len = items.len();
    let list = cast
        .eval(&DataChunk::new(vec![items], len))
        .now_or_never()
        .unwrap()?;
    Ok(ListValue::new(Arc::try_unwrap(list).unwrap()))
}

/// Cast struct of `source_elem_type` to `target_elem_type` by casting each element.
#[function("cast(struct) -> struct", type_infer = "unreachable")]
fn struct_cast(input: StructRef<'_>, ctx: &Context) -> Result<StructValue> {
    let fields = (input.iter_fields_ref())
        .zip_eq_fast(ctx.arg_types[0].as_struct().types())
        .zip_eq_fast(ctx.return_type.as_struct().types())
        .map(|((datum_ref, source_field_type), target_field_type)| {
            if source_field_type == target_field_type {
                return Ok(datum_ref.map(|scalar_ref| scalar_ref.into_scalar_impl()));
            }
            let cast = build_func(
                PbType::Cast,
                target_field_type.clone(),
                vec![InputRefExpression::new(source_field_type.clone(), 0).boxed()],
            )
            .unwrap();
            let value = match datum_ref {
                Some(scalar_ref) => cast
                    .eval_row(&OwnedRow::new(vec![Some(scalar_ref.into_scalar_impl())]))
                    .now_or_never()
                    .unwrap()?,
                None => None,
            };
            Ok(value) as Result<_>
        })
        .try_collect()?;
    Ok(StructValue::new(fields))
}

/// Cast array with `source_elem_type` into array with `target_elem_type` by casting each element.
#[function("cast(anymap) -> anymap", type_infer = "unreachable")]
fn map_cast(map: MapRef<'_>, ctx: &Context) -> Result<MapValue> {
    let new_ctx = Context {
        arg_types: vec![ctx.arg_types[0].clone().as_map().clone().into_list()],
        return_type: ctx.return_type.as_map().clone().into_list(),
        variadic: ctx.variadic,
    };
    list_cast(map.into_inner(), &new_ctx).map(MapValue::from_entries)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use risingwave_common::array::*;
    use risingwave_common::types::*;
    use risingwave_expr::expr::build_from_pretty;

    use super::*;

    #[test]
    fn integer_cast_to_bool() {
        assert!(int_to_bool(32));
        assert!(int_to_bool(-32));
        assert!(!int_to_bool(0));
    }

    #[test]
    fn number_to_string() {
        macro_rules! test {
            ($fn:ident($value:expr), $right:literal) => {
                let mut writer = String::new();
                $fn($value, &mut writer);
                assert_eq!(writer, $right);
            };
        }

        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(false), "false");

        test!(general_to_text(32), "32");
        test!(general_to_text(-32), "-32");
        test!(general_to_text(i32::MIN), "-2147483648");
        test!(general_to_text(i32::MAX), "2147483647");

        test!(general_to_text(i16::MIN), "-32768");
        test!(general_to_text(i16::MAX), "32767");

        test!(general_to_text(i64::MIN), "-9223372036854775808");
        test!(general_to_text(i64::MAX), "9223372036854775807");

        test!(general_to_text(F64::from(32.12)), "32.12");
        test!(general_to_text(F64::from(-32.14)), "-32.14");

        test!(general_to_text(F32::from(32.12_f32)), "32.12");
        test!(general_to_text(F32::from(-32.14_f32)), "-32.14");

        test!(general_to_text(Decimal::try_from(1.222).unwrap()), "1.222");

        test!(general_to_text(Decimal::NaN), "NaN");
    }

    #[test]
    fn test_str_to_list() {
        // Empty List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
            variadic: false,
        };
        assert_eq!(
            str_to_list("{}", &ctx).unwrap(),
            ListValue::empty(&DataType::Varchar)
        );

        let list123 = ListValue::from_iter([1, 2, 3]);

        // Single List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
            variadic: false,
        };
        assert_eq!(str_to_list("{1, 2, 3}", &ctx).unwrap(), list123);

        // Nested List
        let nested_list123 = ListValue::from_iter([list123]);
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[][]").unwrap(),
            variadic: false,
        };
        assert_eq!(str_to_list("{{1, 2, 3}}", &ctx).unwrap(), nested_list123);

        let nested_list445566 = ListValue::from_iter([ListValue::from_iter([44, 55, 66])]);

        let double_nested_list123_445566 =
            ListValue::from_iter([nested_list123.clone(), nested_list445566.clone()]);

        // Double nested List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[][][]").unwrap(),
            variadic: false,
        };
        assert_eq!(
            str_to_list("{{{1, 2, 3}}, {{44, 55, 66}}}", &ctx).unwrap(),
            double_nested_list123_445566
        );

        // Cast previous double nested lists to double nested varchar lists
        let ctx = Context {
            arg_types: vec![DataType::from_str("int[][]").unwrap()],
            return_type: DataType::from_str("varchar[][]").unwrap(),
            variadic: false,
        };
        let double_nested_varchar_list123_445566 = ListValue::from_iter([
            list_cast(nested_list123.as_scalar_ref(), &ctx).unwrap(),
            list_cast(nested_list445566.as_scalar_ref(), &ctx).unwrap(),
        ]);

        // Double nested Varchar List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("varchar[][][]").unwrap(),
            variadic: false,
        };
        assert_eq!(
            str_to_list("{{{1, 2, 3}}, {{44, 55, 66}}}", &ctx).unwrap(),
            double_nested_varchar_list123_445566
        );
    }

    #[test]
    fn test_invalid_str_to_list() {
        // Unbalanced input
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
            variadic: false,
        };
        assert!(str_to_list("{{}", &ctx).is_err());
        assert!(str_to_list("{}}", &ctx).is_err());
        assert!(str_to_list("{{1, 2, 3}, {4, 5, 6}", &ctx).is_err());
        assert!(str_to_list("{{1, 2, 3}, 4, 5, 6}}", &ctx).is_err());
    }

    #[test]
    fn test_struct_cast() {
        let ctx = Context {
            arg_types: vec![DataType::Struct(StructType::new(vec![
                ("a", DataType::Varchar),
                ("b", DataType::Float32),
            ]))],
            return_type: DataType::Struct(StructType::new(vec![
                ("a", DataType::Int32),
                ("b", DataType::Int32),
            ])),
            variadic: false,
        };
        assert_eq!(
            struct_cast(
                StructValue::new(vec![
                    Some("1".into()),
                    Some(F32::from(0.0).to_scalar_value()),
                ])
                .as_scalar_ref(),
                &ctx,
            )
            .unwrap(),
            StructValue::new(vec![
                Some(1i32.to_scalar_value()),
                Some(0i32.to_scalar_value()),
            ])
        );
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(
            try_cast::<_, Timestamp>(Date::from_ymd_uncheck(1994, 1, 1)).unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        )
    }

    #[tokio::test]
    async fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, PbType::Not).await;
        test_unary_date::<TimestampArray, _>(|x| try_cast(x).unwrap(), PbType::Cast).await;
        let ctx_str_to_int16 = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::Int16,
            variadic: false,
        };
        test_str_to_int16::<I16Array, _>(|x| str_parse(x, &ctx_str_to_int16).unwrap()).await;
    }

    #[tokio::test]
    async fn test_i16_to_i32() {
        let mut input = Vec::<Option<i16>>::new();
        let mut target = Vec::<Option<i32>>::new();
        for i in 0..100i16 {
            if i % 2 == 0 {
                target.push(Some(i as i32));
                input.push(Some(i));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = I16Array::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = build_from_pretty("(cast:int4 $0:int2)");
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_neg() {
        let input = [Some(1), Some(0), Some(-1)];
        let target = [Some(-1), Some(0), Some(1)];

        let col1 = I32Array::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 3);
        let expr = build_from_pretty("(neg:int4 $0:int4)");
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_str_to_int16<A, F>(f: F)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(&str) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<Box<str>>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..1u32 {
            if i % 2 == 0 {
                let s = i.to_string().into_boxed_str();
                target.push(Some(f(&s)));
                input.push(Some(s));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1_data = &input.iter().map(|x| x.as_ref().map(|x| &**x)).collect_vec();
        let col1 = Utf8Array::from_iter(col1_data).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 1);
        let expr = build_from_pretty("(cast:int2 $0:varchar)");
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![
                input[i].as_ref().cloned().map(|str| str.to_scalar_value()),
            ]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_unary_bool<A, F>(f: F, kind: PbType)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(bool) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<bool>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                input.push(Some(true));
                target.push(Some(f(true)));
            } else if i % 3 == 0 {
                input.push(Some(false));
                target.push(Some(f(false)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = BoolArray::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = build_from_pretty(format!("({kind:?}:boolean $0:boolean)"));
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|b| b.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_unary_date<A, F>(f: F, kind: PbType)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(Date) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<Date>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                let date = Date::from_num_days_from_ce_uncheck(i);
                input.push(Some(date));
                target.push(Some(f(date)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = DateArray::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = build_from_pretty(format!("({kind:?}:timestamp $0:date)"));
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|d| d.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }
}
