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

use std::collections::HashMap;
use std::sync::LazyLock;

use bk_tree::{BKTree, metrics};
use itertools::Itertools;
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_common::types::{DataType, ListValue, ScalarImpl, Timestamptz};
use risingwave_common::{bail_not_implemented, current_cluster_version, no_function};
use thiserror_ext::AsReport;

use crate::Binder;
use crate::binder::Clause;
use crate::error::{ErrorCode, Result};
use crate::expr::{CastContext, Expr, ExprImpl, ExprType, FunctionCall, Literal, Now};

impl Binder {
    pub(super) fn bind_builtin_scalar_function(
        &mut self,
        function_name: &str,
        inputs: Vec<ExprImpl>,
        variadic: bool,
    ) -> Result<ExprImpl> {
        type Inputs = Vec<ExprImpl>;

        type Handle = Box<dyn Fn(&mut Binder, Inputs) -> Result<ExprImpl> + Sync + Send>;

        fn rewrite(r#type: ExprType, rewriter: fn(Inputs) -> Result<Inputs>) -> Handle {
            Box::new(move |_binder, mut inputs| {
                inputs = (rewriter)(inputs)?;
                Ok(FunctionCall::new(r#type, inputs)?.into())
            })
        }

        fn raw_call(r#type: ExprType) -> Handle {
            rewrite(r#type, Ok)
        }

        fn guard_by_len<const E: usize>(
            handle: impl Fn(&mut Binder, [ExprImpl; E]) -> Result<ExprImpl> + Sync + Send + 'static,
        ) -> Handle {
            Box::new(move |binder, inputs| {
                let input_len = inputs.len();
                let Ok(inputs) = inputs.try_into() else {
                    return Err(ErrorCode::ExprError(
                        format!("unexpected arguments number {}, expect {}", input_len, E).into(),
                    )
                    .into());
                };
                handle(binder, inputs)
            })
        }

        fn raw<F: Fn(&mut Binder, Inputs) -> Result<ExprImpl> + Sync + Send + 'static>(
            f: F,
        ) -> Handle {
            Box::new(f)
        }

        fn dispatch_by_len(mapping: Vec<(usize, Handle)>) -> Handle {
            Box::new(move |binder, inputs| {
                for (len, handle) in &mapping {
                    if inputs.len() == *len {
                        return handle(binder, inputs);
                    }
                }
                Err(ErrorCode::ExprError("unexpected arguments number".into()).into())
            })
        }

        fn raw_literal(literal: ExprImpl) -> Handle {
            Box::new(move |_binder, _inputs| Ok(literal.clone()))
        }

        fn now() -> Handle {
            guard_by_len(move |binder, []| {
                binder.ensure_now_function_allowed()?;
                // NOTE: this will be further transformed during optimization. See the
                // documentation of `Now`.
                Ok(Now.into())
            })
        }

        fn pi() -> Handle {
            raw_literal(ExprImpl::literal_f64(std::f64::consts::PI))
        }

        fn proctime() -> Handle {
            Box::new(move |binder, inputs| {
                binder.ensure_proctime_function_allowed()?;
                raw_call(ExprType::Proctime)(binder, inputs)
            })
        }

        // `SESSION_USER` is the user name of the user that is connected to the database.
        fn session_user() -> Handle {
            guard_by_len(|binder, []| {
                Ok(ExprImpl::literal_varchar(
                    binder.auth_context.user_name.clone(),
                ))
            })
        }

        // `CURRENT_USER` is the user name of the user that is executing the command,
        // `CURRENT_ROLE`, `USER` are synonyms for `CURRENT_USER`. Since we don't support
        // `SET ROLE xxx` for now, they will all returns session user name.
        fn current_user() -> Handle {
            guard_by_len(|binder, []| {
                Ok(ExprImpl::literal_varchar(
                    binder.auth_context.user_name.clone(),
                ))
            })
        }

        // `CURRENT_DATABASE` is the name of the database you are currently connected to.
        // `CURRENT_CATALOG` is a synonym for `CURRENT_DATABASE`.
        fn current_database() -> Handle {
            guard_by_len(|binder, []| Ok(ExprImpl::literal_varchar(binder.db_name.clone())))
        }

        // XXX: can we unify this with FUNC_SIG_MAP?
        // For raw_call here, it seems unnecessary to declare it again here.
        // For some functions, we have validation logic here. Is it still useful now?
        static HANDLES: LazyLock<HashMap<&'static str, Handle>> = LazyLock::new(|| {
            [
                (
                    "booleq",
                    rewrite(ExprType::Equal, rewrite_two_bool_inputs),
                ),
                (
                    "boolne",
                    rewrite(ExprType::NotEqual, rewrite_two_bool_inputs),
                ),
                ("coalesce", rewrite(ExprType::Coalesce, |inputs| {
                    if inputs.iter().any(ExprImpl::has_table_function) {
                        return Err(ErrorCode::BindError("table functions are not allowed in COALESCE".into()).into());
                    }
                    Ok(inputs)
                })),
                (
                    "nullif",
                    rewrite(ExprType::Case, rewrite_nullif_to_case_when),
                ),
                (
                    "round",
                    dispatch_by_len(vec![
                        (2, raw_call(ExprType::RoundDigit)),
                        (1, raw_call(ExprType::Round)),
                    ]),
                ),
                ("pow", raw_call(ExprType::Pow)),
                // "power" is the function name used in PG.
                ("power", raw_call(ExprType::Pow)),
                ("ceil", raw_call(ExprType::Ceil)),
                ("ceiling", raw_call(ExprType::Ceil)),
                ("floor", raw_call(ExprType::Floor)),
                ("trunc", raw_call(ExprType::Trunc)),
                ("abs", raw_call(ExprType::Abs)),
                ("exp", raw_call(ExprType::Exp)),
                ("ln", raw_call(ExprType::Ln)),
                ("log", raw_call(ExprType::Log10)),
                ("log10", raw_call(ExprType::Log10)),
                ("mod", raw_call(ExprType::Modulus)),
                ("sin", raw_call(ExprType::Sin)),
                ("cos", raw_call(ExprType::Cos)),
                ("tan", raw_call(ExprType::Tan)),
                ("cot", raw_call(ExprType::Cot)),
                ("asin", raw_call(ExprType::Asin)),
                ("acos", raw_call(ExprType::Acos)),
                ("atan", raw_call(ExprType::Atan)),
                ("atan2", raw_call(ExprType::Atan2)),
                ("sind", raw_call(ExprType::Sind)),
                ("cosd", raw_call(ExprType::Cosd)),
                ("cotd", raw_call(ExprType::Cotd)),
                ("tand", raw_call(ExprType::Tand)),
                ("sinh", raw_call(ExprType::Sinh)),
                ("cosh", raw_call(ExprType::Cosh)),
                ("tanh", raw_call(ExprType::Tanh)),
                ("coth", raw_call(ExprType::Coth)),
                ("asinh", raw_call(ExprType::Asinh)),
                ("acosh", raw_call(ExprType::Acosh)),
                ("atanh", raw_call(ExprType::Atanh)),
                ("asind", raw_call(ExprType::Asind)),
                ("acosd", raw_call(ExprType::Acosd)),
                ("atand", raw_call(ExprType::Atand)),
                ("atan2d", raw_call(ExprType::Atan2d)),
                ("degrees", raw_call(ExprType::Degrees)),
                ("radians", raw_call(ExprType::Radians)),
                ("sqrt", raw_call(ExprType::Sqrt)),
                ("cbrt", raw_call(ExprType::Cbrt)),
                ("sign", raw_call(ExprType::Sign)),
                ("scale", raw_call(ExprType::Scale)),
                ("min_scale", raw_call(ExprType::MinScale)),
                ("trim_scale", raw_call(ExprType::TrimScale)),
                ("gamma", raw_call(ExprType::Gamma)),
                ("lgamma", raw_call(ExprType::Lgamma)),
                // date and time
                (
                    "to_timestamp",
                    dispatch_by_len(vec![
                        (1, raw_call(ExprType::SecToTimestamptz)),
                        (2, raw_call(ExprType::CharToTimestamptz)),
                    ]),
                ),
                ("date_trunc", raw_call(ExprType::DateTrunc)),
                ("date_bin", raw_call(ExprType::DateBin)),
                ("date_part", raw_call(ExprType::DatePart)),
                ("make_date", raw_call(ExprType::MakeDate)),
                ("make_time", raw_call(ExprType::MakeTime)),
                ("make_timestamp", raw_call(ExprType::MakeTimestamp)),
                ("make_timestamptz", raw_call(ExprType::MakeTimestamptz)),
                ("make_interval", raw_call(ExprType::MakeInterval)),
                ("timezone", guard_by_len(|_binder, [arg0, arg1]| {
                    // swap the first and second argument
                    Ok(FunctionCall::new(ExprType::AtTimeZone, vec![arg1, arg0])?.into())
                })),
                ("to_date", raw_call(ExprType::CharToDate)),
                // string
                ("substr", raw_call(ExprType::Substr)),
                ("length", raw_call(ExprType::Length)),
                ("upper", raw_call(ExprType::Upper)),
                ("lower", raw_call(ExprType::Lower)),
                ("trim", raw_call(ExprType::Trim)),
                ("replace", raw_call(ExprType::Replace)),
                ("overlay", raw_call(ExprType::Overlay)),
                ("btrim", raw_call(ExprType::Trim)),
                ("ltrim", raw_call(ExprType::Ltrim)),
                ("rtrim", raw_call(ExprType::Rtrim)),
                ("md5", raw_call(ExprType::Md5)),
                ("to_char", raw_call(ExprType::ToChar)),
                (
                    "concat",
                    rewrite(ExprType::ConcatWs, rewrite_concat_to_concat_ws),
                ),
                ("concat_ws", raw_call(ExprType::ConcatWs)),
                ("format", raw_call(ExprType::Format)),
                ("translate", raw_call(ExprType::Translate)),
                ("split_part", raw_call(ExprType::SplitPart)),
                ("char_length", raw_call(ExprType::CharLength)),
                ("character_length", raw_call(ExprType::CharLength)),
                ("repeat", raw_call(ExprType::Repeat)),
                ("ascii", raw_call(ExprType::Ascii)),
                ("octet_length", raw_call(ExprType::OctetLength)),
                ("bit_length", raw_call(ExprType::BitLength)),
                ("regexp_match", raw_call(ExprType::RegexpMatch)),
                ("regexp_replace", raw_call(ExprType::RegexpReplace)),
                ("regexp_count", raw_call(ExprType::RegexpCount)),
                ("regexp_split_to_array", raw_call(ExprType::RegexpSplitToArray)),
                ("chr", raw_call(ExprType::Chr)),
                ("starts_with", raw_call(ExprType::StartsWith)),
                ("initcap", raw_call(ExprType::Initcap)),
                ("lpad", raw_call(ExprType::Lpad)),
                ("rpad", raw_call(ExprType::Rpad)),
                ("reverse", raw_call(ExprType::Reverse)),
                ("strpos", raw_call(ExprType::Position)),
                ("to_ascii", raw_call(ExprType::ToAscii)),
                ("to_hex", raw_call(ExprType::ToHex)),
                ("quote_ident", raw_call(ExprType::QuoteIdent)),
                ("quote_literal", guard_by_len(|_binder, [mut input]| {
                    if input.return_type() != DataType::Varchar {
                        // Support `quote_literal(any)` by converting it to `quote_literal(any::text)`
                        // Ref. https://github.com/postgres/postgres/blob/REL_16_1/src/include/catalog/pg_proc.dat#L4641
                        FunctionCall::cast_mut(&mut input, &DataType::Varchar, CastContext::Explicit)?;
                    }
                    Ok(FunctionCall::new_unchecked(ExprType::QuoteLiteral, vec![input], DataType::Varchar).into())
                })),
                ("quote_nullable", guard_by_len(|_binder, [mut input]| {
                    if input.return_type() != DataType::Varchar {
                        // Support `quote_nullable(any)` by converting it to `quote_nullable(any::text)`
                        // Ref. https://github.com/postgres/postgres/blob/REL_16_1/src/include/catalog/pg_proc.dat#L4650
                        FunctionCall::cast_mut(&mut input, &DataType::Varchar, CastContext::Explicit)?;
                    }
                    Ok(FunctionCall::new_unchecked(ExprType::QuoteNullable, vec![input], DataType::Varchar).into())
                })),
                ("string_to_array", raw_call(ExprType::StringToArray)),
                ("get_bit", raw_call(ExprType::GetBit)),
                ("get_byte", raw_call(ExprType::GetByte)),
                ("set_bit", raw_call(ExprType::SetBit)),
                ("set_byte", raw_call(ExprType::SetByte)),
                ("bit_count", raw_call(ExprType::BitCount)),
                ("encode", raw_call(ExprType::Encode)),
                ("decode", raw_call(ExprType::Decode)),
                ("convert_from", raw_call(ExprType::ConvertFrom)),
                ("convert_to", raw_call(ExprType::ConvertTo)),
                ("sha1", raw_call(ExprType::Sha1)),
                ("sha224", raw_call(ExprType::Sha224)),
                ("sha256", raw_call(ExprType::Sha256)),
                ("sha384", raw_call(ExprType::Sha384)),
                ("sha512", raw_call(ExprType::Sha512)),
                ("encrypt", raw_call(ExprType::Encrypt)),
                ("decrypt", raw_call(ExprType::Decrypt)),
                ("hmac", raw_call(ExprType::Hmac)),
                ("crc32", raw_call(ExprType::Crc32)),
                ("crc32c", raw_call(ExprType::Crc32c)),
                ("secure_compare", raw_call(ExprType::SecureCompare)),
                ("left", raw_call(ExprType::Left)),
                ("right", raw_call(ExprType::Right)),
                ("inet_aton", raw_call(ExprType::InetAton)),
                ("inet_ntoa", raw_call(ExprType::InetNtoa)),
                ("int8send", raw_call(ExprType::PgwireSend)),
                ("int8recv", guard_by_len(|_binder, [mut input]| {
                    // Similar to `cast` from string, return type is set explicitly rather than inferred.
                    let hint = if !input.is_untyped() && input.return_type() == DataType::Varchar {
                        " Consider `decode` or cast."
                    } else {
                        ""
                    };
                    input.cast_implicit_mut(&DataType::Bytea).map_err(|e| {
                        ErrorCode::BindError(format!("{} in `recv`.{hint}", e.as_report()))
                    })?;
                    Ok(FunctionCall::new_unchecked(ExprType::PgwireRecv, vec![input], DataType::Int64).into())
                })),
                // array
                ("array_cat", raw_call(ExprType::ArrayCat)),
                ("array_append", raw_call(ExprType::ArrayAppend)),
                ("array_join", raw_call(ExprType::ArrayToString)),
                ("array_prepend", raw_call(ExprType::ArrayPrepend)),
                ("array_to_string", raw_call(ExprType::ArrayToString)),
                ("array_distinct", raw_call(ExprType::ArrayDistinct)),
                ("array_min", raw_call(ExprType::ArrayMin)),
                ("array_sort", raw_call(ExprType::ArraySort)),
                ("array_length", raw_call(ExprType::ArrayLength)),
                ("cardinality", raw_call(ExprType::Cardinality)),
                ("array_remove", raw_call(ExprType::ArrayRemove)),
                ("array_replace", raw_call(ExprType::ArrayReplace)),
                ("array_reverse", raw_call(ExprType::ArrayReverse)),
                ("array_max", raw_call(ExprType::ArrayMax)),
                ("array_sum", raw_call(ExprType::ArraySum)),
                ("array_position", raw_call(ExprType::ArrayPosition)),
                ("array_positions", raw_call(ExprType::ArrayPositions)),
                ("array_contains", raw_call(ExprType::ArrayContains)),
                ("arraycontains", raw_call(ExprType::ArrayContains)),
                ("array_contained", raw_call(ExprType::ArrayContained)),
                ("arraycontained", raw_call(ExprType::ArrayContained)),
                ("array_overlaps", raw_call(ExprType::ArrayOverlaps)),
                ("array_is_overlap", raw_call(ExprType::ArrayOverlaps)),
                ("array_is_intersect", raw_call(ExprType::ArrayOverlaps)),
                ("array_flatten", guard_by_len(|_binder, [input]| {
                    input.ensure_array_type().map_err(|_| ErrorCode::BindError("array_flatten expects `any[][]` input".into()))?;
                    let return_type = input.return_type().into_list_elem();
                    if !return_type.is_array() {
                        return Err(ErrorCode::BindError("array_flatten expects `any[][]` input".into()).into());
                    }
                    Ok(FunctionCall::new_unchecked(ExprType::ArrayFlatten, vec![input], return_type).into())
                })),
                ("trim_array", raw_call(ExprType::TrimArray)),
                (
                    "array_ndims",
                    guard_by_len(|_binder, [input]| {
                        input.ensure_array_type()?;

                        let n = input.return_type().array_ndims()
                            .try_into().map_err(|_| ErrorCode::BindError("array_ndims integer overflow".into()))?;
                        Ok(ExprImpl::literal_int(n))
                    }),
                ),
                (
                    "array_lower",
                    guard_by_len(|binder, [arg0, arg1]| {
                        // rewrite into `CASE WHEN 0 < arg1 AND arg1 <= array_ndims(arg0) THEN 1 END`
                        let ndims_expr = binder.bind_builtin_scalar_function("array_ndims", vec![arg0], false)?;
                        let arg1 = arg1.cast_implicit(&DataType::Int32)?;

                        FunctionCall::new(
                            ExprType::Case,
                            vec![
                                FunctionCall::new(
                                    ExprType::And,
                                    vec![
                                        FunctionCall::new(ExprType::LessThan, vec![ExprImpl::literal_int(0), arg1.clone()])?.into(),
                                        FunctionCall::new(ExprType::LessThanOrEqual, vec![arg1, ndims_expr])?.into(),
                                    ],
                                )?.into(),
                                ExprImpl::literal_int(1),
                            ],
                        ).map(Into::into)
                    }),
                ),
                ("array_upper", raw_call(ExprType::ArrayLength)), // `lower == 1` implies `upper == length`
                ("array_dims", raw_call(ExprType::ArrayDims)),
                // int256
                ("hex_to_int256", raw_call(ExprType::HexToInt256)),
                // jsonb
                ("jsonb_object_field", raw_call(ExprType::JsonbAccess)),
                ("jsonb_array_element", raw_call(ExprType::JsonbAccess)),
                ("jsonb_object_field_text", raw_call(ExprType::JsonbAccessStr)),
                ("jsonb_array_element_text", raw_call(ExprType::JsonbAccessStr)),
                ("jsonb_extract_path", raw_call(ExprType::JsonbExtractPath)),
                ("jsonb_extract_path_text", raw_call(ExprType::JsonbExtractPathText)),
                ("jsonb_typeof", raw_call(ExprType::JsonbTypeof)),
                ("jsonb_array_length", raw_call(ExprType::JsonbArrayLength)),
                ("jsonb_concat", raw_call(ExprType::JsonbConcat)),
                ("jsonb_object", raw_call(ExprType::JsonbObject)),
                ("jsonb_pretty", raw_call(ExprType::JsonbPretty)),
                ("jsonb_contains", raw_call(ExprType::JsonbContains)),
                ("jsonb_contained", raw_call(ExprType::JsonbContained)),
                ("jsonb_exists", raw_call(ExprType::JsonbExists)),
                ("jsonb_exists_any", raw_call(ExprType::JsonbExistsAny)),
                ("jsonb_exists_all", raw_call(ExprType::JsonbExistsAll)),
                ("jsonb_delete", raw_call(ExprType::Subtract)),
                ("jsonb_delete_path", raw_call(ExprType::JsonbDeletePath)),
                ("jsonb_strip_nulls", raw_call(ExprType::JsonbStripNulls)),
                ("to_jsonb", raw_call(ExprType::ToJsonb)),
                ("jsonb_build_array", raw_call(ExprType::JsonbBuildArray)),
                ("jsonb_build_object", raw_call(ExprType::JsonbBuildObject)),
                ("jsonb_populate_record", raw_call(ExprType::JsonbPopulateRecord)),
                ("jsonb_path_match", raw_call(ExprType::JsonbPathMatch)),
                ("jsonb_path_exists", raw_call(ExprType::JsonbPathExists)),
                ("jsonb_path_query_array", raw_call(ExprType::JsonbPathQueryArray)),
                ("jsonb_path_query_first", raw_call(ExprType::JsonbPathQueryFirst)),
                ("jsonb_set", raw_call(ExprType::JsonbSet)),
                ("jsonb_populate_map", raw_call(ExprType::JsonbPopulateMap)),
                ("jsonb_to_array", raw_call(ExprType::JsonbToArray)),
                // variant
                ("to_variant", raw_call(ExprType::ToVariant)),
                ("variant_get", raw_call(ExprType::VariantGet)),
                ("try_variant_get", raw_call(ExprType::TryVariantGet)),
                ("variant_typeof", raw_call(ExprType::VariantTypeof)),
                // map
                ("map_from_entries", raw_call(ExprType::MapFromEntries)),
                ("map_access", raw_call(ExprType::MapAccess)),
                ("map_keys", raw_call(ExprType::MapKeys)),
                ("map_values", raw_call(ExprType::MapValues)),
                ("map_entries", raw_call(ExprType::MapEntries)),
                ("map_from_key_values", raw_call(ExprType::MapFromKeyValues)),
                ("map_cat", raw_call(ExprType::MapCat)),
                ("map_contains", raw_call(ExprType::MapContains)),
                ("map_delete", raw_call(ExprType::MapDelete)),
                ("map_insert", raw_call(ExprType::MapInsert)),
                ("map_length", raw_call(ExprType::MapLength)),
                // vector
                ("l2_distance", raw_call(ExprType::L2Distance)),
                ("cosine_distance", raw_call(ExprType::CosineDistance)),
                ("l1_distance", raw_call(ExprType::L1Distance)),
                ("inner_product", raw_call(ExprType::InnerProduct)),
                ("vector_norm", raw_call(ExprType::L2Norm)),
                ("l2_normalize", raw_call(ExprType::L2Normalize)),
                ("subvector", guard_by_len(|_, [vector_expr, start_expr, len_expr]| {
                    let dimensions = if let DataType::Vector(length) = vector_expr.return_type() {
                        length as i32
                    } else {
                        return Err(ErrorCode::BindError("subvector expects `vector(dim)` input".into()).into());
                    };
                    let start = start_expr
                        .try_fold_const()
                        .transpose()?
                        .and_then(|datum| match datum {
                            Some(ScalarImpl::Int32(v)) => Some(v),
                            _ => None,
                        })
                        .ok_or_else(|| ErrorCode::ExprError("`start` must be an Int32 constant".into()))?;

                    let len = len_expr
                        .try_fold_const()
                        .transpose()?
                        .and_then(|datum| match datum {
                            Some(ScalarImpl::Int32(v)) => Some(v),
                            _ => None,
                        })
                        .ok_or_else(|| ErrorCode::ExprError("`count` must be an Int32 constant".into()))?;
                    if len < 1 || len > DataType::VEC_MAX_SIZE as i32 {
                        return Err(ErrorCode::InvalidParameterValue(format!("Invalid vector size: expected 1..={}, got {}", DataType::VEC_MAX_SIZE, len)).into());
                    }

                    let end = start + len - 1;

                    if start < 1 || end > dimensions {
                        return Err(ErrorCode::InvalidParameterValue(format!(
                                "vector slice range out of bounds: start={}, end={}, valid range is [1, {}]",
                                start,
                                end,
                                dimensions
                            )).into());
                    }

                    Ok(FunctionCall::new_unchecked(ExprType::Subvector, vec![vector_expr, start_expr, len_expr], DataType::Vector(len as usize)).into())
                })),
                // Functions that return a constant value
                ("pi", pi()),
                // greatest and least
                ("greatest", raw_call(ExprType::Greatest)),
                ("least", raw_call(ExprType::Least)),
                // System information operations.
                (
                    "pg_typeof",
                    guard_by_len(|_binder, [input]| {
                        let v = match input.is_untyped() {
                            true => "unknown".into(),
                            false => input.return_type().to_string(),
                        };
                        Ok(ExprImpl::literal_varchar(v))
                    }),
                ),
                ("current_catalog", current_database()),
                ("current_database", current_database()),
                ("current_schema", guard_by_len(|binder, []| {
                    Ok(binder
                        .first_valid_schema()
                        .map(|schema| ExprImpl::literal_varchar(schema.name()))
                        .unwrap_or_else(|_| ExprImpl::literal_null(DataType::Varchar)))
                })),
                ("current_schemas", raw(|binder, mut inputs| {
                    let no_match_err = ErrorCode::ExprError(
                        "No function matches the given name and argument types. You might need to add explicit type casts.".into()
                    );
                    if inputs.len() != 1 {
                        return Err(no_match_err.into());
                    }
                    let input = inputs
                        .pop()
                        .unwrap()
                        .enforce_bool_clause("current_schemas")
                        .map_err(|_| no_match_err)?;

                    let ExprImpl::Literal(literal) = &input else {
                        bail_not_implemented!("Only boolean literals are supported in `current_schemas`.");
                    };

                    let Some(bool) = literal.get_data().as_ref().map(|bool| bool.clone().into_bool()) else {
                        return Ok(ExprImpl::literal_null(DataType::Varchar.list()));
                    };

                    let paths = if bool {
                        binder.search_path.path()
                    } else {
                        binder.search_path.real_path()
                    };

                    let mut schema_names = vec![];
                    for path in paths {
                        let mut schema_name = path;
                        if schema_name == USER_NAME_WILD_CARD {
                            schema_name = &binder.auth_context.user_name;
                        }

                        if binder
                            .catalog
                            .get_schema_by_name(&binder.db_name, schema_name)
                            .is_ok()
                        {
                            schema_names.push(schema_name.as_str());
                        }
                    }

                    Ok(ExprImpl::literal_list(
                        ListValue::from_iter(schema_names),
                        DataType::Varchar,
                    ))
                })),
                ("session_user", session_user()),
                ("current_role", current_user()),
                ("current_user", current_user()),
                ("user", current_user()),
                ("pg_get_userbyid", raw_call(ExprType::PgGetUserbyid)),
                ("pg_get_indexdef", raw_call(ExprType::PgGetIndexdef)),
                ("pg_get_viewdef", raw_call(ExprType::PgGetViewdef)),
                ("pg_index_column_has_property", raw_call(ExprType::PgIndexColumnHasProperty)),
                ("pg_relation_size", raw(|_binder, mut inputs| {
                    if inputs.is_empty() {
                        return Err(ErrorCode::ExprError(
                            "function pg_relation_size() does not exist".into(),
                        )
                            .into());
                    }
                    inputs[0].cast_to_regclass_mut()?;
                    Ok(FunctionCall::new(ExprType::PgRelationSize, inputs)?.into())
                })),
                ("pg_get_serial_sequence", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_table_size", guard_by_len(|_binder, [mut input]| {
                    input.cast_to_regclass_mut()?;
                    Ok(FunctionCall::new(ExprType::PgRelationSize, vec![input])?.into())
                })),
                ("pg_indexes_size", guard_by_len(|_binder, [mut input]| {
                    input.cast_to_regclass_mut()?;
                    Ok(FunctionCall::new(ExprType::PgIndexesSize, vec![input])?.into())
                })),
                ("pg_get_expr", raw(|_binder, inputs| {
                    if inputs.len() == 2 || inputs.len() == 3 {
                        // TODO: implement pg_get_expr rather than just return empty as an workaround.
                        Ok(ExprImpl::literal_varchar("".into()))
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.pg_get_expr()".into(),
                        )
                            .into())
                    }
                })),
                ("pg_my_temp_schema", guard_by_len(|_binder, []| {
                    // Returns the OID of the current session's temporary schema, or zero if it has none (because it has not created any temporary tables).
                    Ok(ExprImpl::literal_int(
                        // always return 0, as we haven't supported temporary tables nor temporary schema yet
                        0,
                    ))
                })),
                ("current_setting", guard_by_len(|binder, [input]| {
                    let input = if let ExprImpl::Literal(literal) = &input &&
                        let Some(ScalarImpl::Utf8(input)) = literal.get_data()
                    {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only literal is supported in `setting_name`.".into(),
                        )
                            .into());
                    };
                    let session_config = binder.session_config.read();
                    Ok(ExprImpl::literal_varchar(session_config.get(input.as_ref())?))
                })),
                ("set_config", guard_by_len(|binder, [arg0, arg1, arg2]| {
                    let setting_name = if let ExprImpl::Literal(literal) = &arg0 && let Some(ScalarImpl::Utf8(input)) = literal.get_data() {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only string literal is supported in `setting_name`.".into(),
                        )
                            .into());
                    };

                    let new_value = if let ExprImpl::Literal(literal) = &arg1 && let Some(ScalarImpl::Utf8(input)) = literal.get_data() {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only string literal is supported in `setting_name`.".into(),
                        )
                            .into());
                    };

                    let is_local = if let ExprImpl::Literal(literal) = &arg2 && let Some(ScalarImpl::Bool(input)) = literal.get_data() {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only bool literal is supported in `is_local`.".into(),
                        )
                            .into());
                    };

                    if *is_local {
                        return Err(ErrorCode::ExprError(
                            "`is_local = true` is not supported now.".into(),
                        )
                            .into());
                    }

                    let mut session_config = binder.session_config.write();

                    // TODO: report session config changes if necessary.
                    session_config.set(setting_name, new_value.to_string(), &mut ())?;

                    Ok(ExprImpl::literal_varchar(new_value.to_string()))
                })),
                ("format_type", raw_call(ExprType::FormatType)),
                ("pg_table_is_visible", raw_call(ExprType::PgTableIsVisible)),
                ("pg_type_is_visible", raw_literal(ExprImpl::literal_bool(true))),
                ("pg_get_constraintdef", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_get_partkeydef", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_encoding_to_char", raw_literal(ExprImpl::literal_varchar("UTF8".into()))),
                ("has_database_privilege", raw(|binder, mut inputs| {
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        Ok(FunctionCall::new(ExprType::HasDatabasePrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_database_privilege()".into(),
                        )
                            .into())
                    }
                })),
                ("has_table_privilege", raw(|binder, mut inputs| {
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        if inputs[1].return_type() == DataType::Varchar {
                            inputs[1].cast_to_regclass_mut()?;
                        }
                        Ok(FunctionCall::new(ExprType::HasTablePrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_table_privilege()".into(),
                        )
                            .into())
                    }
                })),
                ("has_any_column_privilege", raw(|binder, mut inputs| {
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        if inputs[1].return_type() == DataType::Varchar {
                            inputs[1].cast_to_regclass_mut()?;
                        }
                        Ok(FunctionCall::new(ExprType::HasAnyColumnPrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_any_column_privilege()".into(),
                        )
                            .into())
                    }
                })),
                ("has_schema_privilege", raw(|binder, mut inputs| {
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        Ok(FunctionCall::new(ExprType::HasSchemaPrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_schema_privilege()".into(),
                        )
                            .into())
                    }
                })),
                ("has_function_privilege", raw(|binder, mut inputs| {
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        Ok(FunctionCall::new(ExprType::HasFunctionPrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_function_privilege()".into(),
                        )
                            .into())
                    }
                })),
                ("pg_stat_get_numscans", raw_literal(ExprImpl::literal_bigint(0))),
                ("pg_backend_pid", raw(|binder, _inputs| {
                    // FIXME: the session id is not global unique in multi-frontend env.
                    Ok(ExprImpl::literal_int(binder.session_id.0))
                })),
                ("pg_cancel_backend", guard_by_len(|_binder, [_input]| {
                    // TODO: implement real cancel rather than just return false as an workaround.
                    Ok(ExprImpl::literal_bool(false))
                })),
                ("pg_terminate_backend", guard_by_len(|_binder, [_input]| {
                    // TODO: implement real terminate rather than just return false as an
                    // workaround.
                    Ok(ExprImpl::literal_bool(false))
                })),
                ("pg_tablespace_location", guard_by_len(|_binder, [_input]| {
                    Ok(ExprImpl::literal_null(DataType::Varchar))
                })),
                ("pg_postmaster_start_time", guard_by_len(|_binder, []| {
                    let server_start_time = risingwave_variables::get_server_start_time();
                    let datum = server_start_time.map(Timestamptz::from).map(ScalarImpl::from);
                    let literal = Literal::new(datum, DataType::Timestamptz);
                    Ok(literal.into())
                })),
                // TODO: really implement them.
                // https://www.postgresql.org/docs/9.5/functions-info.html#FUNCTIONS-INFO-COMMENT-TABLE
                // WARN: Hacked in [`Binder::bind_function`]!!!
                ("col_description", raw_call(ExprType::ColDescription)),
                ("obj_description", raw_literal(ExprImpl::literal_varchar("".to_owned()))),
                ("shobj_description", raw_literal(ExprImpl::literal_varchar("".to_owned()))),
                ("pg_is_in_recovery", raw_call(ExprType::PgIsInRecovery)),
                ("rw_recovery_status", raw_call(ExprType::RwRecoveryStatus)),
                ("rw_cluster_id", raw_call(ExprType::RwClusterId)),
                ("rw_epoch_to_ts", raw_call(ExprType::RwEpochToTs)),
                ("rw_fragment_vnodes", raw_call(ExprType::RwFragmentVnodes)),
                ("rw_actor_vnodes", raw_call(ExprType::RwActorVnodes)),
                // internal
                ("rw_vnode", raw_call(ExprType::VnodeUser)),
                ("rw_license", raw_call(ExprType::License)),
                ("rw_test_paid_tier", raw_call(ExprType::TestFeature)), // deprecated, kept for compatibility
                ("rw_test_feature", raw_call(ExprType::TestFeature)), // for testing purposes
                // TODO: choose which pg version we should return.
                ("version", raw_literal(ExprImpl::literal_varchar(current_cluster_version()))),
                // non-deterministic
                ("now", now()),
                ("current_timestamp", now()),
                ("proctime", proctime()),
                ("pg_sleep", raw_call(ExprType::PgSleep)),
                ("pg_sleep_for", raw_call(ExprType::PgSleepFor)),
                ("random", raw_call(ExprType::Random)),
                ("clock_timestamp", raw_call(ExprType::ClockTimestamp)),
                ("gen_random_uuid", raw_call(ExprType::GenRandomUuid)),
                // TODO: implement pg_sleep_until
                // ("pg_sleep_until", raw_call(ExprType::PgSleepUntil)),

                // cast functions
                // only functions required by the existing PostgreSQL tool are implemented
                ("date", guard_by_len(|_binder, [input]| {
                    input.cast_explicit(&DataType::Date).map_err(Into::into)
                })),

                // AI model functions
                ("openai_embedding", guard_by_len(|_binder, [arg0, arg1]| {
                    // check if the first two arguments are constants
                    if let ExprImpl::Literal(config) = &arg0 && let Some(ScalarImpl::Jsonb(_config)) = config.get_data() {
                        Ok(FunctionCall::new(ExprType::OpenaiEmbedding, vec![arg0, arg1])?.into())
                    } else {
                        Err(ErrorCode::InvalidInputSyntax(
                            "`embedding_config` must be constant jsonb".to_owned(),
                        ).into())
                    }
                })),
            ]
                .into_iter()
                .collect()
        });

        static FUNCTIONS_BKTREE: LazyLock<BKTree<&str>> = LazyLock::new(|| {
            let mut tree = BKTree::new(metrics::Levenshtein);

            // TODO: Also hint other functions, e.g., Agg or UDF.
            for k in HANDLES.keys() {
                tree.add(*k);
            }

            tree
        });

        if variadic {
            let func = match function_name {
                "format" => ExprType::FormatVariadic,
                "concat" => ExprType::ConcatVariadic,
                "concat_ws" => ExprType::ConcatWsVariadic,
                "jsonb_build_array" => ExprType::JsonbBuildArrayVariadic,
                "jsonb_build_object" => ExprType::JsonbBuildObjectVariadic,
                "jsonb_extract_path" => ExprType::JsonbExtractPathVariadic,
                "jsonb_extract_path_text" => ExprType::JsonbExtractPathTextVariadic,
                _ => {
                    return Err(ErrorCode::BindError(format!(
                        "VARIADIC argument is not allowed in function \"{}\"",
                        function_name
                    ))
                    .into());
                }
            };
            return Ok(FunctionCall::new(func, inputs)?.into());
        }

        // Note: for raw_call, we only check name here. The type check is done later.
        match HANDLES.get(function_name) {
            Some(handle) => handle(self, inputs),
            None => {
                let allowed_distance = if function_name.len() > 3 { 2 } else { 1 };

                let candidates = FUNCTIONS_BKTREE
                    .find(function_name, allowed_distance)
                    .map(|(_idx, c)| c)
                    .join(" or ");

                Err(no_function!(
                    candidates = (!candidates.is_empty()).then_some(candidates),
                    "{}({})",
                    function_name,
                    inputs.iter().map(|e| e.return_type()).join(", ")
                )
                .into())
            }
        }
    }

    fn ensure_now_function_allowed(&self) -> Result<()> {
        if self.is_for_stream()
            && !matches!(
                self.context.clause,
                Some(Clause::Where)
                    | Some(Clause::Having)
                    | Some(Clause::JoinOn)
                    | Some(Clause::From)
            )
        {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "For streaming queries, `NOW()` function is only allowed in `WHERE`, `HAVING`, `ON` and `FROM`. Found in clause: {:?}. \
                Please refer to https://docs.risingwave.com/processing/sql/temporal-filters for more information",
                self.context.clause
            ))
                .into());
        }
        if matches!(self.context.clause, Some(Clause::GeneratedColumn)) {
            return Err(ErrorCode::InvalidInputSyntax(
                "Cannot use `NOW()` function in generated columns. Do you want `PROCTIME()`?"
                    .to_owned(),
            )
            .into());
        }
        Ok(())
    }

    fn ensure_proctime_function_allowed(&self) -> Result<()> {
        if !self.is_for_ddl() {
            return Err(ErrorCode::InvalidInputSyntax(
                "Function `PROCTIME()` is only allowed in CREATE TABLE/SOURCE. Is `NOW()` what you want?".to_owned(),
            )
                .into());
        }
        Ok(())
    }
}

fn rewrite_concat_to_concat_ws(inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
    if inputs.is_empty() {
        Err(ErrorCode::BindError(
            "Function `concat` takes at least 1 arguments (0 given)".to_owned(),
        )
        .into())
    } else {
        let inputs = std::iter::once(ExprImpl::literal_varchar("".to_owned()))
            .chain(inputs)
            .collect();
        Ok(inputs)
    }
}

/// Make sure inputs only have 2 value and rewrite the arguments.
/// Nullif(expr1,expr2) -> Case(Equal(expr1 = expr2),null,expr1).
fn rewrite_nullif_to_case_when(inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
    if inputs.len() != 2 {
        Err(ErrorCode::BindError("Function `nullif` must contain 2 arguments".to_owned()).into())
    } else {
        let inputs = vec![
            FunctionCall::new(ExprType::Equal, inputs.clone())?.into(),
            Literal::new(None, inputs[0].return_type()).into(),
            inputs[0].clone(),
        ];
        Ok(inputs)
    }
}

fn rewrite_two_bool_inputs(mut inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
    if inputs.len() != 2 {
        return Err(
            ErrorCode::BindError("function must contain only 2 arguments".to_owned()).into(),
        );
    }
    let left = inputs.pop().unwrap();
    let right = inputs.pop().unwrap();
    Ok(vec![
        left.cast_implicit(&DataType::Boolean)?,
        right.cast_implicit(&DataType::Boolean)?,
    ])
}
