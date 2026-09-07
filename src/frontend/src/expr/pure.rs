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

use std::borrow::Cow;

use expr_node::Type;
use risingwave_expr::sig::FuncName;
use risingwave_pb::expr::expr_node;

use super::{ExprImpl, ExprVisitor, FunctionCall, TableFunction, UserDefinedFunction};
use crate::expr::table_function::TableFunctionType;

#[derive(Default)]
pub(crate) struct ImpureAnalyzer {
    impure: Option<Cow<'static, str>>,
}

impl ImpureAnalyzer {
    /// Returns `true` if the expression is impure.
    ///
    /// Only call this method after visiting the expression.
    pub fn is_impure(&self) -> bool {
        self.impure.is_some()
    }

    /// Returns the description of the impure expression if it is impure, for error reporting.
    /// `None` if the expression is pure.
    ///
    /// Only call this method after visiting the expression.
    pub fn impure_expr_desc(&self) -> Option<&str> {
        self.impure.as_deref()
    }
}

trait BuiltinFunctionCall {
    fn func_name(&self) -> FuncName;
}

impl BuiltinFunctionCall for FunctionCall {
    fn func_name(&self) -> FuncName {
        self.func_type().into()
    }
}

impl BuiltinFunctionCall for TableFunction {
    fn func_name(&self) -> FuncName {
        self.function_type.into()
    }
}

/// Returns whether a built-in function produces the same result for the same inputs.
///
/// The volatile built-ins are listed once here so every built-in expression variant uses the same
/// purity classification.
fn is_builtin_function_deterministic<F>(func_call: &F) -> bool
where
    F: BuiltinFunctionCall + ?Sized,
{
    match func_call.func_name() {
        FuncName::Scalar(Type::Unspecified) => unreachable!(),
        FuncName::Scalar(
            Type::TestFeature
            | Type::License
            | Type::Proctime
            | Type::PgSleep
            | Type::PgSleepFor
            | Type::PgSleepUntil
            | Type::CastRegclass
            | Type::PgGetIndexdef
            | Type::ColDescription
            | Type::PgGetViewdef
            | Type::PgGetUserbyid
            | Type::PgIndexesSize
            | Type::PgRelationSize
            | Type::PgGetSerialSequence
            | Type::PgIndexColumnHasProperty
            | Type::HasTablePrivilege
            | Type::HasAnyColumnPrivilege
            | Type::HasSchemaPrivilege
            | Type::MakeTimestamptz
            | Type::PgIsInRecovery
            | Type::RwRecoveryStatus
            | Type::RwClusterId
            | Type::RwFragmentVnodes
            | Type::RwActorVnodes
            | Type::PgTableIsVisible
            | Type::HasFunctionPrivilege
            | Type::OpenaiEmbedding
            | Type::HasDatabasePrivilege
            | Type::Random
            | Type::ClockTimestamp
            | Type::GenRandomUuid,
        ) => false,
        #[expect(deprecated)]
        FuncName::Scalar(
            Type::Add
            | Type::Subtract
            | Type::Multiply
            | Type::Divide
            | Type::Modulus
            | Type::Equal
            | Type::NotEqual
            | Type::LessThan
            | Type::LessThanOrEqual
            | Type::GreaterThan
            | Type::GreaterThanOrEqual
            | Type::And
            | Type::Or
            | Type::Not
            | Type::In
            | Type::Some
            | Type::All
            | Type::BitwiseAnd
            | Type::BitwiseOr
            | Type::BitwiseXor
            | Type::BitwiseNot
            | Type::BitwiseShiftLeft
            | Type::BitwiseShiftRight
            | Type::Extract
            | Type::DatePart
            | Type::TumbleStart
            | Type::SecToTimestamptz
            | Type::AtTimeZone
            | Type::DateTrunc
            | Type::DateBin
            | Type::MakeDate
            | Type::MakeTime
            | Type::MakeTimestamp
            | Type::CharToTimestamptz
            | Type::CharToDate
            | Type::CastWithTimeZone
            | Type::AddWithTimeZone
            | Type::SubtractWithTimeZone
            | Type::Cast
            | Type::Substr
            | Type::Length
            | Type::Like
            | Type::ILike
            | Type::SimilarToEscape
            | Type::Upper
            | Type::Lower
            | Type::Trim
            | Type::Replace
            | Type::Position
            | Type::Ltrim
            | Type::Rtrim
            | Type::Case
            | Type::ConstantLookup
            | Type::RoundDigit
            | Type::Round
            | Type::Ascii
            | Type::Translate
            | Type::Coalesce
            | Type::ConcatWs
            | Type::ConcatWsVariadic
            | Type::Abs
            | Type::SplitPart
            | Type::Ceil
            | Type::Floor
            | Type::Trunc
            | Type::ToChar
            | Type::Md5
            | Type::CharLength
            | Type::Repeat
            | Type::ConcatOp
            | Type::ByteaConcatOp
            | Type::Concat
            | Type::ConcatVariadic
            | Type::BoolOut
            | Type::OctetLength
            | Type::BitLength
            | Type::Overlay
            | Type::RegexpMatch
            | Type::RegexpReplace
            | Type::RegexpCount
            | Type::RegexpSplitToArray
            | Type::RegexpEq
            | Type::Pow
            | Type::Exp
            | Type::Ln
            | Type::Log10
            | Type::Chr
            | Type::StartsWith
            | Type::Initcap
            | Type::Lpad
            | Type::Rpad
            | Type::Reverse
            | Type::Strpos
            | Type::ToAscii
            | Type::ToHex
            | Type::QuoteIdent
            | Type::Sin
            | Type::Cos
            | Type::Tan
            | Type::Cot
            | Type::Asin
            | Type::Acos
            | Type::Acosd
            | Type::Atan
            | Type::Atan2
            | Type::Atand
            | Type::Atan2d
            | Type::Sqrt
            | Type::Cbrt
            | Type::Sign
            | Type::Scale
            | Type::MinScale
            | Type::TrimScale
            | Type::Gamma
            | Type::Lgamma
            | Type::Left
            | Type::Right
            | Type::Degrees
            | Type::Radians
            | Type::IsTrue
            | Type::IsNotTrue
            | Type::IsFalse
            | Type::IsNotFalse
            | Type::IsNull
            | Type::IsNotNull
            | Type::IsDistinctFrom
            | Type::IsNotDistinctFrom
            | Type::Neg
            | Type::Field
            | Type::Array
            | Type::ArrayAccess
            | Type::ArrayRangeAccess
            | Type::Row
            | Type::ArrayToString
            | Type::ArrayCat
            | Type::ArrayMax
            | Type::ArraySum
            | Type::ArraySort
            | Type::ArrayAppend
            | Type::ArrayReverse
            | Type::ArrayPrepend
            | Type::FormatType
            | Type::ArrayDistinct
            | Type::ArrayMin
            | Type::ArrayDims
            | Type::ArrayLength
            | Type::Cardinality
            | Type::TrimArray
            | Type::ArrayRemove
            | Type::ArrayReplace
            | Type::ArrayPosition
            | Type::ArrayContains
            | Type::ArrayContained
            | Type::ArrayOverlaps
            | Type::ArrayFlatten
            | Type::HexToInt256
            | Type::JsonbConcat
            | Type::JsonbAccess
            | Type::JsonbAccessStr
            | Type::JsonbExtractPath
            | Type::JsonbExtractPathVariadic
            | Type::JsonbExtractPathText
            | Type::JsonbExtractPathTextVariadic
            | Type::JsonbTypeof
            | Type::JsonbArrayLength
            | Type::JsonbObject
            | Type::JsonbPretty
            | Type::JsonbDeletePath
            | Type::JsonbContains
            | Type::JsonbContained
            | Type::JsonbExists
            | Type::JsonbExistsAny
            | Type::JsonbExistsAll
            | Type::JsonbStripNulls
            | Type::JsonbBuildArray
            | Type::JsonbBuildArrayVariadic
            | Type::JsonbBuildObject
            | Type::JsonbPopulateRecord
            | Type::JsonbToArray
            | Type::JsonbToRecord
            | Type::JsonbBuildObjectVariadic
            | Type::JsonbPathExists
            | Type::JsonbPathMatch
            | Type::JsonbPathQueryArray
            | Type::JsonbPathQueryFirst
            | Type::JsonbSet
            | Type::JsonbPopulateMap
            | Type::IsJson
            | Type::ToJsonb
            | Type::ToVariant
            | Type::VariantGet
            | Type::TryVariantGet
            | Type::VariantTypeof
            | Type::Sind
            | Type::Cosd
            | Type::Cotd
            | Type::Asind
            | Type::Sinh
            | Type::Cosh
            | Type::Coth
            | Type::Tanh
            | Type::Atanh
            | Type::Asinh
            | Type::Acosh
            | Type::Decode
            | Type::Encode
            | Type::GetBit
            | Type::GetByte
            | Type::SetBit
            | Type::SetByte
            | Type::BitCount
            | Type::Sha1
            | Type::Sha224
            | Type::Sha256
            | Type::Sha384
            | Type::Sha512
            | Type::Crc32
            | Type::Crc32c
            | Type::Hmac
            | Type::SecureCompare
            | Type::Decrypt
            | Type::Encrypt
            | Type::Tand
            | Type::ArrayPositions
            | Type::StringToArray
            | Type::Format
            | Type::FormatVariadic
            | Type::PgwireSend
            | Type::PgwireRecv
            | Type::ArrayTransform
            | Type::Greatest
            | Type::Least
            | Type::ConvertFrom
            | Type::ConvertTo
            | Type::IcebergTransform
            | Type::InetNtoa
            | Type::InetAton
            | Type::QuoteLiteral
            | Type::QuoteNullable
            | Type::MapFromEntries
            | Type::MapAccess
            | Type::MapKeys
            | Type::MapValues
            | Type::MapEntries
            | Type::MapFromKeyValues
            | Type::MapCat
            | Type::MapContains
            | Type::MapDelete
            | Type::MapFilter
            | Type::MapInsert
            | Type::MapLength
            | Type::L2Distance
            | Type::CosineDistance
            | Type::L1Distance
            | Type::InnerProduct
            | Type::VecConcat
            | Type::L2Norm
            | Type::L2Normalize
            | Type::Subvector
            // TODO: `rw_vnode` is more like STABLE instead of IMMUTABLE, because even its result is
            // deterministic, it needs to read the total vnode count from the context, which means that
            // it cannot be evaluated during constant folding. We have to treat it pure here so it can be used
            // internally without materialization.
            | Type::Vnode
            | Type::VnodeUser
            | Type::RwEpochToTs
            | Type::CheckNotNull
            | Type::CompositeCast,
        ) => true,
        FuncName::Table(TableFunctionType::Unspecified | TableFunctionType::UserDefined) => {
            unreachable!()
        }
        FuncName::Table(
            TableFunctionType::FileScan
            | TableFunctionType::PostgresQuery
            | TableFunctionType::MysqlQuery
            | TableFunctionType::InternalBackfillProgress
            | TableFunctionType::InternalSourceBackfillProgress
            | TableFunctionType::InternalGetChannelDeltaStats
            | TableFunctionType::PgGetKeywords,
        ) => false,
        FuncName::Table(
            TableFunctionType::GenerateSeries
            | TableFunctionType::Unnest
            | TableFunctionType::RegexpMatches
            | TableFunctionType::Range
            | TableFunctionType::GenerateSubscripts
            | TableFunctionType::PgExpandarray
            | TableFunctionType::JsonbArrayElements
            | TableFunctionType::JsonbArrayElementsText
            | TableFunctionType::JsonbEach
            | TableFunctionType::JsonbEachText
            | TableFunctionType::JsonbObjectKeys
            | TableFunctionType::JsonbPathQuery
            | TableFunctionType::JsonbPopulateRecordset
            | TableFunctionType::JsonbToRecordset,
        ) => true,
        FuncName::Aggregate(_) | FuncName::Udf(_) => {
            unreachable!("only scalar and table built-ins are accepted")
        }
    }
}

impl ExprVisitor for ImpureAnalyzer {
    fn visit_user_defined_function(&mut self, func_call: &UserDefinedFunction) {
        if !func_call.catalog.unsafe_skip_materializing_exprs {
            let name = &func_call.catalog.name;
            self.impure = Some(format!("user-defined function `{name}`").into());
        } else {
            func_call.args.iter().for_each(|expr| self.visit_expr(expr));
        }
    }

    fn visit_table_function(&mut self, func_call: &TableFunction) {
        // Scalar UDFs have their own `UserDefinedFunction` expression variant. UDTFs instead
        // share `TableFunction` with built-ins and carry their catalog in `user_defined`.
        if func_call.function_type == TableFunctionType::UserDefined {
            let catalog = func_call.user_defined.as_ref().unwrap();
            if !catalog.unsafe_skip_materializing_exprs {
                self.impure =
                    Some(format!("user-defined table function `{}`", catalog.name).into());
            } else {
                func_call.args.iter().for_each(|expr| self.visit_expr(expr));
            }
        } else if is_builtin_function_deterministic(func_call) {
            func_call.args.iter().for_each(|expr| self.visit_expr(expr));
        } else {
            self.impure = Some(func_call.function_type.as_str_name().into());
        }
    }

    fn visit_now(&mut self, _: &super::Now) {
        self.impure = Some("NOW or PROCTIME".into());
    }

    fn visit_secret_ref(&mut self, secret_ref: &super::SecretRef) {
        self.impure = Some(format!("secret reference `{}`", secret_ref.secret_name).into());
    }

    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        if is_builtin_function_deterministic(func_call) {
            func_call
                .inputs()
                .iter()
                .for_each(|expr| self.visit_expr(expr));
        } else {
            self.impure = Some(func_call.func_type().as_str_name().into());
        }
    }
}

/// Returns whether the planner classifies an expression as pure.
///
/// This classification combines semantic purity with UDF result-materialization policy.
/// Semantically impure nodes are non-deterministic or have side effects. A UDF with
/// `unsafe_skip_materializing_exprs = true` follows the recursive purity of its arguments because
/// creating such a UDF requires an `IMMUTABLE` declaration. The planner trusts that declaration;
/// it cannot guarantee that the implementation is deterministic or side-effect-free.
///
/// A UDF with `unsafe_skip_materializing_exprs = false` is classified as impure even if it is
/// actually deterministic and side-effect-free. Keeping result materialization enabled may be
/// desirable solely as a caching optimization. Therefore, an expression classified as impure is
/// not necessarily semantically impure.
pub fn is_pure(expr: &ExprImpl) -> bool {
    !is_impure(expr)
}

/// Returns whether the planner classifies an expression as impure.
///
/// This is the inverse of [`is_pure`]. It returns `true` when any node is semantically impure or
/// when a UDF has `unsafe_skip_materializing_exprs = false`. In the latter case, streaming
/// projection planning materializes the complete top-level expression on retract inputs so the
/// evaluated result can be preserved. UPSERT projects bypass result materialization independently
/// of this classification.
///
/// Consequently, `true` can describe a semantically pure UDF that merely requests result caching;
/// it does not necessarily mean that the UDF is non-deterministic or has side effects.
pub fn is_impure(expr: &ExprImpl) -> bool {
    let mut a = ImpureAnalyzer::default();
    a.visit_expr(expr);
    a.is_impure()
}

pub fn is_impure_func_call(func_call: &FunctionCall) -> bool {
    let mut a = ImpureAnalyzer::default();
    a.visit_function_call(func_call);
    a.is_impure()
}

/// Returns the description of the impure expression if it is impure, for error reporting.
/// `None` if the expression is pure.
pub fn impure_expr_desc(expr: &ExprImpl) -> Option<String> {
    let mut a = ImpureAnalyzer::default();
    a.visit_expr(expr);
    a.impure_expr_desc().map(|s| s.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::types::DataType;
    use risingwave_pb::catalog::PbFunction;
    use risingwave_pb::catalog::function::{Kind, ScalarFunction};
    use risingwave_pb::expr::expr_node::Type;

    use crate::catalog::function_catalog::FunctionCatalog;
    use crate::expr::{ExprImpl, FunctionCall, InputRef, UserDefinedFunction, is_impure, is_pure};

    fn udf_expr(
        unsafe_skip_materializing_exprs: bool,
        return_type: DataType,
        args: Vec<ExprImpl>,
    ) -> ExprImpl {
        let catalog = FunctionCatalog::from(&PbFunction {
            name: "test_udf".to_owned(),
            kind: Some(Kind::Scalar(ScalarFunction {})),
            return_type: Some(return_type.into()),
            unsafe_skip_materializing_exprs,
            ..Default::default()
        });
        UserDefinedFunction::new(Arc::new(catalog), args).into()
    }

    fn expect_pure(expr: &ExprImpl) {
        assert!(is_pure(expr));
        assert!(!is_impure(expr));
    }

    fn expect_impure(expr: &ExprImpl) {
        assert!(!is_pure(expr));
        assert!(is_impure(expr));
    }

    #[test]
    fn test_pure_funcs() {
        let e: ExprImpl = FunctionCall::new(
            Type::Add,
            vec![
                InputRef::new(0, DataType::Int16).into(),
                InputRef::new(0, DataType::Int16).into(),
            ],
        )
        .unwrap()
        .into();
        expect_pure(&e);

        let e: ExprImpl = FunctionCall::new(
            Type::GreaterThan,
            vec![
                InputRef::new(0, DataType::Timestamptz).into(),
                FunctionCall::new(Type::Proctime, vec![]).unwrap().into(),
            ],
        )
        .unwrap()
        .into();
        expect_impure(&e);
    }

    /// Verifies that UDF result-materialization settings participate in recursive purity analysis.
    #[test]
    fn test_udf_unsafe_skip_materializing_exprs() {
        let input: ExprImpl = InputRef::new(0, DataType::Int16).into();

        let materialized_udf = udf_expr(false, DataType::Int16, vec![input.clone()]);
        expect_impure(&materialized_udf);

        // Creation requires an opted-out UDF to be declared IMMUTABLE, so the planner classifies
        // it as pure when all of its descendants are pure.
        let skipped_udf = udf_expr(true, DataType::Int16, vec![input]);
        expect_pure(&skipped_udf);

        let text_input: ExprImpl = InputRef::new(0, DataType::Varchar).into();
        let nested_materialized_udf = udf_expr(false, DataType::Varchar, vec![text_input]);
        let regclass_with_materialized_udf: ExprImpl =
            FunctionCall::new(Type::CastRegclass, vec![nested_materialized_udf])
                .unwrap()
                .into();
        let outer_skipped_udf =
            udf_expr(true, DataType::Int32, vec![regclass_with_materialized_udf]);
        // An opted-out outer UDF is still recursively impure when one of its descendants is
        // impure. Stream planning therefore materializes this complete top-level expression.
        expect_impure(&outer_skipped_udf);
    }
}
