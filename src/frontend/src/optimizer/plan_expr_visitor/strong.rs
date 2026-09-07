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

use fixedbitset::FixedBitSet;

use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};

/// This utilities are with the same definition in calcite.
/// Utilities for strong predicates.
/// A predicate is strong (or null-rejecting) with regards to selected subset of inputs
/// if it is UNKNOWN if all inputs in selected subset are UNKNOWN.
/// By the way, UNKNOWN is just the boolean form of NULL.
///
/// Examples:
///
/// UNKNOWN is strong in `[]` (definitely null)
///
/// `c = 1` is strong in `[c]` (definitely null if and only if c is null)
///
/// `c IS NULL` is not strong (always returns TRUE or FALSE, nevernull)
///
/// `p1 AND p2` is strong in `[p1, p2]` (definitely null if either p1 is null or p2 is null)
///
/// `p1 OR p2` is strong if p1 and p2 are strong

#[derive(Default)]
pub struct Strong {
    null_columns: FixedBitSet,
}

impl Strong {
    fn new(null_columns: FixedBitSet) -> Self {
        Self { null_columns }
    }

    /// Returns whether the analyzed expression will *definitely* return null if
    /// all of a given set of input columns are null.
    /// Note: we could not assume any null-related property for the input expression if `is_null` returns false
    pub fn is_null(expr: &ExprImpl, null_columns: FixedBitSet) -> bool {
        let strong = Strong::new(null_columns);
        strong.is_null_visit(expr)
    }

    fn is_input_ref_null(&self, input_ref: &InputRef) -> bool {
        self.null_columns.contains(input_ref.index())
    }

    fn is_null_visit(&self, expr: &ExprImpl) -> bool {
        match expr {
            ExprImpl::InputRef(input_ref) => self.is_input_ref_null(input_ref),
            ExprImpl::Literal(literal) => literal.get_data().is_none(),
            ExprImpl::FunctionCall(func_call) => self.is_null_function_call(func_call),
            ExprImpl::FunctionCallWithLambda(_) => false,
            ExprImpl::AggCall(_) => false,
            ExprImpl::Subquery(_) => false,
            ExprImpl::CorrelatedInputRef(_) => false,
            ExprImpl::TableFunction(_) => false,
            ExprImpl::WindowFunction(_) => false,
            ExprImpl::UserDefinedFunction(_) => false,
            ExprImpl::Parameter(_) => false,
            ExprImpl::Now(_) | ExprImpl::SecretRef(_) => false,
        }
    }

    fn is_null_function_call(&self, func_call: &FunctionCall) -> bool {
        match func_call.func_type() {
            // NOT NULL: This kind of expression is never null. No need to look at its arguments, if it has any.
            ExprType::IsNull
            | ExprType::IsNotNull
            | ExprType::IsDistinctFrom
            | ExprType::IsNotDistinctFrom
            | ExprType::IsTrue
            | ExprType::QuoteNullable
            | ExprType::IsNotTrue
            | ExprType::IsFalse
            | ExprType::IsNotFalse
            | ExprType::CheckNotNull => false,
            // ANY: This kind of expression is null if and only if at least one of its arguments is null.
            ExprType::Not
            | ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::Like
            | ExprType::Add
            | ExprType::AddWithTimeZone
            | ExprType::Subtract
            | ExprType::Multiply
            | ExprType::Modulus
            | ExprType::Divide
            | ExprType::Cast
            | ExprType::Trim
            | ExprType::Ltrim
            | ExprType::Rtrim
            | ExprType::Ceil
            | ExprType::Floor
            | ExprType::Extract
            | ExprType::L2Distance
            | ExprType::CosineDistance
            | ExprType::L1Distance
            | ExprType::InnerProduct
            | ExprType::VecConcat
            | ExprType::L2Norm
            | ExprType::L2Normalize
            | ExprType::Subvector
            | ExprType::Greatest
            | ExprType::Least => self.any_null(func_call),
            // ALL: This kind of expression is null if and only if all of its arguments are null.
            ExprType::And | ExprType::Or | ExprType::Coalesce => self.all_null(func_call),
            // TODO: Function like case when is important but current its structure is complicated, so we need to implement it later if necessary.
            // Assume that any other expressions cannot be simplified.
            #[expect(deprecated)]
            ExprType::In
            | ExprType::Some
            | ExprType::All
            | ExprType::BitwiseAnd
            | ExprType::BitwiseOr
            | ExprType::BitwiseXor
            | ExprType::BitwiseNot
            | ExprType::BitwiseShiftLeft
            | ExprType::BitwiseShiftRight
            | ExprType::DatePart
            | ExprType::TumbleStart
            | ExprType::MakeDate
            | ExprType::MakeTime
            | ExprType::MakeTimestamp
            | ExprType::MakeInterval
            | ExprType::SecToTimestamptz
            | ExprType::AtTimeZone
            | ExprType::DateTrunc
            | ExprType::DateBin
            | ExprType::CharToTimestamptz
            | ExprType::CharToDate
            | ExprType::CastWithTimeZone
            | ExprType::SubtractWithTimeZone
            | ExprType::MakeTimestamptz
            | ExprType::Substr
            | ExprType::Length
            | ExprType::ILike
            | ExprType::SimilarToEscape
            | ExprType::Upper
            | ExprType::Lower
            | ExprType::Replace
            | ExprType::Position
            | ExprType::Case
            | ExprType::ConstantLookup
            | ExprType::RoundDigit
            | ExprType::Round
            | ExprType::Ascii
            | ExprType::Translate
            | ExprType::Concat
            | ExprType::ConcatVariadic
            | ExprType::ConcatWs
            | ExprType::ConcatWsVariadic
            | ExprType::Abs
            | ExprType::SplitPart
            | ExprType::ToChar
            | ExprType::Md5
            | ExprType::CharLength
            | ExprType::Repeat
            | ExprType::ConcatOp
            | ExprType::ByteaConcatOp
            | ExprType::BoolOut
            | ExprType::OctetLength
            | ExprType::BitLength
            | ExprType::Overlay
            | ExprType::RegexpMatch
            | ExprType::RegexpReplace
            | ExprType::RegexpCount
            | ExprType::RegexpSplitToArray
            | ExprType::RegexpEq
            | ExprType::Pow
            | ExprType::Exp
            | ExprType::Chr
            | ExprType::StartsWith
            | ExprType::Initcap
            | ExprType::Lpad
            | ExprType::Rpad
            | ExprType::Reverse
            | ExprType::Strpos
            | ExprType::ToAscii
            | ExprType::ToHex
            | ExprType::QuoteIdent
            | ExprType::QuoteLiteral
            | ExprType::Sin
            | ExprType::Cos
            | ExprType::Tan
            | ExprType::Cot
            | ExprType::Asin
            | ExprType::Acos
            | ExprType::Acosd
            | ExprType::Atan
            | ExprType::Atan2
            | ExprType::Atand
            | ExprType::Atan2d
            | ExprType::Sind
            | ExprType::Cosd
            | ExprType::Cotd
            | ExprType::Tand
            | ExprType::Asind
            | ExprType::Sqrt
            | ExprType::Degrees
            | ExprType::Radians
            | ExprType::Cosh
            | ExprType::Tanh
            | ExprType::Coth
            | ExprType::Asinh
            | ExprType::Acosh
            | ExprType::Atanh
            | ExprType::Sinh
            | ExprType::Trunc
            | ExprType::Ln
            | ExprType::Log10
            | ExprType::Cbrt
            | ExprType::Sign
            | ExprType::Scale
            | ExprType::MinScale
            | ExprType::TrimScale
            | ExprType::Gamma
            | ExprType::Lgamma
            | ExprType::Encode
            | ExprType::Decode
            | ExprType::Sha1
            | ExprType::Sha224
            | ExprType::Sha256
            | ExprType::Sha384
            | ExprType::Sha512
            | ExprType::Crc32
            | ExprType::Crc32c
            | ExprType::GetBit
            | ExprType::GetByte
            | ExprType::SetBit
            | ExprType::SetByte
            | ExprType::BitCount
            | ExprType::Hmac
            | ExprType::SecureCompare
            | ExprType::Left
            | ExprType::Right
            | ExprType::Format
            | ExprType::FormatVariadic
            | ExprType::PgwireSend
            | ExprType::PgwireRecv
            | ExprType::ConvertFrom
            | ExprType::ConvertTo
            | ExprType::Decrypt
            | ExprType::Encrypt
            | ExprType::Neg
            | ExprType::Field
            | ExprType::Array
            | ExprType::ArrayAccess
            | ExprType::Row
            | ExprType::ArrayToString
            | ExprType::ArrayRangeAccess
            | ExprType::ArrayCat
            | ExprType::ArrayAppend
            | ExprType::ArrayPrepend
            | ExprType::FormatType
            | ExprType::ArrayDistinct
            | ExprType::ArrayLength
            | ExprType::Cardinality
            | ExprType::ArrayRemove
            | ExprType::ArrayPositions
            | ExprType::TrimArray
            | ExprType::StringToArray
            | ExprType::ArrayPosition
            | ExprType::ArrayReplace
            | ExprType::ArrayDims
            | ExprType::ArrayTransform
            | ExprType::ArrayMin
            | ExprType::ArrayMax
            | ExprType::ArraySum
            | ExprType::ArraySort
            | ExprType::ArrayReverse
            | ExprType::ArrayContains
            | ExprType::ArrayContained
            | ExprType::ArrayOverlaps
            | ExprType::ArrayFlatten
            | ExprType::HexToInt256
            | ExprType::JsonbAccess
            | ExprType::JsonbAccessStr
            | ExprType::JsonbExtractPath
            | ExprType::JsonbExtractPathVariadic
            | ExprType::JsonbExtractPathText
            | ExprType::JsonbExtractPathTextVariadic
            | ExprType::JsonbTypeof
            | ExprType::JsonbArrayLength
            | ExprType::IsJson
            | ExprType::JsonbConcat
            | ExprType::JsonbObject
            | ExprType::JsonbPretty
            | ExprType::JsonbContains
            | ExprType::JsonbContained
            | ExprType::JsonbExists
            | ExprType::JsonbExistsAny
            | ExprType::JsonbExistsAll
            | ExprType::JsonbDeletePath
            | ExprType::JsonbStripNulls
            | ExprType::ToJsonb
            | ExprType::JsonbBuildArray
            | ExprType::JsonbBuildArrayVariadic
            | ExprType::JsonbBuildObject
            | ExprType::JsonbBuildObjectVariadic
            | ExprType::JsonbPathExists
            | ExprType::JsonbPathMatch
            | ExprType::JsonbPathQueryArray
            | ExprType::JsonbPathQueryFirst
            | ExprType::JsonbPopulateRecord
            | ExprType::JsonbToArray
            | ExprType::JsonbToRecord
            | ExprType::JsonbSet
            | ExprType::JsonbPopulateMap
            | ExprType::ToVariant
            | ExprType::VariantGet
            | ExprType::TryVariantGet
            | ExprType::VariantTypeof
            | ExprType::MapFromEntries
            | ExprType::MapAccess
            | ExprType::MapKeys
            | ExprType::MapValues
            | ExprType::MapEntries
            | ExprType::MapFromKeyValues
            | ExprType::MapCat
            | ExprType::MapContains
            | ExprType::MapDelete
            | ExprType::MapFilter
            | ExprType::MapInsert
            | ExprType::MapLength
            | ExprType::Vnode
            | ExprType::VnodeUser
            | ExprType::TestFeature
            | ExprType::License
            | ExprType::Proctime
            | ExprType::PgSleep
            | ExprType::PgSleepFor
            | ExprType::PgSleepUntil
            | ExprType::CastRegclass
            | ExprType::PgGetIndexdef
            | ExprType::ColDescription
            | ExprType::PgGetViewdef
            | ExprType::PgGetUserbyid
            | ExprType::PgIndexesSize
            | ExprType::PgRelationSize
            | ExprType::PgGetSerialSequence
            | ExprType::PgIndexColumnHasProperty
            | ExprType::PgIsInRecovery
            | ExprType::PgTableIsVisible
            | ExprType::RwRecoveryStatus
            | ExprType::RwClusterId
            | ExprType::RwFragmentVnodes
            | ExprType::RwActorVnodes
            | ExprType::IcebergTransform
            | ExprType::HasTablePrivilege
            | ExprType::HasFunctionPrivilege
            | ExprType::HasAnyColumnPrivilege
            | ExprType::HasSchemaPrivilege
            | ExprType::InetAton
            | ExprType::InetNtoa
            | ExprType::CompositeCast
            | ExprType::RwEpochToTs
            | ExprType::OpenaiEmbedding
            | ExprType::HasDatabasePrivilege
            | ExprType::Random
            | ExprType::ClockTimestamp
            | ExprType::GenRandomUuid => false,
            ExprType::Unspecified => unreachable!(),
        }
    }

    fn any_null(&self, func_call: &FunctionCall) -> bool {
        func_call
            .inputs()
            .iter()
            .any(|expr| self.is_null_visit(expr))
    }

    fn all_null(&self, func_call: &FunctionCall) -> bool {
        func_call
            .inputs()
            .iter()
            .all(|expr| self.is_null_visit(expr))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::ExprImpl::Literal;

    #[test]
    fn test_literal() {
        let null_columns = FixedBitSet::with_capacity(1);
        let expr = Literal(crate::expr::Literal::new(None, DataType::Varchar).into());
        assert!(Strong::is_null(&expr, null_columns.clone()));

        let expr = Literal(
            crate::expr::Literal::new(Some("test".to_owned().into()), DataType::Varchar).into(),
        );
        assert!(!Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_input_ref1() {
        let null_columns = FixedBitSet::with_capacity(2);
        let expr = InputRef::new(0, DataType::Varchar).into();
        assert!(!Strong::is_null(&expr, null_columns.clone()));

        let expr = InputRef::new(1, DataType::Varchar).into();
        assert!(!Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_input_ref2() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        null_columns.insert(1);
        let expr = InputRef::new(0, DataType::Varchar).into();
        assert!(Strong::is_null(&expr, null_columns.clone()));

        let expr = InputRef::new(1, DataType::Varchar).into();
        assert!(Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_c1_equal_1_or_c2_is_null() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        let expr = FunctionCall::new_unchecked(
            ExprType::Or,
            vec![
                FunctionCall::new_unchecked(
                    ExprType::Equal,
                    vec![
                        InputRef::new(0, DataType::Int64).into(),
                        Literal(crate::expr::Literal::new(Some(1.into()), DataType::Int32).into()),
                    ],
                    DataType::Boolean,
                )
                .into(),
                FunctionCall::new_unchecked(
                    ExprType::IsNull,
                    vec![InputRef::new(1, DataType::Int64).into()],
                    DataType::Boolean,
                )
                .into(),
            ],
            DataType::Boolean,
        )
        .into();
        assert!(!Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_divide() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        null_columns.insert(1);
        let expr = FunctionCall::new_unchecked(
            ExprType::Divide,
            vec![
                InputRef::new(0, DataType::Decimal).into(),
                InputRef::new(1, DataType::Decimal).into(),
            ],
            DataType::Varchar,
        )
        .into();
        assert!(Strong::is_null(&expr, null_columns));
    }

    /// generate a test case for (0.8 * sum / count) where sum is null and count is not null
    #[test]
    fn test_multiply_divide() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        let expr = FunctionCall::new_unchecked(
            ExprType::Multiply,
            vec![
                Literal(crate::expr::Literal::new(Some(0.8f64.into()), DataType::Float64).into()),
                FunctionCall::new_unchecked(
                    ExprType::Divide,
                    vec![
                        InputRef::new(0, DataType::Decimal).into(),
                        InputRef::new(1, DataType::Decimal).into(),
                    ],
                    DataType::Decimal,
                )
                .into(),
            ],
            DataType::Decimal,
        )
        .into();
        assert!(Strong::is_null(&expr, null_columns));
    }

    /// generate test cases for is not null
    macro_rules! gen_test {
        ($func:ident, $expr:expr, $expected:expr) => {
            #[test]
            fn $func() {
                let null_columns = FixedBitSet::with_capacity(2);
                let expr = $expr;
                assert_eq!(Strong::is_null(&expr, null_columns), $expected);
            }
        };
    }

    gen_test!(
        test_is_not_null,
        FunctionCall::new_unchecked(
            ExprType::IsNotNull,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_null,
        FunctionCall::new_unchecked(
            ExprType::IsNull,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_distinct_from,
        FunctionCall::new_unchecked(
            ExprType::IsDistinctFrom,
            vec![
                InputRef::new(0, DataType::Varchar).into(),
                InputRef::new(1, DataType::Varchar).into()
            ],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_not_distinct_from,
        FunctionCall::new_unchecked(
            ExprType::IsNotDistinctFrom,
            vec![
                InputRef::new(0, DataType::Varchar).into(),
                InputRef::new(1, DataType::Varchar).into()
            ],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_true,
        FunctionCall::new_unchecked(
            ExprType::IsTrue,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_not_true,
        FunctionCall::new_unchecked(
            ExprType::IsNotTrue,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_false,
        FunctionCall::new_unchecked(
            ExprType::IsFalse,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
}
