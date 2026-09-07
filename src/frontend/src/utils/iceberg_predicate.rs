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

use chrono::Datelike;
use iceberg::expr::{Predicate as IcebergPredicate, Reference};
use iceberg::spec::Datum as IcebergDatum;
use risingwave_common::catalog::Field;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::expr::{Expr, ExprImpl, ExprType, InputRef, Literal};
use crate::utils::Condition;

pub struct ExtractIcebergPredicateResult {
    pub iceberg_predicate: IcebergPredicate,
    pub extracted_condition: Condition,
    pub remaining_condition: Condition,
}

/// NOTE(kwannoel): We do predicate pushdown to the iceberg-sdk here.
/// zone-map is used to evaluate predicates on iceberg tables.
/// Without zone-map, iceberg-sdk will still apply the predicate on its own.
/// See: <https://github.com/apache/iceberg-rust/blob/5c1a9e68da346819072a15327080a498ad91c488/crates/iceberg/src/arrow/reader.rs#L229-L235>.
///
/// `fields` must carry the iceberg-side column types: for engine tables, pass the original
/// source types rather than the remapped Hummock output types.
pub fn extract_iceberg_predicate(
    predicate: Condition,
    fields: &[Field],
) -> ExtractIcebergPredicateResult {
    if predicate.always_true() {
        return ExtractIcebergPredicateResult {
            iceberg_predicate: IcebergPredicate::AlwaysTrue,
            extracted_condition: Condition {
                conjunctions: vec![],
            },
            remaining_condition: Condition {
                conjunctions: vec![],
            },
        };
    }

    let mut iceberg_predicates = Vec::new();
    let mut extracted_conjunctions = Vec::new();
    let mut remaining_conjunctions = Vec::new();

    for conjunction in predicate.conjunctions {
        match rw_expr_to_iceberg_predicate(&conjunction, fields) {
            Some(iceberg_predicate) => {
                iceberg_predicates.push(iceberg_predicate);
                extracted_conjunctions.push(conjunction);
            }
            None => remaining_conjunctions.push(conjunction),
        }
    }

    let iceberg_predicate = iceberg_predicates
        .into_iter()
        .reduce(IcebergPredicate::and)
        .unwrap_or(IcebergPredicate::AlwaysTrue);

    ExtractIcebergPredicateResult {
        iceberg_predicate,
        extracted_condition: Condition {
            conjunctions: extracted_conjunctions,
        },
        remaining_condition: Condition {
            conjunctions: remaining_conjunctions,
        },
    }
}

fn rw_literal_to_iceberg_datum(literal: &Literal) -> Option<IcebergDatum> {
    let Some(scalar) = literal.get_data() else {
        return None;
    };
    match scalar {
        ScalarImpl::Bool(b) => Some(IcebergDatum::bool(*b)),
        ScalarImpl::Int32(i) => Some(IcebergDatum::int(*i)),
        ScalarImpl::Int64(i) => Some(IcebergDatum::long(*i)),
        ScalarImpl::Float32(f) => Some(IcebergDatum::float(*f)),
        ScalarImpl::Float64(f) => Some(IcebergDatum::double(*f)),
        ScalarImpl::Decimal(_) => {
            // TODO(iceberg): iceberg-rust doesn't support decimal predicate pushdown yet.
            None
        }
        ScalarImpl::Date(d) => {
            let Ok(datum) = IcebergDatum::date_from_ymd(d.0.year(), d.0.month(), d.0.day()) else {
                return None;
            };
            Some(datum)
        }
        ScalarImpl::Timestamp(t) => Some(IcebergDatum::timestamp_micros(
            t.0.and_utc().timestamp_micros(),
        )),
        ScalarImpl::Timestamptz(t) => Some(IcebergDatum::timestamptz_micros(t.timestamp_micros())),
        ScalarImpl::Utf8(s) => Some(IcebergDatum::string(s)),
        ScalarImpl::Bytea(b) => Some(IcebergDatum::binary(b.clone())),
        _ => None,
    }
}

/// Whether a column of this iceberg-side type can appear in a pushed-down predicate:
/// iceberg-rust only builds field accessors for primitive columns, so a pushed-down
/// predicate referencing others fails at scan time with "Accessor for Field ... not found".
fn is_iceberg_predicate_pushable_column_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
            | DataType::Varchar
            | DataType::Bytea
    )
}

/// Build the `Reference` for a pushed-down predicate, or `None` for an unpushable column.
/// Every match arm must obtain its `Reference` here so the column-type check cannot be
/// bypassed.
fn input_ref_to_reference(input_ref: &InputRef, fields: &[Field]) -> Option<Reference> {
    let field = &fields[input_ref.index];
    if !is_iceberg_predicate_pushable_column_type(&field.data_type) {
        return None;
    }
    Some(Reference::new(&field.name))
}

/// Convert `<col> <op> <literal>` (either operand order) into an iceberg predicate.
fn comparison_to_iceberg_predicate(
    func_type: ExprType,
    args: &[ExprImpl],
    fields: &[Field],
) -> Option<IcebergPredicate> {
    // `flipped` means the literal is on the left: `lit <op> col` ⇔ `col <mirrored op> lit`.
    let (input_ref, literal, flipped) = match [&args[0], &args[1]] {
        [ExprImpl::InputRef(input_ref), ExprImpl::Literal(literal)] => (input_ref, literal, false),
        [ExprImpl::Literal(literal), ExprImpl::InputRef(input_ref)] => (input_ref, literal, true),
        _ => return None,
    };
    let reference = input_ref_to_reference(input_ref, fields)?;
    let datum = rw_literal_to_iceberg_datum(literal)?;
    let predicate = match (func_type, flipped) {
        (ExprType::Equal, _) => reference.equal_to(datum),
        (ExprType::NotEqual, _) => reference.not_equal_to(datum),
        (ExprType::GreaterThan, false) | (ExprType::LessThan, true) => {
            reference.greater_than(datum)
        }
        (ExprType::GreaterThanOrEqual, false) | (ExprType::LessThanOrEqual, true) => {
            reference.greater_than_or_equal_to(datum)
        }
        (ExprType::LessThan, false) | (ExprType::GreaterThan, true) => reference.less_than(datum),
        (ExprType::LessThanOrEqual, false) | (ExprType::GreaterThanOrEqual, true) => {
            reference.less_than_or_equal_to(datum)
        }
        _ => return None,
    };
    Some(predicate)
}

fn rw_expr_to_iceberg_predicate(expr: &ExprImpl, fields: &[Field]) -> Option<IcebergPredicate> {
    match expr {
        ExprImpl::Literal(l) => match l.get_data() {
            Some(ScalarImpl::Bool(b)) => {
                if *b {
                    Some(IcebergPredicate::AlwaysTrue)
                } else {
                    Some(IcebergPredicate::AlwaysFalse)
                }
            }
            _ => None,
        },
        ExprImpl::FunctionCall(f) => {
            let args = f.inputs();
            match f.func_type() {
                ExprType::Not => {
                    let arg = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                    Some(IcebergPredicate::negate(arg))
                }
                ExprType::And => {
                    let arg0 = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                    let arg1 = rw_expr_to_iceberg_predicate(&args[1], fields)?;
                    Some(IcebergPredicate::and(arg0, arg1))
                }
                ExprType::Or => {
                    let arg0 = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                    let arg1 = rw_expr_to_iceberg_predicate(&args[1], fields)?;
                    Some(IcebergPredicate::or(arg0, arg1))
                }
                ExprType::Equal
                | ExprType::NotEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual
                | ExprType::LessThan
                | ExprType::LessThanOrEqual
                    if args[0].return_type() == args[1].return_type() =>
                {
                    comparison_to_iceberg_predicate(f.func_type(), args, fields)
                }
                ExprType::IsNull => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let reference = input_ref_to_reference(lhs, fields)?;
                        Some(reference.is_null())
                    }
                    _ => None,
                },
                ExprType::IsNotNull => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let reference = input_ref_to_reference(lhs, fields)?;
                        Some(reference.is_not_null())
                    }
                    _ => None,
                },
                ExprType::In => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let reference = input_ref_to_reference(lhs, fields)?;
                        let mut datums = Vec::with_capacity(args.len() - 1);
                        for arg in &args[1..] {
                            if args[0].return_type() != arg.return_type() {
                                return None;
                            }
                            if let ExprImpl::Literal(l) = arg {
                                let datum = rw_literal_to_iceberg_datum(l)?;
                                datums.push(datum);
                            } else {
                                return None;
                            }
                        }
                        Some(reference.is_in(datums))
                    }
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{MapType, ScalarImpl, StructType};

    use super::*;
    use crate::expr::{FunctionCall, InputRef};

    #[test]
    fn comparison_pushdown_mirrors_flipped_operators() {
        let fields = vec![Field::new("x", DataType::Int64)];
        let col = || InputRef::new(0, DataType::Int64).into();
        let lit = || Literal::new(Some(ScalarImpl::Int64(5)), DataType::Int64).into();

        let cases: [(ExprType, ExprImpl, ExprImpl, &str); 12] = [
            (ExprType::Equal, col(), lit(), "x = 5"),
            (ExprType::Equal, lit(), col(), "x = 5"),
            (ExprType::NotEqual, col(), lit(), "x != 5"),
            (ExprType::NotEqual, lit(), col(), "x != 5"),
            (ExprType::GreaterThan, col(), lit(), "x > 5"),
            (ExprType::GreaterThan, lit(), col(), "x < 5"),
            (ExprType::GreaterThanOrEqual, col(), lit(), "x >= 5"),
            (ExprType::GreaterThanOrEqual, lit(), col(), "x <= 5"),
            (ExprType::LessThan, col(), lit(), "x < 5"),
            (ExprType::LessThan, lit(), col(), "x > 5"),
            (ExprType::LessThanOrEqual, col(), lit(), "x <= 5"),
            (ExprType::LessThanOrEqual, lit(), col(), "x >= 5"),
        ];
        for (op, arg0, arg1, expected) in cases {
            let expr = FunctionCall::new(op, vec![arg0, arg1]).unwrap().into();
            let predicate = rw_expr_to_iceberg_predicate(&expr, &fields)
                .unwrap_or_else(|| panic!("{op:?} should be pushable"));
            assert_eq!(predicate.to_string(), expected);
        }
    }

    fn null_check(op: ExprType, input_type: DataType) -> ExprImpl {
        let col: ExprImpl = InputRef::new(0, input_type).into();
        FunctionCall::new_unchecked(op, vec![col], DataType::Boolean).into()
    }

    #[test]
    fn non_primitive_columns_are_not_pushed_down() {
        // iceberg-rust only builds field accessors for primitive columns, so a pushed-down
        // predicate on any other type fails the scan with "Accessor for Field ... not found".
        let unpushable = [
            DataType::Struct(StructType::new(vec![("a", DataType::Int32)])),
            DataType::list(DataType::Int32),
            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32)),
            DataType::Variant,
            DataType::Jsonb,
        ];
        for data_type in unpushable {
            let fields = vec![Field::new("c", data_type.clone())];
            for op in [ExprType::IsNull, ExprType::IsNotNull] {
                let expr = null_check(op, data_type.clone());
                assert!(
                    rw_expr_to_iceberg_predicate(&expr, &fields).is_none(),
                    "{op:?} on {data_type} must not be pushed down"
                );
            }
        }

        let fields = vec![Field::new("c", DataType::Int64)];
        for op in [ExprType::IsNull, ExprType::IsNotNull] {
            let expr = null_check(op, DataType::Int64);
            assert!(
                rw_expr_to_iceberg_predicate(&expr, &fields).is_some(),
                "{op:?} on a primitive column should be pushable"
            );
        }
    }

    #[test]
    fn pushability_follows_the_iceberg_side_column_type() {
        // Engine tables remap the scan schema to Hummock types (here jsonb backed by an iceberg
        // string), so the `InputRef` carries jsonb while the iceberg-side field carries varchar.
        // Judging by the remapped type would silently drop the pushdown.
        let expr = null_check(ExprType::IsNotNull, DataType::Jsonb);

        let iceberg_side = vec![Field::new("c", DataType::Varchar)];
        assert!(rw_expr_to_iceberg_predicate(&expr, &iceberg_side).is_some());

        let remapped = vec![Field::new("c", DataType::Jsonb)];
        assert!(rw_expr_to_iceberg_predicate(&expr, &remapped).is_none());
    }
}
