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

use std::cell::RefCell;
use std::ops::Div;
use std::sync::{Arc, LazyLock};

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_schema::extension::ExtensionType;
use num_traits::abs;
use parquet_variant_compute::{VariantArray as ParquetVariantArray, VariantType};
use risingwave_common_log::LogSuppressor;
use thiserror_ext::AsReport;

pub use super::arrow_58::{
    FromArrow, ToArrow, arrow_array, arrow_buffer, arrow_cast, arrow_schema,
    is_parquet_field_match_source_schema, is_parquet_schema_match_source_schema,
};
use crate::array::{
    Array, ArrayBuilder, ArrayError, ArrayImpl, DataChunk, DataType, DecimalArray, IntervalArray,
    VariantArray as RwVariantArray, VariantArrayBuilder,
};
use crate::types::{Scalar, StructType, VariantVal};

pub struct IcebergArrowConvert;

struct DefaultIcebergFromArrow;

impl FromArrow for DefaultIcebergFromArrow {}

// Arrow Decimal128 supports up to 38 decimal digits. We use precision=38, scale=10:
// - Integer range: up to 10^28 - 1 (28 digits)
// - Fractional precision: 10 digits
// - Covers all RisingWave decimal values (MAX_PRECISION=28)
//
// Note: When reading Arrow decimals that exceed RisingWave's 96-bit / 28-digit
// storage limit, the conversion code in arrow_impl.rs will reduce scale and
// truncate the mantissa (via truncated_i128_and_scale) to make them fit.
pub const ICEBERG_DECIMAL_PRECISION: u8 = 38;
pub const ICEBERG_DECIMAL_SCALE: i8 = 10;

impl IcebergArrowConvert {
    pub fn to_record_batch(
        &self,
        schema: arrow_schema::SchemaRef,
        chunk: &DataChunk,
    ) -> Result<arrow_array::RecordBatch, ArrayError> {
        ToArrow::to_record_batch(self, schema, chunk)
    }

    pub fn chunk_from_record_batch(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<DataChunk, ArrayError> {
        FromArrow::from_record_batch(self, batch)
    }

    pub fn type_from_field(&self, field: &arrow_schema::Field) -> Result<DataType, ArrayError> {
        FromArrow::from_field(self, field)
    }

    pub fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        ToArrow::to_arrow_field(self, name, data_type)
    }

    pub fn struct_from_fields(
        &self,
        fields: &arrow_schema::Fields,
    ) -> Result<StructType, ArrayError> {
        FromArrow::from_fields(self, fields)
    }

    pub fn to_arrow_array(
        &self,
        data_type: &arrow_schema::DataType,
        array: &ArrayImpl,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        ToArrow::to_array(self, data_type, array)
    }

    pub fn array_from_arrow_array(
        &self,
        field: &arrow_schema::Field,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        FromArrow::from_array(self, field, array)
    }

    /// A helper function to convert an Arrow array to RisingWave array without knowing the field.
    /// It will use the datatype from arrow array to infer the RisingWave data type.
    ///
    /// The difference between this function and `array_from_arrow_array` is that `array_from_arrow_array` will try using `ARROW:extension:name` field metadata to determine the RisingWave data type for extension types.
    pub fn array_from_arrow_array_raw(
        &self,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        static FIELD_DUMMY: LazyLock<arrow_schema::Field> =
            LazyLock::new(|| arrow_schema::Field::new("dummy", arrow_schema::DataType::Null, true));
        FromArrow::from_array(self, &FIELD_DUMMY, array)
    }
}

impl ToArrow for IcebergArrowConvert {
    fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        let data_type = match data_type {
            DataType::Boolean => self.bool_type_to_arrow(),
            DataType::Int16 => self.int32_type_to_arrow(),
            DataType::Int32 => self.int32_type_to_arrow(),
            DataType::Int64 => self.int64_type_to_arrow(),
            DataType::Int256 => self.int256_type_to_arrow(),
            DataType::Float32 => self.float32_type_to_arrow(),
            DataType::Float64 => self.float64_type_to_arrow(),
            DataType::Date => self.date_type_to_arrow(),
            DataType::Time => self.time_type_to_arrow(),
            DataType::Timestamp => self.timestamp_type_to_arrow(),
            DataType::Timestamptz => self.timestamptz_type_to_arrow(),
            DataType::Interval => self.interval_type_to_arrow(),
            DataType::Varchar => self.varchar_type_to_arrow(),
            DataType::Bytea => self.bytea_type_to_arrow(),
            DataType::Serial => self.serial_type_to_arrow(),
            DataType::Decimal => return Ok(self.decimal_type_to_arrow(name)),
            DataType::Jsonb => self.varchar_type_to_arrow(),
            DataType::Variant => return Ok(variant_arrow_field(name)),
            DataType::Struct(fields) => self.struct_type_to_arrow(fields)?,
            DataType::List(list) => self.list_type_to_arrow(list)?,
            DataType::Map(map) => self.map_type_to_arrow(map)?,
            DataType::Vector(_) => self.vector_type_to_arrow()?,
        };
        Ok(arrow_schema::Field::new(name, data_type, true))
    }

    #[inline]
    fn interval_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Utf8
    }

    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        // Fixed-point decimal; precision P, scale S Scale is fixed, precision must be less than 38.
        let data_type =
            arrow_schema::DataType::Decimal128(ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE);
        arrow_schema::Field::new(name, data_type, true)
    }

    fn decimal_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &DecimalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let (precision, max_scale) = match data_type {
            arrow_schema::DataType::Decimal128(precision, scale) => (*precision, *scale),
            _ => return Err(ArrayError::to_arrow("Invalid decimal type")),
        };

        // Convert Decimal to i128:
        let max_value = 10_i128.pow(precision as u32) - 1;
        let values: Vec<Option<i128>> = array
            .iter()
            .map(|e| {
                e.and_then(|e| match e {
                    crate::array::Decimal::Normalized(e) => {
                        let value = e.mantissa();
                        let scale = e.scale() as i8;
                        let diff_scale = abs(max_scale - scale);
                        let value = match scale {
                            _ if scale < max_scale => value
                                .checked_mul(10_i128.pow(diff_scale as u32))
                                .filter(|&v| abs(v) <= max_value)
                                .unwrap_or_else(|| {
                                    tracing::warn!(
                                        "Decimal overflow when converting to arrow decimal with precision {} and scale {}. It will be replaced with inf/-inf.",
                                        precision, max_scale
                                    );
                                    if value >= 0 { max_value } else { -max_value }
                                }),
                            _ if scale > max_scale => value.div(10_i128.pow(diff_scale as u32)),
                            _ => value,
                        };
                        Some(value)
                    }
                    // For Inf, we replace them with the max/min value within the precision.
                    crate::array::Decimal::PositiveInf => {
                        Some(max_value)
                    }
                    crate::array::Decimal::NegativeInf => {
                        Some(-max_value)
                    }
                    crate::array::Decimal::NaN => None,
                })
            })
            .collect();

        let array = arrow_array::Decimal128Array::from(values)
            .with_precision_and_scale(precision, max_scale)
            .map_err(ArrayError::from_arrow)?;
        Ok(Arc::new(array) as ArrayRef)
    }

    fn interval_to_arrow(
        &self,
        array: &IntervalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::StringArray::from(array)))
    }

    fn variant_to_arrow(
        &self,
        array: &RwVariantArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        // For a SQL NULL row the children's bytes are ignored by the parent null bitmap,
        // so `raw_iter`'s valid variant-null placeholder keeps both child arrays non-null
        // without a special branch per row.
        let metadata = Arc::new(arrow_array::BinaryArray::from_iter_values(
            array.raw_iter().map(|variant| variant.metadata()),
        )) as ArrayRef;
        let value = Arc::new(arrow_array::BinaryArray::from_iter_values(
            array.raw_iter().map(|variant| variant.value()),
        )) as ArrayRef;
        let nulls = (!array.null_bitmap().all()).then(|| array.null_bitmap().into());

        Ok(Arc::new(arrow_array::StructArray::new(
            variant_arrow_fields(),
            vec![metadata, value],
            nulls,
        )))
    }
}

impl FromArrow for IcebergArrowConvert {
    fn from_extension_type(
        &self,
        type_name: &str,
        physical_type: &arrow_schema::DataType,
    ) -> Result<DataType, ArrayError> {
        match (type_name, physical_type) {
            (VariantType::NAME, arrow_schema::DataType::Struct(_)) => Ok(DataType::Variant),
            (VariantType::NAME, _) => Err(ArrayError::from_arrow(format!(
                "variant extension type requires a struct physical type, got: {physical_type}"
            ))),
            _ => DefaultIcebergFromArrow.from_extension_type(type_name, physical_type),
        }
    }

    fn from_extension_array(
        &self,
        type_name: &str,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        match type_name {
            VariantType::NAME => variant_array_to_variant(array),
            _ => DefaultIcebergFromArrow.from_extension_array(type_name, array),
        }
    }
}

/// The Arrow field layout of an unshredded variant column, tagged with the
/// `arrow.parquet.variant` extension.
fn variant_arrow_field(name: &str) -> arrow_schema::Field {
    arrow_schema::Field::new(
        name,
        arrow_schema::DataType::Struct(variant_arrow_fields()),
        true,
    )
    .with_extension_type(VariantType)
}

fn variant_arrow_fields() -> arrow_schema::Fields {
    [
        Arc::new(arrow_schema::Field::new(
            "metadata",
            arrow_schema::DataType::Binary,
            false,
        )),
        Arc::new(arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::Binary,
            false,
        )),
    ]
    .into()
}

fn variant_array_to_variant(array: &arrow_array::ArrayRef) -> Result<ArrayImpl, ArrayError> {
    let variant_array =
        ParquetVariantArray::try_new(array.as_ref()).map_err(ArrayError::from_arrow)?;
    // The shredded encoding cannot be reconstructed yet, and decoding only metadata/value
    // would yield silently partial objects, so the whole column reads as NULL.
    if variant_array.typed_value_field().is_some() {
        static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);
        if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
            tracing::warn!(
                suppressed_count,
                "shredded variant column (with a `typed_value` field) is not supported yet; \
                 reading it as NULL",
            );
        }
        let mut builder = VariantArrayBuilder::new(variant_array.len());
        for _ in 0..variant_array.len() {
            builder.append_null();
        }
        return Ok(ArrayImpl::Variant(builder.finish()));
    }
    // The physical binary layout is constant across the batch, so resolve the typed
    // accessors once here rather than re-dispatching and downcasting per row.
    let metadata_accessor = BinaryArrayAccessor::resolve(variant_array.metadata_field())?;
    let value_accessor = variant_array
        .value_field()
        .map(BinaryArrayAccessor::resolve)
        .transpose()?;
    let mut builder = VariantArrayBuilder::new(variant_array.len());

    for idx in 0..variant_array.len() {
        if variant_array.is_null(idx) {
            builder.append_null();
            continue;
        }

        // `from_parts` fully validates the untrusted bytes and re-encodes them into
        // RW's canonical form, which the byte-wise `Eq`/`Ord` of VARIANT relies on.
        // TODO: rows of a batch virtually always share one metadata dictionary;
        // cache the validated metadata instead of re-validating it per row.
        let variant_result = match &value_accessor {
            Some(value) if value.is_valid(idx) => {
                VariantVal::from_parts(metadata_accessor.value(idx), value.value(idx))
            }
            // Mirrors the upstream `VariantArray::try_value` fallback. Shredded arrays are
            // rejected above, so reaching this arm means the file is malformed for the
            // unshredded encoding — `value` is required there — and the row silently becomes
            // variant null rather than SQL NULL.
            _ => Ok(VariantVal::null()),
        };

        match variant_result {
            Ok(variant) => builder.append(Some(variant.as_scalar_ref())),
            Err(err) => {
                // A systematically corrupt file would otherwise warn once per row.
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::warn!(
                        error = %err.as_report(),
                        suppressed_count,
                        "failed to decode iceberg variant value at index {}. It will be replaced with null.",
                        idx,
                    );
                }
                builder.append_null();
            }
        }
    }

    Ok(ArrayImpl::Variant(builder.finish()))
}

/// A binary-like Arrow array with its physical layout resolved once per batch, so
/// the per-row hot path just indexes the already-downcast array.
///
/// `VariantArray::try_new` validates the `metadata` and `value` fields as `Binary`,
/// `LargeBinary`, or `BinaryView` before we get here, so [`resolve`](Self::resolve)
/// covers every layout a variant field can carry and its error arm is unreachable
/// for arrays taken from a `VariantArray`.
enum BinaryArrayAccessor<'a> {
    Binary(&'a arrow_array::BinaryArray),
    LargeBinary(&'a arrow_array::LargeBinaryArray),
    BinaryView(&'a arrow_array::BinaryViewArray),
}

impl<'a> BinaryArrayAccessor<'a> {
    fn resolve(array: &'a ArrayRef) -> Result<Self, ArrayError> {
        use arrow_schema::DataType;
        match array.data_type() {
            DataType::Binary => Ok(Self::Binary(array.as_binary::<i32>())),
            DataType::LargeBinary => Ok(Self::LargeBinary(array.as_binary::<i64>())),
            DataType::BinaryView => Ok(Self::BinaryView(array.as_binary_view())),
            other => Err(ArrayError::from_arrow(format!(
                "variant metadata/value has unsupported binary layout: {other}"
            ))),
        }
    }

    #[inline]
    fn is_valid(&self, index: usize) -> bool {
        use arrow_array::Array as _;
        match self {
            Self::Binary(array) => array.is_valid(index),
            Self::LargeBinary(array) => array.is_valid(index),
            Self::BinaryView(array) => array.is_valid(index),
        }
    }

    #[inline]
    fn value(&self, index: usize) -> &[u8] {
        match self {
            Self::Binary(array) => array.value(index),
            Self::LargeBinary(array) => array.value(index),
            Self::BinaryView(array) => array.value(index),
        }
    }
}

/// Iceberg sink with `create_table_if_not_exists` option will use this struct to convert the
/// iceberg data type to arrow data type.
///
/// Specifically, it will add the field id to the arrow field metadata, because iceberg-rust need the field id to be set.
///
/// Note: this is different from [`IcebergArrowConvert`], which is used to read from/write to
/// an _existing_ iceberg table. In that case, we just need to make sure the data is compatible to the existing schema.
/// But to _create a new table_, we need to meet more requirements of iceberg.
#[derive(Default)]
pub struct IcebergCreateTableArrowConvert {
    next_field_id: RefCell<u32>,
}

impl IcebergCreateTableArrowConvert {
    pub fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        ToArrow::to_arrow_field(self, name, data_type)
    }

    fn add_field_id(&self, arrow_field: &mut arrow_schema::Field) {
        *self.next_field_id.borrow_mut() += 1;
        let field_id = *self.next_field_id.borrow();

        // Preserve extension metadata such as `arrow.parquet.variant` while adding the
        // Iceberg field id required by `arrow_schema_to_schema`.
        let mut metadata = arrow_field.metadata().clone();
        // for iceberg-rust
        metadata.insert("PARQUET:field_id".to_owned(), field_id.to_string());
        arrow_field.set_metadata(metadata);
    }
}

impl ToArrow for IcebergCreateTableArrowConvert {
    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        // To create a iceberg table, we need a decimal type with precision and scale to be set
        // We choose 28 here
        // The decimal type finally will be converted to an iceberg decimal type.
        // Iceberg decimal(P,S)
        // Fixed-point decimal; precision P, scale S Scale is fixed, precision must be less than 38.
        let data_type =
            arrow_schema::DataType::Decimal128(ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE);

        let mut arrow_field = arrow_schema::Field::new(name, data_type, true);
        self.add_field_id(&mut arrow_field);
        arrow_field
    }

    #[inline]
    fn interval_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Utf8
    }

    fn jsonb_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        let data_type = arrow_schema::DataType::Utf8;

        let mut arrow_field = arrow_schema::Field::new(name, data_type, true);
        self.add_field_id(&mut arrow_field);
        arrow_field
    }

    /// Convert RisingWave data type to Arrow data type.
    ///
    /// This function returns a `Field` instead of `DataType` because some may be converted to
    /// extension types which require additional metadata in the field.
    fn to_arrow_field(
        &self,
        name: &str,
        value: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        let data_type = match value {
            // using the inline function
            DataType::Boolean => self.bool_type_to_arrow(),
            DataType::Int16 => self.int32_type_to_arrow(),
            DataType::Int32 => self.int32_type_to_arrow(),
            DataType::Int64 => self.int64_type_to_arrow(),
            DataType::Int256 => self.varchar_type_to_arrow(),
            DataType::Float32 => self.float32_type_to_arrow(),
            DataType::Float64 => self.float64_type_to_arrow(),
            DataType::Date => self.date_type_to_arrow(),
            DataType::Time => self.time_type_to_arrow(),
            DataType::Timestamp => self.timestamp_type_to_arrow(),
            DataType::Timestamptz => self.timestamptz_type_to_arrow(),
            DataType::Interval => self.interval_type_to_arrow(),
            DataType::Varchar => self.varchar_type_to_arrow(),
            DataType::Bytea => self.bytea_type_to_arrow(),
            DataType::Serial => self.serial_type_to_arrow(),
            DataType::Decimal => return Ok(self.decimal_type_to_arrow(name)),
            DataType::Jsonb => self.varchar_type_to_arrow(),
            DataType::Variant => {
                let mut arrow_field = variant_arrow_field(name);
                self.add_field_id(&mut arrow_field);
                return Ok(arrow_field);
            }
            DataType::Struct(fields) => self.struct_type_to_arrow(fields)?,
            DataType::List(list) => self.list_type_to_arrow(list)?,
            DataType::Map(map) => self.map_type_to_arrow(map)?,
            DataType::Vector(_) => self.vector_type_to_arrow()?,
        };

        let mut arrow_field = arrow_schema::Field::new(name, data_type, true);
        self.add_field_id(&mut arrow_field);
        Ok(arrow_field)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use parquet_variant::{
        ShortString, Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16,
    };
    use parquet_variant_compute::{VariantArrayBuilder, json_to_variant};
    use uuid::Uuid;

    use super::arrow_array::{ArrayRef, Decimal128Array};
    use super::arrow_schema::DataType as ArrowDataType;
    use super::*;
    use crate::array::{Decimal, DecimalArray};
    use crate::types::{MapType, ToText};

    #[test]
    fn decimal() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let ty = ArrowDataType::Decimal128(6, 3);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            Decimal128Array::from(vec![
                None,
                None,
                Some(999999),
                Some(-999999),
                Some(123400),
                Some(123456),
            ])
            .with_data_type(ty),
        ) as ArrayRef;
        assert_eq!(&arrow_array, &expect_array);
    }

    #[test]
    fn decimal_with_large_scale() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let ty = ArrowDataType::Decimal128(ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            Decimal128Array::from(vec![
                None,
                None,
                // With precision=38, max value is 10^38 - 1
                Some(99999999999999999999999999999999999999),
                Some(-99999999999999999999999999999999999999),
                Some(1234000000000),
                Some(1234560000000),
            ])
            .with_data_type(ty),
        ) as ArrayRef;
        assert_eq!(&arrow_array, &expect_array);
    }

    #[test]
    fn decimal_edge_cases_risingwave_precision() {
        // Test edge cases between RisingWave decimal precision (28 digits) and Arrow Decimal128(38,10)
        let array = DecimalArray::from_iter([
            // Large 27-digit integer (previously would overflow with precision=28, scale=10)
            Some(Decimal::Normalized(
                "999999999999999999999999999".parse().unwrap(),
            )),
            // RisingWave MAX_PRECISION: 28-digit integer
            Some(Decimal::Normalized(
                "9999999999999999999999999999".parse().unwrap(),
            )),
            // Large integer with fractional part
            Some(Decimal::Normalized(
                "999999999999999999.9999999999".parse().unwrap(),
            )),
            // Small value with maximum fractional digits
            Some(Decimal::Normalized(
                "0.9999999999999999999999999999".parse().unwrap(),
            )),
            // Negative large integer
            Some(Decimal::Normalized(
                "-999999999999999999999999999".parse().unwrap(),
            )),
            // Edge case: exactly 10^18 (18 digits) - boundary for old precision=28,scale=10
            Some(Decimal::Normalized("1000000000000000000".parse().unwrap())),
            // Very small decimal
            Some(Decimal::Normalized("0.0000000001".parse().unwrap())),
            // Zero with fractional representation
            Some(Decimal::Normalized("0.0000000000".parse().unwrap())),
        ]);

        let ty = ArrowDataType::Decimal128(ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();

        let expect_array = Arc::new(
            Decimal128Array::from(vec![
                // 999999999999999999999999999 * 10^10 (scale 0 → 10)
                Some(9999999999999999999999999990000000000),
                // 9999999999999999999999999999 * 10^10
                Some(99999999999999999999999999990000000000),
                // 999999999999999999.9999999999 already at scale 10
                Some(9999999999999999999999999999),
                // 0.9999999999999999999999999999: scale 28 → 10, truncates to 0.9999999999
                Some(9999999999),
                // -999999999999999999999999999 * 10^10
                Some(-9999999999999999999999999990000000000),
                // 1000000000000000000 * 10^10
                Some(10000000000000000000000000000),
                // 0.0000000001 already at scale 10
                Some(1),
                // 0.0000000000 (scale 10)
                Some(0),
            ])
            .with_data_type(ty),
        ) as ArrayRef;

        assert_eq!(&arrow_array, &expect_array);
    }

    #[test]
    fn decimal_special_values_roundtrip() {
        // Test that special decimal values (inf, -inf, nan) can be written and read back correctly
        use crate::array::Array;

        let original_array = DecimalArray::from_iter([
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::NaN),
            Some(Decimal::Normalized("123.45".parse().unwrap())),
            None,
        ]);

        // Convert to Arrow
        let ty = ArrowDataType::Decimal128(ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE);
        let arrow_array = IcebergArrowConvert
            .decimal_to_arrow(&ty, &original_array)
            .unwrap();

        // Convert back to RisingWave
        let arrow_decimal: &arrow_array::Decimal128Array = arrow_array
            .as_any()
            .downcast_ref()
            .expect("should be Decimal128Array");

        let roundtrip_array: DecimalArray = arrow_decimal.try_into().unwrap();

        // Verify special values roundtrip correctly
        assert_eq!(original_array.len(), roundtrip_array.len());

        // PositiveInf -> max value -> PositiveInf
        assert_eq!(roundtrip_array.value_at(0), Some(Decimal::PositiveInf));

        // NegativeInf -> min value -> NegativeInf
        assert_eq!(roundtrip_array.value_at(1), Some(Decimal::NegativeInf));

        // NaN -> NULL -> None (NaN cannot roundtrip, becomes NULL in Arrow)
        assert_eq!(roundtrip_array.value_at(2), None);

        // Normal value roundtrips correctly (scale may be adjusted)
        assert!(matches!(
            roundtrip_array.value_at(3),
            Some(Decimal::Normalized(_))
        ));

        // NULL -> NULL -> None
        assert_eq!(roundtrip_array.value_at(4), None);
    }

    #[test]
    fn all_variant_internal_types_convert_to_variant() {
        let long_string = "x".repeat(64);
        let binary_bytes = [0x0a_u8, 0x0b, 0x0c, 0x0d];
        let object_and_list_json = Arc::new(arrow_array::StringArray::from(vec![
            Some(r#"{"a":1,"b":[true,null]}"#),
            Some(r#"[1,{"x":2},"tail"]"#),
        ])) as ArrayRef;
        let object_and_list = json_to_variant(&object_and_list_json).unwrap();

        let timestamp_micros = DateTime::parse_from_rfc3339("2024-11-07T12:33:54.123456+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let timestamp_ntz_micros =
            NaiveDateTime::parse_from_str("2024-11-07 12:33:54.123456", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap();
        let timestamp_nanos = DateTime::parse_from_rfc3339("2024-11-07T12:33:54.123456789+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let timestamp_ntz_nanos =
            NaiveDateTime::parse_from_str("2024-11-07 12:33:54.123456789", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap();
        let time = NaiveTime::from_hms_micro_opt(12, 33, 54, 123_456).unwrap();
        let uuid = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();

        let mut builder = VariantArrayBuilder::new(25);
        builder.append_null(); // null Arrow row (not variant null)
        builder.append_variant(Variant::Null);
        builder.append_variant(Variant::BooleanTrue);
        builder.append_variant(Variant::BooleanFalse);
        builder.append_variant(Variant::Int8(34));
        builder.append_variant(Variant::Int16(1234));
        builder.append_variant(Variant::Int32(100_000));
        builder.append_variant(Variant::Int64(5_000_000_000));
        builder.append_variant(Variant::Float(3.5));
        builder.append_variant(Variant::Double(14.25));
        // NaN normalizes to the canonical bit pattern and renders as the string "NaN".
        builder.append_variant(Variant::Double(f64::NAN));
        builder.append_variant(Variant::Decimal4(
            VariantDecimal4::try_new(12_345, 0).unwrap(),
        ));
        builder.append_variant(Variant::Decimal8(
            VariantDecimal8::try_new(1_234_567_890_123, 0).unwrap(),
        ));
        builder.append_variant(Variant::Decimal16(
            VariantDecimal16::try_new(1_234_567_890_123_456_789, 0).unwrap(),
        ));
        builder.append_variant(Variant::Date(NaiveDate::from_ymd_opt(2024, 11, 7).unwrap()));
        builder.append_variant(Variant::TimestampMicros(timestamp_micros));
        builder.append_variant(Variant::TimestampNtzMicros(timestamp_ntz_micros));
        builder.append_variant(Variant::TimestampNanos(timestamp_nanos));
        builder.append_variant(Variant::TimestampNtzNanos(timestamp_ntz_nanos));
        builder.append_variant(Variant::Time(time));
        builder.append_variant(Variant::Binary(&binary_bytes));
        builder.append_variant(Variant::ShortString(
            ShortString::try_new("iceberg").unwrap(),
        ));
        builder.append_variant(Variant::from(long_string.as_str()));
        builder.append_variant(Variant::Uuid(uuid));
        builder.append_variant(object_and_list.value(0));
        builder.append_variant(object_and_list.value(1));

        let variant_array = builder.build();
        let field = variant_array.field("variant_col");

        assert_eq!(
            IcebergArrowConvert.type_from_field(&field).unwrap(),
            DataType::Variant
        );

        let array = Arc::new(variant_array.into_inner()) as ArrayRef;

        let converted = IcebergArrowConvert
            .array_from_arrow_array(&field, &array)
            .unwrap();
        let values = converted
            .into_variant()
            .iter()
            .map(|value| value.map(|value| value.to_text()))
            .collect::<Vec<_>>();

        let timestamp_micros_json =
            serde_json::to_string("2024-11-07T12:33:54.123456+00:00").unwrap();
        let timestamp_ntz_micros_json =
            serde_json::to_string("2024-11-07T12:33:54.123456").unwrap();
        let timestamp_nanos_json =
            serde_json::to_string("2024-11-07T12:33:54.123456789+00:00").unwrap();
        let timestamp_ntz_nanos_json =
            serde_json::to_string("2024-11-07T12:33:54.123456789").unwrap();
        let time_json = serde_json::to_string("12:33:54.123456").unwrap();
        let binary_json = serde_json::to_string("CgsMDQ==").unwrap();
        let short_string_json = serde_json::to_string("iceberg").unwrap();
        let long_string_json = serde_json::to_string(&long_string).unwrap();
        let uuid_json = serde_json::to_string("123e4567-e89b-12d3-a456-426614174000").unwrap();

        assert_eq!(
            values,
            vec![
                None, // null Arrow row
                Some("null".to_owned()),
                Some("true".to_owned()),
                Some("false".to_owned()),
                Some("34".to_owned()),
                Some("1234".to_owned()),
                Some("100000".to_owned()),
                Some("5000000000".to_owned()),
                Some("3.5".to_owned()),
                Some("14.25".to_owned()),
                Some(r#""NaN""#.to_owned()),
                Some("12345".to_owned()),
                Some("1234567890123".to_owned()),
                Some("1234567890123456789".to_owned()),
                Some(r#""2024-11-07""#.to_owned()),
                Some(timestamp_micros_json),
                Some(timestamp_ntz_micros_json),
                Some(timestamp_nanos_json),
                Some(timestamp_ntz_nanos_json),
                Some(time_json),
                Some(binary_json),
                Some(short_string_json),
                Some(long_string_json),
                Some(uuid_json),
                Some(r#"{"a":1,"b":[true,null]}"#.to_owned()),
                Some(r#"[1,{"x":2},"tail"]"#.to_owned()),
            ],
        );
    }

    #[test]
    fn variant_type_recurses_in_nested_types() {
        let payload_field = arrow_schema::Field::new(
            "payload",
            ArrowDataType::Struct(
                vec![
                    Arc::new(variant_arrow_field("top_variant")),
                    Arc::new(arrow_schema::Field::new(
                        "variant_list",
                        ArrowDataType::List(Arc::new(variant_arrow_field("element"))),
                        true,
                    )),
                    Arc::new(arrow_schema::Field::new(
                        "variant_map",
                        ArrowDataType::Map(
                            Arc::new(arrow_schema::Field::new(
                                "entries",
                                ArrowDataType::Struct(
                                    vec![
                                        Arc::new(arrow_schema::Field::new(
                                            "key",
                                            ArrowDataType::Utf8,
                                            false,
                                        )),
                                        Arc::new(variant_arrow_field("value")),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        true,
                    )),
                ]
                .into(),
            ),
            true,
        );

        assert_eq!(
            IcebergArrowConvert.type_from_field(&payload_field).unwrap(),
            DataType::Struct(StructType::new(vec![
                ("top_variant", DataType::Variant),
                ("variant_list", DataType::list(DataType::Variant)),
                (
                    "variant_map",
                    DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Variant)),
                ),
            ])),
        );
    }

    #[test]
    fn variant_map_key_is_rejected() {
        let field = arrow_schema::Field::new(
            "variant_map_key",
            ArrowDataType::Map(
                Arc::new(arrow_schema::Field::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            Arc::new(variant_arrow_field("key")),
                            Arc::new(arrow_schema::Field::new("value", ArrowDataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );

        let err = IcebergArrowConvert.type_from_field(&field).unwrap_err();
        assert!(err.to_string().contains("invalid map key type: variant"));
    }

    #[test]
    fn invalid_variant_value_decodes_to_null() {
        let field = variant_arrow_field("variant_col");
        // Row 0 is a valid variant string "HI"; row 1 has a corrupt value byte.
        let array = Arc::new(arrow_array::StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "metadata",
                    ArrowDataType::Binary,
                    false,
                )),
                Arc::new(arrow_array::BinaryArray::from_iter_values([
                    &[1_u8, 0, 0][..],
                    &[1_u8, 0, 0][..],
                ])) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "value",
                    ArrowDataType::Binary,
                    true,
                )),
                Arc::new(arrow_array::BinaryArray::from_iter_values([
                    &[0x09_u8, b'H', b'I'][..],
                    &[255_u8][..],
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        let converted = IcebergArrowConvert
            .array_from_arrow_array(&field, &array)
            .unwrap();
        let values = converted
            .into_variant()
            .iter()
            .map(|value| value.map(|value| value.to_text()))
            .collect::<Vec<_>>();

        assert_eq!(values, vec![Some(r#""HI""#.to_owned()), None]);
    }

    #[test]
    fn shredded_variant_decodes_as_null() {
        let field = variant_arrow_field("variant_col");
        let array = Arc::new(arrow_array::StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "metadata",
                    ArrowDataType::Binary,
                    false,
                )),
                Arc::new(arrow_array::BinaryArray::from_iter_values([
                    &[1_u8, 0, 0][..]
                ])) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "typed_value",
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(arrow_array::Int64Array::from(vec![7_i64])) as ArrayRef,
            ),
        ])) as ArrayRef;

        let decoded = IcebergArrowConvert
            .array_from_arrow_array(&field, &array)
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded.as_variant().is_null(0));
    }

    #[test]
    fn variant_in_projected_struct_decodes_by_declared_type() {
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(Variant::from(7_i64));
        let variant_array = builder.build();
        let variant_field = variant_array.field("v");
        let variant_child = Arc::new(variant_array.into_inner()) as ArrayRef;
        let extra_child = Arc::new(arrow_array::Int64Array::from(vec![1_i64])) as ArrayRef;
        let actual = Arc::new(arrow_array::StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "extra",
                    ArrowDataType::Int64,
                    true,
                )),
                extra_child,
            ),
            (Arc::new(variant_field), variant_child),
        ])) as ArrayRef;

        // Declared as struct<v variant>: the parquet struct is a superset, so conversion
        // goes through the projected path, which must decode the variant child.
        let declared_field = arrow_schema::Field::new(
            "s",
            ArrowDataType::Struct(vec![Arc::new(variant_arrow_field("v"))].into()),
            true,
        );
        let converted = IcebergArrowConvert
            .array_from_arrow_array(&declared_field, &actual)
            .unwrap();
        assert_eq!(
            converted.data_type(),
            DataType::Struct(StructType::new(vec![("v", DataType::Variant)])),
        );
        let ArrayImpl::Struct(struct_array) = &converted else {
            panic!("expected struct array");
        };
        let values = struct_array.field_at(0).as_variant();
        assert_eq!(
            values.iter().next().unwrap().map(|v| v.to_text()),
            Some("7".to_owned())
        );
    }
}
