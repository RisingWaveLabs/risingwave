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

//! This is for arrow dependency named `arrow-xxx` such as `arrow-array` in the cargo workspace.
//!
//! This should the default arrow version to be used in our system.
//!
//! The corresponding version of arrow is currently used by `udf`.

use std::sync::Arc;

pub use super::arrow_58::{
    FromArrow, ToArrow, arrow_array, arrow_buffer, arrow_cast, arrow_schema,
};
use crate::array::{ArrayError, ArrayImpl, DataType, DecimalArray, JsonbArray};

/// Arrow conversion for UDF.
#[derive(Default, Debug)]
pub struct UdfArrowConvert {
    /// Whether the UDF talks in legacy mode.
    ///
    /// If true, decimal and jsonb types are mapped to Arrow `LargeBinary` and `LargeUtf8` types.
    /// Otherwise, they are mapped to Arrow extension types.
    /// See <https://github.com/risingwavelabs/arrow-udf/tree/main#extension-types>.
    pub legacy: bool,
}

impl ToArrow for UdfArrowConvert {
    fn decimal_to_arrow(
        &self,
        _data_type: &arrow_schema::DataType,
        array: &DecimalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        if self.legacy {
            // Decimal values are stored as ASCII text representation in a large binary array.
            Ok(Arc::new(arrow_array::LargeBinaryArray::from(array)))
        } else {
            Ok(Arc::new(arrow_array::StringArray::from(array)))
        }
    }

    fn jsonb_to_arrow(&self, array: &JsonbArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        if self.legacy {
            // JSON values are stored as text representation in a large string array.
            Ok(Arc::new(arrow_array::LargeStringArray::from(array)))
        } else {
            Ok(Arc::new(arrow_array::StringArray::from(array)))
        }
    }

    fn jsonb_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        if self.legacy {
            arrow_schema::Field::new(name, arrow_schema::DataType::LargeUtf8, true)
        } else {
            arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true)
                .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
        }
    }

    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        if self.legacy {
            arrow_schema::Field::new(name, arrow_schema::DataType::LargeBinary, true)
        } else {
            arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true)
                .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
        }
    }
}

impl FromArrow for UdfArrowConvert {
    fn from_large_utf8(&self) -> Result<DataType, ArrayError> {
        if self.legacy {
            Ok(DataType::Jsonb)
        } else {
            Ok(DataType::Varchar)
        }
    }

    fn from_large_binary(&self) -> Result<DataType, ArrayError> {
        if self.legacy {
            Ok(DataType::Decimal)
        } else {
            Ok(DataType::Bytea)
        }
    }

    fn from_large_utf8_array(
        &self,
        array: &arrow_array::LargeStringArray,
    ) -> Result<ArrayImpl, ArrayError> {
        if self.legacy {
            Ok(ArrayImpl::Jsonb(array.try_into()?))
        } else {
            Ok(ArrayImpl::Utf8(array.into()))
        }
    }

    fn from_large_binary_array(
        &self,
        array: &arrow_array::LargeBinaryArray,
    ) -> Result<ArrayImpl, ArrayError> {
        if self.legacy {
            Ok(ArrayImpl::Decimal(array.try_into()?))
        } else {
            Ok(ArrayImpl::Bytea(array.into()))
        }
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::Array as _;

    use super::*;
    use crate::array::*;

    /// Wraps an array's own type into a nameless field for the `from_*_array` calls.
    fn typed_field(array: &impl arrow_array::Array) -> arrow_schema::Field {
        arrow_schema::Field::new("", array.data_type().clone(), true)
    }

    #[test]
    fn struct_array() {
        // Empty array - risingwave to arrow conversion.
        let test_arr = StructArray::new(StructType::empty(), vec![], Bitmap::ones(0));
        assert_eq!(
            UdfArrowConvert::default()
                .struct_to_arrow(
                    &arrow_schema::DataType::Struct(arrow_schema::Fields::empty()),
                    &test_arr
                )
                .unwrap()
                .len(),
            0
        );

        // Empty array - arrow to risingwave conversion.
        let test_arr_2 = arrow_array::StructArray::new_empty_fields(0, None);
        let test_arr_2_field = typed_field(&test_arr_2);
        assert_eq!(
            UdfArrowConvert::default()
                .from_struct_array(&test_arr_2_field, &test_arr_2)
                .unwrap()
                .len(),
            0
        );

        // Struct array with primitive types. arrow to risingwave conversion.
        let test_arrow_struct_array = arrow_array::StructArray::try_from(vec![
            (
                "a",
                Arc::new(arrow_array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(true),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
            (
                "b",
                Arc::new(arrow_array::Int32Array::from(vec![
                    Some(42),
                    Some(28),
                    Some(19),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let struct_field = typed_field(&test_arrow_struct_array);
        let actual_risingwave_struct_array = UdfArrowConvert::default()
            .from_struct_array(&struct_field, &test_arrow_struct_array)
            .unwrap()
            .into_struct();
        let expected_risingwave_struct_array = StructArray::new(
            StructType::new(vec![("a", DataType::Boolean), ("b", DataType::Int32)]),
            vec![
                BoolArray::from_iter([Some(false), Some(false), Some(true), None]).into_ref(),
                I32Array::from_iter([Some(42), Some(28), Some(19), None]).into_ref(),
            ],
            [true, true, true, true].into_iter().collect(),
        );
        assert_eq!(
            expected_risingwave_struct_array,
            actual_risingwave_struct_array
        );
    }

    #[test]
    fn list() {
        let array = ListArray::from_iter([None, Some(vec![0, -127, 127, 50]), Some(vec![0; 0])]);
        let data_type = arrow_schema::DataType::new_list(arrow_schema::DataType::Int32, true);
        let arrow = UdfArrowConvert::default()
            .list_to_arrow(&data_type, &array)
            .unwrap();
        let list_field = typed_field(&arrow);
        let rw_array = UdfArrowConvert::default()
            .from_list_array(&list_field, arrow.as_any().downcast_ref().unwrap())
            .unwrap();
        assert_eq!(rw_array.as_list(), &array);
    }

    #[test]
    fn map() {
        let map_type = MapType::from_kv(DataType::Varchar, DataType::Int32);
        let rw_map_type = DataType::Map(map_type.clone());
        let mut builder = MapArrayBuilder::with_type(3, rw_map_type.clone());
        builder.append_owned(Some(
            MapValue::try_from_kv(
                ListValue::from_str("{a,b,c}", &DataType::Varchar.list()).unwrap(),
                ListValue::from_str("{1,2,3}", &DataType::Int32.list()).unwrap(),
            )
            .unwrap(),
        ));
        builder.append_owned(None);
        builder.append_owned(Some(
            MapValue::try_from_kv(
                ListValue::from_str("{a,c}", &DataType::Varchar.list()).unwrap(),
                ListValue::from_str("{1,3}", &DataType::Int32.list()).unwrap(),
            )
            .unwrap(),
        ));
        let rw_array = builder.finish();

        let arrow_map_type = UdfArrowConvert::default()
            .map_type_to_arrow(&map_type)
            .unwrap();
        expect_test::expect![[r#"
            Map(
                Field {
                    name: "entries",
                    data_type: Struct(
                        [
                            Field {
                                name: "key",
                                data_type: Utf8,
                            },
                            Field {
                                name: "value",
                                data_type: Int32,
                                nullable: true,
                            },
                        ],
                    ),
                },
                false,
            )
        "#]]
        .assert_debug_eq(&arrow_map_type);
        let rw_map_type_new = UdfArrowConvert::default()
            .from_field(&arrow_schema::Field::new(
                "map",
                arrow_map_type.clone(),
                true,
            ))
            .unwrap();
        assert_eq!(rw_map_type, rw_map_type_new);
        let arrow = UdfArrowConvert::default()
            .map_to_arrow(&arrow_map_type, &rw_array)
            .unwrap();
        expect_test::expect![[r#"
            MapArray
            [
              StructArray
            -- validity:
            [
              valid,
              valid,
              valid,
            ]
            [
            -- child 0: "key" (Utf8)
            StringArray
            [
              "a",
              "b",
              "c",
            ]
            -- child 1: "value" (Int32)
            PrimitiveArray<Int32>
            [
              1,
              2,
              3,
            ]
            ],
              null,
              StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "key" (Utf8)
            StringArray
            [
              "a",
              "c",
            ]
            -- child 1: "value" (Int32)
            PrimitiveArray<Int32>
            [
              1,
              3,
            ]
            ],
            ]"#]]
        .assert_eq(
            &format!("{:#?}", arrow)
                .lines()
                .map(|s| s.trim_end())
                .collect::<Vec<_>>()
                .join("\n"),
        );

        let map_field = typed_field(&arrow);
        let rw_array_new = UdfArrowConvert::default()
            .from_map_array(&map_field, arrow.as_any().downcast_ref().unwrap())
            .unwrap();
        assert_eq!(&rw_array, rw_array_new.as_map());
    }
}
