// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{DataType as AstDataType, Ident, StructField};

#[easy_ext::ext(DataTypeToAst)]
impl DataType {
    /// Convert the data type to its AST representation.
    pub fn to_ast(&self) -> AstDataType {
        match self {
            DataType::Boolean => AstDataType::Boolean,
            DataType::Int16 => AstDataType::SmallInt,
            DataType::Int32 => AstDataType::Int,
            DataType::Int64 => AstDataType::BigInt,
            DataType::Float32 => AstDataType::Real,
            DataType::Float64 => AstDataType::Double,
            // Note: we don't currently support precision and scale for decimal
            DataType::Decimal => AstDataType::Decimal(None, None),
            DataType::Date => AstDataType::Date,
            DataType::Varchar => AstDataType::Varchar,
            DataType::Time => AstDataType::Time(false),
            DataType::Timestamp => AstDataType::Timestamp(false),
            DataType::Timestamptz => AstDataType::Timestamp(true),
            DataType::Interval => AstDataType::Interval,
            DataType::Jsonb => AstDataType::Jsonb,
            DataType::Variant => AstDataType::Variant,
            DataType::Bytea => AstDataType::Bytea,
            DataType::List(list) => AstDataType::Array(Box::new(list.elem().to_ast())),
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|(name, ty)| StructField {
                        name: Ident::from_real_value(name),
                        data_type: ty.to_ast(),
                    })
                    .collect();
                AstDataType::Struct(fields)
            }
            DataType::Int256 => AstDataType::Custom(vec!["rw_int256".into()].into()),
            DataType::Map(map) => {
                AstDataType::Map(Box::new((map.key().to_ast(), map.value().to_ast())))
            }
            DataType::Vector(n) => AstDataType::Vector(*n as _),
            DataType::Serial => unreachable!("serial type should not be user-defined"),
        }
    }
}
