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

use std::fmt;

use crate::ast::{Ident, ObjectName, display_comma_separated};

/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Fixed-length character type e.g. CHAR(10)
    Char(Option<u64>),
    /// Variable-length character type.
    /// We diverge from postgres by disallowing Varchar(n).
    Varchar,
    /// Uuid type
    Uuid,
    /// Decimal type with optional precision and scale e.g. DECIMAL(10,2)
    Decimal(Option<u64>, Option<u64>),
    /// Floating point with optional precision e.g. FLOAT(8)
    Float(Option<u64>),
    /// SMALLINT (int2)
    SmallInt,
    /// INTEGER (int4)
    Int,
    /// BIGINT (int8)
    BigInt,
    /// Floating point e.g. REAL
    Real,
    /// Double e.g. DOUBLE PRECISION
    Double,
    /// Boolean
    Boolean,
    /// Date
    Date,
    /// Time with optional time zone
    Time(bool),
    /// Timestamp with optional time zone
    Timestamp(bool),
    /// Interval
    Interval,
    /// Regclass used in postgresql serial
    Regclass,
    /// Regproc used in postgresql function
    Regproc,
    /// Text
    Text,
    /// Bytea
    Bytea,
    /// JSONB
    Jsonb,
    /// VARIANT
    Variant,
    /// Custom type such as enums
    Custom(ObjectName),
    /// Arrays
    Array(Box<DataType>),
    /// Structs
    Struct(Vec<StructField>),
    /// Map(key_type, value_type)
    Map(Box<(DataType, DataType)>),
    /// Vector of f32, fixed-length
    Vector(u64),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Char(size) => format_type_with_optional_length(f, "CHAR", size),
            DataType::Varchar => write!(f, "CHARACTER VARYING"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Decimal(precision, scale) => {
                if let Some(scale) = scale {
                    write!(f, "NUMERIC({},{})", precision.unwrap(), scale)
                } else {
                    format_type_with_optional_length(f, "NUMERIC", precision)
                }
            }
            DataType::Float(size) => format_type_with_optional_length(f, "FLOAT", size),
            DataType::SmallInt => {
                write!(f, "SMALLINT")
            }
            DataType::Int => write!(f, "INT"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Real => write!(f, "REAL"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time(tz) => write!(f, "TIME{}", if *tz { " WITH TIME ZONE" } else { "" }),
            DataType::Timestamp(tz) => {
                write!(f, "TIMESTAMP{}", if *tz { " WITH TIME ZONE" } else { "" })
            }
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Regclass => write!(f, "REGCLASS"),
            DataType::Regproc => write!(f, "REGPROC"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::Jsonb => write!(f, "JSONB"),
            DataType::Variant => write!(f, "VARIANT"),
            DataType::Array(ty) => write!(f, "{}[]", ty),
            DataType::Custom(ty) => write!(f, "{}", ty),
            DataType::Struct(defs) => {
                write!(f, "STRUCT<")?;
                if defs.is_empty() {
                    // We require a whitespace for empty(zero-field) struct to prevent `<>` from
                    // being tokenized as a single token `Neq`.
                    write!(f, " ")?;
                } else {
                    write!(f, "{}", display_comma_separated(defs))?;
                }
                write!(f, ">")
            }
            DataType::Map(kv) => {
                write!(f, "MAP({},{})", kv.0, kv.1)
            }
            DataType::Vector(size) => {
                write!(f, "VECTOR({})", size)
            }
        }
    }
}

fn format_type_with_optional_length(
    f: &mut fmt::Formatter<'_>,
    sql_type: &'static str,
    len: &Option<u64>,
) -> fmt::Result {
    write!(f, "{}", sql_type)?;
    if let Some(len) = len {
        write!(f, "({})", len)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructField {
    pub name: Ident,
    pub data_type: DataType,
}

impl fmt::Display for StructField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}
