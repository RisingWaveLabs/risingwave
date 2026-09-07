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

use postgres_types::Type as PgType;

use super::DataType;

/// `DataType` information extracted from PostgreSQL `pg_type`
///
/// ```sql
/// select oid, typarray, typname, typinput, typlen from pg_type
/// where oid in (16, 21, 23, 20, 1700, 700, 701, 1043, 17, 1082, 1114, 1184, 1083, 1186, 3802);
/// ```
///
/// See also:
/// * <https://www.postgresql.org/docs/15/catalog-pg-type.html>
/// * <https://github.com/postgres/postgres/blob/REL_15_4/src/include/catalog/pg_type.dat>
#[macro_export]
macro_rules! for_all_base_types {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            $($x, )*
            { Boolean     |   16 |     1000 | bool        | boolin         |      1 }
            { Bytea       |   17 |     1001 | bytea       | byteain        |     -1 }
            { Int64       |   20 |     1016 | int8        | int8in         |      8 }
            { Int16       |   21 |     1005 | int2        | int2in         |      2 }
            { Int32       |   23 |     1007 | int4        | int4in         |      4 }
            { Float32     |  700 |     1021 | float4      | float4in       |      4 }
            { Float64     |  701 |     1022 | float8      | float8in       |      8 }
            { Varchar     | 1043 |     1015 | varchar     | varcharin      |     -1 }
            { Date        | 1082 |     1182 | date        | date_in        |      4 }
            { Time        | 1083 |     1183 | time        | time_in        |      8 }
            { Timestamp   | 1114 |     1115 | timestamp   | timestamp_in   |      8 }
            { Timestamptz | 1184 |     1185 | timestamptz | timestamptz_in |      8 }
            { Interval    | 1186 |     1187 | interval    | interval_in    |     16 }
            { Decimal     | 1700 |     1231 | numeric     | numeric_in     |     -1 }
            { Jsonb       | 3802 |     3807 | jsonb       | jsonb_in       |     -1 }
            { Variant     | 1307 |     1308 | variant     | variant_in     |     -1 }
        }
    };
}

#[derive(Debug, thiserror::Error)]
#[error("Unsupported oid {0}")]
pub struct UnsupportedOid(i32);

/// Get type information compatible with Postgres type, such as oid, type length.
impl DataType {
    /// For a fixed-size type, typlen is the number of bytes in the internal representation of the type.
    /// But for a variable-length type, typlen is negative.
    /// -1 indicates a “varlena” type (one that has a length word),
    /// -2 indicates a null-terminated C string.
    ///
    /// <https://www.postgresql.org/docs/15/catalog-pg-type.html#:~:text=of%20the%20type-,typlen,-int2>
    pub fn type_len(&self) -> i16 {
        macro_rules! impl_type_len {
            ($( { $enum:ident | $oid:literal | $oid_array:literal | $name:ident | $input:ident | $len:literal } )*) => {
                match self {
                    $(
                    DataType::$enum => $len,
                    )*
                    DataType::Serial => 8,
                    DataType::Int256 => -1,
                    DataType::Vector(_) => -1,
                    DataType::List(_) | DataType::Struct(_) | DataType::Map(_) => -1,
                }
            }
        }
        for_all_base_types! { impl_type_len }
    }

    // NOTE:
    // Refer https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat when add new TypeOid.
    // Be careful to distinguish oid from array_type_oid.
    // Such as:
    //  https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L347
    //  For Numeric(aka Decimal): oid = 1700, array_type_oid = 1231
    pub fn from_oid(oid: i32) -> Result<Self, UnsupportedOid> {
        macro_rules! impl_from_oid {
            ($( { $enum:ident | $oid:literal | $oid_array:literal | $name:ident | $input:ident | $len:literal } )*) => {
                match oid {
                    $(
                    $oid => Ok(DataType::$enum),
                    )*
                    $(
                    $oid_array => Ok(DataType::list(DataType::$enum)),
                    )*
                    // workaround to support text in extended mode.
                    25 => Ok(DataType::Varchar),
                    1009 => Ok(DataType::Varchar.list()),
                    _ => Err(UnsupportedOid(oid)),
                }
            }
        }
        for_all_base_types! { impl_from_oid }
    }

    /// Refer to [`Self::from_oid`]
    pub fn to_oid(&self) -> i32 {
        macro_rules! impl_to_oid {
            ($( { $enum:ident | $oid:literal | $oid_array:literal | $name:ident | $input:ident | $len:literal } )*) => {
                match self {
                    $(
                    DataType::$enum => $oid,
                    )*
                    DataType::List(list) => match list.elem().unnest_list() {
                        $(
                            DataType::$enum => $oid_array,
                        )*
                        DataType::Int256 => 1302,
                        DataType::Serial => 1016,
                        DataType::Struct(_) => 2287, // pseudo-type of array[struct] (see `pg_type.dat`)
                        DataType::List { .. } => unreachable!("Never reach here!"),
                        DataType::Map(_) => 1304,
                        DataType::Vector(_) => 1306,
                    }
                    DataType::Serial => 20,
                    // XXX: what does the oid mean here? Why we don't have from_oid for them?
                    DataType::Int256 => 1301,
                    DataType::Map(_) => 1303,
                    // TODO: Support to give a new oid for custom struct type. #9434
                    DataType::Struct(_) => 2249,  // pseudo-type of struct (see `pg_type.dat`)
                    DataType::Vector(_) => 1305,
                }
            }
        }
        for_all_base_types! { impl_to_oid }
    }

    pub fn pg_name(&self) -> &'static str {
        macro_rules! impl_pg_name {
            ($( { $enum:ident | $oid:literal | $oid_array:literal | $name:ident | $input:ident | $len:literal } )*) => {
                match self {
                    $(
                    DataType::$enum => stringify!($name),
                    )*
                    DataType::Struct(_) => "struct",
                    DataType::List(_) => "list",
                    DataType::Serial => "serial",
                    DataType::Int256 => "rw_int256",
                    DataType::Map(_) => "map",
                    DataType::Vector(_) => "vector",
                }
            }
        }
        for_all_base_types! { impl_pg_name }
    }

    pub fn to_pg_type(&self) -> PgType {
        let oid = self.to_oid();
        PgType::from_oid(oid as u32).unwrap()
    }
}
