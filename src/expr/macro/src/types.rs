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

//! This module provides utility functions for SQL data type conversion and manipulation.

//  name        data type   array type          owned type      ref type            primitive
const TYPE_MATRIX: &str = "
    boolean     Boolean     BoolArray           bool            bool                _
    int2        Int16       I16Array            i16             i16                 y
    int4        Int32       I32Array            i32             i32                 y
    int8        Int64       I64Array            i64             i64                 y
    int256      Int256      Int256Array         Int256          Int256Ref<'_>       _
    float4      Float32     F32Array            F32             F32                 y
    float8      Float64     F64Array            F64             F64                 y
    decimal     Decimal     DecimalArray        Decimal         Decimal             y
    serial      Serial      SerialArray         Serial          Serial              y
    date        Date        DateArray           Date            Date                y
    time        Time        TimeArray           Time            Time                y
    timestamp   Timestamp   TimestampArray      Timestamp       Timestamp           y
    timestamptz Timestamptz TimestamptzArray    Timestamptz     Timestamptz         y
    interval    Interval    IntervalArray       Interval        Interval            y
    varchar     Varchar     Utf8Array           Box<str>        &str                _
    bytea       Bytea       BytesArray          Box<[u8]>       &[u8]               _
    jsonb       Jsonb       JsonbArray          JsonbVal        JsonbRef<'_>        _
    variant     Variant     VariantArray        VariantVal      VariantRef<'_>      _
    vector      Vector      VectorArray         VectorVal       VectorRef<'_>       _
    anyarray    List        ListArray           ListValue       ListRef<'_>         _
    struct      Struct      StructArray         StructValue     StructRef<'_>       _
    anymap      Map         MapArray            MapValue        MapRef<'_>          _
    any         ???         ArrayImpl           ScalarImpl      ScalarRefImpl<'_>   _
";

/// Maps a data type to its corresponding data type name.
pub fn data_type(ty: &str) -> &str {
    lookup_matrix(ty, 1)
}

/// Maps a data type to its corresponding array type name.
pub fn array_type(ty: &str) -> &str {
    lookup_matrix(ty, 2)
}

/// Maps a data type to its corresponding `Scalar` type name.
pub fn owned_type(ty: &str) -> &str {
    lookup_matrix(ty, 3)
}

/// Maps a data type to its corresponding `ScalarRef` type name.
pub fn ref_type(ty: &str) -> &str {
    lookup_matrix(ty, 4)
}

/// Checks if a data type is primitive.
pub fn is_primitive(ty: &str) -> bool {
    lookup_matrix(ty, 5) == "y"
}

fn lookup_matrix(mut ty: &str, idx: usize) -> &str {
    if ty.ends_with("[]") {
        ty = "anyarray";
    } else if ty.starts_with("struct") {
        ty = "struct";
    } else if ty == "void" {
        // XXX: we don't support void type yet.
        //      replace it with int for now.
        ty = "int4";
    }
    let s = TYPE_MATRIX.trim().lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(ty) {
            Some(parts.nth(idx - 1).unwrap())
        } else {
            None
        }
    });
    s.unwrap_or_else(|| panic!("failed to lookup type matrix: unknown type: {}", ty))
}

/// Expands a type wildcard string into a list of concrete types.
pub fn expand_type_wildcard(ty: &str) -> Vec<&str> {
    match ty {
        "*" => TYPE_MATRIX
            .trim()
            .lines()
            .map(|l| l.split_whitespace().next().unwrap())
            .filter(|l| *l != "any")
            .collect(),
        "*int" => vec!["int2", "int4", "int8"],
        "*float" => vec!["float4", "float8"],
        _ => vec![ty],
    }
}

/// Computes the minimal compatible type between a pair of data types.
///
/// This function is used to determine the `auto` type.
pub fn min_compatible_type(types: &[impl AsRef<str>]) -> &str {
    if types.len() == 1 {
        return types[0].as_ref();
    }
    assert_eq!(types.len(), 2);
    match (types[0].as_ref(), types[1].as_ref()) {
        (a, b) if a == b => a,

        ("int2", "int2") => "int2",
        ("int2", "int4") => "int4",
        ("int2", "int8") => "int8",

        ("int4", "int2") => "int4",
        ("int4", "int4") => "int4",
        ("int4", "int8") => "int8",

        ("int8", "int2") => "int8",
        ("int8", "int4") => "int8",
        ("int8", "int8") => "int8",

        ("int2", "int256") => "int256",
        ("int4", "int256") => "int256",
        ("int8", "int256") => "int256",
        ("int256", "int2") => "int256",
        ("int256", "int4") => "int256",
        ("int256", "int8") => "int256",
        ("int256", "float8") => "float8",
        ("float8", "int256") => "float8",

        ("float4", "float4") => "float4",
        ("float4", "float8") => "float8",

        ("float8", "float4") => "float8",
        ("float8", "float8") => "float8",

        ("decimal", "decimal") => "decimal",

        ("date", "timestamp") => "timestamp",
        ("timestamp", "date") => "timestamp",
        ("time", "interval") => "interval",
        ("interval", "time") => "interval",

        (a, b) => panic!("unknown minimal compatible type for {a:?} and {b:?}"),
    }
}
