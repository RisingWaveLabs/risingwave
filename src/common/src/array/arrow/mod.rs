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

// These mods imports arrow_impl.rs to provide FromArrow, ToArrow traits for corresponding arrow versions,
// and the default From/To implementations.

mod arrow_58;
// These mods import mods above and may override some methods.
mod arrow_deltalake;
mod arrow_iceberg;
mod arrow_udf;

pub use arrow_deltalake::DeltaLakeConvert;
pub use arrow_iceberg::{
    ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE, IcebergArrowConvert,
    IcebergCreateTableArrowConvert,
};
pub use arrow_udf::UdfArrowConvert;
pub use reexport::*;
/// For other RisingWave crates, they can directly use arrow re-exported here, without adding
/// `arrow` dependencies in their `Cargo.toml`. And they don't need to care about the version.
mod reexport {
    pub use super::arrow_58::{
        FromArrow as Arrow58FromArrow, ToArrow as Arrow58ToArrow, arrow_array as arrow_array_58,
        arrow_buffer as arrow_buffer_58, arrow_cast as arrow_cast_58,
        arrow_schema as arrow_schema_58,
    };
    pub use super::arrow_deltalake::{
        FromArrow as DeltaLakeFromArrow, ToArrow as DeltaLakeToArrow,
        arrow_array as arrow_array_deltalake, arrow_buffer as arrow_buffer_deltalake,
        arrow_cast as arrow_cast_deltalake, arrow_schema as arrow_schema_deltalake,
    };
    pub use super::arrow_iceberg::{
        FromArrow as IcebergFromArrow, ToArrow as IcebergToArrow,
        arrow_array as arrow_array_iceberg, arrow_buffer as arrow_buffer_iceberg,
        arrow_cast as arrow_cast_iceberg, arrow_schema as arrow_schema_iceberg,
        is_parquet_field_match_source_schema, is_parquet_schema_match_source_schema,
    };
    pub use super::arrow_udf::{
        FromArrow as UdfFromArrow, ToArrow as UdfToArrow, arrow_array as arrow_array_udf,
        arrow_buffer as arrow_buffer_udf, arrow_cast as arrow_cast_udf,
        arrow_schema as arrow_schema_udf,
    };
}
