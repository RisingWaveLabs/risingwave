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

//! Arrow conversion for `LanceDB` sink.
//!
//! `LanceDB` uses arrow-58, same as the `arrow_58` module.
//! This module re-exports the arrow-58 types and provides a `LanceDbConvert` struct
//! with a `to_record_batch` method (same pattern as `DeltaLakeConvert`).

pub use super::arrow_58::{
    FromArrow, ToArrow, arrow_array, arrow_buffer, arrow_cast, arrow_schema,
};
use crate::array::{ArrayError, DataChunk};
use crate::catalog::Schema;

pub struct LanceDbConvert;

impl LanceDbConvert {
    /// Convert a RisingWave `DataChunk` to an Arrow `RecordBatch`.
    pub fn to_record_batch(
        &self,
        schema: arrow_schema::SchemaRef,
        chunk: &DataChunk,
    ) -> Result<arrow_array::RecordBatch, ArrayError> {
        ToArrow::to_record_batch(self, schema, chunk)
    }

    /// Convert a RisingWave `Schema` to an Arrow `Schema`.
    pub fn rw_schema_to_arrow_schema(
        &self,
        rw_schema: &Schema,
    ) -> Result<arrow_schema::Schema, ArrayError> {
        let fields = rw_schema
            .fields()
            .iter()
            .map(|f| self.to_arrow_field(&f.name, &f.data_type))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(arrow_schema::Schema::new(fields))
    }
}

/// Use the default `ToArrow` implementation (no overrides needed for `LanceDB` initially).
/// `DeltaLake` overrides `decimal_to_arrow` because of special Inf/NaN handling.
/// `LanceDB` can use the default behavior. If custom type mapping is needed later
/// (e.g., `Vector` -> `FixedSizeList`), add overrides here.
impl ToArrow for LanceDbConvert {}
impl FromArrow for LanceDbConvert {}
