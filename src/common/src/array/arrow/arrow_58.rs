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

#[allow(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;
type ArrowIntervalType = arrow_buffer::IntervalMonthDayNano;
pub use arrow_58_array as arrow_array;
pub use arrow_58_buffer as arrow_buffer;
pub use arrow_58_cast as arrow_cast;
pub use arrow_58_schema as arrow_schema;
pub use arrow_impl::{
    FromArrow, ToArrow, is_parquet_field_match_source_schema, is_parquet_schema_match_source_schema,
};
