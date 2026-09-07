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

//! UDF implementation interface.
//!
//! To support a new language or runtime for UDF, implement the interface in this module.
//!
//! See expr/impl/src/udf for the implementations.

use anyhow::{Context, Result, bail};
use educe::Educe;
use enum_as_inner::EnumAsInner;
use futures::stream::BoxStream;
use risingwave_common::array::arrow::arrow_array_udf::{ArrayRef, BooleanArray, RecordBatch};
use risingwave_common::types::DataType;

/// The global registry of UDF implementations.
///
/// To register a new UDF implementation:
///
/// ```ignore
/// #[linkme::distributed_slice(UDF_IMPLS)]
/// static MY_UDF_LANGUAGE: UdfImplDescriptor = UdfImplDescriptor {...};
/// ```
#[linkme::distributed_slice]
pub static UDF_IMPLS: [UdfImplDescriptor];

/// Find a UDF implementation by language.
pub fn find_udf_impl(
    language: &str,
    runtime: Option<&str>,
    link: Option<&str>,
) -> Result<&'static UdfImplDescriptor> {
    let mut impls = UDF_IMPLS
        .iter()
        .filter(|desc| (desc.match_fn)(language, runtime, link));
    let impl_ = impls.next().context(
        "language not found.\nHINT: UDF feature flag may not be enabled during compilation",
    )?;
    if impls.next().is_some() {
        bail!("multiple UDF implementations found for language: {language}");
    }
    Ok(impl_)
}

/// UDF implementation descriptor.
///
/// Every UDF implementation describes how it is selected, created, and built, together with the
/// capabilities supported by its execution path.
pub struct UdfImplDescriptor {
    /// Returns if a function matches the implementation.
    ///
    /// This function is used to determine which implementation to use for a UDF.
    pub match_fn: fn(language: &str, runtime: Option<&str>, link: Option<&str>) -> bool,

    /// Creates a function from options.
    ///
    /// This function will be called when `create function` statement is executed on the frontend.
    pub create_fn: fn(opts: CreateOptions<'_>) -> Result<CreateFunctionOutput>,

    /// Builds UDF runtime from verified options.
    ///
    /// This function will be called before the UDF is executed on the backend.
    pub build_fn: fn(opts: BuildOptions<'_>) -> Result<Box<dyn UdfImpl>>,

    /// Returns whether this implementation and function kind honor
    /// `always_retry_on_network_error` during execution.
    pub supports_always_retry_on_network_error: fn(kind: UdfKind) -> bool,
}

/// Options for creating a function.
///
/// These information are parsed from `CREATE FUNCTION` statement.
/// Implementations should verify the options and return a `CreateFunctionOutput` in `create_fn`.
pub struct CreateOptions<'a> {
    pub kind: UdfKind,
    /// The function name registered in RisingWave.
    pub name: &'a str,
    pub arg_names: &'a [String],
    pub arg_types: &'a [DataType],
    pub return_type: &'a DataType,
    /// The function name on the remote side / in the source code, currently only used for external UDF.
    pub as_: Option<&'a str>,
    pub using_link: Option<&'a str>,
    pub using_base64_decoded: Option<&'a [u8]>,
}

/// Output of creating a function.
pub struct CreateFunctionOutput {
    /// The name for identifying the function in the UDF runtime.
    pub name_in_runtime: String,
    pub body: Option<String>,
    pub compressed_binary: Option<Vec<u8>>,
}

/// Options for building a UDF runtime.
#[derive(Educe)]
#[educe(Debug)]
pub struct BuildOptions<'a> {
    pub kind: UdfKind,
    pub body: Option<&'a str>,
    #[educe(Debug(ignore))]
    pub compressed_binary: Option<&'a [u8]>,
    pub link: Option<&'a str>,
    pub name_in_runtime: &'a str,
    pub arg_names: &'a [String],
    pub arg_types: &'a [DataType],
    pub return_type: &'a DataType,
    pub always_retry_on_network_error: bool,
    pub language: &'a str,
    pub is_async: Option<bool>,
    pub is_batched: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumAsInner)]
pub enum UdfKind {
    Scalar,
    Table,
    Aggregate,
}

/// UDF implementation.
#[async_trait::async_trait]
pub trait UdfImpl: std::fmt::Debug + Send + Sync {
    /// Call the scalar function.
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch>;

    /// Call the table function.
    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>>;

    /// For aggregate function, create the initial state.
    async fn call_agg_create_state(&self) -> Result<ArrayRef> {
        bail!("aggregate function is not supported");
    }

    /// For aggregate function, accumulate or retract the state.
    async fn call_agg_accumulate_or_retract(
        &self,
        _state: &ArrayRef,
        _ops: &BooleanArray,
        _input: &RecordBatch,
    ) -> Result<ArrayRef> {
        bail!("aggregate function is not supported");
    }

    /// For aggregate function, get aggregate result from the state.
    async fn call_agg_finish(&self, _state: &ArrayRef) -> Result<ArrayRef> {
        bail!("aggregate function is not supported");
    }

    /// Whether the UDF talks in legacy mode.
    ///
    /// If true, decimal and jsonb types are mapped to Arrow `LargeBinary` and `LargeUtf8` types.
    /// Otherwise, they are mapped to Arrow extension types.
    /// See <https://github.com/risingwavelabs/arrow-udf/tree/main#extension-types>.
    fn is_legacy(&self) -> bool {
        false
    }

    /// Return the memory size consumed by UDF runtime in bytes.
    ///
    /// If not available, return 0.
    fn memory_usage(&self) -> usize {
        0
    }
}
