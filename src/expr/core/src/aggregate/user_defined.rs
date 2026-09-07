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

use std::sync::Arc;

use anyhow::Context;
use risingwave_common::array::Op;
use risingwave_common::array::arrow::arrow_array_udf::{Array as _, ArrayRef};
use risingwave_common::array::arrow::arrow_schema_udf::{Field, Fields, Schema, SchemaRef};
use risingwave_common::array::arrow::{UdfArrowConvert, UdfFromArrow, UdfToArrow};
use risingwave_common::bitmap::Bitmap;
use risingwave_pb::expr::PbUserDefinedFunctionMetadata;

use super::*;
use crate::sig::{BuildOptions, UdfImpl, UdfKind};

#[derive(Debug)]
pub struct UserDefinedAggregateFunction {
    arg_schema: SchemaRef,
    return_type: DataType,
    return_field: Field,
    state_field: Field,
    runtime: Box<dyn UdfImpl>,
}

#[async_trait::async_trait]
impl AggregateFunction for UserDefinedAggregateFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    /// Creates an initial state of the aggregate function.
    fn create_state(&self) -> Result<AggregateState> {
        // FIXME(eric): This is bad. Let's make `create_state` async if someday we allow async UDAF
        futures::executor::block_on(async {
            let state = self.runtime.call_agg_create_state().await?;
            Ok(AggregateState::Any(Box::new(State(state))))
        })
    }

    /// Update the state with multiple rows.
    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = &mut state.downcast_mut::<State>().0;
        let ops = input
            .visibility()
            .iter_ones()
            .map(|i| Some(matches!(input.ops()[i], Op::Delete | Op::UpdateDelete)))
            .collect();
        // this will drop invisible rows
        let arrow_input = UdfArrowConvert::default()
            .to_record_batch(self.arg_schema.clone(), input.data_chunk())?;
        let new_state = self
            .runtime
            .call_agg_accumulate_or_retract(state, &ops, &arrow_input)
            .await?;
        *state = new_state;
        Ok(())
    }

    /// Update the state with a range of rows.
    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        // XXX(runji): this may be inefficient
        let vis = input.visibility() & Bitmap::from_range(input.capacity(), range);
        let input = input.clone_with_vis(vis);
        self.update(state, &input).await
    }

    /// Get aggregate result from the state.
    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        let arrow_output = self.runtime.call_agg_finish(state).await?;
        ensure_single_row(&arrow_output, "output")?;
        let output = UdfArrowConvert::default().from_array(&self.return_field, &arrow_output)?;
        // The UDF runtime is external input: a server may drift from the signature it was
        // checked against at creation time. Surface a mistyped result instead of letting it
        // corrupt downstream value encoding.
        if output.data_type() != self.return_type {
            return Err(anyhow::anyhow!(
                "UDF returned a value of type {} while the declared return type is {}",
                output.data_type(),
                self.return_type
            )
            .into());
        }
        Ok(output.datum_at(0))
    }

    /// Encode the state into a datum that can be stored in state table.
    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        ensure_single_row(state, "state")?;
        let state = UdfArrowConvert::default().from_array(&self.state_field, state)?;
        Ok(state.datum_at(0))
    }

    /// Decode the state from a datum in state table.
    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let array = {
            let mut builder = DataType::Bytea.create_array_builder(1);
            builder.append(datum);
            builder.finish()
        };
        let state = UdfArrowConvert::default().to_array(self.state_field.data_type(), &array)?;
        Ok(AggregateState::Any(Box::new(State(state))))
    }
}

/// The runtime is external input: reject an array of the wrong length so that `datum_at(0)` at
/// the call sites is in bounds.
fn ensure_single_row(array: &ArrayRef, what: &str) -> Result<()> {
    if array.len() != 1 {
        return Err(anyhow::anyhow!(
            "UDF aggregate {what} has {} rows, but expected exactly 1",
            array.len()
        )
        .into());
    }
    Ok(())
}

// In arrow-udf, aggregate state is represented as an `ArrayRef`.
// To avoid unnecessary conversion between `ArrayRef` and `Datum`,
// we store `ArrayRef` directly in our `AggregateState`.
#[derive(Debug)]
struct State(ArrayRef);

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}

impl AggStateDyn for State {}

/// Create a new user-defined aggregate function.
pub fn new_user_defined(
    return_type: &DataType,
    udf: &PbUserDefinedFunctionMetadata,
) -> Result<BoxedAggregateFunction> {
    let arg_types = udf.arg_types.iter().map(|t| t.into()).collect::<Vec<_>>();
    let language = udf.language.as_str();
    let runtime = udf.runtime.as_deref();
    let link = udf.link.as_deref();

    let name_in_runtime = udf
        .name_in_runtime()
        .expect("SQL UDF won't get here, other UDFs must have `name_in_runtime`");

    let build_fn = crate::sig::find_udf_impl(language, runtime, link)?.build_fn;
    let runtime = build_fn(BuildOptions {
        kind: UdfKind::Aggregate,
        body: udf.body.as_deref(),
        compressed_binary: udf.compressed_binary.as_deref(),
        link: udf.link.as_deref(),
        name_in_runtime,
        arg_names: &udf.arg_names,
        arg_types: &arg_types,
        return_type,
        always_retry_on_network_error: false,
        language,
        is_async: udf.is_async,
        is_batched: udf.is_batched,
    })
    .context("failed to build UDF runtime")?;

    // legacy UDF runtimes do not support aggregate functions,
    // so we can assume that the runtime is not legacy
    let arrow_convert = UdfArrowConvert::default();
    let arg_schema = Arc::new(Schema::new(
        arg_types
            .iter()
            .map(|t| arrow_convert.to_arrow_field("", t))
            .try_collect::<_, Fields, _>()?,
    ));

    Ok(Box::new(UserDefinedAggregateFunction {
        return_field: arrow_convert.to_arrow_field("", return_type)?,
        state_field: Field::new(
            "state",
            risingwave_common::array::arrow::arrow_schema_udf::DataType::Binary,
            true,
        ),
        return_type: return_type.clone(),
        arg_schema,
        runtime,
    }))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use futures::stream::BoxStream;
    use risingwave_common::array::arrow::arrow_array_udf::{
        BinaryArray, Float64Array, Int32Array, Int64Array, MapArray, RecordBatch, StringArray,
        StructArray,
    };
    use risingwave_common::array::arrow::arrow_buffer_udf::OffsetBuffer;
    use risingwave_common::array::arrow::arrow_schema_udf::DataType as ArrowDataType;
    use risingwave_common::types::{MapType, StructType};

    use super::*;
    use crate::sig::UdfImpl;

    /// Returns the given array from `finish`, regardless of the declared signature.
    #[derive(Debug)]
    struct StubRuntime(ArrayRef);

    #[async_trait::async_trait]
    impl UdfImpl for StubRuntime {
        async fn call(&self, _input: &RecordBatch) -> Result<RecordBatch> {
            unimplemented!()
        }

        async fn call_table_function<'a>(
            &'a self,
            _input: &'a RecordBatch,
        ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
            unimplemented!()
        }

        async fn call_agg_finish(&self, _state: &ArrayRef) -> Result<ArrayRef> {
            Ok(self.0.clone())
        }
    }

    fn build_agg(return_type: DataType, finish_output: ArrayRef) -> UserDefinedAggregateFunction {
        let convert = UdfArrowConvert::default();
        UserDefinedAggregateFunction {
            return_field: convert.to_arrow_field("", &return_type).unwrap(),
            state_field: Field::new("state", ArrowDataType::Binary, true),
            return_type,
            arg_schema: Arc::new(Schema::new(Vec::<Field>::new())),
            runtime: Box::new(StubRuntime(finish_output)),
        }
    }

    /// Drives `get_result` with a runtime that returns `output` and yields the error message.
    async fn get_result_err(return_type: DataType, output: ArrayRef) -> String {
        let agg = build_agg(return_type, output);
        let state = AggregateState::Any(Box::new(State(
            Arc::new(Int64Array::from(vec![0i64])) as ArrayRef
        )));
        let err = agg.get_result(&state).await.unwrap_err();
        format!("{:?}", anyhow::anyhow!(err))
    }

    /// Drives `encode_state` with `state` and yields the error message.
    fn encode_state_err(state: ArrayRef) -> String {
        let agg = build_agg(
            DataType::Int64,
            Arc::new(Int64Array::from(vec![Some(1i64)])),
        );
        let err = agg
            .encode_state(&AggregateState::Any(Box::new(State(state))))
            .unwrap_err();
        format!("{:?}", anyhow::anyhow!(err))
    }

    #[tokio::test]
    async fn misbehaving_runtime_mistyped_finish() {
        // Struct child diverges from the declared `struct<a bigint>`.
        let fields: Fields = vec![Field::new("a", ArrowDataType::Utf8, true)].into();
        let struct_output: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(StringArray::from(vec![Some("oops")])) as ArrayRef],
            None,
        ));
        let msg = get_result_err(
            DataType::Struct(StructType::new(vec![("a", DataType::Int64)])),
            struct_output,
        )
        .await;
        assert!(
            msg.contains("declared return type"),
            "unexpected error: {msg}"
        );

        // Scalar output diverges from the declared `bigint`.
        let msg = get_result_err(DataType::Int64, Arc::new(Int32Array::from(vec![Some(1)]))).await;
        assert!(
            msg.contains("declared return type"),
            "unexpected error: {msg}"
        );

        // Map key type is unrepresentable in RW: must be a graceful error, not a panic
        // from `MapArray::data_type()`.
        let entries_fields: Fields = vec![
            Field::new("key", ArrowDataType::Float64, false),
            Field::new("value", ArrowDataType::Int32, true),
        ]
        .into();
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.5)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(42)])),
            ],
            None,
        );
        let map_output: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(entries_fields),
                false,
            )),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));
        let msg = get_result_err(
            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32)),
            map_output,
        )
        .await;
        assert!(
            msg.contains("invalid map key type"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn misbehaving_runtime_miscounted_finish() {
        let msg = get_result_err(
            DataType::Int64,
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
        )
        .await;
        assert!(msg.contains("0 rows"), "unexpected error: {msg}");

        // More than one row does not panic, but `datum_at(0)` would silently drop the rest.
        let msg = get_result_err(
            DataType::Int64,
            Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])),
        )
        .await;
        assert!(msg.contains("2 rows"), "unexpected error: {msg}");
    }

    #[test]
    fn misbehaving_runtime_miscounted_state() {
        let msg = encode_state_err(Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())));
        assert!(msg.contains("0 rows"), "unexpected error: {msg}");

        let msg = encode_state_err(Arc::new(BinaryArray::from(vec![
            Some(b"a".as_slice()),
            Some(b"b".as_slice()),
        ])));
        assert!(msg.contains("2 rows"), "unexpected error: {msg}");
    }
}
