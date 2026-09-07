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

use std::marker::PhantomData;
use std::mem::swap;

use anyhow::anyhow;
use futures::pin_mut;
use itertools::Itertools;
use risingwave_batch::task::ShutdownToken;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, VnodeCountCompat};
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::TableIter;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::{StateStore, dispatch_state_store};

use super::AsOfDesc;
use crate::error::Result;
use crate::executor::join::JoinType;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, BufferChunkExecutor, Executor,
    ExecutorBuilder, LookupExecutorBuilder, LookupJoinBase,
};

/// Distributed Lookup Join Executor.
/// High level Execution flow:
/// Repeat 1-3:
///   1. Read N rows from outer side input and send keys to inner side builder after deduplication.
///   2. Inner side input lookups inner side table with keys and builds hash map.
///   3. Outer side rows join each inner side rows by probing the hash map.
///
/// Distributed lookup join already scheduled to its inner side corresponding compute node, so that
/// it can just lookup the compute node locally without sending RPCs to other compute nodes.
pub struct DistributedLookupJoinExecutor<K, S: StateStore> {
    base: LookupJoinBase<K, InnerSideExecutorBuilder<S>>,
}

impl<K: HashKey, S: StateStore> Executor for DistributedLookupJoinExecutor<K, S> {
    fn schema(&self) -> &Schema {
        &self.base.schema
    }

    fn identity(&self) -> &str {
        &self.base.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        Box::new(self.base).do_execute()
    }
}

impl<K, S: StateStore> DistributedLookupJoinExecutor<K, S> {
    fn new(base: LookupJoinBase<K, InnerSideExecutorBuilder<S>>) -> Self {
        Self { base }
    }
}

pub struct DistributedLookupJoinExecutorBuilder {}

impl BoxedExecutorBuilder for DistributedLookupJoinExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [outer_side_input]: [_; 1] = inputs.try_into().unwrap();

        let distributed_lookup_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::DistributedLookupJoin
        )?;

        let join_type = JoinType::from_prost(distributed_lookup_join_node.get_join_type()?);
        let condition = match distributed_lookup_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let output_indices: Vec<usize> = distributed_lookup_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let outer_side_data_types = outer_side_input.schema().data_types();

        let table_desc = distributed_lookup_join_node.get_inner_side_table_desc()?;
        let inner_side_column_ids = distributed_lookup_join_node
            .get_inner_side_column_ids()
            .clone();

        let inner_side_schema = Schema {
            fields: inner_side_column_ids
                .iter()
                .map(|&id| {
                    let column = table_desc
                        .columns
                        .iter()
                        .find(|c| c.column_id == id)
                        .unwrap();
                    Field::from(&ColumnDesc::from(column))
                })
                .collect_vec(),
        };

        let fields = if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
            outer_side_input.schema().fields.clone()
        } else {
            [
                outer_side_input.schema().fields.clone(),
                inner_side_schema.fields.clone(),
            ]
            .concat()
        };

        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();

        let mut outer_side_key_idxs = vec![];
        for outer_side_key in distributed_lookup_join_node.get_outer_side_key() {
            outer_side_key_idxs.push(*outer_side_key as usize)
        }

        let outer_side_key_types: Vec<DataType> = outer_side_key_idxs
            .iter()
            .map(|&i| outer_side_data_types[i].clone())
            .collect_vec();

        let lookup_prefix_len: usize =
            distributed_lookup_join_node.get_lookup_prefix_len() as usize;

        let mut inner_side_key_idxs = vec![];
        for inner_side_key in distributed_lookup_join_node.get_inner_side_key() {
            inner_side_key_idxs.push(*inner_side_key as usize)
        }

        let inner_side_key_types = inner_side_key_idxs
            .iter()
            .map(|&i| inner_side_schema.fields[i].data_type.clone())
            .collect_vec();

        let null_safe = distributed_lookup_join_node.get_null_safe().clone();

        let chunk_size = source.context().get_config().developer.chunk_size;

        let asof_desc = distributed_lookup_join_node
            .asof_desc
            .map(|desc| AsOfDesc::from_protobuf(&desc))
            .transpose()?;

        let column_ids = inner_side_column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();

        // Use a full vnode bitmap so that the lookup can be performed on any worker.
        // For a lookup table with hash distribution, the lookup keys always contain the
        // distribution key; for a lookup table with singleton distribution, all lookups
        // are gathered into a single task. In both cases the lookup is correct regardless
        // of which worker the task is scheduled to.
        let vnodes = Some(Bitmap::ones(table_desc.vnode_count()).into());

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = BatchTable::new_partial(state_store, column_ids, vnodes, table_desc);
            let inner_side_builder = InnerSideExecutorBuilder::new(
                outer_side_key_types,
                inner_side_key_types.clone(),
                lookup_prefix_len,
                distributed_lookup_join_node
                    .query_epoch
                    .ok_or_else(|| anyhow!("query_epoch not set in distributed lookup join"))?,
                vec![],
                table,
                chunk_size,
            );

            let identity = source.plan_node().get_identity().clone();

            Ok(DistributedLookupJoinExecutorArgs {
                join_type,
                condition,
                outer_side_input,
                outer_side_data_types,
                outer_side_key_idxs,
                inner_side_builder,
                inner_side_key_types,
                inner_side_key_idxs,
                null_safe,
                lookup_prefix_len,
                chunk_builder: DataChunkBuilder::new(original_schema.data_types(), chunk_size),
                schema: actual_schema,
                output_indices,
                chunk_size,
                asof_desc,
                identity: identity.clone(),
                shutdown_rx: source.shutdown_rx().clone(),
                mem_ctx: source.context().create_executor_mem_context(&identity),
            }
            .dispatch())
        })
    }
}

struct DistributedLookupJoinExecutorArgs<S: StateStore> {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    outer_side_input: BoxedExecutor,
    outer_side_data_types: Vec<DataType>,
    outer_side_key_idxs: Vec<usize>,
    inner_side_builder: InnerSideExecutorBuilder<S>,
    inner_side_key_types: Vec<DataType>,
    inner_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    lookup_prefix_len: usize,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    chunk_size: usize,
    asof_desc: Option<AsOfDesc>,
    identity: String,
    shutdown_rx: ShutdownToken,
    mem_ctx: MemoryContext,
}

impl<S: StateStore> HashKeyDispatcher for DistributedLookupJoinExecutorArgs<S> {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(DistributedLookupJoinExecutor::<K, S>::new(LookupJoinBase {
            join_type: self.join_type,
            condition: self.condition,
            outer_side_input: self.outer_side_input,
            outer_side_data_types: self.outer_side_data_types,
            outer_side_key_idxs: self.outer_side_key_idxs,
            inner_side_builder: self.inner_side_builder,
            inner_side_key_types: self.inner_side_key_types,
            inner_side_key_idxs: self.inner_side_key_idxs,
            null_safe: self.null_safe,
            lookup_prefix_len: self.lookup_prefix_len,
            chunk_builder: self.chunk_builder,
            schema: self.schema,
            output_indices: self.output_indices,
            chunk_size: self.chunk_size,
            asof_desc: self.asof_desc,
            identity: self.identity,
            shutdown_rx: self.shutdown_rx,
            mem_ctx: self.mem_ctx,
            _phantom: PhantomData,
        }))
    }

    fn data_types(&self) -> &[DataType] {
        &self.inner_side_key_types
    }
}

/// Inner side executor builder for the `DistributedLookupJoinExecutor`
struct InnerSideExecutorBuilder<S: StateStore> {
    outer_side_key_types: Vec<DataType>,
    inner_side_key_types: Vec<DataType>,
    lookup_prefix_len: usize,
    epoch: BatchQueryEpoch,
    row_list: Vec<OwnedRow>,
    table: BatchTable<S>,
    chunk_size: usize,
}

impl<S: StateStore> InnerSideExecutorBuilder<S> {
    fn new(
        outer_side_key_types: Vec<DataType>,
        inner_side_key_types: Vec<DataType>,
        lookup_prefix_len: usize,
        epoch: BatchQueryEpoch,
        row_list: Vec<OwnedRow>,
        table: BatchTable<S>,
        chunk_size: usize,
    ) -> Self {
        Self {
            outer_side_key_types,
            inner_side_key_types,
            lookup_prefix_len,
            epoch,
            row_list,
            table,
            chunk_size,
        }
    }
}

impl<S: StateStore> LookupExecutorBuilder for InnerSideExecutorBuilder<S> {
    fn reset(&mut self) {
        // PASS
    }

    /// Fetch row from inner side table by the scan range added.
    async fn add_scan_range(&mut self, key_datums: Vec<Datum>) -> Result<()> {
        let mut scan_range = ScanRange::full_table_scan();

        for ((datum, outer_type), inner_type) in key_datums
            .into_iter()
            .zip_eq_fast(
                self.outer_side_key_types
                    .iter()
                    .take(self.lookup_prefix_len),
            )
            .zip_eq_fast(
                self.inner_side_key_types
                    .iter()
                    .take(self.lookup_prefix_len),
            )
        {
            let datum = if inner_type == outer_type {
                datum
            } else {
                bail!("Join key types are not aligned: LHS: {outer_type:?}, RHS: {inner_type:?}");
            };

            scan_range.eq_conds.push(datum);
        }

        let pk_prefix = OwnedRow::new(scan_range.eq_conds);

        if self.lookup_prefix_len == self.table.pk_indices().len() {
            let row = self.table.get_row(&pk_prefix, self.epoch.into()).await?;

            if let Some(row) = row {
                self.row_list.push(row);
            }
        } else {
            let iter = self
                .table
                .batch_iter_with_pk_bounds(
                    self.epoch.into(),
                    &pk_prefix,
                    ..,
                    false,
                    PrefetchOptions::default(),
                )
                .await?;

            pin_mut!(iter);
            while let Some(row) = iter.next_row().await? {
                self.row_list.push(row);
            }
        }

        Ok(())
    }

    /// Build a `BufferChunkExecutor` to return all its rows fetched by `add_scan_range` before.
    async fn build_executor(&mut self) -> Result<BoxedExecutor> {
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.table.schema().data_types(), self.chunk_size);
        let mut chunk_list = Vec::new();

        let mut new_row_list = vec![];
        swap(&mut new_row_list, &mut self.row_list);

        for row in new_row_list {
            if let Some(chunk) = data_chunk_builder.append_one_row(row) {
                chunk_list.push(chunk);
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all() {
            chunk_list.push(chunk);
        }

        Ok(Box::new(BufferChunkExecutor::new(
            self.table.schema().clone(),
            chunk_list,
        )))
    }
}
