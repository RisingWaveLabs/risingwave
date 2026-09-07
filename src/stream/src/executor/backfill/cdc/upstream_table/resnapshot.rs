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

// The reader is introduced before planner/executor plumbing so it can be reviewed independently.
#![allow(dead_code)]

use futures::{Stream, pin_mut};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::ColumnId;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::row;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;

use super::diff::{SnapshotReadOutput, diff_ordered_row_streams};
use super::external::ExternalStorageTable;
use super::snapshot::{SnapshotReadArgs, UpstreamTableReader, snapshot_rate_limiter};
use crate::executor::backfill::utils::compute_bounds;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

pub struct ResnapshotDiffRead<S: StateStore> {
    table: BatchTable<S>,
    read_epoch: u64,
}

impl<S: StateStore> ResnapshotDiffRead<S> {
    pub fn new(
        state_store: S,
        table_desc: &StorageTableDesc,
        output_column_ids: &[ColumnId],
        read_epoch: u64,
    ) -> Self {
        let vnodes = Some(Bitmap::ones(table_desc.vnode_count()).into());
        Self {
            table: BatchTable::new_partial(
                state_store,
                output_column_ids.to_vec(),
                vnodes,
                table_desc,
            ),
            read_epoch,
        }
    }

    pub fn snapshot_read_full_table_diff<'a>(
        &'a self,
        upstream_table_reader: &'a UpstreamTableReader<ExternalStorageTable>,
        read_args: SnapshotReadArgs,
        batch_size: u32,
        pk_order: Vec<OrderType>,
        compare_indices: Vec<usize>,
    ) -> StreamExecutorResult<
        impl Stream<Item = StreamExecutorResult<SnapshotReadOutput>> + Send + 'a,
    > {
        let pk_indices = read_args.pk_indices.clone();
        let pk_needs_unsigned_i64_compare =
            upstream_table_reader.pk_column_unsigned_i64_compare_flags()?;
        let rate_limiter = snapshot_rate_limiter(read_args.rate_limit_rps);
        let left = snapshot_read_rows(
            upstream_table_reader.snapshot_read_full_table_with_rate_limiter(
                read_args.clone(),
                batch_size,
                rate_limiter.clone(),
            ),
        );
        let right = self.storage_table_read(read_args);
        Ok(diff_ordered_row_streams(
            left,
            right,
            pk_indices,
            pk_order,
            pk_needs_unsigned_i64_compare,
            compare_indices,
            rate_limiter,
        ))
    }

    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    async fn storage_table_read(&self, read_args: SnapshotReadArgs) {
        let table = &self.table;
        let range_bounds = match compute_bounds(table.pk_indices(), read_args.current_pos) {
            Some(range_bounds) => range_bounds,
            None => return Ok(()),
        };
        let row_iter = table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::Committed(self.read_epoch),
                row::empty(),
                range_bounds,
                true,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;

        #[for_await]
        for row in row_iter {
            yield row?;
        }
    }
}

#[try_stream(ok = OwnedRow, error = StreamExecutorError)]
async fn snapshot_read_rows(
    input: impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send,
) {
    pin_mut!(input);
    #[for_await]
    for output in input {
        match output? {
            Some(chunk) => {
                for (_, row) in chunk.rows() {
                    yield row.to_owned_row();
                }
            }
            None => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_pb::plan_common::StorageTableDesc;
    use risingwave_storage::memory::MemoryStateStore;

    use super::ResnapshotDiffRead;

    #[test]
    fn resnapshot_projects_only_cdc_scan_output_columns() {
        let columns = [
            ColumnDesc::named("id", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("payload", ColumnId::new(2), DataType::Varchar),
            ColumnDesc::named("generated", ColumnId::new(3), DataType::Boolean),
        ];
        let table_desc = StorageTableDesc {
            table_id: 1.into(),
            columns: columns.iter().map(ColumnDesc::to_protobuf).collect(),
            pk: vec![ColumnOrder::new(0, OrderType::ascending()).to_protobuf()],
            dist_key_in_pk_indices: vec![],
            value_indices: vec![0, 1, 2],
            read_prefix_len_hint: 0,
            versioned: false,
            stream_key: vec![0],
            vnode_col_idx_in_pk: None,
            retention_seconds: None,
            maybe_vnode_count: Some(1),
        };

        let read = ResnapshotDiffRead::new(
            MemoryStateStore::new(),
            &table_desc,
            &[ColumnId::new(1), ColumnId::new(2)],
            0,
        );
        assert_eq!(
            read.table.schema().data_types(),
            vec![DataType::Int32, DataType::Varchar]
        );
    }
}
