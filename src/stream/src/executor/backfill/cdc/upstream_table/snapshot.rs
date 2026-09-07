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

use std::future::Future;

use futures::{Stream, pin_mut};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{Scalar, ScalarImpl, Timestamptz};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common_rate_limit::RateLimiter;
use risingwave_connector::source::cdc::external::{
    CdcOffset, ExternalTableReader, ExternalTableReaderImpl, SchemaTableName,
};
use risingwave_pb::plan_common::additional_column::ColumnType;

use super::external::ExternalStorageTable;
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::backfill::utils::{get_new_pos, iter_chunks};
use crate::executor::{StreamExecutorError, StreamExecutorResult};

pub trait UpstreamTableRead {
    fn snapshot_read_full_table(
        &self,
        args: SnapshotReadArgs,
        batch_size: u32,
    ) -> impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + '_;

    fn current_cdc_offset(
        &self,
    ) -> impl Future<Output = StreamExecutorResult<Option<CdcOffset>>> + Send + '_;

    fn estimated_row_count(
        &self,
    ) -> impl Future<Output = StreamExecutorResult<Option<u64>>> + Send + '_;

    async fn disconnect(self) -> StreamExecutorResult<()>;

    fn snapshot_read_table_split(
        &self,
        args: SplitSnapshotReadArgs,
    ) -> impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + '_;
}

#[derive(Debug, Clone)]
pub struct SnapshotReadArgs {
    pub current_pos: Option<OwnedRow>,
    pub rate_limit_rps: Option<u32>,
    pub pk_indices: Vec<usize>,
    pub additional_columns: Vec<ColumnDesc>,
    pub schema_table_name: SchemaTableName,
    pub database_name: String,
}

impl SnapshotReadArgs {
    pub fn new(
        current_pos: Option<OwnedRow>,
        rate_limit_rps: Option<u32>,
        pk_indices: Vec<usize>,
        additional_columns: Vec<ColumnDesc>,
        schema_table_name: SchemaTableName,
        database_name: String,
    ) -> Self {
        Self {
            current_pos,
            rate_limit_rps,
            pk_indices,
            additional_columns,
            schema_table_name,
            database_name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SplitSnapshotReadArgs {
    pub left_bound_inclusive: OwnedRow,
    pub right_bound_exclusive: OwnedRow,
    pub split_columns: Vec<Field>,
    pub rate_limit_rps: Option<u32>,
    pub additional_columns: Vec<ColumnDesc>,
    pub schema_table_name: SchemaTableName,
    pub database_name: String,
}

impl SplitSnapshotReadArgs {
    pub fn new(
        left_bound_inclusive: OwnedRow,
        right_bound_exclusive: OwnedRow,
        split_columns: Vec<Field>,
        rate_limit_rps: Option<u32>,
        additional_columns: Vec<ColumnDesc>,
        schema_table_name: SchemaTableName,
        database_name: String,
    ) -> Self {
        Self {
            left_bound_inclusive,
            right_bound_exclusive,
            split_columns,
            rate_limit_rps,
            additional_columns,
            schema_table_name,
            database_name,
        }
    }
}

/// A wrapper of upstream table for snapshot read
/// because we need to customize the snapshot read for managed upstream table (e.g. mv, index)
/// and external upstream table.
pub struct UpstreamTableReader<T> {
    table: T,
    pub(crate) reader: ExternalTableReaderImpl,
}

impl<T> UpstreamTableReader<T> {
    pub fn new(table: T, reader: ExternalTableReaderImpl) -> Self {
        Self { table, reader }
    }
}

/// Append additional columns with value as null to the snapshot chunk
fn with_additional_columns(
    snapshot_chunk: StreamChunk,
    additional_columns: &[ColumnDesc],
    schema_table_name: SchemaTableName,
    database_name: String,
) -> StreamChunk {
    let (ops, mut columns, visibility) = snapshot_chunk.into_inner();
    for desc in additional_columns {
        let mut builder = desc.data_type.create_array_builder(visibility.len());
        match *desc.additional_column.column_type.as_ref().unwrap() {
            // set default value for timestamp
            ColumnType::Timestamp(_) => builder.append_n(
                visibility.len(),
                Some(Timestamptz::default().to_scalar_value()),
            ),
            ColumnType::DatabaseName(_) => {
                builder.append_n(
                    visibility.len(),
                    Some(ScalarImpl::from(database_name.clone())),
                );
            }
            ColumnType::SchemaName(_) => {
                builder.append_n(
                    visibility.len(),
                    Some(ScalarImpl::from(schema_table_name.schema_name.clone())),
                );
            }
            ColumnType::TableName(_) => {
                builder.append_n(
                    visibility.len(),
                    Some(ScalarImpl::from(schema_table_name.table_name.clone())),
                );
            }
            // set null for other additional columns
            _ => {
                builder.append_n_null(visibility.len());
            }
        }
        columns.push(builder.finish().into());
    }
    StreamChunk::with_visibility(ops, columns, visibility)
}

impl UpstreamTableRead for UpstreamTableReader<ExternalStorageTable> {
    async fn estimated_row_count(&self) -> StreamExecutorResult<Option<u64>> {
        Ok(self
            .reader
            .estimated_row_count(&self.table.schema_table_name())
            .await?)
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read_full_table(&self, args: SnapshotReadArgs, batch_size: u32) {
        let primary_keys = self
            .table
            .pk_indices()
            .iter()
            .map(|idx| {
                let f = &self.table.schema().fields[*idx];
                f.name.clone()
            })
            .collect_vec();

        // prepare rate limiter
        if args.rate_limit_rps == Some(0) {
            // If limit is 0, we should not read any data from the upstream table.
            // Keep waiting util the stream is rebuilt.
            let future = futures::future::pending::<()>();
            future.await;
            unreachable!();
        }

        let rate_limiter = RateLimiter::new(
            args.rate_limit_rps
                .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
                .into(),
        );

        let mut read_args = args;
        let schema_table_name = read_args.schema_table_name.clone();
        let database_name = read_args.database_name.clone();
        // loop to read all data from the table
        loop {
            tracing::debug!(
                "snapshot_read primary keys: {:?}, current_pos: {:?}",
                primary_keys,
                read_args.current_pos
            );

            let mut read_count: usize = 0;
            let row_stream = self.reader.snapshot_read(
                self.table.schema_table_name(),
                read_args.current_pos.clone(),
                primary_keys.clone(),
                batch_size,
            );

            pin_mut!(row_stream);
            let mut builder = DataChunkBuilder::new(
                self.table.schema().data_types(),
                limited_chunk_size(read_args.rate_limit_rps),
            );
            let chunk_stream = iter_chunks(row_stream, &mut builder);
            let mut current_pk_pos = read_args.current_pos.clone().unwrap_or_default();

            #[for_await]
            for chunk in chunk_stream {
                let chunk = chunk?;
                let chunk_size = chunk.capacity();
                read_count += chunk.cardinality();
                current_pk_pos = get_new_pos(&chunk, &read_args.pk_indices);

                if let Some(rate_limit_rps) = read_args.rate_limit_rps
                    && chunk_size != 0
                {
                    // Apply rate limit, see `risingwave_stream::executor::source::apply_rate_limit` for more.
                    // May be should be refactored to a common function later.
                    let limit = rate_limit_rps as usize;

                    // Because we produce chunks with limited-sized data chunk builder and all rows
                    // are `Insert`s, the chunk size should never exceed the limit.
                    assert!(chunk_size <= limit);

                    // `InsufficientCapacity` should never happen because we have check the cardinality
                    rate_limiter.wait(chunk_size as _).await;
                    yield Some(with_additional_columns(
                        chunk,
                        &read_args.additional_columns,
                        schema_table_name.clone(),
                        database_name.clone(),
                    ));
                } else {
                    // no limit, or empty chunk
                    yield Some(with_additional_columns(
                        chunk,
                        &read_args.additional_columns,
                        schema_table_name.clone(),
                        database_name.clone(),
                    ));
                    continue;
                }
            }

            // check read_count if the snapshot batch is finished
            if read_count < batch_size as _ {
                tracing::debug!("finished loading of full table snapshot");
                yield None;
                unreachable!()
            } else {
                // update PK position and continue to read the table
                read_args.current_pos = Some(current_pk_pos);
            }
        }
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read_table_split(&self, args: SplitSnapshotReadArgs) {
        // prepare rate limiter
        if args.rate_limit_rps == Some(0) {
            // If limit is 0, we should not read any data from the upstream table.
            // Keep waiting util the stream is rebuilt.
            let future = futures::future::pending::<()>();
            future.await;
            unreachable!();
        }

        let rate_limiter = RateLimiter::new(
            args.rate_limit_rps
                .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
                .into(),
        );

        let read_args = args;
        let schema_table_name = read_args.schema_table_name.clone();
        let database_name = read_args.database_name.clone();
        // tracing::debug!(?args, "snapshot_read",);

        let row_stream = self.reader.split_snapshot_read(
            self.table.schema_table_name(),
            read_args.left_bound_inclusive.clone(),
            read_args.right_bound_exclusive.clone(),
            read_args.split_columns.clone(),
        );

        pin_mut!(row_stream);
        let mut builder = DataChunkBuilder::new(
            self.table.schema().data_types(),
            limited_chunk_size(read_args.rate_limit_rps),
        );
        let chunk_stream = iter_chunks(row_stream, &mut builder);

        #[for_await]
        for chunk in chunk_stream {
            let chunk = chunk?;
            let chunk_size = chunk.capacity();

            if let Some(rate_limit_rps) = read_args.rate_limit_rps
                && chunk_size != 0
            {
                // Apply rate limit, see `risingwave_stream::executor::source::apply_rate_limit` for more.
                // May be should be refactored to a common function later.
                let limit = rate_limit_rps as usize;

                // Because we produce chunks with limited-sized data chunk builder and all rows
                // are `Insert`s, the chunk size should never exceed the limit.
                assert!(chunk_size <= limit);

                // `InsufficientCapacity` should never happen because we have check the cardinality
                rate_limiter.wait(chunk_size as _).await;
                yield Some(with_additional_columns(
                    chunk,
                    &read_args.additional_columns,
                    schema_table_name.clone(),
                    database_name.clone(),
                ));
            } else {
                // no limit, or empty chunk
                yield Some(with_additional_columns(
                    chunk,
                    &read_args.additional_columns,
                    schema_table_name.clone(),
                    database_name.clone(),
                ));
            }
        }
        yield None;
    }

    async fn current_cdc_offset(&self) -> StreamExecutorResult<Option<CdcOffset>> {
        let binlog = self.reader.current_cdc_offset();
        let binlog = binlog.await?;
        Ok(Some(binlog))
    }

    async fn disconnect(self) -> StreamExecutorResult<()> {
        self.reader.disconnect().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_connector::source::cdc::external::mysql::MySqlExternalTableReader;
    use risingwave_connector::source::cdc::external::{
        ExternalTableConfig, ExternalTableReader, SchemaTableName,
    };

    use crate::executor::backfill::utils::{get_new_pos, iter_chunks};

    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let columns = [
            ColumnDesc::named("o_orderkey", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("o_custkey", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("o_orderstatus", ColumnId::new(3), DataType::Varchar),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };
        let props: HashMap<String, String> = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mydb",
                "table.name" => "orders_rw"));

        let config =
            serde_json::from_value::<ExternalTableConfig>(serde_json::to_value(props).unwrap())
                .unwrap();
        let reader = MySqlExternalTableReader::new(config, rw_schema.clone(), vec![0])
            .await
            .unwrap();

        let mut cnt: usize = 0;
        let mut start_pk = Some(OwnedRow::new(vec![Some(ScalarImpl::Int64(0))]));
        loop {
            let row_stream = reader.snapshot_read(
                SchemaTableName {
                    schema_name: "mydb".to_owned(),
                    table_name: "orders_rw".to_owned(),
                },
                start_pk.clone(),
                vec!["o_orderkey".to_owned()],
                1000,
            );
            let mut builder = DataChunkBuilder::new(rw_schema.clone().data_types(), 256);
            let chunk_stream = iter_chunks(row_stream, &mut builder);
            let pk_indices = vec![0];
            pin_mut!(chunk_stream);
            #[for_await]
            for chunk in chunk_stream {
                let chunk = chunk.expect("data");
                start_pk = Some(get_new_pos(&chunk, &pk_indices));
                cnt += chunk.capacity();
                // println!("chunk: {:#?}", chunk);
                println!("cnt: {}", cnt);
            }
            if cnt >= 1499900 {
                println!("bye!");
                break;
            }
        }
    }
}
