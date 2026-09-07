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

pub use risingwave_batch::executor::*;

pub mod aggregation;
mod azblob_file_scan;
mod delete;
mod expand;
mod filter;
mod gcs_file_scan;
mod generic_exchange;
mod get_channel_delta_stats;
mod group_top_n;
mod hash_agg;
mod hop_window;
mod iceberg_metadata_scan;
mod iceberg_scan;
mod insert;
mod join;
mod limit;
mod log_row_seq_scan;
mod max_one_row;
mod merge_sort;
mod merge_sort_exchange;
mod mssql_query;
mod mysql_query;
mod order_by;
mod postgres_query;
mod project;
mod project_set;
mod row_seq_scan;
mod s3_file_scan;
mod sort_agg;
mod sort_over_window;
mod source;
mod sys_row_seq_scan;
mod table_function;
pub mod test_utils;
mod top_n;
mod union;
mod update;
mod utils;
mod values;
mod vector_index_nearest;

use azblob_file_scan::AzblobFileScanExecutorBuilder;
pub use delete::*;
pub use expand::*;
pub use filter::*;
use gcs_file_scan::GcsFileScanExecutorBuilder;
pub use generic_exchange::*;
use get_channel_delta_stats::GetChannelDeltaStatsExecutor;
pub use group_top_n::*;
pub use hash_agg::*;
pub use hop_window::*;
pub use iceberg_metadata_scan::*;
pub use iceberg_scan::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
use log_row_seq_scan::LogStoreRowSeqScanExecutorBuilder;
pub use max_one_row::*;
pub use merge_sort::*;
pub use merge_sort_exchange::*;
pub use mssql_query::*;
pub use mysql_query::*;
pub use order_by::*;
pub use postgres_query::*;
pub use project::*;
pub use project_set::*;
pub use row_seq_scan::*;
use s3_file_scan::FileScanExecutorBuilder;
pub use sort_agg::*;
pub use sort_over_window::SortOverWindowExecutor;
pub use source::*;
use sys_row_seq_scan::SysRowSeqScanExecutorBuilder;
pub use table_function::*;
pub use top_n::TopNExecutor;
pub use union::*;
pub use update::*;
pub use utils::*;
pub use values::*;
use vector_index_nearest::VectorIndexNearestExecutorBuilder;

register_executor!(RowSeqScan, RowSeqScanExecutorBuilder);
register_executor!(Insert, InsertExecutor);
register_executor!(Delete, DeleteExecutor);
register_executor!(Exchange, GenericExchangeExecutorBuilder);
register_executor!(Update, UpdateExecutor);
register_executor!(Filter, FilterExecutor);
register_executor!(Project, ProjectExecutor);
register_executor!(SortAgg, SortAggExecutor);
register_executor!(Sort, SortExecutor);
register_executor!(TopN, TopNExecutor);
register_executor!(GroupTopN, GroupTopNExecutorBuilder);
register_executor!(Limit, LimitExecutor);
register_executor!(Values, ValuesExecutor);
register_executor!(NestedLoopJoin, NestedLoopJoinExecutor);
register_executor!(HashJoin, HashJoinExecutor<()>);
register_executor!(HashAgg, HashAggExecutorBuilder);
register_executor!(MergeSortExchange, MergeSortExchangeExecutorBuilder);
register_executor!(TableFunction, TableFunctionExecutorBuilder);
register_executor!(HopWindow, HopWindowExecutor);
register_executor!(SysRowSeqScan, SysRowSeqScanExecutorBuilder);
register_executor!(Expand, ExpandExecutor);
register_executor!(LocalLookupJoin, LocalLookupJoinExecutorBuilder);
register_executor!(DistributedLookupJoin, DistributedLookupJoinExecutorBuilder);
register_executor!(ProjectSet, ProjectSetExecutor);
register_executor!(Union, UnionExecutor);
register_executor!(Source, SourceExecutor);
register_executor!(SortOverWindow, SortOverWindowExecutor);
register_executor!(MaxOneRow, MaxOneRowExecutor);
register_executor!(FileScan, FileScanExecutorBuilder);
register_executor!(GcsFileScan, GcsFileScanExecutorBuilder);
register_executor!(AzblobFileScan, AzblobFileScanExecutorBuilder);
register_executor!(IcebergScan, IcebergScanExecutorBuilder);
register_executor!(IcebergMetadataScan, IcebergMetadataScanExecutorBuilder);
register_executor!(PostgresQuery, PostgresQueryExecutorBuilder);
register_executor!(MysqlQuery, MySqlQueryExecutorBuilder);
register_executor!(MssqlQuery, MssqlQueryExecutorBuilder);
register_executor!(LogRowSeqScan, LogStoreRowSeqScanExecutorBuilder);
register_executor!(GetChannelDeltaStats, GetChannelDeltaStatsExecutor);
register_executor!(VectorIndexNearest, VectorIndexNearestExecutorBuilder);
