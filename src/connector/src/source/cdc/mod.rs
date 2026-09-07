// Copyright 2022 RisingWave Labs
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

pub mod enumerator;
pub mod external;
pub mod jni_source;
pub mod source;
pub mod split;

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

pub use enumerator::*;
use itertools::Itertools;
use risingwave_common::id::{ActorId, SourceId};
use risingwave_pb::catalog::PbSource;
use risingwave_pb::connector_service::{PbSourceType, PbTableSchema, SourceType, TableSchema};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::source::{PbCdcTableSnapshotSplit, PbCdcTableSnapshotSplitsWithGeneration};
use risingwave_pb::stream_plan::StreamCdcScanOptions;
use simd_json::prelude::ArrayTrait;
pub use source::*;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::{CdcTableSnapshotSplitRaw, SourceProperties, SplitImpl, TryFromBTreeMap};
use crate::{for_all_classified_sources, impl_cdc_source_type};

pub const CDC_CONNECTOR_NAME_SUFFIX: &str = "-cdc";
pub const CDC_SNAPSHOT_MODE_KEY: &str = "debezium.snapshot.mode";
pub const CDC_SNAPSHOT_BACKFILL: &str = "rw_cdc_backfill";
pub const CDC_SHARING_MODE_KEY: &str = "rw.sharing.mode.enable";
// User can set snapshot='false' to disable cdc backfill
pub const CDC_BACKFILL_ENABLE_KEY: &str = "snapshot";
pub const CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY: &str = "snapshot.interval";
pub const CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY: &str = "snapshot.batch_size";
pub const CDC_BACKFILL_PARALLELISM: &str = "backfill.parallelism";
pub const CDC_BACKFILL_NUM_ROWS_PER_SPLIT: &str = "backfill.num_rows_per_split";
pub const CDC_BACKFILL_AS_EVEN_SPLITS: &str = "backfill.as_even_splits";
pub const CDC_BACKFILL_SPLIT_PK_COLUMN_INDEX: &str = "backfill.split_pk_column_index";
pub const CDC_BACKFILL_SPLIT_COLUMN_NAME: &str = "backfill.split_column_name";
// We enable transaction for shared cdc source by default
pub const CDC_TRANSACTIONAL_KEY: &str = "transactional";
pub const CDC_WAIT_FOR_STREAMING_START_TIMEOUT: &str = "cdc.source.wait.streaming.start.timeout";
pub const CDC_BACKFILL_MAX_PARALLELISM: u32 = 256;

// User can set strong-schema='true' to enable strong schema for mongo cdc source
pub const CDC_MONGODB_STRONG_SCHEMA_KEY: &str = "strong_schema";

pub const MYSQL_CDC_CONNECTOR: &str = Mysql::CDC_CONNECTOR_NAME;
pub const POSTGRES_CDC_CONNECTOR: &str = Postgres::CDC_CONNECTOR_NAME;
pub const CITUS_CDC_CONNECTOR: &str = Citus::CDC_CONNECTOR_NAME;
pub const MONGODB_CDC_CONNECTOR: &str = Mongodb::CDC_CONNECTOR_NAME;
pub const SQL_SERVER_CDC_CONNECTOR: &str = SqlServer::CDC_CONNECTOR_NAME;

/// Build a unique CDC table identifier from a source ID and external table name
pub fn build_cdc_table_id(source_id: SourceId, external_table_name: &str) -> String {
    format!("{}.{}", source_id, external_table_name)
}

pub fn normalize_simple_postgres_quoted_table_name(table_name: &str) -> Option<String> {
    let (schema_name, table_name) = table_name.split_once('.')?;
    let table_name = table_name.strip_prefix('"')?.strip_suffix('"')?;
    if schema_name.is_empty()
        || table_name.is_empty()
        || table_name.contains('.')
        || table_name.contains('"')
        || table_name.contains('\\')
        || schema_name.contains('"')
    {
        return None;
    }

    Some(format!("{schema_name}.{table_name}"))
}

pub trait CdcSourceTypeTrait: Send + Sync + Clone + std::fmt::Debug + 'static {
    const CDC_CONNECTOR_NAME: &'static str;
    fn source_type() -> CdcSourceType;
}

for_all_classified_sources!(impl_cdc_source_type);

impl<'a> From<&'a str> for CdcSourceType {
    fn from(name: &'a str) -> Self {
        match name {
            MYSQL_CDC_CONNECTOR => CdcSourceType::Mysql,
            POSTGRES_CDC_CONNECTOR => CdcSourceType::Postgres,
            CITUS_CDC_CONNECTOR => CdcSourceType::Citus,
            MONGODB_CDC_CONNECTOR => CdcSourceType::Mongodb,
            SQL_SERVER_CDC_CONNECTOR => CdcSourceType::SqlServer,
            _ => CdcSourceType::Unspecified,
        }
    }
}

impl CdcSourceType {
    pub fn as_str_name(&self) -> &str {
        match self {
            CdcSourceType::Mysql => "MySQL",
            CdcSourceType::Postgres => "Postgres",
            CdcSourceType::Citus => "Citus",
            CdcSourceType::Mongodb => "MongoDB",
            CdcSourceType::SqlServer => "SQL Server",
            CdcSourceType::Unspecified => "Unspecified",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CdcProperties<T: CdcSourceTypeTrait> {
    /// Properties specified in the WITH clause by user
    pub properties: BTreeMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: TableSchema,

    /// Whether it is created by a cdc source job
    pub is_cdc_source_job: bool,

    /// For validation purpose, mark if the table is a backfill cdc table
    pub is_backfill_table: bool,

    pub _phantom: PhantomData<T>,
}

pub fn table_schema_exclude_additional_columns(table_schema: &TableSchema) -> TableSchema {
    TableSchema {
        columns: table_schema
            .columns
            .iter()
            .filter(|col| {
                col.additional_column
                    .as_ref()
                    .is_some_and(|val| val.column_type.is_none())
            })
            .cloned()
            .collect(),
        pk_indices: table_schema.pk_indices.clone(),
    }
}

impl<T: CdcSourceTypeTrait> TryFromBTreeMap for CdcProperties<T> {
    fn try_from_btreemap(
        properties: BTreeMap<String, String>,
        _deny_unknown_fields: bool,
    ) -> ConnectorResult<Self> {
        let is_share_source: bool = properties
            .get(CDC_SHARING_MODE_KEY)
            .is_some_and(|v| v == "true");
        Ok(CdcProperties {
            properties,
            table_schema: Default::default(),
            // TODO(siyuan): use serde to deserialize input hashmap
            is_cdc_source_job: is_share_source,
            is_backfill_table: false,
            _phantom: PhantomData,
        })
    }
}

impl<T: CdcSourceTypeTrait> EnforceSecret for CdcProperties<T> {} // todo: enforce jdbc like properties

impl<T: CdcSourceTypeTrait> SourceProperties for CdcProperties<T>
where
    DebeziumCdcSplit<T>: TryFrom<SplitImpl, Error = crate::error::ConnectorError> + Into<SplitImpl>,
    DebeziumSplitEnumerator<T>: ListCdcSplits<CdcSourceType = T> + enumerator::CdcMonitor,
{
    type Split = DebeziumCdcSplit<T>;
    type SplitEnumerator = DebeziumSplitEnumerator<T>;
    type SplitReader = CdcSplitReader<T>;

    const SOURCE_NAME: &'static str = T::CDC_CONNECTOR_NAME;

    fn init_from_pb_source(&mut self, source: &PbSource) {
        let pk_indices = source
            .pk_column_ids
            .iter()
            .map(|&id| {
                source
                    .columns
                    .iter()
                    .position(|col| col.column_desc.as_ref().unwrap().column_id == id)
                    .unwrap() as u32
            })
            .collect_vec();

        let table_schema = PbTableSchema {
            columns: source
                .columns
                .iter()
                .flat_map(|col| &col.column_desc)
                .filter(|col| {
                    !matches!(
                        col.generated_or_default_column,
                        Some(GeneratedOrDefaultColumn::GeneratedColumn(_))
                    )
                })
                .cloned()
                .collect(),
            pk_indices,
        };
        self.table_schema = table_schema;
        if let Some(info) = source.info.as_ref() {
            self.is_cdc_source_job = info.is_shared();
        }
    }

    fn init_from_pb_cdc_table_desc(&mut self, table_desc: &ExternalTableDesc) {
        let table_schema = TableSchema {
            columns: table_desc
                .columns
                .iter()
                .filter(|col| {
                    !matches!(
                        col.generated_or_default_column,
                        Some(GeneratedOrDefaultColumn::GeneratedColumn(_))
                    )
                })
                .cloned()
                .collect(),
            pk_indices: table_desc.stream_key.clone(),
        };

        self.table_schema = table_schema;
        self.is_cdc_source_job = false;
        self.is_backfill_table = true;
    }
}

impl<T: CdcSourceTypeTrait> crate::source::UnknownFields for CdcProperties<T> {
    fn unknown_fields(&self) -> HashMap<String, String> {
        // FIXME: CDC does not handle unknown fields yet
        HashMap::new()
    }
}

impl<T: CdcSourceTypeTrait> CdcProperties<T> {
    pub fn get_source_type_pb(&self) -> SourceType {
        SourceType::from(T::source_type())
    }
}

pub const INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID: u64 = 0;
pub const INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID: u64 = 1;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CdcTableSnapshotSplitAssignmentWithGeneration {
    pub splits: HashMap<ActorId, (Vec<CdcTableSnapshotSplitRaw>, u64)>,
}

impl CdcTableSnapshotSplitAssignmentWithGeneration {
    pub fn new(splits: HashMap<ActorId, (Vec<CdcTableSnapshotSplitRaw>, u64)>) -> Self {
        Self { splits }
    }

    pub fn empty() -> Self {
        Self {
            splits: HashMap::default(),
        }
    }
}

pub fn build_cdc_table_snapshot_split(s: &CdcTableSnapshotSplitRaw) -> PbCdcTableSnapshotSplit {
    PbCdcTableSnapshotSplit {
        split_id: s.split_id,
        left_bound_inclusive: s.left_bound_inclusive.clone(),
        right_bound_exclusive: s.right_bound_exclusive.clone(),
    }
}

pub fn build_actor_cdc_table_snapshot_splits_with_generation(
    pb_cdc_table_snapshot_split_assignment: PbCdcTableSnapshotSplitsWithGeneration,
) -> CdcTableSnapshotSplitAssignmentWithGeneration {
    let splits = pb_cdc_table_snapshot_split_assignment
        .splits
        .into_iter()
        .map(|(actor_id, splits)| {
            let generation = splits.generation;
            let splits = splits
                .splits
                .into_iter()
                .map(|s| CdcTableSnapshotSplitRaw {
                    split_id: s.split_id,
                    left_bound_inclusive: s.left_bound_inclusive,
                    right_bound_exclusive: s.right_bound_exclusive,
                })
                .collect();
            (actor_id, (splits, generation))
        })
        .collect();
    CdcTableSnapshotSplitAssignmentWithGeneration { splits }
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct CdcScanOptions {
    /// Used by Used in non-parallel backfill, i.e. backfill V1.
    pub disable_backfill: bool,
    /// Used by non-parallelized backfill. The frequency of snapshot read resets for consuming the WAL backlog.
    pub snapshot_barrier_interval: u32,
    /// Used by non-parallelized backfill. The number of rows to fetch in a single batch when reading from an external table.
    pub snapshot_batch_size: u32,
    /// Used by parallelized backfill, i.e. backfill V2. The initial parallelism of parallel backfill.
    pub backfill_parallelism: u32,
    /// Used by parallelized backfill. The estimated number of rows per split used in splits generation.
    pub backfill_num_rows_per_split: u64,
    /// Used by parallelized backfill. For supported split column data type, assume an uniform distribution and adopt a much faster splits generation method.
    pub backfill_as_even_splits: bool,
    /// Used by parallelized backfill. Specify the index of primary key column to use as split column.
    pub backfill_split_pk_column_index: u32,
    /// If set, it will override `backfill_split_pk_column_index`.
    pub backfill_split_column_name: Option<String>,
}

impl Default for CdcScanOptions {
    fn default() -> Self {
        Self {
            disable_backfill: false,
            snapshot_barrier_interval: 10,
            snapshot_batch_size: 1000,
            // 0 means disable backfill v2.
            backfill_parallelism: 0,
            backfill_num_rows_per_split: 100_000,
            backfill_as_even_splits: true,
            backfill_split_pk_column_index: 0,
            backfill_split_column_name: None,
        }
    }
}

impl CdcScanOptions {
    pub fn to_proto(&self) -> StreamCdcScanOptions {
        StreamCdcScanOptions {
            disable_backfill: self.disable_backfill,
            snapshot_barrier_interval: self.snapshot_barrier_interval,
            snapshot_batch_size: self.snapshot_batch_size,
            backfill_parallelism: self.backfill_parallelism,
            backfill_num_rows_per_split: self.backfill_num_rows_per_split,
            backfill_as_even_splits: self.backfill_as_even_splits,
            backfill_split_pk_column_index: self.backfill_split_pk_column_index,
            backfill_split_column_name: self.backfill_split_column_name.clone(),
        }
    }

    pub fn from_proto(proto: &StreamCdcScanOptions) -> Self {
        Self {
            disable_backfill: proto.disable_backfill,
            snapshot_barrier_interval: proto.snapshot_barrier_interval,
            snapshot_batch_size: proto.snapshot_batch_size,
            backfill_parallelism: proto.backfill_parallelism,
            backfill_num_rows_per_split: proto.backfill_num_rows_per_split,
            backfill_as_even_splits: proto.backfill_as_even_splits,
            backfill_split_pk_column_index: proto.backfill_split_pk_column_index,
            backfill_split_column_name: proto.backfill_split_column_name.clone(),
        }
    }

    pub fn is_parallelized_backfill(&self) -> bool {
        !self.disable_backfill
            && self.backfill_num_rows_per_split > 0
            && self.backfill_parallelism > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_simple_postgres_quoted_table_name() {
        assert_eq!(
            normalize_simple_postgres_quoted_table_name(r#"public."TableName""#).as_deref(),
            Some("public.TableName")
        );
        assert_eq!(
            normalize_simple_postgres_quoted_table_name(r#"public."pg_my_table""#).as_deref(),
            Some("public.pg_my_table")
        );
        assert_eq!(
            normalize_simple_postgres_quoted_table_name("public.table"),
            None
        );
        assert_eq!(
            normalize_simple_postgres_quoted_table_name(r#""Mixed.Schema"."TableName""#),
            None
        );
        assert_eq!(
            normalize_simple_postgres_quoted_table_name(r#"public."table.name""#),
            None
        );
        assert_eq!(
            normalize_simple_postgres_quoted_table_name(r#"public."table\name""#),
            None
        );
    }
}
