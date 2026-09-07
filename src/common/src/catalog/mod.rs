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

pub mod cdc_type_compatibility;
mod column;
mod external_table;
mod internal_table;
mod physical_table;
mod schema;
pub mod test_utils;

use std::fmt::Binary;
use std::sync::Arc;

pub use column::*;
pub use external_table::*;
use futures::stream::BoxStream;
pub use internal_table::*;
use parse_display::Display;
pub use physical_table::*;
use risingwave_pb::catalog::table::PbEngine;
use risingwave_pb::catalog::{
    CreateType as PbCreateType, HandleConflictBehavior as PbHandleConflictBehavior,
    StreamJobStatus as PbStreamJobStatus,
};
use risingwave_pb::plan_common::ColumnDescVersion;
pub use schema::{Field, FieldDisplay, FieldLike, Schema, test_utils as schema_test_utils};

use crate::array::DataChunk;
pub use crate::constants::hummock;
use crate::error::BoxedError;
pub use crate::id::*;

/// The global version of the catalog.
pub type CatalogVersion = u64;

/// The version number of the per-table catalog.
pub type TableVersionId = u64;
/// The default version ID for a new table.
pub const INITIAL_TABLE_VERSION_ID: u64 = 0;
/// The version number of the per-source catalog.
pub type SourceVersionId = u64;
/// The default version ID for a new source.
pub const INITIAL_SOURCE_VERSION_ID: u64 = 0;

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
pub const PG_CATALOG_SCHEMA_NAME: &str = "pg_catalog";
pub const INFORMATION_SCHEMA_SCHEMA_NAME: &str = "information_schema";
pub const RW_CATALOG_SCHEMA_NAME: &str = "rw_catalog";
pub const RESERVED_PG_SCHEMA_PREFIX: &str = "pg_";
pub const DEFAULT_SUPER_USER: &str = "root";
pub const DEFAULT_SUPER_USER_ID: UserId = UserId::new(1);
// This is for compatibility with customized utils for PostgreSQL.
pub const DEFAULT_SUPER_USER_FOR_PG: &str = "postgres";
pub const DEFAULT_SUPER_USER_FOR_PG_ID: u32 = 2;

// This is the default superuser for admin, which is used only for cloud control plane.
pub const DEFAULT_SUPER_USER_FOR_ADMIN: &str = "rwadmin";
pub const DEFAULT_SUPER_USER_FOR_ADMIN_ID: UserId = UserId::new(3);

pub const NON_RESERVED_USER_ID: UserId = UserId::new(11);

pub const MAX_SYS_CATALOG_NUM: i32 = 5000;
pub const SYS_CATALOG_START_ID: i32 = i32::MAX - MAX_SYS_CATALOG_NUM;

pub use risingwave_pb::id::OBJECT_ID_PLACEHOLDER;

pub const SYSTEM_SCHEMAS: [&str; 3] = [
    PG_CATALOG_SCHEMA_NAME,
    INFORMATION_SCHEMA_SCHEMA_NAME,
    RW_CATALOG_SCHEMA_NAME,
];
pub fn is_system_schema(schema_name: &str) -> bool {
    SYSTEM_SCHEMAS.contains(&schema_name)
}

pub fn is_reserved_admin_user(user_name: &str) -> bool {
    user_name == DEFAULT_SUPER_USER_FOR_ADMIN
}

pub const RW_RESERVED_COLUMN_NAME_PREFIX: &str = "_rw_";

/// When there is no primary key specified while creating source, will use
/// the message key as primary key in `BYTEA` type with this name.
/// Note: the field has version to track, please refer to [`default_key_column_name_version_mapping`]
pub const DEFAULT_KEY_COLUMN_NAME: &str = "_rw_key";

pub fn default_key_column_name_version_mapping(version: &ColumnDescVersion) -> &str {
    match version {
        ColumnDescVersion::Unspecified => DEFAULT_KEY_COLUMN_NAME,
        _ => DEFAULT_KEY_COLUMN_NAME,
    }
}

/// For kafka source, we attach a hidden column [`KAFKA_TIMESTAMP_COLUMN_NAME`] to it, so that we
/// can limit the timestamp range when querying it directly with batch query. The column type is
/// [`crate::types::DataType::Timestamptz`]. For more details, please refer to
/// [this rfc](https://github.com/risingwavelabs/rfcs/pull/20).
pub const KAFKA_TIMESTAMP_COLUMN_NAME: &str = "_rw_kafka_timestamp";

/// RisingWave iceberg table engine will create the column `_risingwave_iceberg_row_id` in the iceberg table.
///
/// Iceberg V3 spec use `_row_id` as a reserved column name for row lineage, so if the table without primary key,
/// we can't use `_row_id` directly for iceberg, so use `_risingwave_iceberg_row_id` instead.
pub const RISINGWAVE_ICEBERG_ROW_ID: &str = "_risingwave_iceberg_row_id";

pub const ROW_ID_COLUMN_NAME: &str = "_row_id";
/// The column ID preserved for the row ID column.
pub const ROW_ID_COLUMN_ID: ColumnId = ColumnId::new(0);

/// The column ID offset for user-defined columns.
///
/// All IDs of user-defined columns must be greater or equal to this value.
pub const USER_COLUMN_ID_OFFSET: i32 = ROW_ID_COLUMN_ID.next().get_id();

pub const RW_TIMESTAMP_COLUMN_NAME: &str = "_rw_timestamp";
pub const RW_TIMESTAMP_COLUMN_ID: ColumnId = ColumnId::new(-1);

/// The column name for the projected row ID in `ProjectSet`.
/// This is a hidden column used to track row indices when expanding set-returning functions.
pub const PROJECTED_ROW_ID_COLUMN_NAME: &str = "_rw_projected_row_id";

pub const ICEBERG_SEQUENCE_NUM_COLUMN_NAME: &str = "_iceberg_sequence_number";
pub const ICEBERG_FILE_PATH_COLUMN_NAME: &str = "_iceberg_file_path";
pub const ICEBERG_FILE_POS_COLUMN_NAME: &str = "_iceberg_file_pos";

pub const CDC_OFFSET_COLUMN_NAME: &str = "_rw_offset";
/// The number of columns output by the cdc source job
/// see [`ColumnCatalog::debezium_cdc_source_cols()`] for details
pub const CDC_SOURCE_COLUMN_NUM: u32 = 3;
pub const CDC_TABLE_NAME_COLUMN_NAME: &str = "_rw_table_name";

pub const ICEBERG_SOURCE_PREFIX: &str = "__iceberg_source_";
pub const ICEBERG_SINK_PREFIX: &str = "__iceberg_sink_";

/// Iceberg snapshot summary property containing the highest RisingWave epoch committed by the
/// sink. Append-only Iceberg engine batch reads use it to exclude committed rows from the sink
/// log store.
pub const RISINGWAVE_ICEBERG_COMMIT_EPOCH: &str = "risingwave.commit.epoch";

/// The local system catalog reader in the frontend node.
pub trait SysCatalogReader: Sync + Send + 'static {
    /// Reads the data of the system catalog table.
    fn read_table(&self, table_id: TableId) -> BoxStream<'_, Result<DataChunk, BoxedError>>;
}

pub type SysCatalogReaderRef = Arc<dyn SysCatalogReader>;

#[derive(Clone, Debug, PartialEq, Default, Copy)]
pub struct TableOption {
    pub retention_seconds: Option<u32>, // second
}

impl From<&risingwave_pb::hummock::TableOption> for TableOption {
    fn from(table_option: &risingwave_pb::hummock::TableOption) -> Self {
        Self {
            retention_seconds: table_option.retention_seconds,
        }
    }
}

impl From<&TableOption> for risingwave_pb::hummock::TableOption {
    fn from(table_option: &TableOption) -> Self {
        Self {
            retention_seconds: table_option.retention_seconds,
        }
    }
}

impl TableOption {
    pub fn new(retention_seconds: Option<u32>) -> Self {
        // now we only support ttl for TableOption
        TableOption { retention_seconds }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConflictBehavior {
    #[default]
    NoCheck,
    Overwrite,
    IgnoreConflict,
    DoUpdateIfNotNull,
}

#[macro_export]
macro_rules! _checked_conflict_behaviors {
    () => {
        ConflictBehavior::Overwrite
            | ConflictBehavior::IgnoreConflict
            | ConflictBehavior::DoUpdateIfNotNull
    };
}
pub use _checked_conflict_behaviors as checked_conflict_behaviors;

impl ConflictBehavior {
    pub fn from_protobuf(tb_conflict_behavior: &PbHandleConflictBehavior) -> Self {
        match tb_conflict_behavior {
            PbHandleConflictBehavior::Overwrite => ConflictBehavior::Overwrite,
            PbHandleConflictBehavior::Ignore => ConflictBehavior::IgnoreConflict,
            PbHandleConflictBehavior::DoUpdateIfNotNull => ConflictBehavior::DoUpdateIfNotNull,
            // This is for backward compatibility, in the previous version
            // `HandleConflictBehavior::Unspecified` represented `NoCheck`, so just treat it as `NoCheck`.
            PbHandleConflictBehavior::NoCheck | PbHandleConflictBehavior::Unspecified => {
                ConflictBehavior::NoCheck
            }
        }
    }

    pub fn to_protobuf(self) -> PbHandleConflictBehavior {
        match self {
            ConflictBehavior::NoCheck => PbHandleConflictBehavior::NoCheck,
            ConflictBehavior::Overwrite => PbHandleConflictBehavior::Overwrite,
            ConflictBehavior::IgnoreConflict => PbHandleConflictBehavior::Ignore,
            ConflictBehavior::DoUpdateIfNotNull => PbHandleConflictBehavior::DoUpdateIfNotNull,
        }
    }

    pub fn debug_to_string(self) -> String {
        match self {
            ConflictBehavior::NoCheck => "NoCheck".to_owned(),
            ConflictBehavior::Overwrite => "Overwrite".to_owned(),
            ConflictBehavior::IgnoreConflict => "IgnoreConflict".to_owned(),
            ConflictBehavior::DoUpdateIfNotNull => "DoUpdateIfNotNull".to_owned(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Engine {
    #[default]
    Hummock,
    Iceberg,
}

impl Engine {
    pub fn from_protobuf(engine: &PbEngine) -> Self {
        match engine {
            PbEngine::Hummock | PbEngine::Unspecified => Engine::Hummock,
            PbEngine::Iceberg => Engine::Iceberg,
        }
    }

    pub fn to_protobuf(self) -> PbEngine {
        match self {
            Engine::Hummock => PbEngine::Hummock,
            Engine::Iceberg => PbEngine::Iceberg,
        }
    }

    pub fn debug_to_string(self) -> String {
        match self {
            Engine::Hummock => "Hummock".to_owned(),
            Engine::Iceberg => "Iceberg".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Display, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub enum StreamJobStatus {
    #[default]
    Creating,
    Created,
}

impl StreamJobStatus {
    pub fn from_proto(stream_job_status: PbStreamJobStatus) -> Self {
        match stream_job_status {
            PbStreamJobStatus::Creating => StreamJobStatus::Creating,
            PbStreamJobStatus::Created | PbStreamJobStatus::Unspecified => StreamJobStatus::Created,
        }
    }

    pub fn to_proto(self) -> PbStreamJobStatus {
        match self {
            StreamJobStatus::Creating => PbStreamJobStatus::Creating,
            StreamJobStatus::Created => PbStreamJobStatus::Created,
        }
    }
}

#[derive(Clone, Copy, Debug, Display, Hash, PartialOrd, PartialEq, Eq, Ord, Default)]
pub enum CreateType {
    #[default]
    Foreground,
    Background,
}

impl CreateType {
    pub fn from_proto(pb_create_type: PbCreateType) -> Self {
        match pb_create_type {
            PbCreateType::Foreground | PbCreateType::Unspecified => CreateType::Foreground,
            PbCreateType::Background => CreateType::Background,
        }
    }

    pub fn to_proto(self) -> PbCreateType {
        match self {
            CreateType::Foreground => PbCreateType::Foreground,
            CreateType::Background => PbCreateType::Background,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AlterDatabaseParam {
    // Barrier related parameters, per database.
    // None represents the default value, which means it follows `SystemParams`.
    BarrierIntervalMs(Option<u32>),
    CheckpointFrequency(Option<u64>),
}

macro_rules! for_all_fragment_type_flags {
    () => {
        for_all_fragment_type_flags! {
            {
                Source,
                Mview,
                Sink,
                Now,
                StreamScan,
                BarrierRecv,
                Values,
                Dml,
                CdcFilter,
                Skipped1,
                SourceScan,
                SnapshotBackfillStreamScan,
                FsFetch,
                CrossDbSnapshotBackfillStreamScan,
                StreamCdcScan,
                VectorIndexWrite,
                UpstreamSinkUnion,
                LocalityProvider
            },
            {},
            0
        }
    };
    (
        {},
        {
            $(
                {$flag:ident, $index:expr}
            ),*
        },
        $next_index:expr
    ) => {
        #[derive(Clone, Copy, Debug, Display, Hash, PartialOrd, PartialEq, Eq)]
        #[repr(u32)]
        pub enum FragmentTypeFlag {
            $(
                $flag = (1 << $index),
            )*
        }

        pub const FRAGMENT_TYPE_FLAG_LIST: [FragmentTypeFlag; $next_index] = [
            $(
                FragmentTypeFlag::$flag,
            )*
        ];

        impl TryFrom<u32> for FragmentTypeFlag {
            type Error = String;

            fn try_from(value: u32) -> Result<Self, Self::Error> {
                match value {
                    $(
                        value if value == (FragmentTypeFlag::$flag as u32) => Ok(FragmentTypeFlag::$flag),
                    )*
                    _ => Err(format!("Invalid FragmentTypeFlag value: {}", value)),
                }
            }
        }

        impl FragmentTypeFlag {
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    $(
                        FragmentTypeFlag::$flag => paste::paste!{stringify!( [< $flag:snake:upper >] )},
                    )*
                }
            }
        }
    };
    (
        {$first:ident $(, $rest:ident)*},
        {
            $(
                {$flag:ident, $index:expr}
            ),*
        },
        $next_index:expr
    ) => {
        for_all_fragment_type_flags! {
            {$($rest),*},
            {
                $({$flag, $index},)*
                {$first, $next_index}
            },
            $next_index + 1
        }
    };
}

for_all_fragment_type_flags!();

impl FragmentTypeFlag {
    pub fn raw_flag(flags: impl IntoIterator<Item = FragmentTypeFlag>) -> u32 {
        flags.into_iter().fold(0, |acc, flag| acc | (flag as u32))
    }

    /// Fragments that may be affected by `BACKFILL_RATE_LIMIT`.
    pub fn backfill_rate_limit_fragments() -> impl Iterator<Item = FragmentTypeFlag> {
        [FragmentTypeFlag::SourceScan, FragmentTypeFlag::StreamScan].into_iter()
    }

    /// Fragments that may be affected by `SOURCE_RATE_LIMIT`.
    /// Note: for `FsFetch`, old fragments don't have this flag set, so don't use this to check.
    pub fn source_rate_limit_fragments() -> impl Iterator<Item = FragmentTypeFlag> {
        [FragmentTypeFlag::Source, FragmentTypeFlag::FsFetch].into_iter()
    }

    /// Fragments that may be affected by `BACKFILL_RATE_LIMIT`.
    pub fn sink_rate_limit_fragments() -> impl Iterator<Item = FragmentTypeFlag> {
        [FragmentTypeFlag::Sink].into_iter()
    }

    /// Note: this doesn't include `FsFetch` created in old versions.
    pub fn rate_limit_fragments() -> impl Iterator<Item = FragmentTypeFlag> {
        Self::backfill_rate_limit_fragments()
            .chain(Self::source_rate_limit_fragments())
            .chain(Self::sink_rate_limit_fragments())
            .chain(Self::dml_rate_limit_fragments())
    }

    pub fn dml_rate_limit_fragments() -> impl Iterator<Item = FragmentTypeFlag> {
        [FragmentTypeFlag::Dml].into_iter()
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialOrd, PartialEq, Eq, Default)]
pub struct FragmentTypeMask(u32);

impl Binary for FragmentTypeMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:b}", self.0)
    }
}

impl From<i32> for FragmentTypeMask {
    fn from(value: i32) -> Self {
        Self(value as u32)
    }
}

impl From<u32> for FragmentTypeMask {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<FragmentTypeMask> for u32 {
    fn from(value: FragmentTypeMask) -> Self {
        value.0
    }
}

impl From<FragmentTypeMask> for i32 {
    fn from(value: FragmentTypeMask) -> Self {
        value.0 as _
    }
}

impl FragmentTypeMask {
    pub fn empty() -> Self {
        FragmentTypeMask(0)
    }

    pub fn add(&mut self, flag: FragmentTypeFlag) {
        self.0 |= flag as u32;
    }

    pub fn contains_any(&self, flags: impl IntoIterator<Item = FragmentTypeFlag>) -> bool {
        let flag = FragmentTypeFlag::raw_flag(flags);
        (self.0 & flag) != 0
    }

    pub fn contains(&self, flag: FragmentTypeFlag) -> bool {
        self.contains_any([flag])
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::catalog::FRAGMENT_TYPE_FLAG_LIST;

    use crate::catalog::FragmentTypeFlag;

    #[test]
    fn test_all_fragment_type_flag() {
        expect_test::expect![[r#"
            [
                (
                    Source,
                    1,
                    "SOURCE",
                ),
                (
                    Mview,
                    2,
                    "MVIEW",
                ),
                (
                    Sink,
                    4,
                    "SINK",
                ),
                (
                    Now,
                    8,
                    "NOW",
                ),
                (
                    StreamScan,
                    16,
                    "STREAM_SCAN",
                ),
                (
                    BarrierRecv,
                    32,
                    "BARRIER_RECV",
                ),
                (
                    Values,
                    64,
                    "VALUES",
                ),
                (
                    Dml,
                    128,
                    "DML",
                ),
                (
                    CdcFilter,
                    256,
                    "CDC_FILTER",
                ),
                (
                    Skipped1,
                    512,
                    "SKIPPED1",
                ),
                (
                    SourceScan,
                    1024,
                    "SOURCE_SCAN",
                ),
                (
                    SnapshotBackfillStreamScan,
                    2048,
                    "SNAPSHOT_BACKFILL_STREAM_SCAN",
                ),
                (
                    FsFetch,
                    4096,
                    "FS_FETCH",
                ),
                (
                    CrossDbSnapshotBackfillStreamScan,
                    8192,
                    "CROSS_DB_SNAPSHOT_BACKFILL_STREAM_SCAN",
                ),
                (
                    StreamCdcScan,
                    16384,
                    "STREAM_CDC_SCAN",
                ),
                (
                    VectorIndexWrite,
                    32768,
                    "VECTOR_INDEX_WRITE",
                ),
                (
                    UpstreamSinkUnion,
                    65536,
                    "UPSTREAM_SINK_UNION",
                ),
                (
                    LocalityProvider,
                    131072,
                    "LOCALITY_PROVIDER",
                ),
            ]
        "#]]
        .assert_debug_eq(
            &FRAGMENT_TYPE_FLAG_LIST
                .into_iter()
                .map(|flag| (flag, flag as u32, flag.as_str_name()))
                .collect_vec(),
        );
        for flag in FRAGMENT_TYPE_FLAG_LIST {
            assert_eq!(FragmentTypeFlag::try_from(flag as u32).unwrap(), flag);
        }
    }
}
