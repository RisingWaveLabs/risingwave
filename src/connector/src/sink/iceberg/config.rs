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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::anyhow;
use iceberg::spec::{FormatVersion, MAIN_BRANCH};
use iceberg::table::Table;
use iceberg::transaction::{MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES_DEFAULT};
use iceberg::{Catalog, TableIdent};
use parquet::basic::Compression;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use super::{SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError};
use crate::connector_common::{
    IcebergCatalogKind, IcebergCommon, IcebergTableIdentifier, ResolvedIcebergCatalogConfig,
    iceberg_java_catalog_props_from_options,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::Result;
use crate::sink::decouple_checkpoint_log_sink::iceberg_default_commit_checkpoint_interval;
use crate::{deserialize_bool_from_string, deserialize_optional_string_seq_from_string};

pub const ICEBERG_COW_BRANCH: &str = "ingestion";

pub const ICEBERG_WRITE_MODE_MERGE_ON_READ: &str = "merge-on-read";
pub const ICEBERG_WRITE_MODE_COPY_ON_WRITE: &str = "copy-on-write";
pub const ICEBERG_COMPACTION_TYPE_AUTO: &str = "auto";
pub const ICEBERG_COMPACTION_TYPE_FULL: &str = "full";
pub const ICEBERG_COMPACTION_TYPE_SMALL_FILES: &str = "small-files";
pub const ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE: &str = "files-with-delete";

pub const PARTITION_DATA_ID_START: i32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IcebergWriteMode {
    #[default]
    MergeOnRead,
    CopyOnWrite,
}

impl IcebergWriteMode {
    pub fn as_str(self) -> &'static str {
        match self {
            IcebergWriteMode::MergeOnRead => ICEBERG_WRITE_MODE_MERGE_ON_READ,
            IcebergWriteMode::CopyOnWrite => ICEBERG_WRITE_MODE_COPY_ON_WRITE,
        }
    }
}

impl std::str::FromStr for IcebergWriteMode {
    type Err = SinkError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            ICEBERG_WRITE_MODE_MERGE_ON_READ => Ok(IcebergWriteMode::MergeOnRead),
            ICEBERG_WRITE_MODE_COPY_ON_WRITE => Ok(IcebergWriteMode::CopyOnWrite),
            _ => Err(SinkError::Config(anyhow!(format!(
                "invalid write_mode: {}, must be one of: {}, {}",
                s, ICEBERG_WRITE_MODE_MERGE_ON_READ, ICEBERG_WRITE_MODE_COPY_ON_WRITE
            )))),
        }
    }
}

impl TryFrom<&str> for IcebergWriteMode {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for IcebergWriteMode {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl std::fmt::Display for IcebergWriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Configuration constants
pub const ENABLE_COMPACTION: &str = "enable_compaction";
pub const COMPACTION_INTERVAL_SEC: &str = "compaction_interval_sec";
pub const ENABLE_SNAPSHOT_EXPIRATION: &str = "enable_snapshot_expiration";
pub const WRITE_MODE: &str = "write_mode";
pub const FORMAT_VERSION: &str = "format_version";
pub const SNAPSHOT_EXPIRATION_RETAIN_LAST: &str = "snapshot_expiration_retain_last";
pub const SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS: &str = "snapshot_expiration_max_age_millis";
pub const SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES: &str = "snapshot_expiration_clear_expired_files";
pub const SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA: &str =
    "snapshot_expiration_clear_expired_meta_data";
pub const ENABLE_MANIFEST_REWRITE: &str = "enable_manifest_rewrite";
pub const MANIFEST_REWRITE_TARGET_SIZE_BYTES: &str = "manifest_rewrite_target_size_bytes";
pub const MANIFEST_REWRITE_MIN_COUNT_TO_MERGE: &str = "manifest_rewrite_min_count_to_merge";
pub const ENABLE_ORPHAN_FILE_CLEANUP: &str = "enable_orphan_file_cleanup";
pub const ORPHAN_FILE_CLEANUP_MIN_AGE_MILLIS: &str = "orphan_file_cleanup_min_age_millis";
pub const COMPACTION_MAX_SNAPSHOTS_NUM: &str = "compaction.max_snapshots_num";

pub const COMPACTION_SMALL_FILES_THRESHOLD_MB: &str = "compaction.small_files_threshold_mb";

pub const COMPACTION_DELETE_FILES_COUNT_THRESHOLD: &str = "compaction.delete_files_count_threshold";

pub const COMPACTION_TRIGGER_SNAPSHOT_COUNT: &str = "compaction.trigger_snapshot_count";

pub const COMPACTION_TARGET_FILE_SIZE_MB: &str = "compaction.target_file_size_mb";

pub const COMPACTION_TYPE: &str = "compaction.type";

pub const COMPACTION_WRITE_PARQUET_COMPRESSION: &str = "compaction.write_parquet_compression";
pub const COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS: &str =
    "compaction.write_parquet_max_row_group_rows";
pub const COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES: &str =
    "compaction.write_parquet_max_row_group_bytes";
pub const ORDER_KEY: &str = "order_key";
pub const DEFAULT_COMPACTION_MAX_SNAPSHOTS_NUM: usize = 1000;
pub const ICEBERG_DEFAULT_WRITE_PARQUET_MAX_ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;
pub const ORPHAN_FILE_CLEANUP_MIN_AGE_MILLIS_DEFAULT: u64 = 7 * 24 * 60 * 60 * 1000;
pub const ORPHAN_FILE_CLEANUP_MIN_AGE_MILLIS_MINIMUM: u64 = 24 * 60 * 60 * 1000;
pub const ENABLE_PK_INDEX: &str = "enable_pk_index";

pub const PARQUET_CREATED_BY: &str = concat!("risingwave version ", env!("CARGO_PKG_VERSION"));

fn default_commit_retry_num() -> u32 {
    8
}

fn default_iceberg_write_mode() -> IcebergWriteMode {
    IcebergWriteMode::MergeOnRead
}

fn default_iceberg_format_version() -> FormatVersion {
    FormatVersion::V2
}

fn default_true() -> bool {
    true
}

fn default_some_true() -> Option<bool> {
    Some(true)
}

fn parse_format_version_str(value: &str) -> std::result::Result<FormatVersion, String> {
    let parsed = value
        .trim()
        .parse::<u8>()
        .map_err(|_| "`format-version` must be one of 1, 2, or 3".to_owned())?;
    match parsed {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        _ => Err("`format-version` must be one of 1, 2, or 3".to_owned()),
    }
}

fn deserialize_format_version<'de, D>(
    deserializer: D,
) -> std::result::Result<FormatVersion, D::Error>
where
    D: Deserializer<'de>,
{
    struct FormatVersionVisitor;

    impl<'de> Visitor<'de> for FormatVersionVisitor {
        type Value = FormatVersion;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("format-version as 1, 2, or 3")
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            let value = u8::try_from(value)
                .map_err(|_| E::custom("`format-version` must be one of 1, 2, or 3"))?;
            parse_format_version_str(&value.to_string()).map_err(E::custom)
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            let value = u8::try_from(value)
                .map_err(|_| E::custom("`format-version` must be one of 1, 2, or 3"))?;
            parse_format_version_str(&value.to_string()).map_err(E::custom)
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_format_version_str(value).map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(FormatVersionVisitor)
}

/// Compaction type for Iceberg sink
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompactionType {
    /// Auto compaction - compacts the union of small and delete-heavy files
    Auto,
    /// Full compaction - rewrites all data files
    Full,
    /// Small files compaction - only compact small files
    SmallFiles,
    /// Files with delete compaction - only compact files that have associated delete files
    FilesWithDelete,
}

impl CompactionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompactionType::Auto => ICEBERG_COMPACTION_TYPE_AUTO,
            CompactionType::Full => ICEBERG_COMPACTION_TYPE_FULL,
            CompactionType::SmallFiles => ICEBERG_COMPACTION_TYPE_SMALL_FILES,
            CompactionType::FilesWithDelete => ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE,
        }
    }
}

impl std::str::FromStr for CompactionType {
    type Err = SinkError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            ICEBERG_COMPACTION_TYPE_AUTO => Ok(CompactionType::Auto),
            ICEBERG_COMPACTION_TYPE_FULL => Ok(CompactionType::Full),
            ICEBERG_COMPACTION_TYPE_SMALL_FILES => Ok(CompactionType::SmallFiles),
            ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE => Ok(CompactionType::FilesWithDelete),
            _ => Err(SinkError::Config(anyhow!(format!(
                "invalid compaction_type: {}, must be one of: {}, {}, {}, {}",
                s,
                ICEBERG_COMPACTION_TYPE_AUTO,
                ICEBERG_COMPACTION_TYPE_FULL,
                ICEBERG_COMPACTION_TYPE_SMALL_FILES,
                ICEBERG_COMPACTION_TYPE_FILES_WITH_DELETE
            )))),
        }
    }
}

impl TryFrom<&str> for CompactionType {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for CompactionType {
    type Error = <Self as std::str::FromStr>::Err;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl std::fmt::Display for CompactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
pub struct IcebergConfig {
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(flatten)]
    pub(crate) common: IcebergCommon,

    #[serde(flatten)]
    pub(crate) table: IcebergTableIdentifier,

    #[serde(
        rename = "primary_key",
        default,
        deserialize_with = "deserialize_optional_string_seq_from_string"
    )]
    pub primary_key: Option<Vec<String>>,

    // Props for java catalog props.
    #[serde(skip)]
    pub java_catalog_props: HashMap<String, String>,

    #[serde(default)]
    #[with_option(iceberg_engine)]
    pub partition_by: Option<String>,

    #[serde(default)]
    #[with_option(iceberg_engine)]
    pub order_key: Option<String>,

    /// Commit every n(>0) checkpoints, default is 60.
    #[serde(default = "iceberg_default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub commit_checkpoint_interval: u64,

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub create_table_if_not_exists: bool,

    /// Whether it is `exactly_once`, the default is true.
    #[serde(default = "default_some_true")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub is_exactly_once: Option<bool>,
    // Retry commit num when iceberg commit fail. default is 8.
    // # TODO
    // Iceberg table may store the retry commit num in table meta.
    // We should try to find and use that as default commit retry num first.
    #[serde(default = "default_commit_retry_num")]
    pub commit_retry_num: u32,

    /// Whether to enable iceberg compaction.
    #[serde(
        rename = "enable_compaction",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub enable_compaction: bool,

    /// The interval of iceberg compaction
    #[serde(rename = "compaction_interval_sec", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub compaction_interval_sec: Option<u64>,

    /// Whether to enable iceberg expired snapshots.
    #[serde(
        rename = "enable_snapshot_expiration",
        default = "default_true",
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub enable_snapshot_expiration: bool,

    /// The iceberg write mode, can be `merge-on-read` or `copy-on-write`.
    #[serde(rename = "write_mode", default = "default_iceberg_write_mode")]
    #[with_option(iceberg_engine)]
    pub write_mode: IcebergWriteMode,

    /// Iceberg format version for table creation.
    #[serde(
        rename = "format_version",
        default = "default_iceberg_format_version",
        deserialize_with = "deserialize_format_version"
    )]
    #[with_option(iceberg_engine)]
    pub format_version: FormatVersion,

    /// The maximum age (in milliseconds) for snapshots before they expire
    /// For example, if set to 3600000, snapshots older than 1 hour will be expired
    #[serde(rename = "snapshot_expiration_max_age_millis", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub snapshot_expiration_max_age_millis: Option<i64>,

    /// The number of snapshots to retain
    #[serde(rename = "snapshot_expiration_retain_last", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub snapshot_expiration_retain_last: Option<i32>,

    #[serde(
        rename = "snapshot_expiration_clear_expired_files",
        default = "default_true",
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub snapshot_expiration_clear_expired_files: bool,

    #[serde(
        rename = "snapshot_expiration_clear_expired_meta_data",
        default = "default_true",
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub snapshot_expiration_clear_expired_meta_data: bool,

    /// Whether to periodically rewrite fragmented Iceberg data manifests.
    #[serde(
        rename = "enable_manifest_rewrite",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub enable_manifest_rewrite: bool,

    /// Target size in bytes for rewritten Iceberg data manifests.
    #[serde(rename = "manifest_rewrite_target_size_bytes", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub manifest_rewrite_target_size_bytes: Option<u64>,

    /// Minimum number of manifests required to rewrite an under-filled batch.
    #[serde(rename = "manifest_rewrite_min_count_to_merge", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub manifest_rewrite_min_count_to_merge: Option<usize>,

    /// Whether to periodically remove files that are unreachable from Iceberg metadata.
    /// Cleanup runs on the global Iceberg orphan-file cleanup interval.
    #[serde(
        rename = "enable_orphan_file_cleanup",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub enable_orphan_file_cleanup: bool,

    /// Minimum file age in milliseconds before an unreachable file may be removed.
    /// Defaults to 7 days and must be at least 24 hours.
    #[serde(rename = "orphan_file_cleanup_min_age_millis", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub orphan_file_cleanup_min_age_millis: Option<u64>,

    /// The maximum number of snapshots allowed since the last rewrite operation
    /// If set, sink will check snapshot count and wait if exceeded
    /// If unset, defaults to 1000 only when compaction is enabled
    #[serde(rename = "compaction.max_snapshots_num", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub max_snapshots_num_before_compaction: Option<usize>,

    #[serde(rename = "compaction.small_files_threshold_mb", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub small_files_threshold_mb: Option<u64>,

    #[serde(rename = "compaction.delete_files_count_threshold", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub delete_files_count_threshold: Option<usize>,

    #[serde(rename = "compaction.trigger_snapshot_count", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub trigger_snapshot_count: Option<usize>,

    #[serde(rename = "compaction.target_file_size_mb", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub target_file_size_mb: Option<u64>,

    /// Compaction type: `auto`, `full`, `small-files`, or `files-with-delete`
    /// If not set, defaults to `auto` when Iceberg compaction is licensed, otherwise `full`
    #[serde(rename = "compaction.type", default)]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub compaction_type: Option<CompactionType>,

    /// Parquet compression codec
    /// Supported values: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd
    /// Default is zstd
    #[serde(rename = "compaction.write_parquet_compression", default)]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub write_parquet_compression: Option<String>,

    /// Deprecated: maximum number of rows in a Parquet row group.
    /// Accepted for backward compatibility, but ignored by the writer.
    #[serde(rename = "compaction.write_parquet_max_row_group_rows", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub write_parquet_max_row_group_rows: Option<usize>,

    /// Maximum size of a Parquet row group in bytes
    /// Default is 128 `MiB`, matching Iceberg defaults.
    #[serde(rename = "compaction.write_parquet_max_row_group_bytes", default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly, iceberg_engine)]
    pub write_parquet_max_row_group_bytes: Option<usize>,

    /// Whether to enable PK index for upsert sink. Default is false.
    /// For upsert iceberg sinks (V2/V3, merge-on-read): maintain a pk index and write
    /// position deletes instead of equality deletes.
    #[serde(
        rename = "enable_pk_index",
        default,
        deserialize_with = "deserialize_bool_from_string"
    )]
    #[with_option(iceberg_engine)]
    pub enable_pk_index: bool,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(IcebergConfig);

impl EnforceSecret for IcebergConfig {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            IcebergCommon::enforce_one(prop)?;
        }
        Ok(())
    }

    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        IcebergCommon::enforce_one(prop)
    }
}

impl IcebergConfig {
    /// Validate that append-only sinks use merge-on-read mode
    /// Copy-on-write is strictly worse than merge-on-read for append-only workloads
    pub fn validate_append_only_write_mode(
        sink_type: &str,
        write_mode: IcebergWriteMode,
    ) -> Result<()> {
        if sink_type == SINK_TYPE_APPEND_ONLY && write_mode == IcebergWriteMode::CopyOnWrite {
            return Err(SinkError::Config(anyhow!(
                "'copy-on-write' mode is not supported for append-only iceberg sink. \
                 Please use 'merge-on-read' instead, which is strictly better for append-only workloads."
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_enable_pk_index(&self) -> Result<()> {
        if !self.enable_pk_index {
            return Ok(());
        }

        if self.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` is only supported for upsert iceberg sink"
            )));
        }

        if self.write_mode != IcebergWriteMode::MergeOnRead {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` is only supported for upsert iceberg sink with merge-on-read mode"
            )));
        }

        if self.format_version < FormatVersion::V2 {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` is only supported for upsert iceberg sink with format version >= 2"
            )));
        }

        if self.force_append_only {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` cannot be true when `force_append_only` is true"
            )));
        }

        Ok(())
    }

    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let mut config =
            serde_json::from_value::<IcebergConfig>(serde_json::to_value(&values).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.enable_compaction && !values.contains_key(COMPACTION_MAX_SNAPSHOTS_NUM) {
            config.max_snapshots_num_before_compaction = Some(DEFAULT_COMPACTION_MAX_SNAPSHOTS_NUM);
        }

        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }

        if config.r#type == SINK_TYPE_UPSERT {
            if let Some(primary_key) = &config.primary_key {
                if primary_key.is_empty() {
                    return Err(SinkError::Config(anyhow!(
                        "`primary-key` must not be empty in {}",
                        SINK_TYPE_UPSERT
                    )));
                }
            } else if !config.enable_pk_index {
                // When `enable_pk_index = true`, the planner auto-derives the iceberg pk
                // from the upstream stream key, so the user does not need to spell it out
                // in WITH options. The derived pk is written back into properties before
                // this validation is consulted again at sink-construction time.
                return Err(SinkError::Config(anyhow!(
                    "Must set `primary-key` in {}",
                    SINK_TYPE_UPSERT
                )));
            }
        }

        // Enforce merge-on-read for append-only sinks
        Self::validate_append_only_write_mode(&config.r#type, config.write_mode)?;
        config.validate_enable_pk_index()?;
        config.validate_manifest_rewrite_format(config.format_version)?;

        // All configs start with "catalog." will be treated as java configs.
        config.java_catalog_props = iceberg_java_catalog_props_from_options(
            values
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        config
            .unknown_fields
            .retain(|key, _| key != "connector" && !key.starts_with("catalog."));

        if config.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit-checkpoint-interval` must be greater than 0"
            )));
        }

        if config.compaction_interval_sec == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction_interval_sec` must be greater than 0"
            )));
        }

        if config.trigger_snapshot_count == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.trigger_snapshot_count` must be greater than 0"
            )));
        }

        if config.max_snapshots_num_before_compaction == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.max_snapshots_num` must be greater than 0"
            )));
        }

        if config.small_files_threshold_mb == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.small_files_threshold_mb` must be greater than 0"
            )));
        }

        if config.delete_files_count_threshold == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.delete_files_count_threshold` must be greater than 0"
            )));
        }

        if config.target_file_size_mb == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.target_file_size_mb` must be greater than 0"
            )));
        }

        if config.write_parquet_max_row_group_rows == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.write_parquet_max_row_group_rows` must be greater than 0"
            )));
        }

        if config.write_parquet_max_row_group_bytes == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`compaction.write_parquet_max_row_group_bytes` must be greater than 0"
            )));
        }

        if config.manifest_rewrite_target_size_bytes == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`manifest_rewrite_target_size_bytes` must be greater than 0"
            )));
        }

        if config.manifest_rewrite_min_count_to_merge == Some(0) {
            return Err(SinkError::Config(anyhow!(
                "`manifest_rewrite_min_count_to_merge` must be greater than 0"
            )));
        }

        if config
            .orphan_file_cleanup_min_age_millis
            .is_some_and(|min_age_millis| {
                min_age_millis < ORPHAN_FILE_CLEANUP_MIN_AGE_MILLIS_MINIMUM
            })
        {
            return Err(SinkError::Config(anyhow!(
                "`orphan_file_cleanup_min_age_millis` must be at least {} (24 hours)",
                ORPHAN_FILE_CLEANUP_MIN_AGE_MILLIS_MINIMUM
            )));
        }

        if config.enable_orphan_file_cleanup || config.orphan_file_cleanup_min_age_millis.is_some()
        {
            config.ensure_orphan_file_cleanup_supported()?;
        }

        // Validate table identifier (e.g., database.name should not contain dots)
        config
            .table
            .validate()
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.write_parquet_max_row_group_rows.is_some() {
            tracing::warn!(
                "`compaction.write_parquet_max_row_group_rows` is deprecated and ignored; use `compaction.write_parquet_max_row_group_bytes` instead"
            );
        }

        Ok(config)
    }

    pub fn catalog_type(&self) -> &str {
        self.common.catalog_type()
    }

    pub fn catalog_kind(&self) -> Result<IcebergCatalogKind> {
        self.common
            .resolve_catalog_kind()
            .map_err(|err| SinkError::Config(anyhow!(err)))
    }

    pub fn ensure_orphan_file_cleanup_supported(&self) -> Result<()> {
        if matches!(self.catalog_kind()?, IcebergCatalogKind::Storage) {
            return Err(SinkError::Config(anyhow!(
                "Iceberg orphan file cleanup is not supported for `catalog.type = 'storage'`"
            )));
        }

        Ok(())
    }

    fn resolved_catalog_config(&self) -> Result<ResolvedIcebergCatalogConfig<'_>> {
        self.common
            .resolve_catalog_config(self.java_catalog_props.clone())
            .map_err(|err| SinkError::Config(anyhow!(err)))
    }

    pub async fn load_table(&self) -> Result<Table> {
        #[cfg(any(test, madsim))]
        if self.catalog_type() == "mock_v3" {
            let catalog =
                crate::sink::iceberg::mock_v3_catalog_registry::get().ok_or_else(|| {
                    SinkError::Config(anyhow!(
                        "mock_v3 catalog_type set but no mock catalog registered"
                    ))
                })?;
            let table_id = self
                .table
                .to_table_ident()
                .map_err(|e| SinkError::Config(anyhow!(e).context("Unable to parse table name")))?;
            let table = catalog
                .load_table(&table_id)
                .await
                .map_err(|e| SinkError::Config(anyhow!(e).context("Failed to load mock table")))?;
            return Ok(table);
        }
        self.resolved_catalog_config()?
            .load_table(&self.table)
            .await
            .map_err(Into::into)
    }

    pub async fn create_catalog(&self) -> Result<Arc<dyn Catalog>> {
        #[cfg(any(test, madsim))]
        if self.catalog_type() == "mock_v3" {
            return Ok(
                crate::sink::iceberg::mock_v3_catalog_registry::get().ok_or_else(|| {
                    anyhow::anyhow!("mock_v3 catalog_type set but no mock catalog registered")
                })?,
            );
        }
        self.resolved_catalog_config()?
            .create_catalog()
            .await
            .map_err(Into::into)
    }

    pub fn full_table_name(&self) -> Result<TableIdent> {
        self.table.to_table_ident().map_err(Into::into)
    }

    pub fn catalog_name(&self) -> String {
        self.common.catalog_name()
    }

    pub fn table_format_version(&self) -> FormatVersion {
        self.format_version
    }

    pub fn validate_manifest_rewrite_format(&self, format_version: FormatVersion) -> Result<()> {
        if self.enable_manifest_rewrite && format_version >= FormatVersion::V3 {
            return Err(SinkError::Config(anyhow!(
                "`enable_manifest_rewrite` is not supported for Iceberg format version 3 because rewrite manifests cannot preserve row lineage"
            )));
        }
        Ok(())
    }

    pub fn compaction_interval_sec(&self) -> u64 {
        // default to 1 hour
        self.compaction_interval_sec.unwrap_or(3600)
    }

    /// Calculate the timestamp (in milliseconds) before which snapshots should be expired
    /// Returns `current_time_ms` - `max_age_millis`
    pub fn snapshot_expiration_timestamp_ms(&self, current_time_ms: i64) -> Option<i64> {
        self.snapshot_expiration_max_age_millis
            .map(|max_age_millis| current_time_ms - max_age_millis)
    }

    pub fn manifest_rewrite_target_size_bytes(&self) -> u64 {
        self.manifest_rewrite_target_size_bytes
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT as u64)
    }

    pub fn manifest_rewrite_min_count_to_merge(&self) -> usize {
        self.manifest_rewrite_min_count_to_merge
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT as usize)
    }

    pub fn orphan_file_cleanup_min_age_millis(&self) -> u64 {
        self.orphan_file_cleanup_min_age_millis
            .unwrap_or(ORPHAN_FILE_CLEANUP_MIN_AGE_MILLIS_DEFAULT)
    }

    pub fn trigger_snapshot_count(&self) -> usize {
        self.trigger_snapshot_count.unwrap_or(usize::MAX)
    }

    pub fn small_files_threshold_mb(&self) -> u64 {
        self.small_files_threshold_mb.unwrap_or(64)
    }

    pub fn delete_files_count_threshold(&self) -> usize {
        self.delete_files_count_threshold.unwrap_or(256)
    }

    pub fn target_file_size_mb(&self) -> u64 {
        self.target_file_size_mb.unwrap_or(1024)
    }

    /// Get the parquet compression codec
    /// Default is "zstd"
    pub fn write_parquet_compression(&self) -> &str {
        self.write_parquet_compression.as_deref().unwrap_or("zstd")
    }

    /// Get the maximum number of rows in a Parquet row group.
    pub fn write_parquet_max_row_group_rows(&self) -> Option<usize> {
        self.write_parquet_max_row_group_rows
    }

    /// Get the maximum size in bytes of a Parquet row group.
    pub fn write_parquet_max_row_group_bytes(&self) -> Option<usize> {
        self.write_parquet_max_row_group_bytes
            .or(Some(ICEBERG_DEFAULT_WRITE_PARQUET_MAX_ROW_GROUP_BYTES))
    }

    /// Parse the compression codec string into Parquet Compression enum.
    /// Invalid values fall back to SNAPPY.
    pub fn get_parquet_compression(&self) -> Compression {
        parse_parquet_compression(self.write_parquet_compression())
    }
}

/// Parse compression codec string to Parquet Compression enum
pub fn parse_parquet_compression(codec: &str) -> Compression {
    match codec.to_lowercase().as_str() {
        "uncompressed" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "lzo" => Compression::LZO,
        "brotli" => Compression::BROTLI(Default::default()),
        "lz4" => Compression::LZ4,
        "zstd" => Compression::ZSTD(Default::default()),
        _ => {
            tracing::warn!(
                "Unknown compression codec '{}', falling back to SNAPPY",
                codec
            );
            Compression::SNAPPY
        }
    }
}

// Helper Functions

pub fn commit_branch(sink_type: &str, write_mode: IcebergWriteMode) -> String {
    if should_enable_iceberg_cow(sink_type, write_mode) {
        ICEBERG_COW_BRANCH.to_owned()
    } else {
        MAIN_BRANCH.to_owned()
    }
}

pub fn should_enable_iceberg_cow(sink_type: &str, write_mode: IcebergWriteMode) -> bool {
    sink_type == SINK_TYPE_UPSERT && write_mode == IcebergWriteMode::CopyOnWrite
}

impl crate::with_options::WithOptions for IcebergWriteMode {}

impl crate::with_options::WithOptions for FormatVersion {}

impl crate::with_options::WithOptions for CompactionType {}
