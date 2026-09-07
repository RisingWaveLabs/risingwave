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

#[cfg(test)]
mod test;

mod commit;
pub mod commit_retry;
mod config;
mod create_table;
mod engine_options;
mod metadata;
#[cfg(any(test, madsim))]
pub mod mock_v3_catalog_registry;
mod position_delete;
mod prometheus;
mod writer;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroU64;

use anyhow::{Context, anyhow};
pub use commit::*;
pub use config::*;
pub use create_table::*;
pub use engine_options::*;
use iceberg::table::Table;
pub use metadata::*;
pub use position_delete::*;
use risingwave_common::bail;
use risingwave_common::license::Feature;
use tokio::sync::mpsc::UnboundedSender;
pub use writer::*;

use super::{
    GLOBAL_SINK_METRICS, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink,
    SinkError, SinkWriterParam,
};
use crate::connector_common::{IcebergCatalogKind, IcebergSinkCompactionUpdate};
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::{Result, SinkCommitCoordinator, SinkParam};

pub const ICEBERG_SINK: &str = "iceberg";

pub struct IcebergSink {
    pub config: IcebergConfig,
    param: SinkParam,
    // In upsert mode, it is never None or empty.
    upsert_primary_key_column_names: Option<Vec<String>>,
}

impl EnforceSecret for IcebergSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            IcebergConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for IcebergSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = IcebergConfig::from_btreemap(param.properties.clone())?;
        IcebergSink::new(config, param)
    }
}

impl Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("config", &self.config)
            .finish()
    }
}

fn validate_explicit_compaction_type(config: &IcebergConfig) -> Result<()> {
    let Some(compaction_type) = config.compaction_type else {
        return Ok(());
    };

    if config.write_mode == IcebergWriteMode::CopyOnWrite {
        bail!(
            "`compaction.type` must not be set when `write_mode` is `copy-on-write`; \
             copy-on-write selects its compaction policy automatically"
        );
    }

    if !matches!(compaction_type, CompactionType::Full) {
        Feature::IcebergCompaction
            .check_available()
            .map_err(|e| anyhow!(e))?;
    }

    Ok(())
}

fn validate_compaction_option_compatibility(config: &IcebergConfig) -> Result<()> {
    // Value ranges are validated by `IcebergConfig::from_btreemap`. This only rejects
    // options that are incompatible with the selected merge-on-read strategy.
    // COW ignores legacy persisted types, so merge-on-read type-option compatibility does not apply.
    if config.write_mode == IcebergWriteMode::CopyOnWrite {
        return Ok(());
    }

    let Some(compaction_type) = config.compaction_type else {
        return Ok(());
    };

    let unsupported_option = match compaction_type {
        // Auto uses both selection thresholds.
        CompactionType::Auto => None,
        // Keep accepting strategy-specific thresholds that Full ignores.
        CompactionType::Full => None,
        CompactionType::SmallFiles => config
            .delete_files_count_threshold
            .is_some()
            .then_some(COMPACTION_DELETE_FILES_COUNT_THRESHOLD),
        CompactionType::FilesWithDelete => config
            .small_files_threshold_mb
            .is_some()
            .then_some(COMPACTION_SMALL_FILES_THRESHOLD_MB),
    };
    if let Some(option) = unsupported_option {
        bail!(
            "`{option}` is not supported for '{}' compaction type",
            compaction_type.as_str()
        );
    }

    Ok(())
}

impl IcebergSink {
    pub async fn create_and_validate_table(&self) -> Result<Table> {
        create_and_validate_table_impl(&self.config, &self.param).await
    }

    /// Returns `true` if this call created the table, `false` if it already existed.
    pub async fn create_table_if_not_exists(&self) -> Result<bool> {
        create_table_if_not_exists_impl(&self.config, &self.param).await
    }

    pub fn new(config: IcebergConfig, param: SinkParam) -> Result<Self> {
        if let Some(order_key) = &config.order_key {
            validate_order_key_columns(
                order_key,
                param.columns.iter().map(|column| column.name.as_str()),
            )
            .context("invalid order_key")
            .map_err(SinkError::Config)?;
        }

        let upsert_primary_key_column_names =
            if config.r#type == SINK_TYPE_UPSERT && !config.force_append_only {
                let pk_indices = param
                    .downstream_pk
                    .as_ref()
                    .filter(|pk| !pk.is_empty())
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "primary key must be specified for upsert iceberg sink"
                        ))
                    })?;
                Some(
                    pk_indices
                        .iter()
                        .map(|&idx| {
                            param
                                .columns
                                .get(idx)
                                .map(|column| column.name.clone())
                                .ok_or_else(|| {
                                    SinkError::Config(anyhow!(
                                        "primary key column index {} out of range in sink schema",
                                        idx
                                    ))
                                })
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                None
            };
        Ok(Self {
            config,
            param,
            upsert_primary_key_column_names,
        })
    }
}

impl Sink for IcebergSink {
    type LogSinker = CoordinatedLogSinker<IcebergSinkWriter>;

    const SINK_NAME: &'static str = ICEBERG_SINK;

    crate::impl_validate_sink_unknown_fields!();

    fn is_exactly_once(properties: &BTreeMap<String, String>) -> Result<bool> {
        let Some(value) = properties.get("is_exactly_once") else {
            return Ok(true);
        };
        value.parse::<bool>().map_err(|_| {
            SinkError::Config(anyhow!(
                "invalid value for `is_exactly_once`: expected `true` or `false`, got `{value}`"
            ))
        })
    }

    async fn validate(&self) -> Result<()> {
        let catalog_kind = self.config.catalog_kind()?;
        if matches!(catalog_kind, IcebergCatalogKind::Snowflake) {
            bail!("Snowflake catalog only supports iceberg sources");
        }

        if matches!(catalog_kind, IcebergCatalogKind::Glue(_)) {
            risingwave_common::license::Feature::IcebergSinkWithGlue
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        // Enforce merge-on-read for append-only tables
        IcebergConfig::validate_append_only_write_mode(
            &self.config.r#type,
            self.config.write_mode,
        )?;
        validate_explicit_compaction_type(&self.config)?;
        validate_compaction_option_compatibility(&self.config)?;

        // VARIANT is not comparable, so it can never be an equality-delete key.
        if self.config.r#type == SINK_TYPE_UPSERT
            && !self.config.force_append_only
            && let Some(pk_indices) = self
                .param
                .downstream_pk
                .as_ref()
                .filter(|pk| !pk.is_empty())
        {
            for &idx in pk_indices {
                if let Some(column) = self.param.columns.get(idx)
                    && column.data_type.contains_variant()
                {
                    bail!(
                        "VARIANT column `{}` cannot be used as the primary key of an upsert iceberg sink",
                        column.name
                    );
                }
            }
        }

        let table = self.create_and_validate_table().await?;
        self.config
            .validate_manifest_rewrite_format(table.metadata().format_version())?;
        Ok(())
    }

    fn support_schema_change() -> bool {
        true
    }

    fn validate_alter_config_change(
        config: &BTreeMap<String, String>,
        alter_props: &BTreeMap<String, String>,
    ) -> Result<()> {
        let compaction_type_changed = alter_props.contains_key(COMPACTION_TYPE);
        let compaction_options_changed = compaction_type_changed
            || alter_props.contains_key(COMPACTION_SMALL_FILES_THRESHOLD_MB)
            || alter_props.contains_key(COMPACTION_DELETE_FILES_COUNT_THRESHOLD);
        let enabling_compaction = alter_props
            .get(ENABLE_COMPACTION)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));

        if compaction_options_changed || enabling_compaction {
            let iceberg_config = IcebergConfig::from_btreemap(config.clone())?;
            let validate_explicit_type = compaction_type_changed
                || (enabling_compaction
                    && iceberg_config.write_mode == IcebergWriteMode::MergeOnRead);

            // Persisted COW types are legacy-only and must not block compaction activation.
            if validate_explicit_type {
                validate_explicit_compaction_type(&iceberg_config)?;
            }
            validate_compaction_option_compatibility(&iceberg_config)?;
        }

        Self::validate_alter_config(config)
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        let iceberg_config = IcebergConfig::from_btreemap(config.clone())?;

        // Validate compaction interval
        if let Some(compaction_interval) = iceberg_config.compaction_interval_sec {
            if iceberg_config.enable_compaction && compaction_interval == 0 {
                bail!(
                    "`compaction-interval-sec` must be greater than 0 when `enable-compaction` is true"
                );
            }

            tracing::info!(
                "Alter config compaction_interval set to {} seconds",
                compaction_interval
            );
        }

        // Validate max snapshots
        if let Some(max_snapshots) = iceberg_config.max_snapshots_num_before_compaction
            && max_snapshots < 1
        {
            bail!(
                "`compaction.max_snapshots_num` must be greater than 0, got: {}",
                max_snapshots
            );
        }

        // Validate target file size
        if let Some(target_file_size_mb) = iceberg_config.target_file_size_mb
            && target_file_size_mb == 0
        {
            bail!("`compaction.target_file_size_mb` must be greater than 0");
        }

        // Validate parquet max row group rows
        if let Some(max_row_group_rows) = iceberg_config.write_parquet_max_row_group_rows
            && max_row_group_rows == 0
        {
            bail!("`compaction.write_parquet_max_row_group_rows` must be greater than 0");
        }

        // Validate parquet max row group bytes
        if let Some(max_row_group_bytes) = iceberg_config.write_parquet_max_row_group_bytes
            && max_row_group_bytes == 0
        {
            bail!("`compaction.write_parquet_max_row_group_bytes` must be greater than 0");
        }

        // Validate parquet compression codec
        if let Some(ref compression) = iceberg_config.write_parquet_compression {
            let valid_codecs = [
                "uncompressed",
                "snappy",
                "gzip",
                "lzo",
                "brotli",
                "lz4",
                "zstd",
            ];
            if !valid_codecs.contains(&compression.to_lowercase().as_str()) {
                bail!(
                    "`compaction.write_parquet_compression` must be one of {:?}, got: {}",
                    valid_codecs,
                    compression
                );
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let writer = IcebergSinkWriter::new(
            self.config.clone(),
            self.param.clone(),
            writer_param.clone(),
            self.upsert_primary_key_column_names.clone(),
        );

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );
        let log_sinker = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(log_sinker)
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let catalog = self.config.create_catalog().await?;
        let table = self.create_and_validate_table().await?;
        let coordinator = IcebergSinkCommitter {
            catalog,
            table,
            last_commit_epoch: 0,
            sink_id: self.param.sink_id,
            config: self.config.clone(),
            param: self.param.clone(),
            commit_retry_num: self.config.commit_retry_num,
            iceberg_compact_stat_sender,
        };
        if Self::is_exactly_once(&self.param.properties)? {
            Ok(SinkCommitCoordinator::TwoPhase(Box::new(coordinator)))
        } else {
            Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
        }
    }
}
