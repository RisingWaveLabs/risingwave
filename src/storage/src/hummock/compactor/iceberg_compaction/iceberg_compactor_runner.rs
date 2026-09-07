// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

use derive_builder::Builder;
use iceberg::spec::{DataContentType, DataFile, FormatVersion, MAIN_BRANCH};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use iceberg_compaction_core::compaction::{
    CommitConsistencyParams, CommitManagerRetryConfig, Compaction, CompactionBuilder,
    CompactionPlan, CompactionPlanner, CompactionResult, RewriteResult,
};
use iceberg_compaction_core::config::{
    AutoCompactionConfigBuilder, CompactionExecutionConfigBuilder, CompactionPlanningConfig,
    FileGroupScope, FilesWithDeletesConfigBuilder, FullCompactionConfigBuilder, GroupFilters,
    SmallFilesConfigBuilder,
};
use iceberg_compaction_core::executor::RewriteFilesStat;
use iceberg_compaction_core::file_selection::FileGroup;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use parquet_58::file::properties::WriterProperties;
use risingwave_common::config::storage::default::storage::{
    iceberg_compaction_enable_heuristic_output_parallelism, iceberg_compaction_enable_prefetch,
    iceberg_compaction_max_concurrent_closes,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, IcebergWriteMode, commit_branch, should_enable_iceberg_cow,
};
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::id::IcebergCompactionTaskId;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::memory::estimate_plan_memory;
use super::{IcebergTaskMeta, PkIndexCompactionResult, build_pk_index_compaction_result};
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::CompactorMetrics;

pub struct IcebergTaskExecution {
    pub sink_id: u32,
    pub plan_runners: Vec<IcebergCompactionPlanRunner>,
}

static ICEBERG_COMPACTION_METRICS_REGISTRY: LazyLock<Box<PrometheusMetricsRegistry>> =
    LazyLock::new(|| {
        Box::new(PrometheusMetricsRegistry::new(
            GLOBAL_METRICS_REGISTRY.clone(),
        ))
    });

#[derive(Builder, Debug, Clone)]
pub struct IcebergCompactorRunnerConfig {
    #[builder(default = "4")]
    pub max_parallelism: u32,
    #[builder(default = "1024 * 1024 * 1024")] // 1GB"
    pub min_size_per_partition: u64,
    #[builder(default = "32")]
    pub max_file_count_per_partition: u32,
    #[builder(default = "false")]
    pub enable_validate_compaction: bool,
    #[builder(default = "1024")]
    pub max_record_batch_rows: usize,
    #[builder(default = "iceberg_compaction_enable_heuristic_output_parallelism()")]
    pub enable_heuristic_output_parallelism: bool,
    #[builder(default = "iceberg_compaction_max_concurrent_closes()")]
    pub max_concurrent_closes: usize,
    /// Whether to prefetch entire data files before compaction.
    /// See `StorageConfig::iceberg_compaction_enable_prefetch` for full documentation.
    #[builder(default = "iceberg_compaction_enable_prefetch()")]
    pub enable_prefetch: bool,
    #[builder]
    pub target_binpack_group_size_mb: Option<u64>,
    #[builder]
    pub min_group_size_mb: Option<u64>,
    #[builder]
    pub min_group_file_count: Option<usize>,
}

pub struct IcebergCompactionTaskStatistics {
    pub total_data_file_size: u64,
    pub total_data_file_count: u32,
    pub total_pos_del_file_size: u64,
    pub total_pos_del_file_count: u32,
    pub total_eq_del_file_size: u64,
    pub total_eq_del_file_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IcebergCompactionKind {
    Auto,
    SmallFiles,
    Full,
    FilesWithDeletes,
    CopyOnWriteAuto,
    CopyOnWrite,
}

impl IcebergCompactionKind {
    fn resolve(task_type: TaskType, iceberg_config: &IcebergConfig) -> HummockResult<Self> {
        if should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode) {
            return match task_type {
                TaskType::Auto => Ok(Self::CopyOnWriteAuto),
                TaskType::Full => Ok(Self::CopyOnWrite),
                _ => Err(HummockError::compaction_executor(anyhow::anyhow!(
                    "Unsupported task type for copy-on-write iceberg compaction: {task_type:?}"
                ))),
            };
        }

        match task_type {
            TaskType::Auto => Ok(Self::Auto),
            TaskType::SmallFiles => Ok(Self::SmallFiles),
            TaskType::Full => Ok(Self::Full),
            TaskType::FilesWithDelete => Ok(Self::FilesWithDeletes),
            _ => Err(HummockError::compaction_executor(anyhow::anyhow!(
                "Unsupported task type in iceberg compaction task: {task_type:?}"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::SmallFiles => "small-files",
            Self::Full => "full",
            Self::FilesWithDeletes => "files-with-delete",
            Self::CopyOnWriteAuto => "copy-on-write-auto",
            Self::CopyOnWrite => "copy-on-write",
        }
    }

    fn is_copy_on_write(self) -> bool {
        matches!(self, Self::CopyOnWriteAuto | Self::CopyOnWrite)
    }
}

#[derive(Debug)]
struct CowPublishPlan {
    snapshot_id: i64,
    rewritten_data_file_paths: HashSet<String>,
}

#[derive(Debug, Default)]
struct CowRewriteStatistics {
    snapshot_data_file_count: usize,
    snapshot_data_file_size_bytes: u64,
    rewritten_data_file_count: usize,
    rewritten_data_file_size_bytes: u64,
    skipped_rewrite_data_file_count: usize,
    skipped_rewrite_data_file_size_bytes: u64,
}

impl CowPublishPlan {
    fn from_compaction_plan(plan: &CompactionPlan) -> Self {
        Self {
            snapshot_id: plan.snapshot_id,
            rewritten_data_file_paths: plan
                .file_group
                .data_files
                .iter()
                .map(|task| task.data_file_path.clone())
                .collect(),
        }
    }
}

impl Debug for IcebergCompactionTaskStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCompactionTaskStatistics")
            .field("total_data_file_size", &self.total_data_file_size)
            .field("total_data_file_count", &self.total_data_file_count)
            .field("total_pos_del_file_size", &self.total_pos_del_file_size)
            .field("total_pos_del_file_count", &self.total_pos_del_file_count)
            .field("total_eq_del_file_size", &self.total_eq_del_file_size)
            .field("total_eq_del_file_count", &self.total_eq_del_file_count)
            .finish()
    }
}

/// Compaction plan runner that executes a single compaction plan.
#[derive(Debug)]
pub struct IcebergCompactionPlanRunner {
    pub task_id: IcebergCompactionTaskId,
    pub sink_id: u32,
    pub plan_index: usize,

    pub catalog: Arc<dyn Catalog>,
    pub table_ident: TableIdent,
    pub iceberg_config: IcebergConfig,

    config: IcebergCompactorRunnerConfig,
    metrics: Arc<CompactorMetrics>,

    compaction_kind: IcebergCompactionKind,
    branch: String,
    compaction_plan: CompactionPlan,
    /// When true, run the rewrite without committing and report the result back to meta for
    /// pk-index coordinated compaction. When false, behavior is unchanged (rewrite + commit).
    pk_index_coordinated: bool,
    pub memory_reservation_bytes: usize,
}

impl IcebergCompactionPlanRunner {
    pub fn required_parallelism(&self) -> u32 {
        self.compaction_plan.recommended_executor_parallelism() as u32
    }

    pub(crate) fn compaction_kind(&self) -> IcebergCompactionKind {
        self.compaction_kind
    }

    /// Returns a human-readable identifier for this plan, used for logging and debugging.
    ///
    /// The identifier includes catalog, table, compaction kind, and plan-index to provide context
    /// in logs. Format: `{catalog}-{table}-{compaction_kind}-plan-{index}`
    ///
    /// # Note
    ///
    /// This identifier is **for display only**. The queue does not use it for deduplication.
    /// Task management (dedup, cancellation) is Meta's responsibility via unique `task_id`.
    ///
    /// # Example
    ///
    /// ```text
    /// catalog: "glue"
    /// table: "my_db.my_table"
    /// compaction_kind: SmallFiles
    /// plan_index: 0
    ///
    /// → unique_ident: "glue-my_db.my_table-small-plan-0"
    /// ```
    pub fn unique_ident(&self) -> String {
        format!(
            "{}-{}-{}-plan-{}",
            self.iceberg_config.catalog_name(),
            self.table_ident,
            self.compaction_kind.as_str(),
            self.plan_index
        )
    }

    pub fn to_meta(&self) -> IcebergTaskMeta {
        IcebergTaskMeta {
            task_id: self.task_id,
            plan_index: self.plan_index,
            required_parallelism: self.required_parallelism(),
            memory_reservation_bytes: self.memory_reservation_bytes,
        }
    }

    pub async fn compact(
        self,
        shutdown_rx: Receiver<()>,
    ) -> HummockResult<(RewriteFilesStat, Option<PkIndexCompactionResult>)> {
        let task_id = self.task_id;
        let sink_id = self.sink_id;
        let plan_index = self.plan_index;
        let compaction_kind = self.compaction_kind;
        let table_ident = self.table_ident.clone();
        let branch = self.branch.clone();
        let unique_ident = self.unique_ident();
        let now = std::time::Instant::now();

        let compact_task = Self::compact_impl(
            self.task_id,
            self.plan_index,
            self.catalog,
            self.table_ident,
            self.iceberg_config,
            self.config,
            self.metrics,
            self.compaction_kind,
            self.branch,
            self.compaction_plan,
            self.pk_index_coordinated,
            self.memory_reservation_bytes,
        );

        tokio::select! {
            _ = shutdown_rx => {
                tracing::info!(
                    iceberg_component = "compaction_worker",
                    iceberg_operation = "execute_plan",
                    task_id = %task_id,
                    sink_id = sink_id,
                    plan_index = plan_index,
                    task_type = ?compaction_kind,
                    table = %table_ident,
                    branch = %branch,
                    unique_ident = %unique_ident,
                    "iceberg_compaction_plan_cancelled",
                );
                Err(HummockError::compaction_executor("Plan cancelled"))
            }
            result = compact_task => {
                match &result {
                    Ok((stats, _pk_index_result)) => {
                        tracing::info!(
                            iceberg_component = "compaction_worker",
                            iceberg_operation = "execute_plan",
                            task_id = %task_id,
                            sink_id = sink_id,
                            plan_index = plan_index,
                            task_type = ?compaction_kind,
                            table = %table_ident,
                            branch = %branch,
                            unique_ident = %unique_ident,
                            elapsed_millis = now.elapsed().as_millis(),
                            stats = ?stats,
                            "iceberg_compaction_plan_succeeded",
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            iceberg_component = "compaction_worker",
                            iceberg_operation = "execute_plan",
                            error = %e.as_report(),
                            task_id = %task_id,
                            sink_id = sink_id,
                            plan_index = plan_index,
                            task_type = ?compaction_kind,
                            table = %table_ident,
                            branch = %branch,
                            unique_ident = %unique_ident,
                            "iceberg_compaction_plan_failed",
                        );
                    }
                }
                result
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn compact_impl(
        task_id: IcebergCompactionTaskId,
        plan_index: usize,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        iceberg_config: IcebergConfig,
        config: IcebergCompactorRunnerConfig,
        metrics: Arc<CompactorMetrics>,
        compaction_kind: IcebergCompactionKind,
        branch: String,
        compaction_plan: CompactionPlan,
        pk_index_coordinated: bool,
        memory_reservation_bytes: usize,
    ) -> HummockResult<(RewriteFilesStat, Option<PkIndexCompactionResult>)> {
        let retry_config = CommitManagerRetryConfig::default();
        let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
            .with_catalog_name(iceberg_config.catalog_name())
            .with_executor_type(iceberg_compaction_core::executor::ExecutorType::DataFusion)
            .with_registry(ICEBERG_COMPACTION_METRICS_REGISTRY.clone())
            .with_retry_config(retry_config)
            .with_to_branch(branch.clone())
            .build();

        // COW publishing must stay bound to the snapshot used for planning. The ingestion branch
        // may advance while the rewrite is running, and publishing that newer branch state without
        // its delete files would make stale or duplicate rows visible on `main`.
        let cow_publish_plan = compaction_kind
            .is_copy_on_write()
            .then(|| CowPublishPlan::from_compaction_plan(&compaction_plan));

        if !compaction_plan.has_files() {
            if let Some(cow_publish_plan) = cow_publish_plan {
                let table = catalog
                    .load_table(&table_ident)
                    .await
                    .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
                publish_cow_snapshot_to_main(
                    task_id,
                    &compaction,
                    &table,
                    &branch,
                    &cow_publish_plan,
                    vec![],
                )
                .await?;
            }
            tracing::info!(
                iceberg_component = "compaction_worker",
                iceberg_operation = "plan_compaction",
                task_id = %task_id,
                plan_index,
                task_type = ?compaction_kind,
                table = %table_ident,
                branch = %branch,
                "iceberg_compaction_plan_skipped_empty",
            );
            return Ok((RewriteFilesStat::default(), None));
        }

        let statistics = analyze_task_statistics(&compaction_plan);

        // Build writer properties from sink configuration
        let write_parquet_properties = WriterProperties::builder()
            .set_compression(iceberg_config.get_parquet_compression())
            .set_max_row_group_bytes(iceberg_config.write_parquet_max_row_group_bytes())
            .set_created_by(concat!("risingwave version ", env!("CARGO_PKG_VERSION")).to_owned())
            .build();

        let compaction_execution_config = CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(config.enable_validate_compaction)
            .max_record_batch_rows(config.max_record_batch_rows)
            .write_parquet_properties(write_parquet_properties)
            .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
            .max_concurrent_closes(config.max_concurrent_closes)
            .enable_prefetch(config.enable_prefetch)
            .build()
            .map_err(|e| {
                HummockError::compaction_executor(
                    anyhow::Error::new(e)
                        .context("failed to build iceberg compaction execution config"),
                )
            })?;

        tracing::info!(
            iceberg_component = "compaction_worker",
            iceberg_operation = "execute_plan",
            task_id = %task_id,
            plan_index = plan_index,
            task_type = ?compaction_kind,
            table = %table_ident,
            branch = %branch,
            input_parallelism = compaction_plan.recommended_executor_parallelism(),
            output_parallelism = compaction_plan.recommended_output_parallelism(),
            memory_reservation_bytes,
            statistics = ?statistics,
            "iceberg_compaction_plan_started",
        );

        metrics.compact_task_pending_num.inc();
        let input_parallelism = compaction_plan.recommended_executor_parallelism() as u32;
        metrics
            .compact_task_pending_parallelism
            .add(input_parallelism as _);

        let _release_guard = scopeguard::guard(
            (input_parallelism, metrics.clone()),
            |(val, metrics_guard)| {
                metrics_guard.compact_task_pending_num.dec();
                metrics_guard.compact_task_pending_parallelism.sub(val as _);
            },
        );

        // Capture plan-derived report inputs before the rewrite consumes `compaction_plan`.
        // Only used by the pk-index coordinated path below.
        let pk_index_read_snapshot_id = compaction_plan.snapshot_id;
        let pk_index_input_file_paths: Vec<String> = if pk_index_coordinated {
            compaction_plan
                .file_group
                .data_files
                .iter()
                .chain(compaction_plan.file_group.position_delete_files.iter())
                .chain(compaction_plan.file_group.equality_delete_files.iter())
                .map(|task| task.data_file_path.clone())
                .collect()
        } else {
            Vec::new()
        };

        if pk_index_coordinated {
            // `compact_with_plan` commits its rewrite internally. A pk-index compaction must leave
            // the input files live until meta has paused the writer and can atomically commit the
            // overwrite together with the resolved pk-index. Run only the rewrite phase here and
            // report its output to meta, which is the sole committer for this path.
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            let RewriteResult {
                output_data_files: data_files,
                stats,
                ..
            } = compaction
                .rewrite_plan(compaction_plan, &compaction_execution_config, &table)
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            let pk_index_result = build_pk_index_compaction_result(
                &table,
                data_files,
                pk_index_input_file_paths,
                pk_index_read_snapshot_id,
            )?;

            return Ok((stats, Some(pk_index_result)));
        }

        let compaction_result = compaction
            .compact_with_plan(compaction_plan, &compaction_execution_config)
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?
            .ok_or_else(|| {
                HummockError::compaction_executor(anyhow::anyhow!(
                    "compact_with_plan returned no result for a non-empty iceberg compaction plan"
                ))
            })?;

        let CompactionResult {
            data_files,
            stats,
            table,
        } = compaction_result;

        if let (Some(committed_table), Some(cow_publish_plan)) = (table, cow_publish_plan) {
            publish_cow_snapshot_to_main(
                task_id,
                &compaction,
                &committed_table,
                &branch,
                &cow_publish_plan,
                data_files,
            )
            .await?;
        }

        Ok((stats, None))
    }
}

async fn live_data_files_for_snapshot(
    table: &Table,
    snapshot_id: i64,
) -> HummockResult<Vec<DataFile>> {
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| {
            HummockError::compaction_executor(anyhow::anyhow!(
                "No snapshot found with ID {snapshot_id} while publishing COW compaction"
            ))
        })?;

    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    let mut data_files = vec![];

    for manifest_file in manifest_list
        .entries()
        .iter()
        .filter(|entry| entry.has_added_files() || entry.has_existing_files())
    {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let (entries, _) = manifest.into_parts();
        data_files.extend(
            entries
                .into_iter()
                .filter(|entry| entry.is_alive())
                .filter(|entry| entry.content_type() == DataContentType::Data)
                .map(|entry| entry.data_file().clone()),
        );
    }

    Ok(data_files)
}

async fn live_data_files_for_branch(table: &Table, branch: &str) -> HummockResult<Vec<DataFile>> {
    let Some(snapshot_id) = table
        .metadata()
        .snapshot_for_ref(branch)
        .map(|snapshot| snapshot.snapshot_id())
    else {
        return Ok(vec![]);
    };

    live_data_files_for_snapshot(table, snapshot_id).await
}

async fn build_cow_publish_data_files(
    table: &Table,
    publish_plan: &CowPublishPlan,
    output_data_files: Vec<DataFile>,
) -> HummockResult<(Vec<DataFile>, CowRewriteStatistics)> {
    let mut planned_snapshot_files =
        live_data_files_for_snapshot(table, publish_plan.snapshot_id).await?;
    let rewrite_statistics = retain_unrewritten_data_files(
        &mut planned_snapshot_files,
        &publish_plan.rewritten_data_file_paths,
    );
    planned_snapshot_files.extend(output_data_files);
    Ok((planned_snapshot_files, rewrite_statistics))
}

fn retain_unrewritten_data_files(
    planned_snapshot_files: &mut Vec<DataFile>,
    rewritten_data_file_paths: &HashSet<String>,
) -> CowRewriteStatistics {
    let mut statistics = CowRewriteStatistics::default();
    planned_snapshot_files.retain(|file| {
        statistics.snapshot_data_file_count = statistics.snapshot_data_file_count.saturating_add(1);
        statistics.snapshot_data_file_size_bytes = statistics
            .snapshot_data_file_size_bytes
            .saturating_add(file.file_size_in_bytes());

        if rewritten_data_file_paths.contains(file.file_path()) {
            statistics.rewritten_data_file_count =
                statistics.rewritten_data_file_count.saturating_add(1);
            statistics.rewritten_data_file_size_bytes = statistics
                .rewritten_data_file_size_bytes
                .saturating_add(file.file_size_in_bytes());
            false
        } else {
            true
        }
    });

    statistics.skipped_rewrite_data_file_count = statistics
        .snapshot_data_file_count
        .saturating_sub(statistics.rewritten_data_file_count);
    statistics.skipped_rewrite_data_file_size_bytes = statistics
        .snapshot_data_file_size_bytes
        .saturating_sub(statistics.rewritten_data_file_size_bytes);
    statistics
}

fn diff_data_files(
    source_files: Vec<DataFile>,
    target_files: Vec<DataFile>,
) -> (Vec<DataFile>, Vec<DataFile>) {
    let source_paths = source_files
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect::<HashSet<_>>();
    let target_paths = target_files
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect::<HashSet<_>>();

    let added_files = source_files
        .into_iter()
        .filter(|file| !target_paths.contains(file.file_path()))
        .collect();
    let deleted_files = target_files
        .into_iter()
        .filter(|file| !source_paths.contains(file.file_path()))
        .collect();
    (added_files, deleted_files)
}

async fn publish_cow_snapshot_to_main(
    task_id: IcebergCompactionTaskId,
    compaction: &Compaction,
    table: &Table,
    ingestion_branch: &str,
    publish_plan: &CowPublishPlan,
    output_data_files: Vec<DataFile>,
) -> HummockResult<()> {
    let (published_files, rewrite_statistics) =
        build_cow_publish_data_files(table, publish_plan, output_data_files).await?;

    tracing::info!(
        iceberg_component = "compaction_worker",
        iceberg_operation = "prune_cow_rewrite",
        task_id = %task_id,
        table = %table.identifier(),
        ingestion_branch,
        planned_snapshot_id = publish_plan.snapshot_id,
        snapshot_data_file_count = rewrite_statistics.snapshot_data_file_count,
        snapshot_data_file_size_bytes = rewrite_statistics.snapshot_data_file_size_bytes,
        rewritten_data_file_count = rewrite_statistics.rewritten_data_file_count,
        rewritten_data_file_size_bytes = rewrite_statistics.rewritten_data_file_size_bytes,
        skipped_rewrite_data_file_count = rewrite_statistics.skipped_rewrite_data_file_count,
        skipped_rewrite_data_file_size_bytes = rewrite_statistics.skipped_rewrite_data_file_size_bytes,
        "iceberg_cow_data_files_pruned",
    );

    let main_files = live_data_files_for_branch(table, MAIN_BRANCH).await?;
    let (added_files, deleted_files) = diff_data_files(published_files, main_files);

    if added_files.is_empty() && deleted_files.is_empty() {
        return Ok(());
    }

    let added_file_count = added_files.len();
    let deleted_file_count = deleted_files.len();
    let consistency_params = CommitConsistencyParams {
        starting_snapshot_id: publish_plan.snapshot_id,
        use_starting_sequence_number: true,
        basic_schema_id: table.metadata().current_schema().schema_id(),
    };
    let commit_manager = compaction.build_commit_manager(consistency_params);
    commit_manager
        .overwrite_files(added_files, deleted_files, MAIN_BRANCH)
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    tracing::info!(
        iceberg_component = "compaction_worker",
        iceberg_operation = "publish_cow_snapshot",
        ingestion_branch,
        planned_snapshot_id = publish_plan.snapshot_id,
        added_file_count,
        deleted_file_count,
        "iceberg_cow_snapshot_published",
    );
    Ok(())
}

fn analyze_task_statistics(plan: &CompactionPlan) -> IcebergCompactionTaskStatistics {
    let mut total_data_file_size: u64 = 0;
    let mut total_data_file_count = 0;
    let mut total_pos_del_file_size: u64 = 0;
    let mut total_pos_del_file_count = 0;
    let mut total_eq_del_file_size: u64 = 0;
    let mut total_eq_del_file_count = 0;

    for data_file in &plan.file_group.data_files {
        total_data_file_size += data_file.file_size_in_bytes;
        total_data_file_count += 1;
    }

    for pos_del_file in &plan.file_group.position_delete_files {
        total_pos_del_file_size += pos_del_file.file_size_in_bytes;
        total_pos_del_file_count += 1;
    }

    for eq_del_file in &plan.file_group.equality_delete_files {
        total_eq_del_file_size += eq_del_file.file_size_in_bytes;
        total_eq_del_file_count += 1;
    }

    IcebergCompactionTaskStatistics {
        total_data_file_size,
        total_data_file_count,
        total_pos_del_file_size,
        total_pos_del_file_count,
        total_eq_del_file_size,
        total_eq_del_file_count,
    }
}

fn build_task_planning_config(
    compaction_kind: IcebergCompactionKind,
    iceberg_config: &IcebergConfig,
    config: &IcebergCompactorRunnerConfig,
    max_file_sequence_number: Option<i64>,
) -> HummockResult<CompactionPlanningConfig> {
    let grouping_strategy = match iceberg_config.write_mode {
        IcebergWriteMode::CopyOnWrite => iceberg_compaction_core::config::GroupingStrategy::Single,
        IcebergWriteMode::MergeOnRead => match config.target_binpack_group_size_mb {
            Some(target_binpack_group_size_mb) => {
                iceberg_compaction_core::config::GroupingStrategy::BinPack(
                    iceberg_compaction_core::config::BinPackConfig::new(
                        target_binpack_group_size_mb * 1024 * 1024,
                    ),
                )
            }
            None => iceberg_compaction_core::config::GroupingStrategy::Single,
        },
    };

    let group_filters =
        if config.min_group_size_mb.is_some() || config.min_group_file_count.is_some() {
            Some(GroupFilters {
                min_group_size_bytes: config.min_group_size_mb.map(|mb| mb * 1024 * 1024),
                min_group_file_count: config.min_group_file_count,
            })
        } else {
            None
        };

    let planning_config = match compaction_kind {
        IcebergCompactionKind::Auto | IcebergCompactionKind::CopyOnWriteAuto => {
            let is_copy_on_write = compaction_kind.is_copy_on_write();
            let mut builder = AutoCompactionConfigBuilder::default();
            builder
                .max_input_parallelism(config.max_parallelism as usize)
                .max_output_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .small_file_threshold_bytes(iceberg_config.small_files_threshold_mb() * 1024 * 1024)
                // COW publishes data files without delete files to `main`, so every data file
                // affected by at least one delete must be rewritten before publication.
                .min_delete_file_count_threshold(if is_copy_on_write {
                    1
                } else {
                    iceberg_config.delete_files_count_threshold()
                })
                .grouping_strategy(grouping_strategy)
                .file_group_scope(if is_copy_on_write {
                    FileGroupScope::Table
                } else {
                    FileGroupScope::Partition
                });

            if let Some(boundary) = max_file_sequence_number {
                builder.max_file_sequence_number(boundary);
            }

            // COW must not filter out a group containing delete-affected files, otherwise its
            // publish-only fallback could expose rows that should have been deleted.
            if !is_copy_on_write && let Some(group_filters) = group_filters {
                builder.group_filters(group_filters);
            }

            let config = builder
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            CompactionPlanningConfig::Auto(config)
        }
        IcebergCompactionKind::SmallFiles => {
            let mut builder = SmallFilesConfigBuilder::default();
            builder
                .max_input_parallelism(config.max_parallelism as usize)
                .max_output_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .small_file_threshold_bytes(iceberg_config.small_files_threshold_mb() * 1024 * 1024)
                .grouping_strategy(grouping_strategy);

            if let Some(boundary) = max_file_sequence_number {
                builder.max_file_sequence_number(boundary);
            }

            if let Some(group_filters) = group_filters {
                builder.group_filters(group_filters);
            }

            let config = builder
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            CompactionPlanningConfig::SmallFiles(config)
        }
        IcebergCompactionKind::Full => {
            let mut builder = FullCompactionConfigBuilder::default();
            builder
                .max_input_parallelism(config.max_parallelism as usize)
                .max_output_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .grouping_strategy(grouping_strategy)
                .file_group_scope(FileGroupScope::Partition);
            if let Some(boundary) = max_file_sequence_number {
                builder.max_file_sequence_number(boundary);
            }
            let config = builder
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            CompactionPlanningConfig::Full(config)
        }
        IcebergCompactionKind::FilesWithDeletes => {
            let mut builder = FilesWithDeletesConfigBuilder::default();
            builder
                .max_input_parallelism(config.max_parallelism as usize)
                .max_output_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .grouping_strategy(grouping_strategy)
                .min_delete_file_count_threshold(iceberg_config.delete_files_count_threshold());
            if let Some(boundary) = max_file_sequence_number {
                builder.max_file_sequence_number(boundary);
            }
            let config = builder
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            CompactionPlanningConfig::FilesWithDeletes(config)
        }
        IcebergCompactionKind::CopyOnWrite => {
            // A COW task publishes the complete ingestion-branch state, but only data files
            // affected by deletes need a physical rewrite. Clean files are carried to main by
            // the metadata diff in `publish_cow_snapshot_to_main`.
            let config = FilesWithDeletesConfigBuilder::default()
                .max_input_parallelism(config.max_parallelism as usize)
                .max_output_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .grouping_strategy(grouping_strategy)
                .file_group_scope(FileGroupScope::Table)
                .min_delete_file_count_threshold(1_usize)
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            CompactionPlanningConfig::FilesWithDeletes(config)
        }
    };

    Ok(planning_config)
}

/// Creates a task execution context from an iceberg compaction task.
pub async fn create_task_execution(
    iceberg_compaction_task: IcebergCompactionTask,
    config: IcebergCompactorRunnerConfig,
    metrics: Arc<CompactorMetrics>,
) -> HummockResult<IcebergTaskExecution> {
    let IcebergCompactionTask {
        task_id,
        sink_id,
        props,
        task_type,
        pk_index_coordinated,
        max_file_sequence_number,
    } = iceberg_compaction_task;

    let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(props))
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let catalog = iceberg_config
        .create_catalog()
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let table_ident = iceberg_config
        .full_table_name()
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let parsed_task_type = TaskType::try_from(task_type)
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    let compaction_kind = IcebergCompactionKind::resolve(parsed_task_type, &iceberg_config)?;
    if max_file_sequence_number.is_some()
        && (pk_index_coordinated || compaction_kind.is_copy_on_write())
    {
        return Err(HummockError::compaction_executor(anyhow::anyhow!(
            "bounded compaction is not supported for copy-on-write tasks"
        )));
    }

    let branch = commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    if let Some(boundary) = max_file_sequence_number {
        // An empty bounded plan is reported as `Drained`, so fail closed unless
        // the loaded branch can prove that this fixed boundary is meaningful.
        if table.metadata().format_version() < FormatVersion::V2 {
            return Err(HummockError::compaction_executor(anyhow::anyhow!(
                "bounded compaction requires Iceberg format V2 or V3"
            )));
        }
        let head = table.metadata().snapshot_for_ref(&branch).ok_or_else(|| {
            HummockError::compaction_executor(anyhow::anyhow!(
                "bounded compaction branch {branch} has no snapshot"
            ))
        })?;
        if head.sequence_number() < boundary {
            return Err(HummockError::compaction_executor(anyhow::anyhow!(
                "bounded compaction head sequence {} is older than boundary {}",
                head.sequence_number(),
                boundary
            )));
        }
    }

    let planning_config = build_task_planning_config(
        compaction_kind,
        &iceberg_config,
        &config,
        max_file_sequence_number,
    )?;

    let compaction_plans = CompactionPlanner::new(planning_config)
        .plan_compaction_with_branch(&table, &branch)
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let compaction_plans = if compaction_plans.is_empty() && compaction_kind.is_copy_on_write() {
        // Keep a publish-only COW task executable when no data file needs a rewrite, for example
        // after inserts that produced only clean data files.
        table
            .metadata()
            .snapshot_for_ref(&branch)
            .map(|snapshot| {
                vec![CompactionPlan::new(
                    FileGroup::empty(),
                    branch.clone(),
                    snapshot.snapshot_id(),
                )]
            })
            .unwrap_or_default()
    } else {
        compaction_plans
    };

    // Each COW plan publishes a complete table state to `main`, so independent plans could
    // overwrite one another instead of composing their rewrites.
    if compaction_kind.is_copy_on_write() && compaction_plans.len() > 1 {
        return Err(HummockError::compaction_executor(anyhow::anyhow!(
            "COW compaction must produce at most one table-scoped plan, got {}",
            compaction_plans.len()
        )));
    }

    if compaction_plans.is_empty() {
        tracing::info!(
            iceberg_component = "compaction_worker",
            iceberg_operation = "plan_task",
            task_id = %task_id,
            sink_id = sink_id,
            task_type = ?compaction_kind,
            table = %table_ident,
            branch = %branch,
            "iceberg_compaction_task_skipped_no_files",
        );
        return Ok(IcebergTaskExecution {
            sink_id,
            plan_runners: vec![],
        });
    }

    let table_schema = table.metadata().current_schema();
    let format_version = table.metadata().format_version();
    let requires_sort = !table.metadata().default_sort_order().fields.is_empty();
    let mut runners = Vec::with_capacity(compaction_plans.len());

    for (plan_index, compaction_plan) in compaction_plans.into_iter().enumerate() {
        let memory_reservation_bytes = estimate_plan_memory(
            &compaction_plan,
            table_schema,
            format_version,
            config.max_record_batch_rows,
            config.enable_prefetch,
            requires_sort,
        );
        runners.push(IcebergCompactionPlanRunner {
            task_id,
            sink_id,
            plan_index,
            catalog: catalog.clone(),
            table_ident: table_ident.clone(),
            iceberg_config: iceberg_config.clone(),
            config: config.clone(),
            metrics: metrics.clone(),
            compaction_kind,
            branch: branch.clone(),
            compaction_plan,
            pk_index_coordinated,
            memory_reservation_bytes,
        });
    }

    tracing::info!(
        iceberg_component = "compaction_worker",
        iceberg_operation = "plan_task",
        task_id = %task_id,
        sink_id = sink_id,
        task_type = ?compaction_kind,
        table = %table_ident,
        branch = %branch,
        plan_count = runners.len(),
        "iceberg_compaction_task_planned",
    );

    Ok(IcebergTaskExecution {
        sink_id,
        plan_runners: runners,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::io::FileIO;
    use iceberg::spec::{
        DataFileBuilder, DataFileFormat, FormatVersion, ManifestListWriter, ManifestWriterBuilder,
        NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, SortOrder, Struct, Summary, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use iceberg::{NamespaceIdent, Runtime};

    use super::*;

    fn test_runner_config() -> IcebergCompactorRunnerConfig {
        IcebergCompactorRunnerConfig {
            max_parallelism: 8,
            min_size_per_partition: 512 * 1024 * 1024,
            max_file_count_per_partition: 16,
            enable_validate_compaction: false,
            max_record_batch_rows: 1024,
            enable_heuristic_output_parallelism: true,
            max_concurrent_closes: 4,
            enable_prefetch: false,
            target_binpack_group_size_mb: Some(64),
            min_group_size_mb: Some(32),
            min_group_file_count: Some(3),
        }
    }

    fn test_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .unwrap()
    }

    fn test_equality_delete_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    fn test_snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        manifest_list: String,
        timestamp_ms: i64,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(manifest_list)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
    }

    fn test_snapshot_ref(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None))
    }

    fn test_table() -> Table {
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::new(1, "id", Type::Primitive(PrimitiveType::Int), false).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "memory://warehouse/test_table".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("test".to_owned()),
                "table".to_owned(),
            ))
            .file_io(FileIO::new_with_memory())
            .runtime(Runtime::try_current().unwrap())
            .metadata(metadata)
            .build()
            .unwrap()
    }

    async fn write_snapshot_manifests(
        table: &Table,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        data_files: Vec<DataFile>,
        delete_files: Vec<DataFile>,
    ) -> String {
        let mut manifests = vec![];

        if !data_files.is_empty() {
            let path =
                format!("memory://warehouse/test_table/metadata/snapshot-{snapshot_id}-data.avro");
            let mut writer = ManifestWriterBuilder::new(
                table.file_io().new_output(&path).unwrap(),
                Some(snapshot_id),
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            for file in data_files {
                writer.add_file(file, snapshot_id).unwrap();
            }
            manifests.push(writer.write_manifest_file().await.unwrap());
        }

        if !delete_files.is_empty() {
            let path = format!(
                "memory://warehouse/test_table/metadata/snapshot-{snapshot_id}-deletes.avro"
            );
            let mut writer = ManifestWriterBuilder::new(
                table.file_io().new_output(&path).unwrap(),
                Some(snapshot_id),
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().as_ref().clone(),
            )
            .build_v2_deletes();
            for file in delete_files {
                writer.add_file(file, snapshot_id).unwrap();
            }
            manifests.push(writer.write_manifest_file().await.unwrap());
        }

        let manifest_list_path = format!(
            "memory://warehouse/test_table/metadata/snapshot-{snapshot_id}-manifest-list.avro"
        );
        let output = table
            .file_io()
            .new_output(&manifest_list_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut writer =
            ManifestListWriter::v2(output, snapshot_id, parent_snapshot_id, snapshot_id);
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        manifest_list_path
    }

    fn sorted_file_paths(files: &[DataFile]) -> Vec<&str> {
        let mut paths = files
            .iter()
            .map(|file| file.file_path())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        paths
    }

    #[cfg_attr(madsim, ignore = "requires Iceberg's native Tokio runtime")]
    #[tokio::test]
    async fn test_cow_publish_file_set_uses_planned_snapshot() {
        let table = test_table();
        let old_file = test_data_file("data/old.parquet");
        let clean_file = test_data_file("data/clean.parquet");
        let late_replacement = test_data_file("data/late-replacement.parquet");
        let equality_delete = test_equality_delete_file("data/late-equality-delete.parquet");

        let planned_manifest_list = write_snapshot_manifests(
            &table,
            1,
            None,
            vec![old_file.clone(), clean_file.clone()],
            vec![],
        )
        .await;
        let latest_manifest_list = write_snapshot_manifests(
            &table,
            2,
            Some(1),
            vec![old_file, clean_file, late_replacement],
            vec![equality_delete],
        )
        .await;
        let base_timestamp_ms = table.metadata().last_updated_ms();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(test_snapshot(
                1,
                None,
                planned_manifest_list,
                base_timestamp_ms + 1,
            ))
            .unwrap()
            .add_snapshot(test_snapshot(
                2,
                Some(1),
                latest_manifest_list,
                base_timestamp_ms + 2,
            ))
            .unwrap()
            .set_ref("ingestion", test_snapshot_ref(2))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = Table::builder()
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .runtime(Runtime::try_current().unwrap())
            .metadata(metadata)
            .build()
            .unwrap();

        assert_eq!(
            sorted_file_paths(
                &live_data_files_for_branch(&table, "ingestion")
                    .await
                    .unwrap()
            ),
            vec![
                "data/clean.parquet",
                "data/late-replacement.parquet",
                "data/old.parquet",
            ]
        );

        let publish_plan = CowPublishPlan {
            snapshot_id: 1,
            rewritten_data_file_paths: HashSet::from(["data/old.parquet".to_owned()]),
        };
        let (published_files, rewrite_statistics) = build_cow_publish_data_files(
            &table,
            &publish_plan,
            vec![test_data_file("data/compacted.parquet")],
        )
        .await
        .unwrap();
        assert_eq!(
            sorted_file_paths(&published_files),
            vec!["data/clean.parquet", "data/compacted.parquet"]
        );
        assert_eq!(rewrite_statistics.snapshot_data_file_count, 2);
        assert_eq!(rewrite_statistics.snapshot_data_file_size_bytes, 2);
        assert_eq!(rewrite_statistics.rewritten_data_file_count, 1);
        assert_eq!(rewrite_statistics.rewritten_data_file_size_bytes, 1);
        assert_eq!(rewrite_statistics.skipped_rewrite_data_file_count, 1);
        assert_eq!(rewrite_statistics.skipped_rewrite_data_file_size_bytes, 1);
    }

    #[test]
    fn test_build_auto_compaction_planning_config() {
        let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from([
            ("connector".to_owned(), "iceberg".to_owned()),
            ("type".to_owned(), "append-only".to_owned()),
            ("force_append_only".to_owned(), "true".to_owned()),
            ("catalog.name".to_owned(), "test-catalog".to_owned()),
            ("catalog.type".to_owned(), "storage".to_owned()),
            ("warehouse.path".to_owned(), "s3://iceberg".to_owned()),
            ("database.name".to_owned(), "test_db".to_owned()),
            ("table.name".to_owned(), "test_table".to_owned()),
            ("compaction.type".to_owned(), "auto".to_owned()),
            (
                "compaction.small_files_threshold_mb".to_owned(),
                "96".to_owned(),
            ),
            (
                "compaction.delete_files_count_threshold".to_owned(),
                "7".to_owned(),
            ),
            (
                "compaction.target_file_size_mb".to_owned(),
                "256".to_owned(),
            ),
        ]))
        .unwrap();
        let runner_config = test_runner_config();

        let CompactionPlanningConfig::Auto(config) = build_task_planning_config(
            IcebergCompactionKind::Auto,
            &iceberg_config,
            &runner_config,
            None,
        )
        .unwrap() else {
            panic!("expected auto planning config");
        };

        assert_eq!(config.max_input_parallelism, 8);
        assert_eq!(config.max_output_parallelism, 8);
        assert_eq!(config.min_size_per_partition, 512 * 1024 * 1024);
        assert_eq!(config.max_file_count_per_partition, 16);
        assert_eq!(config.target_file_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.small_file_threshold_bytes, 96 * 1024 * 1024);
        assert_eq!(config.min_delete_file_count_threshold, 7);
        assert_eq!(config.file_group_scope, FileGroupScope::Partition);

        let iceberg_compaction_core::config::GroupingStrategy::BinPack(bin_pack) =
            config.grouping_strategy
        else {
            panic!("expected bin-pack grouping strategy");
        };
        assert_eq!(bin_pack.target_group_size_bytes, 64 * 1024 * 1024);

        let group_filters = config.group_filters.unwrap();
        assert_eq!(group_filters.min_group_size_bytes, Some(32 * 1024 * 1024));
        assert_eq!(group_filters.min_group_file_count, Some(3));
    }

    #[test]
    fn test_build_cow_auto_compaction_planning_config() {
        let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from([
            ("connector".to_owned(), "iceberg".to_owned()),
            ("type".to_owned(), "upsert".to_owned()),
            ("primary_key".to_owned(), "id".to_owned()),
            ("catalog.name".to_owned(), "test-catalog".to_owned()),
            ("catalog.type".to_owned(), "storage".to_owned()),
            ("warehouse.path".to_owned(), "s3://iceberg".to_owned()),
            ("database.name".to_owned(), "test_db".to_owned()),
            ("table.name".to_owned(), "test_table".to_owned()),
            ("write_mode".to_owned(), "copy-on-write".to_owned()),
            (
                "compaction.small_files_threshold_mb".to_owned(),
                "96".to_owned(),
            ),
            (
                "compaction.delete_files_count_threshold".to_owned(),
                "7".to_owned(),
            ),
        ]))
        .unwrap();

        let compaction_kind =
            IcebergCompactionKind::resolve(TaskType::Auto, &iceberg_config).unwrap();
        assert_eq!(compaction_kind, IcebergCompactionKind::CopyOnWriteAuto);
        assert_eq!(
            IcebergCompactionKind::resolve(TaskType::Full, &iceberg_config).unwrap(),
            IcebergCompactionKind::CopyOnWrite
        );

        let CompactionPlanningConfig::Auto(config) = build_task_planning_config(
            compaction_kind,
            &iceberg_config,
            &test_runner_config(),
            None,
        )
        .unwrap() else {
            panic!("expected auto planning config");
        };

        assert_eq!(config.small_file_threshold_bytes, 96 * 1024 * 1024);
        assert_eq!(config.min_delete_file_count_threshold, 1);
        assert_eq!(config.file_group_scope, FileGroupScope::Table);
        assert_eq!(
            config.grouping_strategy,
            iceberg_compaction_core::config::GroupingStrategy::Single
        );
        assert_eq!(config.group_filters, None);
    }
}
