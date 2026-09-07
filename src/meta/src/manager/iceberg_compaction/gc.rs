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

use std::collections::{HashMap, HashSet};

use iceberg::spec::{FormatVersion, ManifestContentType, ManifestFile};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use itertools::Itertools;
use risingwave_connector::sink::SinkError;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::commit_branch;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::*;

const MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, PartialEq, Eq)]
struct ManifestRewritePlan {
    rewrite_paths: HashSet<String>,
    data_manifest_count: usize,
    selected_manifest_count: usize,
    estimated_output_manifest_count: usize,
}

fn plan_manifest_rewrite(
    manifests: &[ManifestFile],
    target_size_bytes: u64,
    min_count_to_merge: usize,
) -> ManifestRewritePlan {
    debug_assert!(target_size_bytes > 0);
    debug_assert!(min_count_to_merge > 0);

    let mut candidates_by_spec = HashMap::<i32, Vec<&ManifestFile>>::new();
    let mut data_manifest_count = 0;
    for manifest in manifests {
        if manifest.content != ManifestContentType::Data {
            continue;
        }
        data_manifest_count += 1;
        if manifest.manifest_length >= 0 && (manifest.manifest_length as u64) < target_size_bytes {
            candidates_by_spec
                .entry(manifest.partition_spec_id)
                .or_default()
                .push(manifest);
        }
    }

    let mut rewrite_paths = HashSet::new();
    let mut estimated_output_manifest_count = 0;
    for mut candidates in candidates_by_spec.into_values() {
        // The iceberg-rust version currently pinned by RisingWave creates one
        // output manifest per cluster key and partition spec. Limit each spec
        // to one target-sized batch so the rewrite cannot replace many small
        // manifests with a single oversized manifest. Later maintenance runs
        // will consume the remaining candidates.
        //
        // Pack from the oldest end so completed bins are rewritten first and
        // the newest under-filled bin can accumulate across maintenance runs.
        // A stable sort preserves manifest-list order for V1 manifests and
        // manifests written by the same snapshot.
        candidates.sort_by_key(|manifest| manifest.sequence_number);
        let mut current_bin = Vec::new();
        let mut current_bin_size_bytes = 0_u64;
        let mut completed_bin = None;
        for candidate in candidates {
            let candidate_size_bytes = candidate.manifest_length as u64;
            let next_size_bytes = current_bin_size_bytes.saturating_add(candidate_size_bytes);
            if !current_bin.is_empty() && next_size_bytes > target_size_bytes {
                if current_bin.len() >= 2 {
                    completed_bin = Some(std::mem::take(&mut current_bin));
                    break;
                }
                current_bin = Vec::new();
                current_bin_size_bytes = 0;
            }

            current_bin.push(candidate);
            current_bin_size_bytes = current_bin_size_bytes.saturating_add(candidate_size_bytes);
        }

        // A completed target-sized bin is always worth merging. The newest
        // under-filled bin is merged only after it reaches the configured
        // count threshold. Either path must reduce at least two manifests to
        // one; otherwise the rewrite would only create snapshot churn.
        let selected = if let Some(completed_bin) = completed_bin {
            completed_bin
        } else if current_bin.len() >= 2
            && (current_bin_size_bytes >= target_size_bytes
                || current_bin.len() >= min_count_to_merge)
        {
            current_bin
        } else {
            continue;
        };

        estimated_output_manifest_count += 1;
        rewrite_paths.extend(
            selected
                .into_iter()
                .map(|manifest| manifest.manifest_path.clone()),
        );
    }

    ManifestRewritePlan {
        selected_manifest_count: rewrite_paths.len(),
        rewrite_paths,
        data_manifest_count,
        estimated_output_manifest_count,
    }
}

fn snapshot_expiration_cutoff_ms(iceberg_config: &IcebergConfig, now: i64) -> i64 {
    iceberg_config
        .snapshot_expiration_timestamp_ms(now)
        .unwrap_or(now - MAX_SNAPSHOT_AGE_MS_DEFAULT)
}

impl IcebergCompactionManager {
    pub fn gc_loop(manager: Arc<Self>, interval_sec: u64) -> (JoinHandle<()>, Sender<()>) {
        assert!(
            interval_sec > 0,
            "Iceberg GC interval must be greater than 0"
        );
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            tracing::info!(
                interval_sec = interval_sec,
                "Starting Iceberg GC loop with configurable interval"
            );
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_sec));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = manager.perform_gc_operations().await {
                            tracing::error!(error = ?e.as_report(), "GC operations failed");
                        }
                    },
                    _ = &mut shutdown_rx => {
                        tracing::info!("Iceberg GC loop is stopped");
                        return;
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    async fn perform_gc_operations(&self) -> MetaResult<()> {
        let (snapshot_expiration_sink_ids, manifest_rewrite_sink_ids) = {
            let guard = self.inner.read();
            (
                guard
                    .snapshot_expiration_sink_ids
                    .iter()
                    .copied()
                    .collect::<Vec<_>>(),
                guard
                    .manifest_rewrite_sink_ids
                    .iter()
                    .copied()
                    .collect::<Vec<_>>(),
            )
        };

        tracing::info!(
            snapshot_expiration_sink_count = snapshot_expiration_sink_ids.len(),
            manifest_rewrite_sink_count = manifest_rewrite_sink_ids.len(),
            "Starting Iceberg metadata maintenance operations",
        );

        // Rewrite first so snapshot expiration in the same tick can clean up
        // manifests replaced by the new snapshot.
        for sink_id in manifest_rewrite_sink_ids {
            if let Err(e) = self.check_and_rewrite_manifests(sink_id).await {
                tracing::error!(
                    error = ?e.as_report(),
                    %sink_id,
                    "Failed to rewrite Iceberg manifests",
                );
            }
        }

        for sink_id in snapshot_expiration_sink_ids {
            if let Err(e) = self.check_and_expire_snapshots(sink_id).await {
                tracing::error!(error = ?e.as_report(), "Failed to perform GC for sink {}", sink_id);
            }
        }

        tracing::info!("Iceberg metadata maintenance operations completed");
        Ok(())
    }

    pub async fn check_and_expire_snapshots(&self, sink_id: SinkId) -> MetaResult<()> {
        let now = chrono::Utc::now().timestamp_millis();

        let iceberg_config = self.load_iceberg_config(sink_id).await?;
        if !iceberg_config.enable_snapshot_expiration {
            let mut guard = self.inner.write();
            guard.snapshot_expiration_sink_ids.remove(&sink_id);
            return Ok(());
        }

        let active_attempt_gc_watermark_snapshot = {
            let guard = self.inner.read();
            guard
                .sink_schedules
                .get(&sink_id)
                .and_then(|track| track.active_attempt_gc_watermark_snapshot())
                .map(|snapshot| snapshot.cloned())
        };

        let mut snapshot_expiration_timestamp_ms =
            snapshot_expiration_cutoff_ms(&iceberg_config, now);

        // Outer `None` means there is no active compaction attempt. Inner `None`
        // means an active attempt has no safe snapshot watermark, so GC skips.
        match active_attempt_gc_watermark_snapshot {
            None => {}
            Some(None) => {
                tracing::info!(
                    catalog_name = iceberg_config.catalog_name(),
                    table_name = iceberg_config.full_table_name()?.to_string(),
                    %sink_id,
                    "Skip snapshot expiration because an active iceberg compaction attempt has no observed GC watermark",
                );
                return Ok(());
            }
            Some(Some(snapshot)) => {
                // A running compaction task may still need snapshots up to its
                // captured watermark, so GC must not expire newer snapshots.
                snapshot_expiration_timestamp_ms =
                    snapshot_expiration_timestamp_ms.min(snapshot.timestamp_ms);
                tracing::info!(
                    catalog_name = iceberg_config.catalog_name(),
                    table_name = iceberg_config.full_table_name()?.to_string(),
                    %sink_id,
                    gc_watermark_branch = %snapshot.branch,
                    gc_watermark_snapshot_id = snapshot.snapshot_id,
                    gc_watermark_timestamp_ms = snapshot.timestamp_ms,
                    protected_snapshot_expiration_timestamp_ms = snapshot_expiration_timestamp_ms,
                    "Protect snapshots expiration with iceberg compaction GC watermark",
                );
            }
        }

        let catalog = iceberg_config.create_catalog().await?;
        let mut table = catalog
            .load_table(&iceberg_config.full_table_name()?)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        let metadata = table.metadata();
        let mut snapshots = metadata.snapshots().collect_vec();
        snapshots.sort_by_key(|s| s.timestamp_ms());

        if snapshots.is_empty()
            || snapshots.first().unwrap().timestamp_ms() > snapshot_expiration_timestamp_ms
        {
            return Ok(());
        }

        tracing::info!(
            catalog_name = iceberg_config.catalog_name(),
            table_name = iceberg_config.full_table_name()?.to_string(),
            %sink_id,
            snapshots_len = snapshots.len(),
            snapshot_expiration_timestamp_ms = snapshot_expiration_timestamp_ms,
            snapshot_expiration_retain_last = ?iceberg_config.snapshot_expiration_retain_last,
            clear_expired_files = ?iceberg_config.snapshot_expiration_clear_expired_files,
            clear_expired_meta_data = ?iceberg_config.snapshot_expiration_clear_expired_meta_data,
            "try trigger snapshots expiration",
        );

        let txn = Transaction::new(&table);

        let mut expired_snapshots = txn
            .expire_snapshots()
            .expire_older_than_ms(snapshot_expiration_timestamp_ms)
            .clear_expired_meta_data(iceberg_config.snapshot_expiration_clear_expired_meta_data);

        if let Some(retain_last) = iceberg_config.snapshot_expiration_retain_last {
            expired_snapshots = expired_snapshots.retain_last(
                usize::try_from(retain_last).map_err(|e| SinkError::Config(e.into()))?,
            );
        }

        let before_metadata = table.metadata_ref();
        let tx = expired_snapshots
            .apply(txn)
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        table = tx
            .commit(catalog.as_ref())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        if iceberg_config.snapshot_expiration_clear_expired_files {
            table
                .cleanup_expired_files(&before_metadata)
                .await
                .map_err(|e| SinkError::Iceberg(e.into()))?;
        }

        tracing::info!(
            catalog_name = iceberg_config.catalog_name(),
            table_name = iceberg_config.full_table_name()?.to_string(),
            %sink_id,
            "Expired snapshots for iceberg table",
        );

        Ok(())
    }

    pub async fn check_and_rewrite_manifests(&self, sink_id: SinkId) -> MetaResult<()> {
        let iceberg_config = self.load_iceberg_config(sink_id).await?;
        if !iceberg_config.enable_manifest_rewrite {
            self.inner
                .write()
                .manifest_rewrite_sink_ids
                .remove(&sink_id);
            return Ok(());
        }

        let catalog = iceberg_config.create_catalog().await?;
        let table_ident = iceberg_config.full_table_name()?;
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        if table.metadata().format_version() >= FormatVersion::V3 {
            // Iceberg format upgrades cannot be downgraded, so stop retrying
            // periodic rewrites for this sink.
            self.inner
                .write()
                .manifest_rewrite_sink_ids
                .remove(&sink_id);
            tracing::warn!(
                iceberg_component = "manifest_maintenance",
                iceberg_operation = "rewrite_manifests",
                catalog_name = iceberg_config.catalog_name(),
                table = %table_ident,
                %sink_id,
                format_version = %table.metadata().format_version(),
                "Skipping manifest rewrite because Iceberg row lineage is enabled",
            );
            return Ok(());
        }

        let branch = commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);
        let Some(current_snapshot) = table.metadata().snapshot_for_ref(&branch) else {
            tracing::debug!(
                iceberg_component = "manifest_maintenance",
                iceberg_operation = "rewrite_manifests",
                table = %table_ident,
                %sink_id,
                %branch,
                "Skipping manifest rewrite because the target branch has no snapshot",
            );
            return Ok(());
        };
        let current_snapshot_id = current_snapshot.snapshot_id();
        let manifest_list = table
            .object_cache()
            .get_manifest_list(current_snapshot, &table.metadata_ref())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        let target_size_bytes = iceberg_config.manifest_rewrite_target_size_bytes();
        let min_count_to_merge = iceberg_config.manifest_rewrite_min_count_to_merge();
        let plan = plan_manifest_rewrite(
            manifest_list.entries(),
            target_size_bytes,
            min_count_to_merge,
        );
        if plan.rewrite_paths.is_empty() {
            tracing::debug!(
                iceberg_component = "manifest_maintenance",
                iceberg_operation = "rewrite_manifests",
                table = %table_ident,
                %sink_id,
                %branch,
                data_manifest_count = plan.data_manifest_count,
                target_size_bytes,
                min_count_to_merge,
                "Skipping manifest rewrite because manifests are not fragmented enough",
            );
            return Ok(());
        }

        tracing::info!(
            iceberg_component = "manifest_maintenance",
            iceberg_operation = "rewrite_manifests",
            catalog_name = iceberg_config.catalog_name(),
            table = %table_ident,
            %sink_id,
            %branch,
            current_snapshot_id,
            data_manifest_count = plan.data_manifest_count,
            selected_manifest_count = plan.selected_manifest_count,
            estimated_output_manifest_count = plan.estimated_output_manifest_count,
            target_size_bytes,
            min_count_to_merge,
            "Starting Iceberg manifest rewrite",
        );

        let selected_manifest_count = plan.selected_manifest_count;
        let estimated_output_manifest_count = plan.estimated_output_manifest_count;
        let rewrite_paths = plan.rewrite_paths;
        let txn = Transaction::new(&table);
        let tx = txn
            .rewrite_manifests()
            .rewrite_if(Box::new(move |manifest| {
                rewrite_paths.contains(&manifest.manifest_path)
            }))
            .cluster_by(Box::new(|_| "risingwave-maintenance".to_owned()))
            .set_target_branch(branch.clone())
            .apply(txn)
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let table = tx
            .commit(catalog.as_ref())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        let Some(rewritten_snapshot) = table.metadata().snapshot_for_ref(&branch) else {
            return Err(anyhow!(
                "Iceberg branch {} disappeared after manifest rewrite for sink {}",
                branch,
                sink_id
            )
            .into());
        };
        if rewritten_snapshot.snapshot_id() == current_snapshot_id {
            tracing::warn!(
                iceberg_component = "manifest_maintenance",
                iceberg_operation = "rewrite_manifests",
                table = %table_ident,
                %sink_id,
                %branch,
                current_snapshot_id,
                selected_manifest_count,
                "Manifest rewrite completed without creating a new snapshot",
            );
            return Ok(());
        }

        let summary = &rewritten_snapshot.summary().additional_properties;
        tracing::info!(
            iceberg_component = "manifest_maintenance",
            iceberg_operation = "rewrite_manifests",
            catalog_name = iceberg_config.catalog_name(),
            table = %table_ident,
            %sink_id,
            %branch,
            previous_snapshot_id = current_snapshot_id,
            snapshot_id = rewritten_snapshot.snapshot_id(),
            selected_manifest_count,
            estimated_output_manifest_count,
            manifests_created = summary.get("manifests-created").map(String::as_str),
            manifests_replaced = summary.get("manifests-replaced").map(String::as_str),
            manifests_kept = summary.get("manifests-kept").map(String::as_str),
            entries_processed = summary.get("entries-processed").map(String::as_str),
            "Iceberg manifest rewrite succeeded",
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(
        path: &str,
        manifest_length: i64,
        partition_spec_id: i32,
        content: ManifestContentType,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_owned(),
            manifest_length,
            partition_spec_id,
            content,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn test_plan_manifest_rewrite_selects_fragmented_data_manifests_per_spec() {
        let manifests = vec![
            manifest("small-a", 40, 0, ManifestContentType::Data),
            manifest("small-b", 40, 0, ManifestContentType::Data),
            manifest("single-other-spec", 40, 1, ManifestContentType::Data),
            manifest("already-large", 100, 0, ManifestContentType::Data),
            manifest("delete", 10, 0, ManifestContentType::Deletes),
        ];

        let plan = plan_manifest_rewrite(&manifests, 100, 2);

        assert_eq!(plan.data_manifest_count, 4);
        assert_eq!(plan.selected_manifest_count, 2);
        assert_eq!(plan.estimated_output_manifest_count, 1);
        assert_eq!(
            plan.rewrite_paths,
            ["small-a".to_owned(), "small-b".to_owned()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn test_plan_manifest_rewrite_skips_when_packing_cannot_reduce_count() {
        let manifests = vec![
            manifest("almost-full-a", 60, 0, ManifestContentType::Data),
            manifest("almost-full-b", 60, 0, ManifestContentType::Data),
        ];

        let plan = plan_manifest_rewrite(&manifests, 100, 2);

        assert!(plan.rewrite_paths.is_empty());
        assert_eq!(plan.selected_manifest_count, 0);
        assert_eq!(plan.estimated_output_manifest_count, 0);
    }

    #[test]
    fn test_plan_manifest_rewrite_limits_each_spec_to_one_target_sized_batch() {
        let manifests = vec![
            manifest("small-a", 40, 0, ManifestContentType::Data),
            manifest("small-b", 40, 0, ManifestContentType::Data),
            manifest("small-c", 40, 0, ManifestContentType::Data),
        ];

        let plan = plan_manifest_rewrite(&manifests, 100, 2);

        assert_eq!(plan.selected_manifest_count, 2);
        assert_eq!(plan.estimated_output_manifest_count, 1);
        assert_eq!(
            plan.rewrite_paths,
            ["small-a".to_owned(), "small-b".to_owned()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn test_plan_manifest_rewrite_selects_oldest_sequence_first() {
        let mut newest = manifest("newest", 40, 0, ManifestContentType::Data);
        newest.sequence_number = 3;
        let mut oldest_a = manifest("oldest-a", 40, 0, ManifestContentType::Data);
        oldest_a.sequence_number = 1;
        let mut middle = manifest("middle", 40, 0, ManifestContentType::Data);
        middle.sequence_number = 2;
        let mut oldest_b = manifest("oldest-b", 40, 0, ManifestContentType::Data);
        oldest_b.sequence_number = 1;

        let plan = plan_manifest_rewrite(&[newest, oldest_a, middle, oldest_b], 100, 2);

        assert_eq!(plan.selected_manifest_count, 2);
        assert_eq!(
            plan.rewrite_paths,
            ["oldest-a".to_owned(), "oldest-b".to_owned()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn test_plan_manifest_rewrite_selects_completed_bin_below_min_count() {
        let manifests = (0..17)
            .map(|index| {
                manifest(
                    &format!("small-{index:03}"),
                    512 * 1024,
                    0,
                    ManifestContentType::Data,
                )
            })
            .collect_vec();

        let plan = plan_manifest_rewrite(&manifests, 8 * 1024 * 1024, 100);

        assert_eq!(plan.data_manifest_count, 17);
        assert_eq!(plan.selected_manifest_count, 16);
        assert_eq!(plan.estimated_output_manifest_count, 1);
        assert!(!plan.rewrite_paths.contains("small-016"));
    }

    #[test]
    fn test_plan_manifest_rewrite_waits_for_underfilled_bin_min_count() {
        let manifests = (0..99)
            .map(|index| {
                manifest(
                    &format!("small-{index:03}"),
                    1024,
                    0,
                    ManifestContentType::Data,
                )
            })
            .collect_vec();

        let plan = plan_manifest_rewrite(&manifests, 1024 * 1024, 100);

        assert_eq!(plan.data_manifest_count, 99);
        assert_eq!(plan.selected_manifest_count, 0);
        assert_eq!(plan.estimated_output_manifest_count, 0);
    }
}
