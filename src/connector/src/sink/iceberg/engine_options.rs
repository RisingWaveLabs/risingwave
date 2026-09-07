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

// THIS FILE IS AUTO_GENERATED. DO NOT EDIT.
// UPDATE WITH: ./risedev generate-with-options

pub const ICEBERG_ENGINE_OPTION_KEYS: &[&str] = &[
    "partition_by",
    "order_key",
    "commit_checkpoint_interval",
    "enable_compaction",
    "compaction_interval_sec",
    "enable_snapshot_expiration",
    "write_mode",
    "format_version",
    "snapshot_expiration_max_age_millis",
    "snapshot_expiration_retain_last",
    "snapshot_expiration_clear_expired_files",
    "snapshot_expiration_clear_expired_meta_data",
    "enable_manifest_rewrite",
    "manifest_rewrite_target_size_bytes",
    "manifest_rewrite_min_count_to_merge",
    "compaction.max_snapshots_num",
    "compaction.small_files_threshold_mb",
    "compaction.delete_files_count_threshold",
    "compaction.trigger_snapshot_count",
    "compaction.target_file_size_mb",
    "compaction.type",
    "compaction.write_parquet_compression",
    "compaction.write_parquet_max_row_group_rows",
    "compaction.write_parquet_max_row_group_bytes",
    "enable_pk_index",
];

pub fn is_iceberg_engine_option(key: &str) -> bool {
    ICEBERG_ENGINE_OPTION_KEYS.contains(&key)
}
