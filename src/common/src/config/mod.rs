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

//! This pub module defines the structure of the configuration file `risingwave.toml`.
//!
//! [`RwConfig`] corresponds to the whole config file and each other config struct corresponds to a
//! section in `risingwave.toml`.

pub mod batch;
pub use batch::BatchConfig;
pub mod frontend;
pub use frontend::FrontendConfig;
pub mod hba;
pub use hba::{AddressPattern, AuthMethod, ConnectionType, HbaConfig, HbaEntry};
pub mod meta;
pub use meta::{
    CheckpointCompression, CompactionConfig, DefaultParallelism, MetaBackend, MetaConfig,
    MetaStoreConfig,
};
pub mod streaming;
pub use streaming::{AsyncStackTraceOption, StreamingConfig};
pub mod server;
pub use server::{HeapProfilingConfig, ServerConfig};

pub use crate::session_config::SessionInitConfig;
pub mod udf;
pub use udf::UdfConfig;
pub mod storage;
pub use storage::{
    CacheEvictionConfig, EvictionConfig, ObjectStoreConfig, StorageConfig, StorageMemoryConfig,
    extract_storage_memory_config,
};
pub mod merge;
pub mod mutate;
pub mod none_as_empty_string;
pub mod role;
pub mod system;
pub mod utils;
use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;

use anyhow::Context;
use clap::ValueEnum;
use educe::Educe;
pub use merge::*;
use risingwave_common_proc_macro::ConfigDoc;
pub use risingwave_common_proc_macro::OverrideConfig;
use risingwave_pb::meta::SystemParams;
pub use role::*;
use serde::{Deserialize, Serialize, Serializer};
use serde_default::DefaultFromSerde;
use serde_json::Value;
pub use system::SystemConfig;
pub use utils::*;

use crate::for_all_params;

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB

/// [`RwConfig`] corresponds to the whole config file `risingwave.toml`. Each field corresponds to a
/// section.
#[derive(Educe, Clone, Serialize, Deserialize, Default, ConfigDoc)]
#[educe(Debug)]
pub struct RwConfig {
    #[serde(default)]
    #[config_doc(nested)]
    pub server: ServerConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta: MetaConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub batch: BatchConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub frontend: FrontendConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub streaming: StreamingConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub storage: StorageConfig,

    #[serde(default)]
    #[educe(Debug(ignore))]
    #[config_doc(nested)]
    pub system: SystemConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub udf: UdfConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub session_init: SessionInitConfig,

    #[serde(flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

/// `[meta.developer.meta_compute_client_config]`
/// `[meta.developer.meta_stream_client_config]`
/// `[meta.developer.meta_frontend_client_config]`
/// `[batch.developer.batch_compute_client_config]`
/// `[batch.developer.batch_frontend_client_config]`
/// `[streaming.developer.stream_compute_client_config]`
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct RpcClientConfig {
    #[serde(default = "default::developer::rpc_client_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Maximum concurrency when setting up an RPC client pool.
    /// Set to 0 to keep the previous unlimited behavior.
    #[serde(default = "default::developer::rpc_client_pool_setup_concurrency")]
    pub pool_setup_concurrency: usize,
}

pub use risingwave_common_metrics::MetricLevel;

impl RwConfig {
    pub const fn default_connection_pool_size(&self) -> u16 {
        self.server.connection_pool_size
    }

    /// Returns [`streaming::StreamingDeveloperConfig::exchange_connection_pool_size`] if set,
    /// otherwise [`ServerConfig::connection_pool_size`].
    pub fn streaming_exchange_connection_pool_size(&self) -> u16 {
        self.streaming
            .developer
            .exchange_connection_pool_size
            .unwrap_or_else(|| self.default_connection_pool_size())
    }

    /// Returns [`batch::BatchDeveloperConfig::exchange_connection_pool_size`] if set,
    /// otherwise [`ServerConfig::connection_pool_size`].
    pub fn batch_exchange_connection_pool_size(&self) -> u16 {
        self.batch
            .developer
            .exchange_connection_pool_size
            .unwrap_or_else(|| self.default_connection_pool_size())
    }
}

pub mod default {

    pub mod developer {
        use crate::config::streaming::CacheRefillPolicy;

        pub fn meta_cached_traces_num() -> u32 {
            256
        }

        pub fn meta_cached_traces_memory_limit_bytes() -> usize {
            1 << 27 // 128 MiB
        }

        pub fn batch_output_channel_size() -> usize {
            64
        }

        pub fn batch_receiver_channel_size() -> usize {
            1000
        }

        pub fn batch_root_stage_channel_size() -> usize {
            100
        }

        pub fn batch_chunk_size() -> usize {
            1024
        }

        pub fn batch_local_execute_buffer_size() -> usize {
            64
        }

        /// Default to unset to be compatible with the behavior before this config is introduced,
        /// that is, follow the value of `server.connection_pool_size`.
        pub fn batch_exchange_connection_pool_size() -> Option<u16> {
            None
        }

        pub fn stream_enable_executor_row_count() -> bool {
            false
        }

        pub fn connector_message_buffer_size() -> usize {
            16
        }

        pub fn unsafe_stream_extreme_cache_size() -> usize {
            10
        }

        pub fn stream_topn_cache_min_capacity() -> usize {
            10
        }

        pub fn stream_chunk_size() -> usize {
            256
        }

        pub fn stream_exchange_initial_permits() -> usize {
            2048
        }

        pub fn stream_exchange_batched_permits() -> usize {
            256
        }

        pub fn stream_exchange_concurrent_barriers() -> usize {
            1
        }

        pub fn stream_exchange_concurrent_dispatchers() -> usize {
            0
        }

        pub fn stream_project_expr_concurrency() -> usize {
            1
        }

        pub fn stream_project_expr_inflight_request_concurrency() -> usize {
            0
        }

        pub fn stream_dml_channel_initial_permits() -> usize {
            32768
        }

        pub fn stream_max_barrier_batch_size() -> u32 {
            1024
        }

        pub fn stream_hash_agg_max_dirty_groups_heap_size() -> usize {
            64 << 20 // 64MB
        }

        pub fn enable_trivial_move() -> bool {
            true
        }

        pub fn enable_check_task_level_overlap() -> bool {
            false
        }

        pub fn max_trivial_move_task_count_per_loop() -> usize {
            256
        }

        pub fn max_get_task_probe_times() -> usize {
            5
        }

        pub fn actor_cnt_per_worker_parallelism_soft_limit() -> usize {
            100
        }

        pub fn actor_cnt_per_worker_parallelism_hard_limit() -> usize {
            400
        }

        pub fn hummock_time_travel_sst_info_fetch_batch_size() -> usize {
            10_000
        }

        pub fn hummock_time_travel_sst_info_insert_batch_size() -> usize {
            100
        }

        pub fn time_travel_vacuum_interval_sec() -> u64 {
            30
        }

        pub fn time_travel_vacuum_max_version_count() -> Option<u32> {
            Some(10000)
        }

        pub fn hummock_time_travel_epoch_version_insert_batch_size() -> usize {
            1000
        }

        pub fn hummock_time_travel_delta_fetch_batch_size() -> usize {
            100
        }

        pub fn hummock_gc_history_insert_batch_size() -> usize {
            1000
        }

        pub fn hummock_time_travel_filter_out_objects_batch_size() -> usize {
            1000
        }

        pub fn hummock_time_travel_filter_out_objects_v1() -> bool {
            false
        }

        pub fn hummock_time_travel_filter_out_objects_list_version_batch_size() -> usize {
            10
        }

        pub fn hummock_time_travel_filter_out_objects_list_delta_batch_size() -> usize {
            1000
        }

        pub fn memory_controller_threshold_aggressive() -> f64 {
            0.9
        }

        pub fn memory_controller_threshold_graceful() -> f64 {
            0.81
        }

        pub fn memory_controller_threshold_stable() -> f64 {
            0.72
        }

        pub fn memory_controller_eviction_factor_aggressive() -> f64 {
            2.0
        }

        pub fn memory_controller_eviction_factor_graceful() -> f64 {
            1.5
        }

        pub fn memory_controller_eviction_factor_stable() -> f64 {
            1.0
        }

        pub fn memory_controller_update_interval_ms() -> usize {
            100
        }

        pub fn memory_controller_sequence_tls_step() -> u64 {
            128
        }

        pub fn memory_controller_sequence_tls_lag() -> u64 {
            32
        }

        pub fn stream_enable_arrangement_backfill() -> bool {
            true
        }

        pub fn stream_enable_snapshot_backfill() -> bool {
            true
        }

        pub fn enable_shared_source() -> bool {
            true
        }

        pub fn stream_high_join_amplification_threshold() -> usize {
            2048
        }

        pub fn stream_high_gap_fill_amplification_threshold() -> usize {
            2048
        }

        /// Default to 1 to be compatible with the behavior before this config is introduced.
        pub fn stream_exchange_connection_pool_size() -> Option<u16> {
            Some(1)
        }

        pub fn enable_actor_tokio_metrics() -> bool {
            true
        }

        pub fn stream_enable_auto_schema_change() -> bool {
            true
        }

        pub fn switch_jdbc_pg_to_native() -> bool {
            false
        }

        pub fn streaming_hash_join_entry_state_max_rows() -> usize {
            // NOTE(kwannoel): This is just an arbitrary number.
            30000
        }

        pub fn streaming_join_hash_map_evict_interval_rows() -> u32 {
            16
        }

        pub fn streaming_now_progress_ratio() -> Option<f32> {
            None
        }

        pub fn stream_snapshot_iter_rebuild_interval_secs() -> u64 {
            10 * 60
        }

        pub fn enable_explain_analyze_stats() -> bool {
            true
        }

        pub fn rpc_client_connect_timeout_secs() -> u64 {
            5
        }

        pub fn rpc_client_pool_setup_concurrency() -> usize {
            0
        }

        pub fn iceberg_list_interval_sec() -> u64 {
            10
        }

        pub fn iceberg_fetch_batch_size() -> u64 {
            1024
        }

        pub fn iceberg_sink_positional_delete_cache_size() -> usize {
            1024
        }

        pub fn iceberg_sink_write_parquet_max_row_group_rows() -> usize {
            100_000
        }

        pub fn materialize_force_overwrite_on_no_check() -> bool {
            false
        }

        pub fn refresh_scheduler_interval_sec() -> u64 {
            60
        }

        pub fn sync_log_store_pause_duration_ms() -> usize {
            64
        }

        pub fn sync_log_store_buffer_size() -> usize {
            2048
        }

        pub fn disable_sync_log_store_dispatcher() -> bool {
            false
        }

        pub fn table_change_log_insert_batch_size() -> u64 {
            1000
        }

        pub fn table_change_log_delete_batch_size() -> u64 {
            1000
        }

        pub fn table_change_log_truncate_interval_sec() -> u64 {
            600
        }

        pub fn enable_state_table_vnode_stats_pruning() -> bool {
            false
        }

        pub fn enable_locality_sort_buffer() -> bool {
            true
        }

        pub fn locality_sort_buffer_activate_threshold() -> u64 {
            65536
        }

        pub fn cache_refill_policy() -> CacheRefillPolicy {
            CacheRefillPolicy::Enabled
        }

        pub fn enable_vnode_key_stats_for_materialize() -> bool {
            false
        }

        pub fn max_concurrent_kv_log_store_historical_read() -> usize {
            0
        }
    }
}

pub const MAX_META_CACHE_SHARD_BITS: usize = 4;
pub const MIN_BUFFER_SIZE_PER_SHARD: usize = 256;
pub const MAX_BLOCK_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.

#[cfg(test)]
pub mod tests {
    use expect_test::expect;
    use risingwave_license::LicenseKey;

    use super::*;

    fn default_config_for_docs() -> RwConfig {
        let mut config = RwConfig::default();
        // Set `license_key` to empty in the docs to avoid any confusion.
        config.system.license_key = Some(LicenseKey::empty());
        // Keep generated docs and example config aligned with the production-safe default.
        config.frontend.unsafe_enable_local_fs_connector = false;
        config
    }

    /// This test ensures that `config/example.toml` is up-to-date with the default values specified
    /// in this file. Developer should run `./risedev generate-example-config` to update it if this
    /// test fails.
    #[test]
    fn test_example_up_to_date() {
        const HEADER: &str = "# This file is generated by ./risedev generate-example-config
# Check detailed comments in src/common/src/config.rs";

        let actual = expect_test::expect_file!["../../../config/example.toml"];
        let default = toml::to_string(&default_config_for_docs()).expect("failed to serialize");

        let expected = format!("{HEADER}\n\n{default}");
        actual.assert_eq(&expected);

        let expected = rw_config_to_markdown();
        let actual = expect_test::expect_file!["../../../config/docs.md"];
        actual.assert_eq(&expected);
    }

    #[test]
    fn test_session_init_entries_distinguishes_omitted_from_default() {
        let config: RwConfig = toml::from_str(
            r#"
            [session_init]
            streaming_parallelism = "bounded(8)"
            streaming_parallelism_for_table = "default"
            "#,
        )
        .unwrap();

        // Omitted fields are `None`; an explicit `default` is `Some("default")`.
        assert_eq!(
            config.session_init.streaming_parallelism.as_deref(),
            Some("bounded(8)")
        );
        assert_eq!(
            config
                .session_init
                .streaming_parallelism_for_table
                .as_deref(),
            Some("default")
        );
        assert_eq!(config.session_init.streaming_parallelism_for_sink, None);

        // Only explicitly-configured parameters are reported, by their session parameter name.
        assert_eq!(
            config.session_init.entries(),
            vec![
                ("streaming_parallelism", "bounded(8)"),
                ("streaming_parallelism_for_table", "default"),
            ]
        );
    }

    #[test]
    fn test_session_init_rejects_unrecognized_key() {
        let err = toml::from_str::<RwConfig>(
            r#"
            [session_init]
            streaming_parallelism = "bounded(8)"
            not_a_real_param = "oops"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("unknown field `not_a_real_param`"));
    }

    #[derive(Debug)]
    struct ConfigItemDoc {
        desc: String,
        default: String,
    }

    fn rw_config_to_markdown() -> String {
        let mut config_rustdocs = BTreeMap::<String, Vec<(String, String)>>::new();
        RwConfig::config_docs("".to_owned(), &mut config_rustdocs);

        // Section -> Config Name -> ConfigItemDoc
        let mut configs: BTreeMap<String, BTreeMap<String, ConfigItemDoc>> = config_rustdocs
            .into_iter()
            .map(|(k, v)| {
                let docs: BTreeMap<String, ConfigItemDoc> = v
                    .into_iter()
                    .map(|(name, desc)| {
                        (
                            name,
                            ConfigItemDoc {
                                desc,
                                default: "".to_owned(), // unset
                            },
                        )
                    })
                    .collect();
                (k, docs)
            })
            .collect();

        let toml_doc: BTreeMap<String, toml::Value> =
            toml::from_str(&toml::to_string(&default_config_for_docs()).unwrap()).unwrap();
        toml_doc.into_iter().for_each(|(name, value)| {
            set_default_values("".to_owned(), name, value, &mut configs);
        });

        let mut markdown = "# RisingWave System Configurations\n\n".to_owned()
            + "This page is automatically generated by `./risedev generate-example-config`\n";
        for (section, configs) in configs {
            if configs.is_empty() {
                continue;
            }
            markdown.push_str(&format!("\n## {}\n\n", section));
            markdown.push_str("| Config | Description | Default |\n");
            markdown.push_str("|--------|-------------|---------|\n");
            for (config, doc) in configs {
                markdown.push_str(&format!(
                    "| {} | {} | {} |\n",
                    config, doc.desc, doc.default
                ));
            }
        }
        markdown
    }

    fn set_default_values(
        section: String,
        name: String,
        value: toml::Value,
        configs: &mut BTreeMap<String, BTreeMap<String, ConfigItemDoc>>,
    ) {
        // Set the default value if it's a config name-value pair, otherwise it's a sub-section (Table) that should be recursively processed.
        if let toml::Value::Table(table) = value {
            let section_configs: BTreeMap<String, toml::Value> = table.into_iter().collect();
            let sub_section = if section.is_empty() {
                name
            } else {
                format!("{}.{}", section, name)
            };
            section_configs
                .into_iter()
                .for_each(|(k, v)| set_default_values(sub_section.clone(), k, v, configs))
        } else if let Some(t) = configs.get_mut(&section)
            && let Some(item_doc) = t.get_mut(&name)
        {
            item_doc.default = format!("{}", value);
        }
    }

    #[test]
    fn test_object_store_configs_backward_compatibility() {
        // Define configs with the old name and make sure it still works
        {
            let config: RwConfig = toml::from_str(
                r#"
            [storage.object_store]
            object_store_set_atomic_write_dir = true

            [storage.object_store.s3]
            object_store_keepalive_ms = 1
            object_store_send_buffer_size = 1
            object_store_recv_buffer_size = 1
            object_store_nodelay = false

            [storage.object_store.s3.developer]
            object_store_retry_unknown_service_error = true
            object_store_retryable_service_error_codes = ['dummy']


            "#,
            )
            .unwrap();

            assert!(config.storage.object_store.set_atomic_write_dir);
            assert_eq!(config.storage.object_store.s3.keepalive_ms, Some(1));
            assert_eq!(config.storage.object_store.s3.send_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.recv_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.nodelay, Some(false));
            assert!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retry_unknown_service_error
            );
            assert_eq!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retryable_service_error_codes,
                vec!["dummy".to_owned()]
            );
        }

        // Define configs with the new name and make sure it works
        {
            let config: RwConfig = toml::from_str(
                r#"
            [storage.object_store]
            set_atomic_write_dir = true

            [storage.object_store.s3]
            keepalive_ms = 1
            send_buffer_size = 1
            recv_buffer_size = 1
            nodelay = false

            [storage.object_store.s3.developer]
            retry_unknown_service_error = true
            retryable_service_error_codes = ['dummy']


            "#,
            )
            .unwrap();

            assert!(config.storage.object_store.set_atomic_write_dir);
            assert_eq!(config.storage.object_store.s3.keepalive_ms, Some(1));
            assert_eq!(config.storage.object_store.s3.send_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.recv_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.nodelay, Some(false));
            assert!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retry_unknown_service_error
            );
            assert_eq!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retryable_service_error_codes,
                vec!["dummy".to_owned()]
            );
        }
    }

    #[test]
    fn test_file_cache_separated_runtime_config_backward_compatibility() {
        let config: RwConfig = toml::from_str(
            r#"
            [storage.data_file_cache.runtime_config.Separated.read_runtime_options]
            worker_threads = 2
            max_blocking_threads = 4

            [storage.data_file_cache.runtime_config.Separated.write_runtime_options]
            worker_threads = 6
            max_blocking_threads = 8
            "#,
        )
        .unwrap();

        let storage::FileCacheRuntimeConfig::Separated {
            read_runtime_options,
            write_runtime_options,
        } = config.storage.data_file_cache.runtime_config
        else {
            panic!("expected legacy separated file-cache runtime config");
        };
        assert_eq!(read_runtime_options.worker_threads, 2);
        assert_eq!(read_runtime_options.max_blocking_threads, 4);
        assert_eq!(write_runtime_options.worker_threads, 6);
        assert_eq!(write_runtime_options.max_blocking_threads, 8);
    }

    #[test]
    fn test_meta_configs_backward_compatibility() {
        // Test periodic_space_reclaim_compaction_interval_sec
        {
            let config: RwConfig = toml::from_str(
                r#"
            [meta]
            periodic_split_compact_group_interval_sec = 1
            table_write_throughput_threshold = 10
            min_table_split_write_throughput = 5
            "#,
            )
            .unwrap();

            assert_eq!(
                config
                    .meta
                    .periodic_scheduling_compaction_group_split_interval_sec,
                1
            );
            assert_eq!(config.meta.table_high_write_throughput_threshold, 10);
            assert_eq!(config.meta.table_low_write_throughput_threshold, 5);
        }
    }

    #[test]
    fn test_meta_max_normalize_splits_per_round_must_be_positive() {
        let config = toml::from_str::<RwConfig>(
            r#"
            [meta]
            max_normalize_splits_per_round = 0
            "#,
        )
        .unwrap_err();

        expect![[r#"
            TOML parse error at line 3, column 46
              |
            3 |             max_normalize_splits_per_round = 0
              |                                              ^
            meta.max_normalize_splits_per_round must be greater than 0
        "#]]
        .assert_eq(&config.to_string());
    }

    // Previously, we have prefixes like `stream_` for all configs under `streaming.developer`.
    // Later we removed the prefixes, but we still want to guarantee the backward compatibility.
    #[test]
    fn test_prefix_alias() {
        let config: RwConfig = toml::from_str(
            "
            [streaming.developer]
            stream_chunk_size = 114514

            [streaming.developer.stream_compute_client_config]
            connect_timeout_secs = 42
            pool_setup_concurrency = 10
            ",
        )
        .unwrap();

        assert_eq!(config.streaming.developer.chunk_size, 114514);
        assert_eq!(
            config
                .streaming
                .developer
                .compute_client_config
                .connect_timeout_secs,
            42
        );
        assert_eq!(
            config
                .streaming
                .developer
                .compute_client_config
                .pool_setup_concurrency,
            10
        );
    }

    #[test]
    fn test_prefix_alias_duplicate() {
        let config = toml::from_str::<RwConfig>(
            "
            [streaming.developer]
            stream_chunk_size = 114514
            chunk_size = 1919810
            ",
        )
        .unwrap_err();

        expect![[r#"
            TOML parse error at line 2, column 13
              |
            2 |             [streaming.developer]
              |             ^^^^^^^^^^^^^^^^^^^^^
            duplicate field `chunk_size`
        "#]]
        .assert_eq(&config.to_string());

        let config = toml::from_str::<RwConfig>(
            "
            [streaming.developer.stream_compute_client_config]
            connect_timeout_secs = 5

            [streaming.developer.compute_client_config]
            connect_timeout_secs = 10
            ",
        )
        .unwrap_err();

        expect![[r#"
            TOML parse error at line 2, column 24
              |
            2 |             [streaming.developer.stream_compute_client_config]
              |                        ^^^^^^^^^
            duplicate field `compute_client_config`
        "#]]
        .assert_eq(&config.to_string());
    }

    #[test]
    fn test_storage_max_prefetch_block_number_must_be_positive() {
        let config = toml::from_str::<RwConfig>(
            r#"
            [storage]
            max_prefetch_block_number = 0
            "#,
        )
        .unwrap_err();

        expect![[r#"
            TOML parse error at line 3, column 41
              |
            3 |             max_prefetch_block_number = 0
              |                                         ^
            storage.max_prefetch_block_number must be greater than 0
        "#]]
        .assert_eq(&config.to_string());
    }

    #[test]
    fn test_storage_iceberg_compaction_pull_interval_ms_must_be_positive() {
        let config = toml::from_str::<RwConfig>(
            r#"
            [storage]
            iceberg_compaction_pull_interval_ms = 0
            "#,
        )
        .unwrap_err();

        expect![[r#"
            TOML parse error at line 3, column 51
              |
            3 |             iceberg_compaction_pull_interval_ms = 0
              |                                                   ^
            storage.iceberg_compaction_pull_interval_ms must be greater than 0
        "#]]
        .assert_eq(&config.to_string());
    }
}
