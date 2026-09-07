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

mod iceberg_query_storage_mode;
mod locality_backfill_mode;
mod non_zero64;
mod opt;
pub mod parallelism;
mod query_mode;
mod search_path;
pub mod sink_decouple;
mod statement_timeout;
mod transaction_isolation_level;
mod visibility_mode;

use chrono_tz::Tz;
pub use iceberg_query_storage_mode::IcebergQueryStorageMode;
use itertools::Itertools;
pub use locality_backfill_mode::LocalityBackfillMode;
pub use opt::OptionConfig;
pub use query_mode::QueryMode;
use risingwave_common_proc_macro::{ConfigDoc, SessionConfig};
pub use search_path::{SearchPath, USER_NAME_WILD_CARD};
use serde::{Deserialize, Serialize};
pub use statement_timeout::StatementTimeout;
use thiserror::Error;

use self::non_zero64::ConfigNonZeroU64;
use crate::config::mutate::TomlTableMutateExt;
use crate::config::streaming::{CacheRefillPolicy, JoinEncodingType, OverWindowCachePolicy};
use crate::config::{ConfigMergeError, StreamingConfig, merge_streaming_config_section};
use crate::hash::VirtualNode;
use crate::session_config::parallelism::{ConfigBackfillParallelism, ConfigParallelism};
use crate::session_config::sink_decouple::SinkDecouple;
use crate::session_config::transaction_isolation_level::IsolationLevel;
pub use crate::session_config::visibility_mode::VisibilityMode;
use crate::{PG_VERSION, SERVER_ENCODING, SERVER_VERSION_NUM, STANDARD_CONFORMING_STRINGS};

pub const SESSION_CONFIG_LIST_SEP: &str = ", ";

#[derive(Error, Debug)]
pub enum SessionConfigError {
    #[error("Invalid value `{value}` for `{entry}`")]
    InvalidValue {
        entry: &'static str,
        value: String,
        source: anyhow::Error,
    },

    #[error("Unrecognized config entry `{0}`")]
    UnrecognizedEntry(String),
}

type SessionConfigResult<T> = std::result::Result<T, SessionConfigError>;

const AUTO_LOCALITY_BACKFILL_MIN_SIZE: u64 = 10 * 1024 * 1024 * 1024;

fn default_auto_locality_backfill_min_size() -> u64 {
    AUTO_LOCALITY_BACKFILL_MIN_SIZE
}

fn default_legacy_locality_backfill_mode() -> LocalityBackfillMode {
    LocalityBackfillMode::Always
}

// NOTE(kwannoel): We declare it separately as a constant,
// otherwise seems like it can't infer the type of -1 when written inline.
const DISABLE_BACKFILL_RATE_LIMIT: i32 = -1;
const DISABLE_SOURCE_RATE_LIMIT: i32 = -1;
const DISABLE_DML_RATE_LIMIT: i32 = -1;
const DISABLE_SINK_RATE_LIMIT: i32 = -1;

/// Default to bypass cluster limits iff in debug mode.
const BYPASS_CLUSTER_LIMITS: bool = cfg!(debug_assertions);

/// This is the Session Config of RisingWave.
///
/// All config entries implement `Display` and `FromStr` for getter and setter, to be read and
/// altered within a session.
///
/// Users can change the default value of a configuration entry using `ALTER SYSTEM SET`. To
/// facilitate this, a `serde` implementation is used as the wire format for retrieving initial
/// configurations and updates from the meta service. It's important to note that the meta
/// service stores the overridden value of each configuration entry per row with `Display` in
/// the meta store, rather than using the `serde` format. However, we still delegate the `serde`
/// impl of all fields to `Display`/`FromStr` to make it consistent.
#[serde_with::apply(_ => #[serde_as(as = "serde_with::DisplayFromStr")] )]
#[serde_with::serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, SessionConfig, ConfigDoc, PartialEq)]
pub struct SessionConfig {
    /// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
    /// until the entire dataflow is refreshed. In other words, every related table & MV will
    /// be able to see the write.
    #[parameter(default = false, alias = "rw_implicit_flush")]
    implicit_flush: bool,

    /// If `DML_WAIT_PERSISTENCE` is on, then every INSERT/UPDATE/DELETE statement waits until
    /// the transaction is included in a checkpoint. This is ignored when `IMPLICIT_FLUSH` is on.
    #[parameter(default = false)]
    dml_wait_persistence: bool,

    /// If `CREATE_COMPACTION_GROUP_FOR_MV` is on, dedicated compaction groups will be created in
    /// MV creation.
    #[parameter(default = false)]
    create_compaction_group_for_mv: bool,

    /// A temporary config variable to force query running in either local or distributed mode.
    /// The default value is auto which means let the system decide to run batch queries in local
    /// or distributed mode automatically.
    #[parameter(default = QueryMode::default())]
    query_mode: QueryMode,

    /// For Iceberg engine tables, which storage to use for batch SELECT: Iceberg (columnar) or
    /// Hummock (row). Only affects batch SELECT on tables with ENGINE = ICEBERG.
    #[parameter(default = IcebergQueryStorageMode::default())]
    iceberg_query_storage_mode: IcebergQueryStorageMode,

    /// Sets the number of digits displayed for floating-point values.
    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#:~:text=for%20more%20information.-,extra_float_digits,-(integer)>
    #[parameter(default = 1)]
    extra_float_digits: i32,

    /// Sets the application name to be reported in statistics and logs.
    /// See <https://www.postgresql.org/docs/14/runtime-config-logging.html#:~:text=What%20to%20Log-,application_name,-(string)>
    #[parameter(default = "", flags = "REPORT")]
    application_name: String,

    /// It is typically set by an application upon connection to the server.
    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE>
    #[parameter(default = "", rename = "datestyle")]
    date_style: String,

    /// Force the use of lookup join instead of hash join when possible for local batch execution.
    #[parameter(default = true, alias = "rw_batch_enable_lookup_join")]
    batch_enable_lookup_join: bool,

    /// Enable usage of sortAgg instead of hash agg when order property is satisfied in batch
    /// execution
    #[parameter(default = true, alias = "rw_batch_enable_sort_agg")]
    batch_enable_sort_agg: bool,

    /// Enable distributed DML, so an insert, delete, and update statement can be executed in a distributed way (e.g. running in multiple compute nodes).
    /// No atomicity guarantee in this mode. Its goal is to gain the best ingestion performance for initial batch ingestion where users always can drop their table when failure happens.
    #[parameter(default = false, rename = "batch_enable_distributed_dml")]
    batch_enable_distributed_dml: bool,

    /// Evaluate expression in strict mode for batch queries.
    /// If set to false, an expression failure will not cause an error but leave a null value
    /// on the result set.
    #[parameter(default = true)]
    batch_expr_strict_mode: bool,

    /// The max gap allowed to transform small range scan into multi point lookup.
    #[parameter(default = 8)]
    max_split_range_gap: i32,

    /// Sets the order in which schemas are searched when an object (table, data type, function, etc.)
    /// is referenced by a simple name with no schema specified.
    /// See <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
    #[parameter(default = SearchPath::default())]
    search_path: SearchPath,

    /// If `VISIBILITY_MODE` is all, we will support querying data without checkpoint.
    #[parameter(default = VisibilityMode::default())]
    visibility_mode: VisibilityMode,

    /// See <https://www.postgresql.org/docs/current/transaction-iso.html>
    #[parameter(default = IsolationLevel::default())]
    transaction_isolation: IsolationLevel,

    /// Select as of specific epoch.
    /// Sets the historical epoch for querying data. If 0, querying latest data.
    #[parameter(default = ConfigNonZeroU64::default())]
    query_epoch: ConfigNonZeroU64,

    /// Session timezone. Defaults to UTC.
    #[parameter(default = "UTC", check_hook = check_timezone)]
    timezone: String,

    /// The execution parallelism for streaming queries, including tables, materialized views,
    /// indexes, and sinks. Defaults to `default`, which preserves the legacy adaptive
    /// scheduling behavior during effective resolution.
    #[parameter(default = ConfigParallelism::Default, flags = "SESSION_INIT")]
    streaming_parallelism: ConfigParallelism,

    /// Specific parallelism for backfill. Only `default` and a fixed positive integer are
    /// supported here. Adaptive backfill strategies are deferred to a later change.
    #[parameter(
        default = ConfigBackfillParallelism::Default,
        check_hook = check_streaming_parallelism_for_backfill,
        flags = "SESSION_INIT"
    )]
    streaming_parallelism_for_backfill: ConfigBackfillParallelism,

    /// Specific parallelism for table. Defaults to `default`, which preserves the legacy
    /// bounded adaptive behavior only when the global parallelism itself remains `default`.
    /// Otherwise it follows the explicit global parallelism.
    #[parameter(default = ConfigParallelism::Default, flags = "SESSION_INIT")]
    streaming_parallelism_for_table: ConfigParallelism,

    /// Specific parallelism for sink. By default, it will fall back to `STREAMING_PARALLELISM`.
    #[parameter(default = ConfigParallelism::Default, flags = "SESSION_INIT")]
    streaming_parallelism_for_sink: ConfigParallelism,

    /// Specific parallelism for index. By default, it will fall back to `STREAMING_PARALLELISM`.
    #[parameter(default = ConfigParallelism::Default, flags = "SESSION_INIT")]
    streaming_parallelism_for_index: ConfigParallelism,

    /// Specific parallelism for source. Defaults to `default`, which preserves the legacy
    /// bounded adaptive behavior only when the global parallelism itself remains `default`.
    /// Otherwise it follows the explicit global parallelism.
    #[parameter(default = ConfigParallelism::Default, flags = "SESSION_INIT")]
    streaming_parallelism_for_source: ConfigParallelism,

    /// Specific parallelism for materialized view. By default, it will fall back to `STREAMING_PARALLELISM`.
    #[parameter(default = ConfigParallelism::Default, flags = "SESSION_INIT")]
    streaming_parallelism_for_materialized_view: ConfigParallelism,

    /// Enable delta join for streaming queries. Defaults to false.
    #[parameter(default = false, alias = "rw_streaming_enable_delta_join")]
    streaming_enable_delta_join: bool,

    /// Enable bushy join for streaming queries. Defaults to true.
    #[parameter(default = true, alias = "rw_streaming_enable_bushy_join")]
    streaming_enable_bushy_join: bool,

    /// Force filtering to be done inside the join whenever there's a choice between optimizations.
    /// Defaults to false.
    #[parameter(default = false, alias = "rw_streaming_force_filter_inside_join")]
    streaming_force_filter_inside_join: bool,

    /// Deprecated. Arrangement backfill is always used as the fallback backfill type for new
    /// streaming jobs, and this setting is ignored.
    #[parameter(
        default = true,
        deprecated = "The session variable STREAMING_USE_ARRANGEMENT_BACKFILL has been deprecated and is ignored. Arrangement backfill is always used as the fallback backfill type for new streaming jobs."
    )]
    streaming_use_arrangement_backfill: bool,

    #[parameter(default = true)]
    streaming_use_snapshot_backfill: bool,

    /// Enable serverless backfill for streaming queries. Defaults to false.
    #[parameter(default = false)]
    enable_serverless_backfill: bool,

    /// Allow `jsonb` in stream key
    #[parameter(default = false, alias = "rw_streaming_allow_jsonb_in_stream_key")]
    streaming_allow_jsonb_in_stream_key: bool,

    /// Unsafe: allow impure expressions on non-append-only streams without materialization.
    ///
    /// This may lead to inconsistent results or panics due to re-evaluation on updates/retracts.
    #[parameter(default = false)]
    streaming_unsafe_allow_unmaterialized_impure_expr: bool,

    /// Unsafe: allow an upsert sink to use downstream primary-key columns that are not part of
    /// the upstream stream key.
    ///
    /// This may leave stale rows in the downstream system if a downstream primary-key column
    /// changes without the upsert stream providing its old value.
    #[parameter(default = false)]
    streaming_unsafe_allow_upsert_sink_pk_mismatch: bool,

    /// Separate consecutive `StreamHashJoin` by no-shuffle `StreamExchange`
    #[parameter(default = false)]
    streaming_separate_consecutive_join: bool,

    /// Separate `StreamSink` by no-shuffle `StreamExchange`
    #[parameter(default = false)]
    streaming_separate_sink: bool,

    /// Determine which encoding will be used to encode join rows in operator cache.
    ///
    /// This overrides the corresponding entry from the `[streaming.developer]` section in the config file,
    /// taking effect for new streaming jobs created in the current session.
    #[parameter(default = None)]
    streaming_join_encoding: OptionConfig<JoinEncodingType>,

    /// Enable join ordering for streaming and batch queries. Defaults to true.
    #[parameter(default = true, alias = "rw_enable_join_ordering")]
    enable_join_ordering: bool,

    /// Enable two phase agg optimization. Defaults to true.
    /// Setting this to true will always set `FORCE_TWO_PHASE_AGG` to false.
    #[parameter(default = true, flags = "SETTER", alias = "rw_enable_two_phase_agg")]
    enable_two_phase_agg: bool,

    /// Force two phase agg optimization whenever there's a choice between
    /// optimizations. Defaults to false.
    /// Setting this to true will always set `ENABLE_TWO_PHASE_AGG` to false.
    #[parameter(default = false, flags = "SETTER", alias = "rw_force_two_phase_agg")]
    force_two_phase_agg: bool,

    /// Enable sharing of common sub-plans.
    /// This means that DAG structured query plans can be constructed,
    #[parameter(default = true, alias = "rw_enable_share_plan")]
    /// rather than only tree structured query plans.
    enable_share_plan: bool,

    /// Enable split distinct agg
    #[parameter(default = false, alias = "rw_force_split_distinct_agg")]
    force_split_distinct_agg: bool,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-INTERVALSTYLE>
    #[parameter(default = "", rename = "intervalstyle")]
    interval_style: String,

    /// If `BATCH_PARALLELISM` is non-zero, batch queries will use this parallelism.
    #[parameter(default = ConfigNonZeroU64::default())]
    batch_parallelism: ConfigNonZeroU64,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[parameter(default = PG_VERSION)]
    server_version: String,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[parameter(default = SERVER_VERSION_NUM)]
    server_version_num: i32,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES>
    #[parameter(default = "notice")]
    client_min_messages: String,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-ENCODING>
    #[parameter(default = SERVER_ENCODING, check_hook = check_client_encoding)]
    client_encoding: String,

    /// Enable decoupling sink and internal streaming graph or not
    #[parameter(default = SinkDecouple::default())]
    sink_decouple: SinkDecouple,

    /// See <https://www.postgresql.org/docs/current/runtime-config-compatible.html#RUNTIME-CONFIG-COMPATIBLE-VERSION>
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = false)]
    synchronize_seqscans: bool,

    /// Abort query statement that takes more than the specified amount of time in sec. If
    /// `log_min_error_statement` is set to ERROR or lower, the statement that timed out will also be
    /// logged. If this value is specified without units, it is taken as milliseconds. A value of
    /// zero (the default) disables the timeout.
    #[parameter(default = StatementTimeout::default())]
    statement_timeout: StatementTimeout,

    /// Terminate any session that has been idle (that is, waiting for a client query) within an open transaction for longer than the specified amount of time in milliseconds.
    #[parameter(default = 60000u32)]
    idle_in_transaction_session_timeout: u32,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-LOCK-TIMEOUT>
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = 0)]
    lock_timeout: i32,

    /// For limiting the startup time of a shareable CDC streaming source when the source is being created. Unit: seconds.
    #[parameter(default = 60)]
    cdc_source_wait_streaming_start_timeout: i32,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-ROW-SECURITY>.
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = true)]
    row_security: bool,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
    #[parameter(default = STANDARD_CONFORMING_STRINGS)]
    standard_conforming_strings: String,

    /// Set streaming rate limit (rows per second) for each parallelism for mv / source / sink backfilling
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the snapshot read / source read.
    #[parameter(default = DISABLE_BACKFILL_RATE_LIMIT)]
    backfill_rate_limit: i32,

    /// Set streaming rate limit (rows per second) for each parallelism for mv / source backfilling, source reads.
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the snapshot read / source read.
    #[parameter(default = DISABLE_SOURCE_RATE_LIMIT)]
    source_rate_limit: i32,

    /// Set streaming rate limit (rows per second) for each parallelism for table DML.
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the DML.
    #[parameter(default = DISABLE_DML_RATE_LIMIT)]
    dml_rate_limit: i32,

    /// Set sink rate limit (rows per second) for each parallelism for external sink.
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the sink.
    #[parameter(default = DISABLE_SINK_RATE_LIMIT)]
    sink_rate_limit: i32,

    /// Cache policy for partition cache in streaming over window.
    /// Can be `full`, `recent`, `recent_first_n` or `recent_last_n`.
    ///
    /// This overrides the corresponding entry from the `[streaming.developer]` section in the config file,
    /// taking effect for new streaming jobs created in the current session.
    #[parameter(default = None, alias = "rw_streaming_over_window_cache_policy")]
    streaming_over_window_cache_policy: OptionConfig<OverWindowCachePolicy>,

    /// Cache refill policy for streaming cache refill feature.
    /// Can be `enabled`, `disabled`, `streaming`, `serving` or `both`.
    ///
    /// This overrides the corresponding entry from the `[streaming.developer]` section in the config file,
    /// taking effect for new streaming jobs created in the current session.
    #[parameter(default = None)]
    streaming_cache_refill_policy: OptionConfig<CacheRefillPolicy>,

    /// Run DDL statements in background
    #[parameter(default = false)]
    background_ddl: bool,

    /// Enable shared source. Currently only for Kafka.
    ///
    /// When enabled, `CREATE SOURCE` will create a source streaming job, and `CREATE MATERIALIZED VIEWS` from the source
    /// will forward the data from the same source streaming job, and also backfill prior data from the external source.
    #[parameter(default = true)]
    streaming_use_shared_source: bool,

    /// Enable in-memory cache for `AsOf` join executor.
    ///
    /// When enabled (default), `AsOf` join uses the cache-based implementation.
    ///
    /// When disabled, `AsOf` join uses a no-cache implementation that directly queries
    /// the state table on-demand, reducing unnecessary data fetches for cache.
    #[parameter(default = true)]
    streaming_asof_join_use_cache: bool,

    /// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time.
    #[parameter(default = SERVER_ENCODING)]
    server_encoding: String,

    #[parameter(default = "hex", check_hook = check_bytea_output)]
    bytea_output: String,

    /// Bypass checks on cluster limits
    ///
    /// When enabled, `CREATE MATERIALIZED VIEW` will not fail if the cluster limit is hit.
    #[parameter(default = BYPASS_CLUSTER_LIMITS)]
    bypass_cluster_limits: bool,

    /// The maximum number of parallelism a streaming query can use. Defaults to 256.
    ///
    /// Compared to `STREAMING_PARALLELISM`, which configures the initial parallelism, this configures
    /// the maximum parallelism a streaming query can use in the future, if the cluster size changes or
    /// users manually change the parallelism with `ALTER .. SET PARALLELISM`.
    ///
    /// It's not always a good idea to set this to a very large number, as it may cause performance
    /// degradation when performing range scans on the table or the materialized view.
    // a.k.a. vnode count
    #[parameter(default = VirtualNode::COUNT_FOR_COMPAT, check_hook = check_streaming_max_parallelism)]
    streaming_max_parallelism: usize,

    /// Used to provide the connection information for the iceberg engine.
    /// Format: `iceberg_engine_connection` = `schema_name.connection_name`.
    #[parameter(default = "", check_hook = check_iceberg_engine_connection)]
    iceberg_engine_connection: String,

    /// Whether the streaming join should be unaligned or not.
    #[parameter(default = false)]
    streaming_enable_unaligned_join: bool,

    /// The timeout for reading from the buffer of the sync log store on barrier.
    /// Every epoch we will attempt to read the full buffer of the sync log store.
    /// If we hit the timeout, we will stop reading and continue.
    ///
    /// This overrides the corresponding entry from the `[streaming.developer]` section in the config file,
    /// taking effect for new streaming jobs created in the current session.
    #[parameter(default = None)]
    streaming_sync_log_store_pause_duration_ms: OptionConfig<usize>,

    /// The max buffer size for sync logstore, before we start flushing.
    ///
    /// This overrides the corresponding entry from the `[streaming.developer]` section in the config file,
    /// taking effect for new streaming jobs created in the current session.
    #[parameter(default = None)]
    streaming_sync_log_store_buffer_size: OptionConfig<usize>,

    /// Whether to disable purifying the definition of the table or source upon retrieval.
    /// Only set this if encountering issues with functionalities like `SHOW` or `ALTER TABLE/SOURCE`.
    /// This config may be removed in the future.
    #[parameter(default = false, flags = "NO_ALTER_SYS")]
    disable_purify_definition: bool,

    /// The `ef_search` used in querying hnsw vector index
    #[parameter(default = 40_usize)] // default value borrowed from pg_vector
    batch_hnsw_ef_search: usize,

    /// Enable index selection for queries
    #[parameter(default = true)]
    enable_index_selection: bool,

    /// Enable mv selection for queries
    #[parameter(default = false)]
    enable_mv_selection: bool,

    /// Whether to enable locality backfill. When enabled, `locality_backfill_mode` controls
    /// whether it is selected automatically or always used.
    #[parameter(default = true)]
    enable_locality_backfill: bool,

    /// How to apply locality backfill when it is enabled. `auto` uses the estimated backfill size,
    /// while `always` skips the size check. Missing values from older versions mean `always` to
    /// preserve the previous boolean behavior.
    #[serde(default = "default_legacy_locality_backfill_mode")]
    #[parameter(default = LocalityBackfillMode::Auto)]
    locality_backfill_mode: LocalityBackfillMode,

    /// Auto-enable locality backfill when estimated scan backfill data reaches this size in bytes.
    /// Defaults to 10 `GiB` (10737418240 bytes).
    #[serde(default = "default_auto_locality_backfill_min_size")]
    #[parameter(default = AUTO_LOCALITY_BACKFILL_MIN_SIZE)]
    auto_locality_backfill_min_size: u64,

    /// Enable the sort buffer of locality providers for streaming queries created in this session.
    /// When enabled, locality providers buffer and re-order amplified intermediate results (e.g.,
    /// caused by join amplification) within an epoch to provide better locality for downstream
    /// operators, with bounded memory usage. Only takes effect on streaming jobs planned with
    /// locality providers (see `enable_locality_backfill`). Defaults to true.
    #[parameter(default = true)]
    enable_locality_sort_buffer: bool,

    /// Duration in seconds before notifying the user that a long-running DDL operation (e.g., DROP TABLE, CANCEL JOBS)
    /// is still running. Set to 0 to disable notifications. Defaults to 30 seconds.
    #[parameter(default = 30u32)]
    slow_ddl_notification_secs: u32,

    /// Unsafe: Enable storage retention for non-append-only tables.
    /// Enabling this can lead to streaming inconsistency and node panic
    /// if there is any row INSERT/UPDATE/DELETE operation corresponding to the ttled primary key.
    #[parameter(default = false)]
    unsafe_enable_storage_retention_for_non_append_only_tables: bool,

    /// Enable DataFusion Engine
    /// When enabled, queries involving Iceberg tables will be executed using the DataFusion engine.
    #[parameter(default = true)]
    enable_datafusion_engine: bool,

    /// Prefer hash join over sort merge join in DataFusion engine
    /// When enabled, the DataFusion engine will prioritize hash joins for query execution plans,
    /// potentially improving performance for certain workloads, but may cause OOM for large datasets.
    #[parameter(default = true)]
    datafusion_prefer_hash_join: bool,

    /// Emit chunks in upsert format for `UPDATE` and `DELETE` DMLs.
    /// May lead to undefined behavior if the table is created with `ON CONFLICT DO NOTHING`.
    ///
    /// When enabled:
    /// - `UPDATE` will only emit `Insert` records for new rows, instead of `Update` records.
    /// - `DELETE` will only include key columns and pad the rest with NULL, instead of emitting complete rows.
    #[parameter(default = false)]
    upsert_dml: bool,
}

fn check_iceberg_engine_connection(val: &str) -> Result<(), String> {
    if val.is_empty() {
        return Ok(());
    }

    let parts: Vec<&str> = val.split('.').collect();
    if parts.len() != 2 {
        return Err("Invalid iceberg engine connection format, Should be set to this format: schema_name.connection_name.".to_owned());
    }

    Ok(())
}

fn check_timezone(val: &str) -> Result<(), String> {
    // Check if the provided string is a valid timezone.
    Tz::from_str_insensitive(val).map_err(|_e| "Not a valid timezone")?;
    Ok(())
}

fn check_client_encoding(val: &str) -> Result<(), String> {
    // https://github.com/postgres/postgres/blob/REL_15_3/src/common/encnames.c#L525
    let clean = val.replace(|c: char| !c.is_ascii_alphanumeric(), "");
    if !clean.eq_ignore_ascii_case("UTF8") {
        Err("Only support 'UTF8' for CLIENT_ENCODING".to_owned())
    } else {
        Ok(())
    }
}

fn check_bytea_output(val: &str) -> Result<(), String> {
    if val == "hex" {
        Ok(())
    } else {
        Err("Only support 'hex' for BYTEA_OUTPUT".to_owned())
    }
}

/// Check if the provided value is a valid max parallelism.
fn check_streaming_max_parallelism(val: &usize) -> Result<(), String> {
    match val {
        // TODO(var-vnode): this is to prevent confusion with singletons, after we distinguish
        // them better, we may allow 1 as the max parallelism (though not much point).
        0 | 1 => Err("STREAMING_MAX_PARALLELISM must be greater than 1".to_owned()),
        2..=VirtualNode::MAX_COUNT => Ok(()),
        _ => Err(format!(
            "STREAMING_MAX_PARALLELISM must be less than or equal to {}",
            VirtualNode::MAX_COUNT
        )),
    }
}

fn check_streaming_parallelism_for_backfill(val: &ConfigBackfillParallelism) -> Result<(), String> {
    match val {
        ConfigBackfillParallelism::Default | ConfigBackfillParallelism::Fixed(_) => Ok(()),
        ConfigBackfillParallelism::Adaptive
        | ConfigBackfillParallelism::Bounded(_)
        | ConfigBackfillParallelism::Ratio(_) => Err(
            "Only `default` or fixed backfill parallelism is supported here; adaptive backfill strategy is deferred to a later change.".to_owned(),
        ),
    }
}

impl SessionConfig {
    pub fn set_force_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> SessionConfigResult<bool> {
        let set_val = self.set_force_two_phase_agg_inner(val, reporter)?;
        if self.force_two_phase_agg {
            self.set_enable_two_phase_agg(true, reporter)
        } else {
            Ok(set_val)
        }
    }

    pub fn set_enable_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> SessionConfigResult<bool> {
        let set_val = self.set_enable_two_phase_agg_inner(val, reporter)?;
        if !self.force_two_phase_agg {
            self.set_force_two_phase_agg(false, reporter)
        } else {
            Ok(set_val)
        }
    }
}

pub struct VariableInfo {
    pub name: String,
    pub setting: String,
    pub description: String,
}

/// Report status or notice to caller.
pub trait ConfigReporter {
    fn report_status(&mut self, key: &str, new_val: String);
}

// Report nothing.
impl ConfigReporter for () {
    fn report_status(&mut self, _key: &str, _new_val: String) {}
}

def_anyhow_newtype! {
    pub SessionConfigToOverrideError,
    toml::ser::Error => "failed to serialize session config",
    ConfigMergeError => transparent,
}

impl SessionConfig {
    /// Generate an initial override for the streaming config from the session config.
    pub fn to_initial_streaming_config_override(
        &self,
    ) -> Result<String, SessionConfigToOverrideError> {
        let mut table = toml::Table::new();

        // TODO: make this more type safe.
        // We `unwrap` here to assert the hard-coded keys are correct.
        if let Some(v) = self.streaming_join_encoding.as_ref() {
            table
                .upsert("streaming.developer.join_encoding_type", v)
                .unwrap();
        }
        if let Some(v) = self.streaming_sync_log_store_pause_duration_ms.as_ref() {
            table
                .upsert("streaming.developer.sync_log_store_pause_duration_ms", v)
                .unwrap();
        }
        if let Some(v) = self.streaming_sync_log_store_buffer_size.as_ref() {
            table
                .upsert("streaming.developer.sync_log_store_buffer_size", v)
                .unwrap();
        }
        if let Some(v) = self.streaming_over_window_cache_policy.as_ref() {
            table
                .upsert("streaming.developer.over_window_cache_policy", v)
                .unwrap();
        }
        if let Some(v) = self.streaming_cache_refill_policy.as_ref() {
            table
                .upsert("streaming.developer.cache_refill_policy", v)
                .unwrap();
        }

        let res = toml::to_string(&table)?;

        // Validate all fields are valid by trying to merge it to the default config.
        if !res.is_empty() {
            let merged =
                merge_streaming_config_section(&StreamingConfig::default(), res.as_str())?.unwrap();

            let unrecognized_keys = merged.unrecognized_keys().collect_vec();
            if !unrecognized_keys.is_empty() {
                bail!("unrecognized configs: {:?}", unrecognized_keys);
            }
        }

        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use expect_test::expect;

    use super::*;

    #[derive(SessionConfig)]
    struct TestConfig {
        #[parameter(default = 1, flags = "NO_ALTER_SYS", alias = "test_param_alias" | "alias_param_test")]
        test_param: i32,
        #[parameter(default = false, deprecated = "deprecated test notice")]
        deprecated_test_param: bool,
    }

    #[test]
    fn test_session_config_alias() {
        let mut config = TestConfig::default();
        config.set("test_param", "2".to_owned(), &mut ()).unwrap();
        assert_eq!(config.get("test_param_alias").unwrap(), "2");
        config
            .set("alias_param_test", "3".to_owned(), &mut ())
            .unwrap();
        assert_eq!(config.get("test_param_alias").unwrap(), "3");
        assert!(TestConfig::check_no_alter_sys("test_param").unwrap());
        assert_eq!(
            TestConfig::deprecated_notice("deprecated_test_param").unwrap(),
            Some("deprecated test notice")
        );
        assert_eq!(TestConfig::deprecated_notice("test_param").unwrap(), None);
    }

    #[test]
    fn test_initial_streaming_config_override() {
        let mut config = SessionConfig::default();
        config
            .set_streaming_join_encoding(Some(JoinEncodingType::Cpu).into(), &mut ())
            .unwrap();
        config
            .set_streaming_over_window_cache_policy(
                Some(OverWindowCachePolicy::RecentFirstN).into(),
                &mut (),
            )
            .unwrap();
        config
            .set_streaming_cache_refill_policy(Some(CacheRefillPolicy::Both).into(), &mut ())
            .unwrap();

        // Check the converted config override string.
        let override_str = config.to_initial_streaming_config_override().unwrap();
        expect![[r#"
            [streaming.developer]
            cache_refill_policy = "both"
            join_encoding_type = "cpu_optimized"
            over_window_cache_policy = "recent_first_n"
        "#]]
        .assert_eq(&override_str);

        // Try merging it to the default streaming config.
        let merged = merge_streaming_config_section(&StreamingConfig::default(), &override_str)
            .unwrap()
            .unwrap();
        assert_eq!(merged.developer.join_encoding_type, JoinEncodingType::Cpu);
        assert_eq!(
            merged.developer.over_window_cache_policy,
            OverWindowCachePolicy::RecentFirstN
        );
        assert_eq!(
            merged.developer.cache_refill_policy,
            CacheRefillPolicy::Both
        );
    }

    #[test]
    fn test_streaming_parallelism_defaults() {
        let config = SessionConfig::default();

        assert_eq!(config.streaming_parallelism(), ConfigParallelism::Default);
        assert_eq!(
            config.streaming_parallelism_for_table(),
            ConfigParallelism::Default
        );
        assert_eq!(
            config.streaming_parallelism_for_source(),
            ConfigParallelism::Default
        );
        assert_eq!(
            config.streaming_parallelism_for_sink(),
            ConfigParallelism::Default
        );
        assert_eq!(
            config.streaming_parallelism_for_index(),
            ConfigParallelism::Default
        );
        assert_eq!(
            config.streaming_parallelism_for_materialized_view(),
            ConfigParallelism::Default
        );
        assert!(!config.streaming_unsafe_allow_upsert_sink_pk_mismatch());
    }

    #[test]
    fn test_streaming_parallelism_default_round_trip() {
        let mut config = SessionConfig::default();

        assert_eq!(config.get("streaming_parallelism").unwrap(), "default");
        assert_eq!(
            config.get("streaming_parallelism_for_table").unwrap(),
            "default"
        );
        assert_eq!(
            config.get("streaming_parallelism_for_source").unwrap(),
            "default"
        );

        config
            .set("streaming_parallelism", "default".to_owned(), &mut ())
            .unwrap();
        assert_eq!(config.get("streaming_parallelism").unwrap(), "default");

        config
            .set("streaming_parallelism", "bounded(16)".to_owned(), &mut ())
            .unwrap();
        config
            .set(
                "streaming_parallelism_for_table",
                "bounded(8)".to_owned(),
                &mut (),
            )
            .unwrap();
        config
            .set(
                "streaming_parallelism_for_source",
                "bounded(8)".to_owned(),
                &mut (),
            )
            .unwrap();

        assert_eq!(
            config.reset("streaming_parallelism", &mut ()).unwrap(),
            "default"
        );
        assert_eq!(
            config
                .reset("streaming_parallelism_for_table", &mut ())
                .unwrap(),
            "default"
        );
        assert_eq!(
            config
                .reset("streaming_parallelism_for_source", &mut ())
                .unwrap(),
            "default"
        );
    }
    #[test]
    fn test_streaming_parallelism_for_backfill_accepts_default_and_fixed() {
        let mut config = SessionConfig::default();

        config
            .set(
                "streaming_parallelism_for_backfill",
                "default".to_owned(),
                &mut (),
            )
            .unwrap();
        assert_eq!(
            config.get("streaming_parallelism_for_backfill").unwrap(),
            "default"
        );

        config
            .set(
                "streaming_parallelism_for_backfill",
                "2".to_owned(),
                &mut (),
            )
            .unwrap();
        assert_eq!(config.streaming_parallelism_for_backfill().to_string(), "2");
    }

    #[test]
    fn test_streaming_parallelism_for_backfill_rejects_adaptive_modes() {
        let mut config = SessionConfig::default();
        let expected = "Only `default` or fixed backfill parallelism is supported here; adaptive backfill strategy is deferred to a later change.";

        for value in ["adaptive", "bounded(2)", "ratio(0.5)"] {
            let err = config
                .set(
                    "streaming_parallelism_for_backfill",
                    value.to_owned(),
                    &mut (),
                )
                .unwrap_err();

            match err {
                SessionConfigError::InvalidValue {
                    entry,
                    value: actual_value,
                    source,
                } => {
                    assert_eq!(entry, "streaming_parallelism_for_backfill");
                    assert_eq!(actual_value, value);
                    assert_eq!(source.to_string(), expected);
                }
                other => panic!("unexpected error: {other:?}"),
            }
        }
    }
}
