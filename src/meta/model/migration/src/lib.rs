#![allow(clippy::enum_variant_names)]

pub use sea_orm_migration::MigrationStatus;
pub use sea_orm_migration::prelude::*;
mod m20230908_072257_init;
mod m20231008_020431_hummock;
mod m20240304_074901_subscription;
mod m20240410_082733_with_version_column_migration;
mod m20240410_154406_session_params;
mod m20240417_062305_subscription_internal_table_name;
mod m20240418_142249_function_runtime;
mod m20240506_112555_subscription_partial_ckpt;
mod m20240525_090457_secret;
mod m20240617_070131_index_column_properties;
mod m20240617_071625_sink_into_table_column;
mod m20240618_072634_function_compressed_binary;
mod m20240630_131430_remove_parallel_unit;
mod m20240701_060504_hummock_time_travel;
mod m20240702_080451_system_param_value;
mod m20240702_084927_unnecessary_fk;
mod m20240726_063833_auto_schema_change;
mod m20240806_143329_add_rate_limit_to_source_catalog;
mod m20240820_081248_add_time_travel_per_table_epoch;
mod m20240911_083152_variable_vnode_count;
mod m20241001_013810_webhook_source;
mod m20241016_065621_hummock_gc_history;
mod m20241022_072553_node_label;
mod m20241025_062548_singleton_vnode_count;
mod m20241115_085007_remove_function_type;
mod m20241120_182555_hummock_add_time_travel_sst_index;
mod m20241121_101830_table_engine;
mod m20241125_043732_connection_params;
mod m20241202_071413_resource_group;
mod m20241226_074013_clean_watermark_index_in_pk;
mod m20250106_072104_fragment_relation;
mod m20250121_085800_change_wasm_udf_identifier;
mod m20250210_170743_function_options;
mod m20250319_062702_mysql_utf8mb4;
mod m20250325_061743_exactly_once_iceberg_sink_metadata;
mod m20250509_102041_remove_dispatcher;
mod m20250514_114514_dispatcher_type_mapping;
mod m20250522_074525_iceberg_tables;
mod m20250522_074947_iceberg_namespace_properties;
mod m20250528_064717_barrier_interval_per_database;
mod m20250603_084830_default_privilege;
mod m20250702_062029_cdc_table_snapshot_splits;
mod m20250710_065220_sink_auto_refresh_schema;
mod m20250722_155040_table_refreshable;
mod m20250729_174630_add_vector_index_info;
mod m20250810_000000_add_user_admin_field;
mod m20250819_014448_add_version_column_indices_to_table;
mod m20250820_120000_add_cdc_table_type;
mod m20250821_081110_cdc_table_snapshot_splits_add_column;
mod m20250905_144810_deprecate_table_incoming_sinks;
mod m20250916_120000_add_refresh_fields;
mod m20251005_000000_fragment_splits;
mod m20251016_220528_fragment_parallelism;
mod m20251022_294610_source_refresh_mode;
mod m20251030_120000_refresh_jobs;
mod m20251112_114514_streaming_job_config_override;
mod m20251124_195858_pending_sink_state;
mod m20251126_093529_add_is_iceberg_compactor;
mod m20251130_120000_streaming_job_backfill_parallelism;
mod m20251208_134652_clean_watermark_indices;
mod m20251215_000000_streaming_job_parallelism_strategy;
mod m20251223_064450_table_change_log;
mod m20251224_142321_sink_schema_change;
mod m20251231_000000_sink_ignore_delete;
mod m20260119_153927_streaming_job_is_serverless_backfill;
mod m20260120_120000_streaming_job_backfill_orders;
mod m20260311_000000_legacy_streaming_parallelism_session_params;
mod m20260312_000000_streaming_job_backfill_parallelism_strategy;
mod m20260518_000000_disable_unused_read_prefix_hints;
mod m20260519_000000_streaming_job_batch_refresh_seconds;
mod m20260624_000000_pending_sink_state_object_fk;
mod m20260705_000000_subscription_dependent_object_fk;
mod m20260804_000000_worker_property_resource;
mod m20260805_000000_object_belong_to_oid;
mod m20260819_000000_function_unsafe_skip_materializing_exprs;
mod utils;

pub struct Migrator;

#[macro_export]
macro_rules! assert_not_has_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            assert!(
                !$manager
                    .has_table($table::Table.to_string())
                    .await?,
                "Table `{}` already exists",
                $table::Table.to_string()
            );
        )+
    };
}

#[macro_export]
macro_rules! drop_tables {
    ($manager:expr, $( $table:ident ),+) => {
        $(
            $manager
                .drop_table(
                    sea_orm_migration::prelude::Table::drop()
                        .table($table::Table)
                        .if_exists()
                        .cascade()
                        .to_owned(),
                )
                .await?;
        )+
    };
}

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230908_072257_init::Migration),
            Box::new(m20231008_020431_hummock::Migration),
            Box::new(m20240304_074901_subscription::Migration),
            Box::new(m20240410_082733_with_version_column_migration::Migration),
            Box::new(m20240410_154406_session_params::Migration),
            Box::new(m20240417_062305_subscription_internal_table_name::Migration),
            Box::new(m20240418_142249_function_runtime::Migration),
            Box::new(m20240506_112555_subscription_partial_ckpt::Migration),
            Box::new(m20240525_090457_secret::Migration),
            Box::new(m20240617_070131_index_column_properties::Migration),
            Box::new(m20240617_071625_sink_into_table_column::Migration),
            Box::new(m20240618_072634_function_compressed_binary::Migration),
            Box::new(m20240630_131430_remove_parallel_unit::Migration),
            Box::new(m20240701_060504_hummock_time_travel::Migration),
            Box::new(m20240702_080451_system_param_value::Migration),
            Box::new(m20240702_084927_unnecessary_fk::Migration),
            Box::new(m20240726_063833_auto_schema_change::Migration),
            Box::new(m20240806_143329_add_rate_limit_to_source_catalog::Migration),
            Box::new(m20240820_081248_add_time_travel_per_table_epoch::Migration),
            Box::new(m20240911_083152_variable_vnode_count::Migration),
            Box::new(m20241001_013810_webhook_source::Migration),
            Box::new(m20241016_065621_hummock_gc_history::Migration),
            Box::new(m20241022_072553_node_label::Migration),
            Box::new(m20241025_062548_singleton_vnode_count::Migration),
            Box::new(m20241115_085007_remove_function_type::Migration),
            Box::new(m20241120_182555_hummock_add_time_travel_sst_index::Migration),
            Box::new(m20241121_101830_table_engine::Migration),
            Box::new(m20241125_043732_connection_params::Migration),
            Box::new(m20241202_071413_resource_group::Migration),
            Box::new(m20241226_074013_clean_watermark_index_in_pk::Migration),
            Box::new(m20250106_072104_fragment_relation::Migration),
            Box::new(m20250121_085800_change_wasm_udf_identifier::Migration),
            Box::new(m20250210_170743_function_options::Migration),
            Box::new(m20250319_062702_mysql_utf8mb4::Migration),
            Box::new(m20250325_061743_exactly_once_iceberg_sink_metadata::Migration),
            Box::new(m20250509_102041_remove_dispatcher::Migration),
            Box::new(m20250514_114514_dispatcher_type_mapping::Migration),
            Box::new(m20250522_074525_iceberg_tables::Migration),
            Box::new(m20250522_074947_iceberg_namespace_properties::Migration),
            Box::new(m20250528_064717_barrier_interval_per_database::Migration),
            Box::new(m20250603_084830_default_privilege::Migration),
            Box::new(m20250702_062029_cdc_table_snapshot_splits::Migration),
            Box::new(m20250710_065220_sink_auto_refresh_schema::Migration),
            Box::new(m20250722_155040_table_refreshable::Migration),
            Box::new(m20250729_174630_add_vector_index_info::Migration),
            Box::new(m20250810_000000_add_user_admin_field::Migration),
            Box::new(m20250819_014448_add_version_column_indices_to_table::Migration),
            Box::new(m20250820_120000_add_cdc_table_type::Migration),
            Box::new(m20250821_081110_cdc_table_snapshot_splits_add_column::Migration),
            Box::new(m20250905_144810_deprecate_table_incoming_sinks::Migration),
            Box::new(m20250916_120000_add_refresh_fields::Migration),
            Box::new(m20251005_000000_fragment_splits::Migration),
            Box::new(m20251016_220528_fragment_parallelism::Migration),
            Box::new(m20251022_294610_source_refresh_mode::Migration),
            Box::new(m20251030_120000_refresh_jobs::Migration),
            Box::new(m20251112_114514_streaming_job_config_override::Migration),
            Box::new(m20251126_093529_add_is_iceberg_compactor::Migration),
            Box::new(m20251124_195858_pending_sink_state::Migration),
            Box::new(m20251130_120000_streaming_job_backfill_parallelism::Migration),
            Box::new(m20251208_134652_clean_watermark_indices::Migration),
            Box::new(m20251215_000000_streaming_job_parallelism_strategy::Migration),
            Box::new(m20251223_064450_table_change_log::Migration),
            Box::new(m20251224_142321_sink_schema_change::Migration),
            Box::new(m20251231_000000_sink_ignore_delete::Migration),
            Box::new(m20260119_153927_streaming_job_is_serverless_backfill::Migration),
            Box::new(m20260120_120000_streaming_job_backfill_orders::Migration),
            Box::new(m20260311_000000_legacy_streaming_parallelism_session_params::Migration),
            Box::new(m20260312_000000_streaming_job_backfill_parallelism_strategy::Migration),
            Box::new(m20260518_000000_disable_unused_read_prefix_hints::Migration),
            Box::new(m20260519_000000_streaming_job_batch_refresh_seconds::Migration),
            Box::new(m20260624_000000_pending_sink_state_object_fk::Migration),
            Box::new(m20260705_000000_subscription_dependent_object_fk::Migration),
            Box::new(m20260804_000000_worker_property_resource::Migration),
            Box::new(m20260805_000000_object_belong_to_oid::Migration),
            Box::new(m20260819_000000_function_unsafe_skip_materializing_exprs::Migration),
        ]
    }
}
