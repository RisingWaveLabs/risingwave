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

use std::collections::BTreeMap;

use iceberg::spec::{
    FormatVersion, NestedField, NullOrder, PrimitiveType, Schema as IcebergSchema, SortDirection,
    Type,
};
use iceberg::transaction::{MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES_DEFAULT};
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Fields as ArrowFields, Schema as ArrowSchema,
};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, MapType, StructType};

use crate::connector_common::{IcebergCommon, IcebergTableIdentifier};
use crate::sink::decouple_checkpoint_log_sink::ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL;
use crate::sink::iceberg::{
    CompactionType, DEFAULT_COMPACTION_MAX_SNAPSHOTS_NUM,
    ICEBERG_DEFAULT_WRITE_PARQUET_MAX_ROW_GROUP_BYTES, IcebergConfig, IcebergOrderKeyField,
    IcebergWriteMode, parse_order_key_exprs, validate_order_key_columns,
};

pub const DEFAULT_ICEBERG_COMPACTION_INTERVAL: u64 = 3600; // 1 hour

#[test]
fn test_resolve_equality_delete_field_ids_from_iceberg_schema() {
    let iceberg_schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::new(42, "k", Type::Primitive(PrimitiveType::Int), true).into(),
            NestedField::new(99, "b", Type::Primitive(PrimitiveType::Int), true).into(),
        ])
        .build()
        .unwrap();

    let field_ids =
        super::writer::resolve_equality_delete_field_ids(&["k".to_owned()], &iceberg_schema)
            .unwrap();

    assert_eq!(field_ids, vec![42]);
}

#[test]
fn test_resolve_equality_delete_field_ids_case_sensitive() {
    let iceberg_schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::new(42, "Key", Type::Primitive(PrimitiveType::Int), true).into(),
            NestedField::new(99, "Value", Type::Primitive(PrimitiveType::Int), true).into(),
        ])
        .build()
        .unwrap();

    let field_ids =
        super::writer::resolve_equality_delete_field_ids(&["Key".to_owned()], &iceberg_schema)
            .unwrap();

    assert_eq!(field_ids, vec![42]);

    assert!(
        super::writer::resolve_equality_delete_field_ids(&["key".to_owned()], &iceberg_schema)
            .is_err()
    );
}

#[test]
fn test_compatible_arrow_schema() {
    use super::*;
    let risingwave_schema = Schema::new(vec![
        Field::with_name(DataType::Int32, "a"),
        Field::with_name(DataType::Int32, "b"),
        Field::with_name(DataType::Int32, "c"),
    ]);
    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new("a", ArrowDataType::Int32, false),
        ArrowField::new("b", ArrowDataType::Int32, false),
        ArrowField::new("c", ArrowDataType::Int32, false),
    ]);

    try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();

    let risingwave_schema = Schema::new(vec![
        Field::with_name(DataType::Int32, "d"),
        Field::with_name(DataType::Int32, "c"),
        Field::with_name(DataType::Int32, "a"),
        Field::with_name(DataType::Int32, "b"),
    ]);
    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new("d", ArrowDataType::Int32, false),
        ArrowField::new("c", ArrowDataType::Int32, false),
        ArrowField::new("a", ArrowDataType::Int32, false),
        ArrowField::new("b", ArrowDataType::Int32, false),
    ]);
    try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();

    let risingwave_schema = Schema::new(vec![
        Field::with_name(
            DataType::Struct(StructType::new(vec![
                ("a1", DataType::Int32),
                (
                    "a2",
                    DataType::Struct(StructType::new(vec![
                        ("a21", DataType::Bytea),
                        (
                            "a22",
                            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Jsonb)),
                        ),
                    ])),
                ),
            ])),
            "a",
        ),
        Field::with_name(
            DataType::list(DataType::Struct(StructType::new(vec![
                ("b1", DataType::Int32),
                ("b2", DataType::Bytea),
                (
                    "b3",
                    DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Jsonb)),
                ),
            ]))),
            "b",
        ),
        Field::with_name(
            DataType::Map(MapType::from_kv(
                DataType::Varchar,
                DataType::list(DataType::Struct(StructType::new([
                    ("c1", DataType::Int32),
                    ("c2", DataType::Bytea),
                    (
                        "c3",
                        DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Jsonb)),
                    ),
                ]))),
            )),
            "c",
        ),
    ]);
    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new(
            "a",
            ArrowDataType::Struct(ArrowFields::from(vec![
                ArrowField::new("a1", ArrowDataType::Int32, false),
                ArrowField::new(
                    "a2",
                    ArrowDataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("a21", ArrowDataType::LargeBinary, false),
                        ArrowField::new_map(
                            "a22",
                            "entries",
                            ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                            ArrowFieldRef::new(ArrowField::new(
                                "value",
                                ArrowDataType::Utf8,
                                false,
                            )),
                            false,
                            false,
                        ),
                    ])),
                    false,
                ),
            ])),
            false,
        ),
        ArrowField::new(
            "b",
            ArrowDataType::List(ArrowFieldRef::new(ArrowField::new_list_field(
                ArrowDataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("b1", ArrowDataType::Int32, false),
                    ArrowField::new("b2", ArrowDataType::LargeBinary, false),
                    ArrowField::new_map(
                        "b3",
                        "entries",
                        ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                        ArrowFieldRef::new(ArrowField::new("value", ArrowDataType::Utf8, false)),
                        false,
                        false,
                    ),
                ])),
                false,
            ))),
            false,
        ),
        ArrowField::new_map(
            "c",
            "entries",
            ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
            ArrowFieldRef::new(ArrowField::new(
                "value",
                ArrowDataType::List(ArrowFieldRef::new(ArrowField::new_list_field(
                    ArrowDataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("c1", ArrowDataType::Int32, false),
                        ArrowField::new("c2", ArrowDataType::LargeBinary, false),
                        ArrowField::new_map(
                            "c3",
                            "entries",
                            ArrowFieldRef::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                            ArrowFieldRef::new(ArrowField::new(
                                "value",
                                ArrowDataType::Utf8,
                                false,
                            )),
                            false,
                            false,
                        ),
                    ])),
                    false,
                ))),
                false,
            )),
            false,
            false,
        ),
    ]);
    try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();
}

#[test]
fn test_arrow_schema_column_order_mismatch() {
    use super::*;
    // Same names and types but a different column order must be rejected.
    let risingwave_schema = Schema::new(vec![
        Field::with_name(DataType::Int32, "col_a"),
        Field::with_name(DataType::Int32, "col_b"),
    ]);
    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new("col_b", ArrowDataType::Int32, false),
        ArrowField::new("col_a", ArrowDataType::Int32, false),
    ]);
    let err = try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap_err();
    assert!(
        err.to_string().contains("Column order mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_parse_order_key_exprs() {
    let parsed =
        parse_order_key_exprs("v1, v2 desc nulls first, v3 asc nulls last".to_owned()).unwrap();
    assert_eq!(
        parsed,
        vec![
            IcebergOrderKeyField {
                column: "v1".to_owned(),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            },
            IcebergOrderKeyField {
                column: "v2".to_owned(),
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            },
            IcebergOrderKeyField {
                column: "v3".to_owned(),
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            },
        ]
    );
}

#[test]
fn test_parse_order_key_exprs_reject_invalid_inputs() {
    assert!(parse_order_key_exprs("bucket(4, v1)".to_owned()).is_err());
    assert!(parse_order_key_exprs("v1, v1 desc".to_owned()).is_err());
    assert!(parse_order_key_exprs("v1 nulls".to_owned()).is_err());
}

#[test]
fn test_validate_order_key_columns() {
    let parsed = validate_order_key_columns("v1 desc", ["v1", "v2"]).unwrap();
    assert_eq!(parsed[0].column, "v1");
    assert!(validate_order_key_columns("_row_id", ["_row_id"]).is_err());
    assert!(validate_order_key_columns("v3", ["v1", "v2"]).is_err());
}

#[test]
fn test_parse_iceberg_config() {
    let values = [
            ("connector", "iceberg"),
            ("type", "upsert"),
            ("primary_key", "v1"),
            ("partition_by", "v1, identity(v1), truncate(4,v2), bucket(5,v1), year(v3), month(v4), day(v5), hour(v6), void(v1)"),
            ("warehouse.path", "s3://iceberg"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.path.style.access", "true"),
            ("s3.region", "us-east-1"),
            ("catalog.type", "jdbc"),
            ("catalog.name", "demo"),
            ("catalog.uri", "jdbc://postgresql://postgres:5432/iceberg"),
            ("catalog.jdbc.user", "admin"),
            ("catalog.jdbc.password", "123456"),
            ("database.name", "demo_db"),
            ("table.name", "demo_table"),
            ("enable_compaction", "true"),
            ("compaction_interval_sec", "1800"),
            ("enable_snapshot_expiration", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

    let iceberg_config = IcebergConfig::from_btreemap(values).unwrap();

    let expected_iceberg_config = IcebergConfig {
            common: IcebergCommon {
                warehouse_path: Some("s3://iceberg".to_owned()),
                catalog_uri: Some("jdbc://postgresql://postgres:5432/iceberg".to_owned()),
                s3_region: Some("us-east-1".to_owned()),
                s3_endpoint: Some("http://127.0.0.1:9301".to_owned()),
                s3_access_key: Some("hummockadmin".to_owned()),
                s3_secret_key: Some("hummockadmin".to_owned()),
                s3_iam_role_arn: None,
                gcs_credential: None,
                catalog_type: Some("jdbc".to_owned()),
                glue_id: None,
                glue_region: None,
                glue_access_key: None,
                glue_secret_key: None,
                glue_iam_role_arn: None,
                glue_endpoint: None,
                catalog_name: Some("demo".to_owned()),
                s3_path_style_access: Some(true),
                catalog_credential: None,
                catalog_oauth2_server_uri: None,
                catalog_scope: None,
                catalog_token: None,
                enable_config_load: None,
                rest_signing_name: None,
                rest_signing_region: None,
                rest_sigv4_enabled: None,
                hosted_catalog: None,
                azblob_account_name: None,
                azblob_account_key: None,
                azblob_endpoint_url: None,
                catalog_header: None,
                adlsgen2_account_name: None,
                adlsgen2_account_key: None,
                adlsgen2_endpoint: None,
                adlsgen2_tenant_id: None,
                adlsgen2_client_id: None,
                adlsgen2_client_secret: None,
                adlsgen2_authority_host: None,
                vended_credentials: None,
                catalog_security: None,
                gcp_auth_scopes: None,
                catalog_io_impl: None,
            },
            table: IcebergTableIdentifier {
                database_name: Some("demo_db".to_owned()),
                table_name: "demo_table".to_owned(),
            },
            r#type: "upsert".to_owned(),
            force_append_only: false,
            primary_key: Some(vec!["v1".to_owned()]),
            partition_by: Some("v1, identity(v1), truncate(4,v2), bucket(5,v1), year(v3), month(v4), day(v5), hour(v6), void(v1)".to_owned()),
            order_key: None,
            java_catalog_props: [("jdbc.user", "admin"), ("jdbc.password", "123456")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            commit_checkpoint_interval: ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL,
            create_table_if_not_exists: false,
            is_exactly_once: Some(true),
            commit_retry_num: 8,
            enable_compaction: true,
            compaction_interval_sec: Some(DEFAULT_ICEBERG_COMPACTION_INTERVAL / 2),
            enable_snapshot_expiration: true,
            write_mode: IcebergWriteMode::MergeOnRead,
            format_version: FormatVersion::V2,
            snapshot_expiration_max_age_millis: None,
            snapshot_expiration_retain_last: None,
            snapshot_expiration_clear_expired_files: true,
            snapshot_expiration_clear_expired_meta_data: true,
            enable_manifest_rewrite: false,
            manifest_rewrite_target_size_bytes: None,
            manifest_rewrite_min_count_to_merge: None,
            max_snapshots_num_before_compaction: Some(DEFAULT_COMPACTION_MAX_SNAPSHOTS_NUM),
            small_files_threshold_mb: None,
            delete_files_count_threshold: None,
            trigger_snapshot_count: None,
            target_file_size_mb: None,
            compaction_type: None,
            write_parquet_compression: None,
            write_parquet_max_row_group_rows: None,
            write_parquet_max_row_group_bytes: None,
            enable_pk_index: false,
            unknown_fields: Default::default(),
        };

    assert_eq!(iceberg_config, expected_iceberg_config);

    assert_eq!(
        &iceberg_config.full_table_name().unwrap().to_string(),
        "demo_db.demo_table"
    );
}

/// Test parsing Google Lakehouse Iceberg REST authentication configuration.
#[test]
fn test_parse_google_auth_config() {
    let values: BTreeMap<String, String> = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("catalog.name", "lakehouse-catalog"),
            ("catalog.type", "rest"),
            ("catalog.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog"),
            ("warehouse.path", "bq://projects/my-gcp-project"),
            ("catalog.header", "x-goog-user-project=my-gcp-project"),
            ("catalog.security", "google"),
            ("gcp.auth.scopes", "https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery"),
            ("database.name", "my_dataset"),
            ("table.name", "my_table"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

    let config = IcebergConfig::from_btreemap(values).unwrap();
    assert_eq!(config.catalog_type(), "rest");
    assert_eq!(config.common.catalog_security.as_deref(), Some("google"));
    assert_eq!(
        config.common.gcp_auth_scopes.as_deref(),
        Some(
            "https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery"
        )
    );
    assert_eq!(
        config.common.warehouse_path.as_deref(),
        Some("bq://projects/my-gcp-project")
    );
    assert_eq!(
        config.common.catalog_header.as_deref(),
        Some("x-goog-user-project=my-gcp-project")
    );
}

/// Test parsing `oauth2` security configuration.
#[test]
fn test_parse_oauth2_security_config() {
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "oauth2-catalog"),
        ("catalog.type", "rest"),
        ("catalog.uri", "https://example.com/iceberg/rest"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("catalog.security", "oauth2"),
        ("catalog.credential", "client_id:client_secret"),
        ("catalog.token", "bearer-token"),
        (
            "catalog.oauth2_server_uri",
            "https://oauth.example.com/token",
        ),
        ("catalog.scope", "read write"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let iceberg_config = IcebergConfig::from_btreemap(values).unwrap();

    // Verify catalog type
    assert_eq!(iceberg_config.catalog_type(), "rest");

    // Verify OAuth2-specific options
    assert_eq!(
        iceberg_config.common.catalog_security.as_deref(),
        Some("oauth2")
    );
    assert_eq!(
        iceberg_config.common.catalog_credential.as_deref(),
        Some("client_id:client_secret")
    );
    assert_eq!(
        iceberg_config.common.catalog_token.as_deref(),
        Some("bearer-token")
    );
    assert_eq!(
        iceberg_config.common.catalog_oauth2_server_uri.as_deref(),
        Some("https://oauth.example.com/token")
    );
    assert_eq!(
        iceberg_config.common.catalog_scope.as_deref(),
        Some("read write")
    );
}

/// Test parsing invalid security configuration.
#[test]
fn test_parse_invalid_security_config() {
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "invalid-catalog"),
        ("catalog.type", "rest"),
        ("catalog.uri", "https://example.com/iceberg/rest"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("catalog.security", "invalid_security_type"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    // This should still parse successfully, but with a warning for unknown security type
    let iceberg_config = IcebergConfig::from_btreemap(values).unwrap();

    // Verify that the invalid security type is still stored
    assert_eq!(
        iceberg_config.common.catalog_security.as_deref(),
        Some("invalid_security_type")
    );

    // Verify catalog type
    assert_eq!(iceberg_config.catalog_type(), "rest");
}

/// Test parsing custom `FileIO` implementation configuration.
#[test]
fn test_parse_custom_io_impl_config() {
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "gcs-catalog"),
        ("catalog.type", "rest"),
        ("catalog.uri", "https://example.com/iceberg/rest"),
        ("warehouse.path", "gs://my-bucket/warehouse"),
        ("catalog.security", "google"),
        ("catalog.io_impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let iceberg_config = IcebergConfig::from_btreemap(values).unwrap();

    // Verify catalog type
    assert_eq!(iceberg_config.catalog_type(), "rest");

    // Verify custom `FileIO` implementation
    assert_eq!(
        iceberg_config.common.catalog_io_impl.as_deref(),
        Some("org.apache.iceberg.gcp.gcs.GCSFileIO")
    );

    // Verify Google security is set
    assert_eq!(
        iceberg_config.common.catalog_security.as_deref(),
        Some("google")
    );
}

/// Test parsing all compaction.* prefix configs and their default values.
#[test]
fn test_parse_compaction_config() {
    // Test with all compaction configs specified
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "upsert"),
        ("primary_key", "id"),
        ("warehouse.path", "s3://iceberg"),
        ("s3.endpoint", "http://127.0.0.1:9301"),
        ("s3.access.key", "test"),
        ("s3.secret.key", "test"),
        ("s3.region", "us-east-1"),
        ("catalog.type", "storage"),
        ("catalog.name", "demo"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("enable_compaction", "true"),
        ("compaction.max_snapshots_num", "100"),
        ("compaction.small_files_threshold_mb", "512"),
        ("compaction.delete_files_count_threshold", "50"),
        ("compaction.trigger_snapshot_count", "10"),
        ("compaction.target_file_size_mb", "256"),
        ("compaction.type", "auto"),
        ("compaction.write_parquet_compression", "zstd"),
        ("compaction.write_parquet_max_row_group_rows", "50000"),
        ("compaction.write_parquet_max_row_group_bytes", "67108864"),
        ("enable_manifest_rewrite", "true"),
        ("manifest_rewrite_target_size_bytes", "16777216"),
        ("manifest_rewrite_min_count_to_merge", "12"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let config = IcebergConfig::from_btreemap(values).unwrap();
    assert!(config.enable_compaction);
    assert_eq!(config.max_snapshots_num_before_compaction, Some(100));
    assert_eq!(config.small_files_threshold_mb, Some(512));
    assert_eq!(config.delete_files_count_threshold, Some(50));
    assert_eq!(config.trigger_snapshot_count, Some(10));
    assert_eq!(config.target_file_size_mb, Some(256));
    assert_eq!(config.compaction_type, Some(CompactionType::Auto));
    assert_eq!(config.target_file_size_mb(), 256);
    assert_eq!(config.write_parquet_compression(), "zstd");
    assert_eq!(config.write_parquet_max_row_group_rows(), Some(50000));
    assert_eq!(config.write_parquet_max_row_group_bytes(), Some(67_108_864));
    assert!(config.enable_manifest_rewrite);
    assert_eq!(config.manifest_rewrite_target_size_bytes, Some(16_777_216));
    assert_eq!(config.manifest_rewrite_min_count_to_merge, Some(12));
    assert_eq!(config.manifest_rewrite_target_size_bytes(), 16_777_216);
    assert_eq!(config.manifest_rewrite_min_count_to_merge(), 12);

    // Test default values (no compaction configs specified)
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "test-catalog"),
        ("catalog.type", "storage"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let config = IcebergConfig::from_btreemap(values).unwrap();
    assert!(!config.enable_compaction);
    assert!(config.enable_snapshot_expiration);
    assert_eq!(config.snapshot_expiration_max_age_millis, None);
    assert_eq!(config.snapshot_expiration_retain_last, None);
    assert!(!config.enable_manifest_rewrite);
    assert_eq!(config.manifest_rewrite_target_size_bytes, None);
    assert_eq!(config.manifest_rewrite_min_count_to_merge, None);
    assert_eq!(
        config.manifest_rewrite_target_size_bytes(),
        MANIFEST_TARGET_SIZE_BYTES_DEFAULT as u64
    );
    assert_eq!(
        config.manifest_rewrite_min_count_to_merge(),
        MANIFEST_MIN_MERGE_COUNT_DEFAULT as usize
    );
    assert_eq!(config.max_snapshots_num_before_compaction, None);
    assert_eq!(config.compaction_type, None);
    assert_eq!(config.target_file_size_mb(), 1024); // Default
    assert_eq!(config.write_parquet_compression(), "zstd"); // Default
    assert_eq!(config.write_parquet_max_row_group_rows(), None); // Default
    assert_eq!(
        config.write_parquet_max_row_group_bytes(),
        Some(ICEBERG_DEFAULT_WRITE_PARQUET_MAX_ROW_GROUP_BYTES)
    );

    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "test-catalog"),
        ("catalog.type", "storage"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("compaction.max_snapshots_num", "1000"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let config = IcebergConfig::from_btreemap(values).unwrap();
    assert!(!config.enable_compaction);
    assert_eq!(config.max_snapshots_num_before_compaction, Some(1000));
}

#[test]
fn test_reject_manifest_rewrite_for_v3() {
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "test-catalog"),
        ("catalog.type", "storage"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("format_version", "3"),
        ("enable_manifest_rewrite", "true"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let err = IcebergConfig::from_btreemap(values).unwrap_err();
    assert!(
        err.to_string()
            .contains("`enable_manifest_rewrite` is not supported for Iceberg format version 3")
    );
}

#[test]
fn test_reject_zero_manifest_rewrite_settings() {
    for option in [
        "manifest_rewrite_target_size_bytes",
        "manifest_rewrite_min_count_to_merge",
    ] {
        let values: BTreeMap<String, String> = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("catalog.name", "test-catalog"),
            ("catalog.type", "storage"),
            ("warehouse.path", "s3://my-bucket/warehouse"),
            ("database.name", "test_db"),
            ("table.name", "test_table"),
            (option, "0"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();

        let err = IcebergConfig::from_btreemap(values).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("`{option}` must be greater than 0"))
        );
    }
}

#[test]
fn test_reject_zero_trigger_snapshot_count() {
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "test-catalog"),
        ("catalog.type", "storage"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("compaction.trigger_snapshot_count", "0"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let err = IcebergConfig::from_btreemap(values).unwrap_err();
    assert!(
        err.to_string()
            .contains("`compaction.trigger_snapshot_count` must be greater than 0")
    );
}

#[test]
fn test_reject_zero_max_snapshots_num() {
    let values: BTreeMap<String, String> = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("force_append_only", "true"),
        ("catalog.name", "test-catalog"),
        ("catalog.type", "storage"),
        ("warehouse.path", "s3://my-bucket/warehouse"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("compaction.max_snapshots_num", "0"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let err = IcebergConfig::from_btreemap(values).unwrap_err();
    assert!(
        err.to_string()
            .contains("`compaction.max_snapshots_num` must be greater than 0")
    );
}

#[test]
fn test_reject_zero_compaction_size_settings() {
    for option in [
        "compaction_interval_sec",
        "compaction.small_files_threshold_mb",
        "compaction.delete_files_count_threshold",
        "compaction.target_file_size_mb",
        "compaction.write_parquet_max_row_group_rows",
        "compaction.write_parquet_max_row_group_bytes",
    ] {
        let mut values: BTreeMap<String, String> = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("catalog.name", "test-catalog"),
            ("catalog.type", "storage"),
            ("warehouse.path", "s3://my-bucket/warehouse"),
            ("database.name", "test_db"),
            ("table.name", "test_table"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        values.insert(option.to_owned(), "0".to_owned());

        let err = IcebergConfig::from_btreemap(values).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("`{option}` must be greater than 0")),
            "unexpected error for {option}: {err}"
        );
    }
}

/// Test parquet compression parsing.
#[test]
fn test_parse_parquet_compression() {
    use parquet::basic::Compression;

    use super::parse_parquet_compression;

    // Test valid compression types
    assert!(matches!(
        parse_parquet_compression("snappy"),
        Compression::SNAPPY
    ));
    assert!(matches!(
        parse_parquet_compression("gzip"),
        Compression::GZIP(_)
    ));
    assert!(matches!(
        parse_parquet_compression("zstd"),
        Compression::ZSTD(_)
    ));
    assert!(matches!(parse_parquet_compression("lz4"), Compression::LZ4));
    assert!(matches!(
        parse_parquet_compression("brotli"),
        Compression::BROTLI(_)
    ));
    assert!(matches!(
        parse_parquet_compression("uncompressed"),
        Compression::UNCOMPRESSED
    ));

    // Test case insensitivity
    assert!(matches!(
        parse_parquet_compression("SNAPPY"),
        Compression::SNAPPY
    ));
    assert!(matches!(
        parse_parquet_compression("Zstd"),
        Compression::ZSTD(_)
    ));

    // Test invalid compression (should fall back to SNAPPY)
    assert!(matches!(
        parse_parquet_compression("invalid"),
        Compression::SNAPPY
    ));
}

#[test]
fn test_append_only_rejects_copy_on_write() {
    // Test that append-only sinks reject copy-on-write mode
    let values = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("warehouse.path", "s3://iceberg"),
        ("s3.endpoint", "http://127.0.0.1:9301"),
        ("s3.access.key", "test"),
        ("s3.secret.key", "test"),
        ("s3.region", "us-east-1"),
        ("catalog.type", "storage"),
        ("catalog.name", "demo"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("write_mode", "copy-on-write"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let result = IcebergConfig::from_btreemap(values);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("'copy-on-write' mode is not supported for append-only iceberg sink")
    );
}

#[test]
fn test_append_only_accepts_merge_on_read() {
    // Test that append-only sinks accept merge-on-read mode (explicit)
    let values = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("warehouse.path", "s3://iceberg"),
        ("s3.endpoint", "http://127.0.0.1:9301"),
        ("s3.access.key", "test"),
        ("s3.secret.key", "test"),
        ("s3.region", "us-east-1"),
        ("catalog.type", "storage"),
        ("catalog.name", "demo"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("write_mode", "merge-on-read"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let result = IcebergConfig::from_btreemap(values);
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.write_mode, IcebergWriteMode::MergeOnRead);
}

#[test]
fn test_append_only_defaults_to_merge_on_read() {
    // Test that append-only sinks default to merge-on-read when write_mode is not specified
    let values = [
        ("connector", "iceberg"),
        ("type", "append-only"),
        ("warehouse.path", "s3://iceberg"),
        ("s3.endpoint", "http://127.0.0.1:9301"),
        ("s3.access.key", "test"),
        ("s3.secret.key", "test"),
        ("s3.region", "us-east-1"),
        ("catalog.type", "storage"),
        ("catalog.name", "demo"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let result = IcebergConfig::from_btreemap(values);
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.write_mode, IcebergWriteMode::MergeOnRead);
}

#[test]
fn test_upsert_accepts_copy_on_write() {
    // Test that upsert sinks accept copy-on-write mode
    let values = [
        ("connector", "iceberg"),
        ("type", "upsert"),
        ("primary_key", "id"),
        ("warehouse.path", "s3://iceberg"),
        ("s3.endpoint", "http://127.0.0.1:9301"),
        ("s3.access.key", "test"),
        ("s3.secret.key", "test"),
        ("s3.region", "us-east-1"),
        ("catalog.type", "storage"),
        ("catalog.name", "demo"),
        ("database.name", "test_db"),
        ("table.name", "test_table"),
        ("write_mode", "copy-on-write"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();

    let result = IcebergConfig::from_btreemap(values);
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.write_mode, IcebergWriteMode::CopyOnWrite);
}

fn iceberg_compaction_alter_config(
    write_mode: &str,
    compaction_type: &str,
    enable_compaction: bool,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("connector".to_owned(), "iceberg".to_owned()),
        ("type".to_owned(), "upsert".to_owned()),
        ("primary_key".to_owned(), "id".to_owned()),
        ("warehouse.path".to_owned(), "s3://iceberg".to_owned()),
        ("catalog.type".to_owned(), "storage".to_owned()),
        ("catalog.name".to_owned(), "demo".to_owned()),
        ("database.name".to_owned(), "test_db".to_owned()),
        ("table.name".to_owned(), "test_table".to_owned()),
        ("write_mode".to_owned(), write_mode.to_owned()),
        ("compaction.type".to_owned(), compaction_type.to_owned()),
        (
            "enable_compaction".to_owned(),
            enable_compaction.to_string(),
        ),
    ])
}

#[test]
fn test_alter_allows_legacy_copy_on_write_compaction_type() {
    use crate::sink::Sink;
    use crate::sink::iceberg::IcebergSink;

    let mut values = iceberg_compaction_alter_config("copy-on-write", "full", false);
    values.insert("compaction_interval_sec".to_owned(), "120".to_owned());
    let alter_props = BTreeMap::from([("compaction_interval_sec".to_owned(), "120".to_owned())]);

    IcebergSink::validate_alter_config_change(&values, &alter_props).unwrap();

    values.insert("enable_compaction".to_owned(), "true".to_owned());
    let alter_props = BTreeMap::from([("enable_compaction".to_owned(), "true".to_owned())]);
    IcebergSink::validate_alter_config_change(&values, &alter_props).unwrap();
}

#[test]
fn test_alter_merge_on_read_compaction_checks_explicit_type_license() {
    use crate::sink::Sink;
    use crate::sink::iceberg::IcebergSink;

    let values = iceberg_compaction_alter_config("merge-on-read", "auto", false);
    let alter_props = BTreeMap::from([("compaction.type".to_owned(), "auto".to_owned())]);
    let error = IcebergSink::validate_alter_config_change(&values, &alter_props).unwrap_err();
    assert!(
        error.to_string().contains("feature IcebergCompaction"),
        "unexpected error: {error}"
    );

    let values = iceberg_compaction_alter_config("merge-on-read", "auto", true);
    let alter_props = BTreeMap::from([("enable_compaction".to_owned(), "true".to_owned())]);
    let error = IcebergSink::validate_alter_config_change(&values, &alter_props).unwrap_err();
    assert!(
        error.to_string().contains("feature IcebergCompaction"),
        "unexpected error: {error}"
    );
}

// Regression: an upsert sink whose pk column has upper-case letters must resolve.
// `primary_key` is lower-cased on deserialization, so re-matching it against the
// (case-sensitive) column names would fail. The connector must instead use the pk
// indices resolved by the frontend (`SinkParam::downstream_pk`).
#[test]
fn test_iceberg_sink_upper_case_primary_key() {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::id::SinkId;

    use crate::sink::SinkParam;
    use crate::sink::catalog::SinkType;
    use crate::sink::iceberg::IcebergSink;

    let values = [
        ("connector", "iceberg"),
        ("type", "upsert"),
        ("primary_key", "Key"),
        ("warehouse.path", "s3://iceberg"),
        ("s3.endpoint", "http://127.0.0.1:9301"),
        ("s3.access.key", "hummockadmin"),
        ("s3.secret.key", "hummockadmin"),
        ("s3.region", "us-east-1"),
        ("catalog.type", "storage"),
        ("catalog.name", "demo"),
        ("database.name", "demo_db"),
        ("table.name", "demo_table"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    let config = IcebergConfig::from_btreemap(values).unwrap();

    let param = SinkParam {
        sink_id: SinkId::from(1u32),
        sink_name: "test_sink".to_owned(),
        properties: BTreeMap::new(),
        columns: vec![
            ColumnDesc::named("Key", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("Value", ColumnId::new(2), DataType::Int32),
        ],
        downstream_pk: Some(vec![0]),
        sink_type: SinkType::Upsert,
        ignore_delete: false,
        format_desc: None,
        db_name: "demo_db".to_owned(),
        sink_from_name: "test_sink".to_owned(),
    };

    let sink = IcebergSink::new(config, param).unwrap();
    // Keep the case-preserving column name selected by the frontend so the writer can
    // resolve the actual Iceberg field id after loading table metadata.
    assert_eq!(
        sink.upsert_primary_key_column_names,
        Some(vec!["Key".to_owned()])
    );
}
