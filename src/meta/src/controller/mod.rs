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

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, anyhow};
use risingwave_common::bail;
use risingwave_common::cast::datetime_to_timestamp_millis;
use risingwave_common::hash::VnodeCount;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::{
    PrivateLinkService, connection, database, function, index, object, schema, secret, sink,
    source, streaming_job as streaming_job_model, subscription, table, view,
};
use risingwave_meta_model_migration::{MigrationStatus, Migrator, MigratorTrait};
use risingwave_pb::catalog::connection::PbInfo as PbConnectionInfo;
use risingwave_pb::catalog::table::{CdcTableType as PbCdcTableType, PbEngine, PbTableType};
use risingwave_pb::catalog::{
    PbConnection, PbCreateType, PbDatabase, PbFunction, PbHandleConflictBehavior, PbIndex,
    PbSchema, PbSecret, PbSink, PbSinkType, PbSource, PbStreamJobStatus, PbSubscription, PbTable,
    PbView,
};
use sea_orm::{ConnectOptions, DatabaseConnection, DbBackend, ModelTrait};

use crate::{MetaError, MetaResult, MetaStoreBackend};

pub mod catalog;
pub mod cluster;
pub mod fragment;
pub mod id;
pub mod rename;
pub mod scale;
pub mod session_params;
pub mod streaming_job;
pub mod system_param;
pub mod user;
pub mod utils;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        if let Some(err) = err.sql_err() {
            return anyhow!(err).into();
        }
        anyhow!(err).into()
    }
}

#[derive(Clone)]
pub struct SqlMetaStore {
    pub conn: DatabaseConnection,
    pub endpoint: String,
}

impl SqlMetaStore {
    /// Connect to the SQL meta store based on the given configuration.
    pub async fn connect(backend: MetaStoreBackend) -> MetaResult<Self> {
        const MAX_DURATION: Duration = Duration::new(u64::MAX / 4, 0);

        #[easy_ext::ext]
        impl ConnectOptions {
            /// Apply common settings for `SQLite` connections.
            fn sqlite_common(&mut self) -> &mut Self {
                self
                    // Since Sqlite is prone to the error "(code: 5) database is locked" under concurrent access,
                    // here we forcibly specify the number of connections as 1.
                    .min_connections(1)
                    .max_connections(1)
                    // Workaround for https://github.com/risingwavelabs/risingwave/issues/18966.
                    // Note: don't quite get the point but `acquire_timeout` and `connect_timeout` maps to the
                    //       same underlying setting in `sqlx` under current implementation.
                    .acquire_timeout(MAX_DURATION)
                    .connect_timeout(MAX_DURATION)
            }
        }

        Ok(match backend {
            MetaStoreBackend::Mem => {
                const IN_MEMORY_STORE: &str = "sqlite::memory:";

                let mut options = ConnectOptions::new(IN_MEMORY_STORE);

                options
                    .sqlite_common()
                    // Releasing the connection to in-memory SQLite database is unacceptable
                    // because it will clear the database. Set a large enough timeout to prevent it.
                    // `sqlx` actually supports disabling these timeouts by passing a `None`, but
                    // `sea-orm` does not expose this option.
                    .idle_timeout(MAX_DURATION)
                    .max_lifetime(MAX_DURATION);

                let conn = sea_orm::Database::connect(options).await?;
                Self {
                    conn,
                    endpoint: IN_MEMORY_STORE.to_owned(),
                }
            }
            MetaStoreBackend::Sql { endpoint, config } => {
                let mut options = ConnectOptions::new(endpoint.clone());
                options
                    .max_connections(config.max_connections)
                    .min_connections(config.min_connections)
                    .connect_timeout(Duration::from_secs(config.connection_timeout_sec))
                    .idle_timeout(Duration::from_secs(config.idle_timeout_sec))
                    .acquire_timeout(Duration::from_secs(config.acquire_timeout_sec));

                if DbBackend::Sqlite.is_prefix_of(&endpoint) {
                    if endpoint.contains(":memory:") || endpoint.contains("mode=memory") {
                        bail!(
                            "use the `mem` backend instead of specifying a URL of in-memory SQLite"
                        );
                    }
                    options.sqlite_common();
                }

                let conn = sea_orm::Database::connect(options).await?;
                Self { conn, endpoint }
            }
        })
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn for_test() -> Self {
        let this = Self::connect(MetaStoreBackend::Mem).await.unwrap();
        Migrator::up(&this.conn, None).await.unwrap();
        this
    }

    /// Check whether the cluster, which uses SQL as the backend, is a new cluster.
    /// It determines this by inspecting the applied migrations. If the migration `m20230908_072257_init` has been applied,
    /// then it is considered an old cluster.
    ///
    /// Note: this check should be performed before [`Self::up()`].
    async fn is_first_launch(&self) -> MetaResult<bool> {
        let migrations = Migrator::get_applied_migrations(&self.conn)
            .await
            .context("failed to get applied migrations")?;
        for migration in migrations {
            if migration.name() == "m20230908_072257_init"
                && migration.status() == MigrationStatus::Applied
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Apply all the migrations to the meta store before starting the service.
    ///
    /// Returns whether the cluster is the first launch.
    pub async fn up(&self) -> MetaResult<bool> {
        let cluster_first_launch = self.is_first_launch().await?;
        // Try to upgrade if any new model changes are added.
        Migrator::up(&self.conn, None)
            .await
            .context("failed to upgrade models in meta store")?;

        Ok(cluster_first_launch)
    }
}

pub struct ObjectModel<M: ModelTrait>(M, object::Model, Option<streaming_job_model::Model>);

fn stream_job_status_and_create_type(
    streaming_job: &Option<streaming_job_model::Model>,
) -> (i32, i32) {
    streaming_job
        .as_ref()
        .map(|job| {
            (
                PbStreamJobStatus::from(job.job_status) as i32,
                PbCreateType::from(job.create_type.clone()) as i32,
            )
        })
        .unwrap_or((
            PbStreamJobStatus::Created as i32,
            PbCreateType::Foreground as i32,
        ))
}

impl From<ObjectModel<database::Model>> for PbDatabase {
    fn from(value: ObjectModel<database::Model>) -> Self {
        Self {
            id: value.0.database_id,
            name: value.0.name,
            owner: value.1.owner_id as _,
            resource_group: value.0.resource_group.clone(),
            barrier_interval_ms: value.0.barrier_interval_ms.map(|v| v as u32),
            checkpoint_frequency: value.0.checkpoint_frequency.map(|v| v as u64),
        }
    }
}

impl From<ObjectModel<secret::Model>> for PbSecret {
    fn from(value: ObjectModel<secret::Model>) -> Self {
        Self {
            id: value.0.secret_id,
            name: value.0.name,
            database_id: value.1.database_id.unwrap(),
            value: value.0.value,
            owner: value.1.owner_id as _,
            schema_id: value.1.schema_id.unwrap(),
        }
    }
}

impl From<ObjectModel<schema::Model>> for PbSchema {
    fn from(value: ObjectModel<schema::Model>) -> Self {
        Self {
            id: value.0.schema_id,
            name: value.0.name,
            database_id: value.1.database_id.unwrap(),
            owner: value.1.owner_id as _,
        }
    }
}

impl From<ObjectModel<table::Model>> for PbTable {
    fn from(value: ObjectModel<table::Model>) -> Self {
        let (stream_job_status, create_type) = stream_job_status_and_create_type(&value.2);
        Self {
            id: value.0.table_id,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            columns: value.0.columns.to_protobuf(),
            pk: value.0.pk.to_protobuf(),
            table_type: PbTableType::from(value.0.table_type) as _,
            distribution_key: value.0.distribution_key.0,
            stream_key: value.0.stream_key.0,
            append_only: value.0.append_only,
            owner: value.1.owner_id as _,
            fragment_id: value.0.fragment_id.unwrap_or_default(),
            vnode_col_index: value.0.vnode_col_index.map(|index| index as _),
            row_id_index: value.0.row_id_index.map(|index| index as _),
            value_indices: value.0.value_indices.0,
            definition: value.0.definition,
            handle_pk_conflict_behavior: PbHandleConflictBehavior::from(
                value.0.handle_pk_conflict_behavior,
            ) as _,
            version_column_indices: value
                .0
                .version_column_indices
                .unwrap_or_default()
                .0
                .iter()
                .map(|&idx| idx as u32)
                .collect(),
            read_prefix_len_hint: value.0.read_prefix_len_hint as _,
            watermark_indices: value.0.watermark_indices.0,
            dist_key_in_pk: value.0.dist_key_in_pk.0,
            dml_fragment_id: value.0.dml_fragment_id,
            cardinality: value
                .0
                .cardinality
                .map(|cardinality| cardinality.to_protobuf()),
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.initialized_at) as _)
                    .0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            #[expect(deprecated)]
            cleaned_by_watermark: value.0.cleaned_by_watermark,
            stream_job_status,
            create_type,
            version: value.0.version.map(|v| v.to_protobuf()),
            optional_associated_source_id: value.0.optional_associated_source_id.map(Into::into),
            description: value.0.description,
            #[expect(deprecated)]
            incoming_sinks: vec![],
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            retention_seconds: value.0.retention_seconds.map(|id| id as u32),
            cdc_table_id: value.0.cdc_table_id,
            maybe_vnode_count: VnodeCount::set(value.0.vnode_count).to_protobuf(),
            webhook_info: value.0.webhook_info.map(|info| info.to_protobuf()),
            job_id: value.0.belongs_to_job_id,
            engine: value.0.engine.map(|engine| PbEngine::from(engine) as i32),
            #[expect(deprecated)]
            clean_watermark_index_in_pk: value.0.clean_watermark_index_in_pk,
            clean_watermark_indices: value
                .0
                .clean_watermark_indices
                .map(|indices| indices.0.iter().map(|&x| x as u32).collect())
                .unwrap_or_default(),
            refreshable: value.0.refreshable,
            vector_index_info: value.0.vector_index_info.map(|index| index.to_protobuf()),
            cdc_table_type: value
                .0
                .cdc_table_type
                .map(|cdc_type| PbCdcTableType::from(cdc_type) as i32),
        }
    }
}

impl From<ObjectModel<source::Model>> for PbSource {
    fn from(value: ObjectModel<source::Model>) -> Self {
        let mut secret_ref_map = BTreeMap::new();
        if let Some(secret_ref) = value.0.secret_ref {
            secret_ref_map = secret_ref.to_protobuf();
        }
        Self {
            id: value.0.source_id as _,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            row_id_index: value.0.row_id_index.map(|id| id as _),
            columns: value.0.columns.to_protobuf(),
            pk_column_ids: value.0.pk_column_ids.0,
            with_properties: value.0.with_properties.0,
            owner: value.1.owner_id as _,
            info: value.0.source_info.map(|info| info.to_protobuf()),
            watermark_descs: value.0.watermark_descs.to_protobuf(),
            definition: value.0.definition,
            connection_id: value.0.connection_id,
            // todo: using the timestamp from the database directly.
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.initialized_at) as _)
                    .0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            version: value.0.version as _,
            optional_associated_table_id: value.0.optional_associated_table_id.map(Into::into),
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            secret_refs: secret_ref_map,
            rate_limit: value.0.rate_limit.map(|v| v as _),
            refresh_mode: value
                .0
                .refresh_mode
                .map(|refresh_mode| refresh_mode.to_protobuf()),
        }
    }
}

impl From<ObjectModel<sink::Model>> for PbSink {
    fn from(value: ObjectModel<sink::Model>) -> Self {
        let (stream_job_status, create_type) = stream_job_status_and_create_type(&value.2);
        let mut secret_ref_map = BTreeMap::new();
        if let Some(secret_ref) = value.0.secret_ref {
            secret_ref_map = secret_ref.to_protobuf();
        }
        Self {
            id: value.0.sink_id as _,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            columns: value.0.columns.to_protobuf(),
            plan_pk: value.0.plan_pk.to_protobuf(),
            distribution_key: value.0.distribution_key.0,
            downstream_pk: value.0.downstream_pk.0,
            sink_type: PbSinkType::from(value.0.sink_type) as _,
            raw_ignore_delete: value.0.ignore_delete,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            definition: value.0.definition,
            connection_id: value.0.connection_id,
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.initialized_at) as _)
                    .0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            db_name: value.0.db_name,
            sink_from_name: value.0.sink_from_name,
            stream_job_status,
            format_desc: value.0.sink_format_desc.map(|desc| desc.to_protobuf()),
            target_table: value.0.target_table,
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            create_type,
            secret_refs: secret_ref_map,
            original_target_columns: value
                .0
                .original_target_columns
                .map(|cols| cols.to_protobuf())
                .unwrap_or_default(),
            auto_refresh_schema_from_table: value.0.auto_refresh_schema_from_table,
        }
    }
}

impl From<ObjectModel<subscription::Model>> for PbSubscription {
    fn from(value: ObjectModel<subscription::Model>) -> Self {
        Self {
            id: value.0.subscription_id as _,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            owner: value.1.owner_id as _,
            retention_seconds: value.0.retention_seconds as _,
            definition: value.0.definition,
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.initialized_at) as _)
                    .0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            dependent_table_id: value.0.dependent_table_id,
            subscription_state: value.0.subscription_state as _,
        }
    }
}

impl From<ObjectModel<index::Model>> for PbIndex {
    fn from(value: ObjectModel<index::Model>) -> Self {
        let (stream_job_status, create_type) = stream_job_status_and_create_type(&value.2);
        Self {
            id: value.0.index_id as _,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            owner: value.1.owner_id as _,
            index_table_id: value.0.index_table_id,
            primary_table_id: value.0.primary_table_id,
            index_item: value.0.index_items.to_protobuf(),
            index_column_properties: value
                .0
                .index_column_properties
                .map(|p| p.to_protobuf())
                .unwrap_or_default(),
            index_columns_len: value.0.index_columns_len as _,
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.initialized_at) as _)
                    .0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            stream_job_status,
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            create_type,
        }
    }
}

impl From<ObjectModel<view::Model>> for PbView {
    fn from(value: ObjectModel<view::Model>) -> Self {
        Self {
            id: value.0.view_id as _,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            sql: value.0.definition,
            columns: value.0.columns.to_protobuf(),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            created_at_cluster_version: value.1.created_at_cluster_version,
        }
    }
}

impl From<ObjectModel<connection::Model>> for PbConnection {
    fn from(value: ObjectModel<connection::Model>) -> Self {
        let info: PbConnectionInfo = if value.0.info == PrivateLinkService::default() {
            PbConnectionInfo::ConnectionParams(value.0.params.to_protobuf())
        } else {
            #[expect(deprecated)]
            PbConnectionInfo::PrivateLinkService(value.0.info.to_protobuf())
        };
        Self {
            id: value.1.oid.as_connection_id(),
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            owner: value.1.owner_id as _,
            info: Some(info),
        }
    }
}

impl From<ObjectModel<function::Model>> for PbFunction {
    fn from(value: ObjectModel<function::Model>) -> Self {
        Self {
            id: value.0.function_id as _,
            schema_id: value.1.schema_id.unwrap(),
            database_id: value.1.database_id.unwrap(),
            name: value.0.name,
            owner: value.1.owner_id as _,
            arg_names: value.0.arg_names.split(',').map(|s| s.to_owned()).collect(),
            arg_types: value.0.arg_types.to_protobuf(),
            return_type: Some(value.0.return_type.to_protobuf()),
            language: value.0.language,
            runtime: value.0.runtime,
            link: value.0.link,
            name_in_runtime: value.0.name_in_runtime,
            body: value.0.body,
            compressed_binary: value.0.compressed_binary,
            kind: Some(value.0.kind.into()),
            always_retry_on_network_error: value.0.always_retry_on_network_error,
            unsafe_skip_materializing_exprs: value.0.unsafe_skip_materializing_exprs,
            is_async: value
                .0
                .options
                .as_ref()
                .and_then(|o| o.0.get("async").map(|v| v == "true")),
            is_batched: value
                .0
                .options
                .as_ref()
                .and_then(|o| o.0.get("batch").map(|v| v == "true")),
            created_at_epoch: Some(
                Epoch::from_unix_millis(datetime_to_timestamp_millis(value.1.created_at) as _).0,
            ),
            created_at_cluster_version: value.1.created_at_cluster_version,
        }
    }
}
