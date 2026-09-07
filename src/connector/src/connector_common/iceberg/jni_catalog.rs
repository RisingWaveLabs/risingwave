// Copyright 2024 RisingWave Labs
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

//! This module provide jni catalog.

#![expect(
    clippy::disallowed_types,
    reason = "construct iceberg::Error to implement the trait"
)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Runtime, TableCommit, TableCreation, TableIdent,
    TableRequirement, TableUpdate,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use itertools::Itertools;
use jni::objects::{GlobalRef, JObject};
use risingwave_common::global_jvm::Jvm;
use risingwave_jni_core::call_method;
use risingwave_jni_core::jvm_runtime::{execute_with_jni_env, jobj_to_str};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::error::ConnectorResult;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadTableResponse {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub _config: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateTableRequest {
    /// The name of the table.
    pub name: String,
    /// The location of the table.
    pub location: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
    /// The partition spec of the table, could be None.
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// The sort order of the table.
    pub write_order: Option<SortOrder>,
    /// The properties of the table.
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitTableRequest {
    identifier: TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CommitTableResponse {
    metadata_location: String,
    metadata: TableMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListNamespacesResponse {
    namespaces: Vec<NamespaceIdent>,
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListTablesResponse {
    identifiers: Vec<TableIdent>,
    next_page_token: Option<String>,
}

impl From<&TableCreation> for CreateTableRequest {
    fn from(value: &TableCreation) -> Self {
        Self {
            name: value.name.clone(),
            location: value.location.clone(),
            schema: value.schema.clone(),
            partition_spec: value.partition_spec.clone(),
            write_order: value.sort_order.clone(),
            properties: value.properties.clone(),
        }
    }
}

fn namespace_to_string(namespace: &NamespaceIdent) -> String {
    namespace.iter().join(".")
}

#[derive(Debug)]
struct JniCatalogInner {
    java_catalog: GlobalRef,
    jvm: Jvm,
}

#[derive(Debug)]
pub struct JniCatalog {
    /// A blocking JNI operation can outlive its async caller after cancellation. Keep the Java
    /// catalog alive until the last such operation finishes.
    inner: Arc<JniCatalogInner>,
    file_io_props: Arc<HashMap<String, String>>,
}

/// Iceberg's Java catalog API is synchronous and may perform remote catalog I/O. Running it
/// directly in an async `Catalog` method would block a Tokio worker thread, so async JNI operations
/// go through the runtime's blocking pool.
async fn execute_blocking_jni<T>(
    task: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .context("Failed to join blocking Iceberg JNI catalog task")?
}

#[async_trait]
impl Catalog for JniCatalog {
    /// List namespaces from the catalog.
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        let inner = self.inner.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let result_json =
                    call_method!(env, inner.java_catalog.as_obj(), {String listNamespaces()})
                        .with_context(|| "Failed to list iceberg namespaces".to_owned())?;

                let rust_json_str = jobj_to_str(env, result_json)?;

                let resp: ListNamespacesResponse = serde_json::from_str(&rust_json_str)?;

                Ok(resp.namespaces)
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to list iceberg namespaces.",
            )
            .with_source(e)
        })
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        let inner = self.inner.clone();
        let namespace = namespace.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let namespace_str = namespace_to_string(&namespace);
                let namespace_jstr = env.new_string(&namespace_str).unwrap();

                call_method!(env, inner.java_catalog.as_obj(), {void createNamespace(String)},
                    &namespace_jstr)
                .with_context(|| format!("Failed to create namespace: {namespace}"))?;

                Ok(Namespace::new(namespace))
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to create namespace.",
            )
            .with_source(e)
        })
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        todo!()
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        let inner = self.inner.clone();
        let namespace = namespace.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let namespace_str = namespace_to_string(&namespace);
                let namespace_jstr = env.new_string(&namespace_str).unwrap();

                let exists =
                    call_method!(env, inner.java_catalog.as_obj(), {boolean namespaceExists(String)},
                    &namespace_jstr)
                    .with_context(|| format!("Failed to check namespace exists: {namespace}"))?;

                Ok(exists)
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to check namespace exists.",
            )
            .with_source(e)
        })
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        let inner = self.inner.clone();
        let namespace = namespace.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let namespace_str = namespace_to_string(&namespace);
                let namespace_jstr = env.new_string(&namespace_str).unwrap();

                let result_json =
                    call_method!(env, inner.java_catalog.as_obj(), {String listTables(String)},
                    &namespace_jstr)
                    .with_context(|| {
                        format!("Failed to list iceberg tables in namespace: {}", namespace)
                    })?;

                let rust_json_str = jobj_to_str(env, result_json)?;

                let resp: ListTablesResponse = serde_json::from_str(&rust_json_str)?;

                Ok(resp.identifiers)
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to list iceberg  tables.",
            )
            .with_source(e)
        })
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        let inner = self.inner.clone();
        let file_io_props = self.file_io_props.clone();
        let runtime = Runtime::try_current()?;
        let namespace = namespace.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let namespace_str = namespace_to_string(&namespace);
                let namespace_jstr = env.new_string(&namespace_str).unwrap();

                let creation_str = serde_json::to_string(&CreateTableRequest::from(&creation))?;

                let creation_jstr = env.new_string(&creation_str).unwrap();

                let result_json =
                    call_method!(env, inner.java_catalog.as_obj(), {String createTable(String, String)},
                    &namespace_jstr, &creation_jstr)
                    .with_context(|| {
                        format!("Failed to create iceberg table: {}", creation.name)
                    })?;

                let rust_json_str = jobj_to_str(env, result_json)?;

                let resp: LoadTableResponse = serde_json::from_str(&rust_json_str)?;

                let _metadata_location = resp.metadata_location.ok_or_else(|| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::FeatureUnsupported,
                        "Loading uncommitted table is not supported!",
                    )
                })?;

                let table_metadata = resp.metadata;

                let file_io =
                    FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
                    .with_props(file_io_props.iter())
                    .build();

                Ok(Table::builder()
                    .file_io(file_io)
                    .identifier(TableIdent::new(namespace, creation.name))
                    .metadata(table_metadata)
                    .runtime(runtime)
                    .build())
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to create iceberg table.",
            )
            .with_source(e)
        })?
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let inner = self.inner.clone();
        let file_io_props = self.file_io_props.clone();
        let runtime = Runtime::try_current()?;
        let table = table.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let table_name_str = table.to_string();

                let table_name_jstr = env.new_string(&table_name_str).unwrap();

                let result_json =
                    call_method!(env, inner.java_catalog.as_obj(), {String loadTable(String)},
                    &table_name_jstr)
                    .with_context(|| format!("Failed to load iceberg table: {table_name_str}"))?;

                let rust_json_str = jobj_to_str(env, result_json)?;

                let resp: LoadTableResponse = serde_json::from_str(&rust_json_str)?;

                let metadata_location = resp.metadata_location.ok_or_else(|| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::FeatureUnsupported,
                        "Loading uncommitted table is not supported!",
                    )
                })?;

                tracing::info!(
                    "Table metadata location of {table_name_str} is {metadata_location}"
                );

                let table_metadata = resp.metadata;

                let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
                    .with_props(file_io_props.iter())
                    .build();

                Ok(Table::builder()
                    .file_io(file_io)
                    .identifier(table)
                    .metadata(table_metadata)
                    .runtime(runtime)
                    .build())
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to load iceberg table.",
            )
            .with_source(e)
        })?
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let inner = self.inner.clone();
        let table = table.to_owned();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let table_name_str = table.to_string();

                let table_name_jstr = env.new_string(&table_name_str).unwrap();

                call_method!(env, inner.java_catalog.as_obj(), {boolean dropTable(String)},
                &table_name_jstr)
                .with_context(|| format!("Failed to drop iceberg table: {table_name_str}"))?;

                Ok(())
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to drop iceberg table.",
            )
            .with_source(e)
        })
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let table_info = self.load_table(table).await?;
        self.drop_table(table).await?;
        iceberg::drop_table_data(&table_info).await
    }

    async fn register_table(
        &self,
        _table_ident: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "register_table is not supported by JniCatalog",
        ))
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        let inner = self.inner.clone();
        let table = table.clone();
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let table_name_str = table.to_string();

                let table_name_jstr = env.new_string(&table_name_str).unwrap();

                let exists =
                    call_method!(env, inner.java_catalog.as_obj(), {boolean tableExists(String)},
                    &table_name_jstr)
                    .with_context(|| {
                        format!("Failed to check iceberg table exists: {table_name_str}")
                    })?;

                Ok(exists)
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to check iceberg table exists.",
            )
            .with_source(e)
        })
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        let inner = self.inner.clone();
        let file_io_props = self.file_io_props.clone();
        let runtime = Runtime::try_current()?;
        execute_blocking_jni(move || {
            execute_with_jni_env(inner.jvm, |env| {
                let requirements = commit.take_requirements();
                let updates = commit.take_updates();
                let request = CommitTableRequest {
                    identifier: commit.identifier().clone(),
                    requirements,
                    updates,
                };
                let request_str = serde_json::to_string(&request)?;

                let request_jni_str = env.new_string(&request_str).with_context(|| {
                    format!("Failed to create jni string from request json: {request_str}.")
                })?;

                let result_json =
                    call_method!(env, inner.java_catalog.as_obj(), {String updateTable(String)},
                    &request_jni_str)
                    .with_context(|| {
                        format!("Failed to update iceberg table: {}", commit.identifier())
                    })?;

                let rust_json_str = jobj_to_str(env, result_json)?;

                let response: CommitTableResponse = serde_json::from_str(&rust_json_str)?;

                tracing::info!(
                    "Table metadata location of {} is {}",
                    commit.identifier(),
                    response.metadata_location
                );

                let table_metadata = response.metadata;

                let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
                    .with_props(file_io_props.iter())
                    .build();

                Ok(Table::builder()
                    .file_io(file_io)
                    .identifier(commit.identifier().clone())
                    .metadata(table_metadata)
                    .runtime(runtime)
                    .build()?)
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to update iceberg table.",
            )
            .with_source(e)
        })
    }
}

impl Drop for JniCatalogInner {
    fn drop(&mut self) {
        let _ = execute_with_jni_env(self.jvm, |env| {
            call_method!(env, self.java_catalog.as_obj(), {void close()})
                .with_context(|| "Failed to close iceberg catalog".to_owned())?;
            Ok(())
        })
        .inspect_err(
            |e| tracing::error!(error = ?e.as_report(), "Failed to close iceberg catalog"),
        );
    }
}

impl JniCatalog {
    fn build(
        file_io_props: HashMap<String, String>,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Self> {
        let jvm = Jvm::get_or_init()?;

        execute_with_jni_env(jvm, |env| {
            // Convert props to string array
            let props = env.new_object_array(
                (java_catalog_props.len() * 2) as i32,
                "java/lang/String",
                JObject::null(),
            )?;
            for (i, (key, value)) in java_catalog_props.iter().enumerate() {
                let key_j_str = env.new_string(key)?;
                let value_j_str = env.new_string(value)?;
                env.set_object_array_element(&props, i as i32 * 2, key_j_str)?;
                env.set_object_array_element(&props, i as i32 * 2 + 1, value_j_str)?;
            }

            let jni_catalog_wrapper = env
                .call_static_method(
                    "com/risingwave/connector/catalog/JniCatalogWrapper",
                    "create",
                    "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)Lcom/risingwave/connector/catalog/JniCatalogWrapper;",
                    &[
                        (&env.new_string(name.to_string()).unwrap()).into(),
                        (&env.new_string(catalog_impl.to_string()).unwrap()).into(),
                        (&props).into(),
                    ],
                )?;

            let jni_catalog = env.new_global_ref(jni_catalog_wrapper.l().unwrap())?;

            Ok(Self {
                inner: Arc::new(JniCatalogInner {
                    java_catalog: jni_catalog,
                    jvm,
                }),
                file_io_props: Arc::new(file_io_props),
            })
        })
            .map_err(Into::into)
    }

    pub async fn build_catalog(
        file_io_props: HashMap<String, String>,
        name: impl ToString + Send + 'static,
        catalog_impl: impl ToString + Send + 'static,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Arc<dyn Catalog>> {
        let catalog = execute_blocking_jni(move || {
            Ok(Self::build(
                file_io_props,
                name,
                catalog_impl,
                java_catalog_props,
            )?)
        })
        .await?;
        Ok(Arc::new(catalog) as Arc<dyn Catalog>)
    }
}
