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

//! This module provide storage catalog.

#![expect(
    clippy::disallowed_types,
    reason = "construct iceberg::Error to implement the trait"
)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::{
    AZBLOB_ACCOUNT_KEY, AZBLOB_ACCOUNT_NAME, AZBLOB_ENDPOINT, FileIO, FileIOBuilder,
    GCS_CREDENTIALS_JSON, GCS_DISABLE_CONFIG_LOAD, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD,
    S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::spec::{TableMetadata, TableMetadataBuilder, TableProperties};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, Runtime, TableCommit,
    TableCreation, TableIdent,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use thiserror_ext::AsReport;
use typed_builder::TypedBuilder;

#[derive(Debug)]
pub enum StorageCatalogConfig {
    S3(StorageCatalogS3Config),
    Gcs(StorageCatalogGcsConfig),
    Azblob(StorageCatalogAzblobConfig),
}

#[derive(Clone, Debug, TypedBuilder)]
pub struct StorageCatalogS3Config {
    warehouse: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    path_style_access: Option<bool>,
    enable_config_load: Option<bool>,
}

#[derive(Clone, Debug, TypedBuilder)]
pub struct StorageCatalogGcsConfig {
    warehouse: String,
    credential: Option<String>,
    enable_config_load: Option<bool>,
}

#[derive(Clone, Debug, TypedBuilder)]
pub struct StorageCatalogAzblobConfig {
    warehouse: String,
    account_name: Option<String>,
    account_key: Option<String>,
    endpoint: Option<String>,
}

/// File system catalog.
#[derive(Debug)]
pub struct StorageCatalog {
    warehouse: String,
    file_io: FileIO,
}

impl StorageCatalog {
    pub fn new(config: StorageCatalogConfig) -> Result<Self> {
        let (warehouse, file_io) = match config {
            StorageCatalogConfig::S3(config) => {
                let mut file_io_builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::s3()));
                if let Some(access_key) = &config.access_key {
                    file_io_builder = file_io_builder.with_prop(S3_ACCESS_KEY_ID, access_key)
                };
                if let Some(secret_key) = &config.secret_key {
                    file_io_builder = file_io_builder.with_prop(S3_SECRET_ACCESS_KEY, secret_key)
                };
                if let Some(endpoint) = &config.endpoint {
                    file_io_builder = file_io_builder.with_prop(S3_ENDPOINT, endpoint)
                }
                if let Some(region) = &config.region {
                    file_io_builder = file_io_builder.with_prop(S3_REGION, region)
                }
                if let Some(path_style_access) = &config.path_style_access {
                    file_io_builder =
                        file_io_builder.with_prop(S3_PATH_STYLE_ACCESS, path_style_access);
                }
                let enable_config_load = config.enable_config_load.unwrap_or(false);
                file_io_builder = file_io_builder
                    .with_prop(S3_DISABLE_CONFIG_LOAD, (!enable_config_load).to_string());
                (config.warehouse, file_io_builder.build())
            }
            StorageCatalogConfig::Gcs(config) => {
                let mut file_io_builder =
                    FileIOBuilder::new(Arc::new(OpenDalStorageFactory::gcs()));
                if let Some(credential) = &config.credential {
                    file_io_builder = file_io_builder.with_prop(GCS_CREDENTIALS_JSON, credential)
                };
                let enable_config_load = config.enable_config_load.unwrap_or(false);
                file_io_builder = file_io_builder
                    .with_prop(GCS_DISABLE_CONFIG_LOAD, (!enable_config_load).to_string());
                (config.warehouse, file_io_builder.build())
            }
            StorageCatalogConfig::Azblob(config) => {
                let mut file_io_builder =
                    FileIOBuilder::new(Arc::new(OpenDalStorageFactory::azblob()));
                if let Some(account_name) = &config.account_name {
                    file_io_builder = file_io_builder.with_prop(AZBLOB_ACCOUNT_NAME, account_name)
                };
                if let Some(account_key) = &config.account_key {
                    file_io_builder = file_io_builder.with_prop(AZBLOB_ACCOUNT_KEY, account_key)
                };
                if let Some(endpoint) = &config.endpoint {
                    file_io_builder = file_io_builder.with_prop(AZBLOB_ENDPOINT, endpoint)
                };
                (config.warehouse, file_io_builder.build())
            }
        };

        Ok(StorageCatalog { warehouse, file_io })
    }

    /// Check if version hint file exist.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn is_version_hint_exist(&self, table_path: &str) -> Result<bool> {
        self.file_io
            .exists(format!("{table_path}/metadata/version-hint.text").as_str())
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("check if version hint exist failed: {}", err.as_report()),
                )
            })
    }

    /// Read version hint of table.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn read_version_hint(&self, table_path: &str) -> Result<i32> {
        let content = self
            .file_io
            .new_input(format!("{table_path}/metadata/version-hint.text").as_str())?
            .read()
            .await?;
        let version_hint = String::from_utf8(content.to_vec()).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Fail to convert version_hint from utf8 to string: {}",
                    err.as_report()
                ),
            )
        })?;

        version_hint
            .parse()
            .map_err(|_| Error::new(ErrorKind::DataInvalid, "parse version hint failed"))
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    fn table_path(&self, table: &TableIdent) -> String {
        let mut names = table.namespace.clone().inner();
        names.push(table.name.clone());
        if self.warehouse.ends_with('/') {
            format!("{}{}", self.warehouse, names.join("/"))
        } else {
            format!("{}/{}", self.warehouse, names.join("/"))
        }
    }

    async fn commit_table(&self, table_path: &str, next_metadata: TableMetadata) -> Result<()> {
        let current_version = if self.is_version_hint_exist(table_path).await? {
            self.read_version_hint(table_path).await?
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "no version hint found for table",
            ));
        };

        // # NOTE
        // Iceberg rust didn't support rename operation now, so this commit operation is not atomic.
        let final_metadata_file_path = format!(
            "{table_path}/metadata/v{}.metadata.json",
            current_version + 1
        );
        self.file_io()
            .new_output(final_metadata_file_path)?
            .write(serde_json::to_string(&next_metadata)?.into())
            .await?;

        // write version hint
        let final_file_path = format!("{table_path}/metadata/version-hint.text");
        if self
            .file_io()
            .exists(final_file_path.as_str())
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Fail to check exist"))?
        {
            self.file_io().delete(final_file_path.as_str()).await?;
        }
        self.file_io()
            .new_output(final_file_path)?
            .write(format!("{}", current_version + 1).into())
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Catalog for StorageCatalog {
    /// List namespaces from table.
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        return Ok(vec![]);
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        Ok(iceberg::Namespace::new(namespace.clone()))
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        todo!()
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        Ok(false)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// List tables from namespace.
    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        // FIXME: the iceberg `file_io` doesn't provide enough api to list files in a directory.
        Ok(vec![])
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
        mut creation: TableCreation,
    ) -> iceberg::Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let table_path = self.table_path(&table_ident);

        // Remove format-version from properties if exists, because TableMetadataBuilder doesn't allow this field in properties.
        // But we still allow users to set this field in TableCreation properties for other catalog like jdbc.
        creation
            .properties
            .remove(TableProperties::PROPERTY_FORMAT_VERSION);

        // Create the metadata directory
        let metadata_path = format!("{table_path}/metadata");

        // Create the initial table metadata
        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;

        // Write the initial metadata file
        let metadata_file_path = format!("{metadata_path}/v1.metadata.json");
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;

        // Write the version hint file
        let version_hint_path = format!("{table_path}/metadata/version-hint.text");
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output.write("1".into()).await?;

        Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .runtime(Runtime::try_current()?)
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let table_path = self.table_path(table);
        let path = if self.is_version_hint_exist(&table_path).await? {
            let version_hint = self.read_version_hint(&table_path).await?;
            format!("{table_path}/metadata/v{}.metadata.json", version_hint)
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "no version hint found for table",
            ));
        };

        let metadata_file = self.file_io.new_input(path)?;
        let metadata_file_content = metadata_file.read().await?;
        let table_metadata = serde_json::from_slice::<TableMetadata>(&metadata_file_content)?;

        Table::builder()
            .metadata(table_metadata)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            .runtime(Runtime::try_current()?)
            // Only support readonly table for storage catalog now.
            .readonly(true)
            .build()
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let table = self.load_table(table).await?;
        table
            .file_io()
            .delete_prefix(table.metadata().location())
            .await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.drop_table(table).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        let table_path = self.table_path(table);
        let metadata_path = format!("{table_path}/metadata/version-hint.text");
        self.file_io.exists(&metadata_path).await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to check if table exists: {}", err.as_report()),
            )
        })
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        let table = self.load_table(commit.identifier()).await?;
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        let metadata = table.metadata().clone();
        for requirement in requirements {
            requirement.check(Some(&metadata))?;
        }

        let mut metadata_builder = metadata.into_builder(None);
        for update in updates {
            metadata_builder = update.apply(metadata_builder)?;
        }

        self.commit_table(
            &self.table_path(table.identifier()),
            metadata_builder.build()?.metadata,
        )
        .await?;

        self.load_table(commit.identifier()).await
    }

    async fn register_table(
        &self,
        _table_ident: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "register_table is not supported in storage catalog",
        ))
    }
}
