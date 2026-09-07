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

pub mod compaction;
mod jni_catalog;
mod mock_catalog;
mod storage_catalog;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use ::iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use ::iceberg::table::Table;
use ::iceberg::{Catalog, CatalogBuilder, TableIdent};
use anyhow::{Context, anyhow};
use iceberg::io::object_cache::ObjectCache;
use iceberg::io::{
    ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID, ADLS_CLIENT_SECRET,
    ADLS_TENANT_ID, AZBLOB_ACCOUNT_KEY, AZBLOB_ACCOUNT_NAME, AZBLOB_ENDPOINT, GCS_CREDENTIALS_JSON,
    GCS_DISABLE_CONFIG_LOAD, S3_DISABLE_CONFIG_LOAD, S3_PATH_STYLE_ACCESS,
};
use iceberg_catalog_glue::{AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY};
use moka::future::Cache as MokaCache;
use phf::{Set, phf_set};
use risingwave_common::bail;
use risingwave_common::error::IcebergError;
use risingwave_common::util::deployment::Deployment;
use risingwave_common::util::env_var::env_var_is_true;
use serde::Deserialize;
use serde_with::serde_as;
use url::Url;
use uuid::Uuid;
use with_options::WithOptions;

use crate::connector_common::common::DISABLE_DEFAULT_CREDENTIAL;
use crate::connector_common::iceberg::storage_catalog::StorageCatalogConfig;
use crate::deserialize_optional_bool_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
pub struct IcebergCommon {
    // Catalog type supported by iceberg, such as "storage", "rest".
    // If not set, we use "storage" as default.
    #[serde(rename = "catalog.type")]
    pub catalog_type: Option<String>,
    #[serde(rename = "s3.region")]
    pub s3_region: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub s3_endpoint: Option<String>,
    #[serde(rename = "s3.access.key")]
    #[with_option(allow_alter_on_fly)]
    pub s3_access_key: Option<String>,
    #[serde(rename = "s3.secret.key")]
    #[with_option(allow_alter_on_fly)]
    pub s3_secret_key: Option<String>,
    #[serde(rename = "s3.iam_role_arn")]
    #[with_option(allow_alter_on_fly)]
    pub s3_iam_role_arn: Option<String>,

    #[serde(rename = "glue.access.key")]
    #[with_option(allow_alter_on_fly)]
    pub glue_access_key: Option<String>,
    #[serde(rename = "glue.secret.key")]
    #[with_option(allow_alter_on_fly)]
    pub glue_secret_key: Option<String>,
    #[serde(rename = "glue.iam_role_arn")]
    #[with_option(allow_alter_on_fly)]
    pub glue_iam_role_arn: Option<String>,
    #[serde(rename = "glue.region")]
    pub glue_region: Option<String>,
    #[serde(rename = "glue.endpoint")]
    pub glue_endpoint: Option<String>,
    /// AWS Client id, can be omitted for storage catalog or when
    /// caller's AWS account ID matches glue id
    #[serde(rename = "glue.id")]
    pub glue_id: Option<String>,

    #[serde(rename = "gcs.credential")]
    #[with_option(allow_alter_on_fly)]
    pub gcs_credential: Option<String>,

    #[serde(rename = "azblob.account_name")]
    pub azblob_account_name: Option<String>,
    #[serde(rename = "azblob.account_key")]
    pub azblob_account_key: Option<String>,
    #[serde(rename = "azblob.endpoint_url")]
    pub azblob_endpoint_url: Option<String>,

    #[serde(rename = "adlsgen2.account_name")]
    pub adlsgen2_account_name: Option<String>,
    #[serde(rename = "adlsgen2.account_key")]
    #[with_option(allow_alter_on_fly)]
    pub adlsgen2_account_key: Option<String>,
    #[serde(rename = "adlsgen2.endpoint")]
    pub adlsgen2_endpoint: Option<String>,
    #[serde(rename = "adlsgen2.tenant_id")]
    pub adlsgen2_tenant_id: Option<String>,
    #[serde(rename = "adlsgen2.client_id")]
    pub adlsgen2_client_id: Option<String>,
    #[serde(rename = "adlsgen2.client_secret")]
    pub adlsgen2_client_secret: Option<String>,
    #[serde(rename = "adlsgen2.authority_host")]
    pub adlsgen2_authority_host: Option<String>,

    /// Path of iceberg warehouse.
    #[serde(rename = "warehouse.path")]
    pub warehouse_path: Option<String>,
    /// Catalog name, default value is risingwave.
    #[serde(rename = "catalog.name")]
    pub catalog_name: Option<String>,
    /// URI of iceberg catalog, only applicable in rest catalog.
    #[serde(rename = "catalog.uri")]
    pub catalog_uri: Option<String>,
    /// Credential for accessing iceberg catalog, only applicable in rest catalog.
    /// A credential to exchange for a token in the `OAuth2` client credentials flow.
    #[serde(rename = "catalog.credential")]
    #[with_option(allow_alter_on_fly)]
    pub catalog_credential: Option<String>,
    /// token for accessing iceberg catalog, only applicable in rest catalog.
    /// A Bearer token which will be used for interaction with the server.
    #[serde(rename = "catalog.token")]
    #[with_option(allow_alter_on_fly)]
    pub catalog_token: Option<String>,
    /// `oauth2_server_uri` for accessing iceberg catalog, only applicable in rest catalog.
    /// Token endpoint URI to fetch token from if the Rest Catalog is not the authorization server.
    #[serde(rename = "catalog.oauth2_server_uri")]
    #[with_option(allow_alter_on_fly)]
    pub catalog_oauth2_server_uri: Option<String>,
    /// scope for accessing iceberg catalog, only applicable in rest catalog.
    /// Additional scope for `OAuth2`.
    #[serde(rename = "catalog.scope")]
    pub catalog_scope: Option<String>,

    /// The signing region to use when signing requests to the REST catalog.
    #[serde(rename = "catalog.rest.signing_region")]
    pub rest_signing_region: Option<String>,

    /// The signing name to use when signing requests to the REST catalog.
    #[serde(rename = "catalog.rest.signing_name")]
    pub rest_signing_name: Option<String>,

    /// Whether to use `SigV4` for signing requests to the REST catalog.
    #[serde(
        rename = "catalog.rest.sigv4_enabled",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub rest_sigv4_enabled: Option<bool>,

    #[serde(
        rename = "s3.path.style.access",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub s3_path_style_access: Option<bool>,
    /// Enable config load. This parameter set to true will load warehouse credentials from the environment. Only allowed to be used in a self-hosted environment.
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub enable_config_load: Option<bool>,

    /// This is only used by iceberg engine to enable the hosted catalog.
    #[serde(
        rename = "hosted_catalog",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub hosted_catalog: Option<bool>,

    /// The HTTP header to be used in catalog requests.
    /// Example:
    /// `catalog.header = "key1=value1;key2=value2;key3=value3"`
    /// For Google Cloud Lakehouse Iceberg REST catalogs, set
    /// `catalog.header = "x-goog-user-project=PROJECT_ID"` to specify the billing project.
    /// Explain the format of the header:
    /// - Each header is a key-value pair, separated by an '='.
    /// - Multiple headers can be specified, separated by a ';'.
    #[serde(rename = "catalog.header")]
    pub catalog_header: Option<String>,

    /// Enable vended credentials for Iceberg REST catalog.
    /// For Google Cloud Lakehouse Iceberg REST catalogs, this sends
    /// `X-Iceberg-Access-Delegation: vended-credentials`.
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub vended_credentials: Option<bool>,

    /// Security type for REST catalog authentication.
    /// Supported values: `none`, `oauth2`, `google`.
    /// When set to `google`, uses Iceberg's `GoogleAuthManager` (requires Iceberg 1.10+)
    /// for authentication using Google Application Default Credentials (ADC).
    #[serde(rename = "catalog.security")]
    pub catalog_security: Option<String>,

    /// OAuth-based scopes for Google authentication.
    /// Comma-separated list of OAuth-based scopes to request.
    /// Only applicable when `catalog.security` is set to `google`.
    /// Default: <https://www.googleapis.com/auth/cloud-platform>
    #[serde(rename = "gcp.auth.scopes")]
    pub gcp_auth_scopes: Option<String>,

    /// Custom `FileIO` implementation class for the Iceberg catalog.
    /// Allows specifying a custom `FileIO` implementation instead of the default.
    /// Examples:
    /// - `org.apache.iceberg.aws.s3.S3FileIO` for Amazon S3 (default)
    /// - `org.apache.iceberg.gcp.gcs.GCSFileIO` for Google Cloud Storage
    /// - `org.apache.iceberg.azure.adlsv2.ADLSFileIO` for Azure Data Lake Storage Gen2
    /// Google Cloud Lakehouse Iceberg REST catalogs with credential vending require
    /// `org.apache.iceberg.gcp.gcs.GCSFileIO`.
    #[serde(rename = "catalog.io_impl")]
    pub catalog_io_impl: Option<String>,
}

// Matches iceberg::io::object_cache default size (32MB).
// TODO: change it after object cache get refactored.
const DEFAULT_OBJECT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024;
const SHARED_OBJECT_CACHE_BUDGET_BYTES: u64 = 512 * 1024 * 1024;
const SHARED_OBJECT_CACHE_MAX_TABLES: u64 =
    SHARED_OBJECT_CACHE_BUDGET_BYTES / DEFAULT_OBJECT_CACHE_SIZE_BYTES;

/// Default Microsoft Entra (AAD) authority host for public Azure. Sovereign-cloud
/// users override via `adlsgen2.authority_host`.
const ADLS_DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";

impl EnforceSecret for IcebergCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "s3.access.key",
        "s3.secret.key",
        "gcs.credential",
        "catalog.credential",
        "catalog.token",
        "catalog.oauth2_server_uri",
        "adlsgen2.account_key",
        "adlsgen2.client_secret",
        "glue.access.key",
        "glue.secret.key",
    };
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
#[serde(deny_unknown_fields)]
pub struct IcebergTableIdentifier {
    #[serde(rename = "database.name")]
    pub database_name: Option<String>,
    /// Table name or namespace-qualified table name. Dots are treated as
    /// Iceberg namespace separators.
    #[serde(rename = "table.name")]
    pub table_name: String,
}

impl IcebergTableIdentifier {
    pub fn database_name(&self) -> Option<&str> {
        self.database_name.as_deref()
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    fn identifier_parts(&self) -> ConnectorResult<Vec<&str>> {
        let mut parts = Vec::new();
        if let Some(database_name) = &self.database_name {
            parts.extend(database_name.split('.'));
        }
        parts.extend(self.table_name.split('.'));

        if parts.iter().any(|part| part.is_empty()) {
            bail!(
                "Invalid iceberg table identifier '{}': identifier parts must not be empty",
                self.full_identifier()
            );
        }

        Ok(parts)
    }

    fn full_identifier(&self) -> String {
        match &self.database_name {
            Some(database_name) => format!("{}.{}", database_name, self.table_name),
            None => self.table_name.clone(),
        }
    }

    pub fn to_table_ident(&self) -> ConnectorResult<TableIdent> {
        let ret = TableIdent::from_strs(self.identifier_parts()?);

        Ok(ret.context("Failed to create table identifier")?)
    }

    pub fn validate(&self) -> ConnectorResult<()> {
        self.identifier_parts().map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergCatalogRuntime {
    NativeRust,
    JavaJni,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergCatalogKind {
    Storage,
    Rest(IcebergCatalogRuntime),
    Glue(IcebergCatalogRuntime),
    Hive,
    Jdbc,
    Snowflake,
    Mock,
}

impl IcebergCatalogKind {
    fn resolve(common: &IcebergCommon) -> ConnectorResult<Self> {
        let catalog_type = common.catalog_type();
        let kind = match catalog_type {
            "storage" => Self::Storage,
            "rest" if common.vended_credentials() => Self::Rest(IcebergCatalogRuntime::NativeRust),
            "rest" => Self::Rest(IcebergCatalogRuntime::JavaJni),
            "rest_rust" => Self::Rest(IcebergCatalogRuntime::NativeRust),
            "glue" => Self::Glue(IcebergCatalogRuntime::JavaJni),
            "glue_rust" => Self::Glue(IcebergCatalogRuntime::NativeRust),
            "hive" => Self::Hive,
            "jdbc" => Self::Jdbc,
            "snowflake" => Self::Snowflake,
            #[cfg(any(test, madsim))]
            "mock_v3" => Self::Mock,
            "mock" => Self::Mock,
            _ => {
                bail!(
                    "Unsupported catalog type: {}, only support `storage`, `rest`, `hive`, `jdbc`, `glue`, `snowflake`",
                    catalog_type
                )
            }
        };
        Ok(kind)
    }

    pub fn is_rest(self) -> bool {
        matches!(self, Self::Rest(_))
    }

    fn jni_impl(self) -> Option<JniCatalogImpl> {
        match self {
            Self::Rest(IcebergCatalogRuntime::JavaJni) => Some(JniCatalogImpl::Rest),
            Self::Glue(IcebergCatalogRuntime::JavaJni) => Some(JniCatalogImpl::Glue),
            Self::Hive => Some(JniCatalogImpl::Hive),
            Self::Jdbc => Some(JniCatalogImpl::Jdbc),
            Self::Snowflake => Some(JniCatalogImpl::Snowflake),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JniCatalogImpl {
    Hive,
    Jdbc,
    Snowflake,
    Rest,
    Glue,
}

impl JniCatalogImpl {
    fn catalog_type(self) -> &'static str {
        match self {
            Self::Hive => "hive",
            Self::Jdbc => "jdbc",
            Self::Snowflake => "snowflake",
            Self::Rest => "rest",
            Self::Glue => "glue",
        }
    }

    fn class_name(self) -> &'static str {
        match self {
            Self::Hive => "org.apache.iceberg.hive.HiveCatalog",
            Self::Jdbc => "org.apache.iceberg.jdbc.JdbcCatalog",
            Self::Snowflake => "org.apache.iceberg.snowflake.SnowflakeCatalog",
            Self::Rest => "org.apache.iceberg.rest.RESTCatalog",
            Self::Glue => "org.apache.iceberg.aws.glue.GlueCatalog",
        }
    }
}

enum CatalogBuildPlan {
    Storage(StorageCatalogConfig),
    NativeRest(HashMap<String, String>),
    NativeGlue(HashMap<String, String>),
    Jni {
        file_io_props: HashMap<String, String>,
        catalog_name: String,
        catalog_impl: JniCatalogImpl,
        java_catalog_props: HashMap<String, String>,
    },
    Mock,
}

pub struct ResolvedIcebergCatalogConfig<'a> {
    common: &'a IcebergCommon,
    kind: IcebergCatalogKind,
    java_catalog_props: HashMap<String, String>,
}

impl<'a> ResolvedIcebergCatalogConfig<'a> {
    pub fn kind(&self) -> IcebergCatalogKind {
        self.kind
    }

    fn build_plan(&self) -> ConnectorResult<CatalogBuildPlan> {
        match self.kind {
            IcebergCatalogKind::Storage => self.common.build_storage_catalog_config(),
            IcebergCatalogKind::Rest(IcebergCatalogRuntime::NativeRust) => {
                self.common.build_native_rest_catalog_props()
            }
            IcebergCatalogKind::Glue(IcebergCatalogRuntime::NativeRust) => {
                self.common.build_native_glue_catalog_props()
            }
            IcebergCatalogKind::Mock => Ok(CatalogBuildPlan::Mock),
            kind => {
                let catalog_impl = kind.jni_impl().expect("java catalog kind has JNI impl");
                let (file_io_props, java_catalog_props) = self
                    .common
                    .build_jni_catalog_configs(catalog_impl, &self.java_catalog_props)?;
                Ok(CatalogBuildPlan::Jni {
                    file_io_props,
                    catalog_name: self.common.catalog_name(),
                    catalog_impl,
                    java_catalog_props,
                })
            }
        }
    }

    pub async fn create_catalog(&self) -> ConnectorResult<Arc<dyn Catalog>> {
        match self.build_plan()? {
            CatalogBuildPlan::Storage(config) => {
                let catalog = storage_catalog::StorageCatalog::new(config)?;
                Ok(Arc::new(catalog))
            }
            CatalogBuildPlan::NativeRest(iceberg_configs) => {
                let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
                    .load("rest", iceberg_configs)
                    .await
                    .map_err(|e| anyhow!(IcebergError::from(e)))?;
                Ok(Arc::new(catalog))
            }
            CatalogBuildPlan::NativeGlue(iceberg_configs) => {
                let catalog = iceberg_catalog_glue::GlueCatalogBuilder::default()
                    .load("glue", iceberg_configs)
                    .await
                    .map_err(|e| anyhow!(IcebergError::from(e)))?;
                Ok(Arc::new(catalog))
            }
            CatalogBuildPlan::Jni {
                file_io_props,
                catalog_name,
                catalog_impl,
                java_catalog_props,
            } => {
                jni_catalog::JniCatalog::build_catalog(
                    file_io_props,
                    catalog_name,
                    catalog_impl.class_name(),
                    java_catalog_props,
                )
                .await
            }
            CatalogBuildPlan::Mock => Ok(Arc::new(mock_catalog::MockCatalog {})),
        }
    }

    pub async fn load_table(&self, table: &IcebergTableIdentifier) -> ConnectorResult<Table> {
        let catalog = self
            .create_catalog()
            .await
            .context("Unable to load iceberg catalog")?;

        let table_id = table
            .to_table_ident()
            .context("Unable to parse table name")?;

        let table = catalog.load_table(&table_id).await?;
        Ok(rebuild_table_with_shared_cache(table).await)
    }
}

pub fn iceberg_java_catalog_props_from_options<'a>(
    options: impl Iterator<Item = (&'a str, &'a str)>,
) -> HashMap<String, String> {
    options
        .filter(|(k, _v)| {
            k.starts_with("catalog.")
                && k != &"catalog.uri"
                && k != &"catalog.type"
                && k != &"catalog.name"
                && k != &"catalog.header"
        })
        .map(|(k, v)| (k[8..].to_owned(), v.to_owned()))
        .collect()
}

impl IcebergCommon {
    pub fn catalog_type(&self) -> &str {
        self.catalog_type.as_deref().unwrap_or("storage")
    }

    pub fn vended_credentials(&self) -> bool {
        self.vended_credentials.unwrap_or(false)
    }

    pub fn resolve_catalog_kind(&self) -> ConnectorResult<IcebergCatalogKind> {
        IcebergCatalogKind::resolve(self)
    }

    pub fn is_rest_catalog(&self) -> ConnectorResult<bool> {
        Ok(self.resolve_catalog_kind()?.is_rest())
    }

    pub fn resolve_catalog_config(
        &self,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<ResolvedIcebergCatalogConfig<'_>> {
        Ok(ResolvedIcebergCatalogConfig {
            common: self,
            kind: self.resolve_catalog_kind()?,
            java_catalog_props,
        })
    }

    fn glue_access_key(&self) -> Option<&str> {
        self.glue_access_key
            .as_deref()
            .or(self.s3_access_key.as_deref())
    }

    fn glue_secret_key(&self) -> Option<&str> {
        self.glue_secret_key
            .as_deref()
            .or(self.s3_secret_key.as_deref())
    }

    fn glue_region(&self) -> Option<&str> {
        self.glue_region.as_deref().or(self.s3_region.as_deref())
    }

    fn glue_endpoint(&self) -> Option<&str> {
        self.glue_endpoint
            .as_deref()
            .or_else(|| match self.resolve_catalog_kind() {
                Ok(IcebergCatalogKind::Glue(IcebergCatalogRuntime::JavaJni)) => {
                    self.catalog_uri.as_deref()
                }
                _ => None,
            })
    }

    pub fn catalog_name(&self) -> String {
        self.catalog_name
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "risingwave".to_owned())
    }

    pub fn headers(&self) -> ConnectorResult<HashMap<String, String>> {
        let mut headers = HashMap::new();
        let user_agent = match Deployment::current() {
            Deployment::Ci => "RisingWave(CI)".to_owned(),
            Deployment::Cloud => "RisingWave(Cloud)".to_owned(),
            Deployment::Other => "RisingWave(OSS)".to_owned(),
        };
        if self.vended_credentials() {
            headers.insert(
                "X-Iceberg-Access-Delegation".to_owned(),
                "vended-credentials".to_owned(),
            );
        }
        headers.insert("User-Agent".to_owned(), user_agent);
        if let Some(header) = &self.catalog_header {
            for pair in header.split(';') {
                let mut parts = pair.split('=');
                if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                    headers.insert(key.to_owned(), value.to_owned());
                } else {
                    bail!("Invalid header format: {}", pair);
                }
            }
        }
        Ok(headers)
    }

    pub fn enable_config_load(&self) -> bool {
        // If the env var is set to true, we disable the default config load. (Cloud environment)
        if env_var_is_true(DISABLE_DEFAULT_CREDENTIAL) {
            if matches!(self.enable_config_load, Some(true)) {
                tracing::warn!(
                    "`enable_config_load` can't be enabled in SaaS environment, the behavior might be unexpected"
                );
            }
            return false;
        }
        self.enable_config_load.unwrap_or(false)
    }

    fn effective_s3_path_style_access(&self) -> bool {
        // RisingWave historically inherited OpenDAL's path-style default. Iceberg now
        // defaults to virtual-host style, so preserve existing connector behavior unless
        // the user explicitly opts into virtual-host style with `false`.
        self.s3_path_style_access.unwrap_or(true)
    }

    fn build_storage_catalog_config(&self) -> ConnectorResult<CatalogBuildPlan> {
        let warehouse = self
            .warehouse_path
            .clone()
            .ok_or_else(|| anyhow!("`warehouse.path` must be set in storage catalog"))?;
        let url = Url::parse(warehouse.as_ref())
            .map_err(|_| anyhow!("Invalid warehouse path: {}", warehouse))?;

        let config = match url.scheme() {
            "s3" | "s3a" => StorageCatalogConfig::S3(
                storage_catalog::StorageCatalogS3Config::builder()
                    .warehouse(warehouse)
                    .access_key(self.s3_access_key.clone())
                    .secret_key(self.s3_secret_key.clone())
                    .region(self.s3_region.clone())
                    .endpoint(self.s3_endpoint.clone())
                    .path_style_access(Some(self.effective_s3_path_style_access()))
                    .enable_config_load(Some(self.enable_config_load()))
                    .build(),
            ),
            "gs" | "gcs" => StorageCatalogConfig::Gcs(
                storage_catalog::StorageCatalogGcsConfig::builder()
                    .warehouse(warehouse)
                    .credential(self.gcs_credential.clone())
                    .enable_config_load(Some(self.enable_config_load()))
                    .build(),
            ),
            "azblob" => StorageCatalogConfig::Azblob(
                storage_catalog::StorageCatalogAzblobConfig::builder()
                    .warehouse(warehouse)
                    .account_name(self.azblob_account_name.clone())
                    .account_key(self.azblob_account_key.clone())
                    .endpoint(self.azblob_endpoint_url.clone())
                    .build(),
            ),
            scheme => bail!("Unsupported warehouse scheme: {}", scheme),
        };

        Ok(CatalogBuildPlan::Storage(config))
    }

    fn build_native_rest_catalog_props(&self) -> ConnectorResult<CatalogBuildPlan> {
        let mut iceberg_configs = HashMap::new();

        // check gcs credential or s3 access key and secret key
        if let Some(gcs_credential) = &self.gcs_credential {
            iceberg_configs.insert(GCS_CREDENTIALS_JSON.to_owned(), gcs_credential.clone());
        } else {
            if let Some(region) = &self.s3_region {
                iceberg_configs.insert(S3_REGION.to_owned(), region.clone());
            }
            if let Some(endpoint) = &self.s3_endpoint {
                iceberg_configs.insert(S3_ENDPOINT.to_owned(), endpoint.clone());
            }
            if let Some(access_key) = &self.s3_access_key {
                iceberg_configs.insert(S3_ACCESS_KEY_ID.to_owned(), access_key.clone());
            }
            if let Some(secret_key) = &self.s3_secret_key {
                iceberg_configs.insert(S3_SECRET_ACCESS_KEY.to_owned(), secret_key.clone());
            }
            if let Some(path_style_access) = &self.s3_path_style_access {
                iceberg_configs.insert(
                    S3_PATH_STYLE_ACCESS.to_owned(),
                    path_style_access.to_string(),
                );
            }
        };

        if let Some(credential) = &self.catalog_credential {
            iceberg_configs.insert("credential".to_owned(), credential.clone());
        }
        if let Some(token) = &self.catalog_token {
            iceberg_configs.insert("token".to_owned(), token.clone());
        }
        if let Some(oauth2_server_uri) = &self.catalog_oauth2_server_uri {
            iceberg_configs.insert("oauth2-server-uri".to_owned(), oauth2_server_uri.clone());
        }
        if let Some(scope) = &self.catalog_scope {
            iceberg_configs.insert("scope".to_owned(), scope.clone());
        }

        let headers = self.headers()?;
        for (header_name, header_value) in headers {
            iceberg_configs.insert(format!("header.{}", header_name), header_value);
        }

        iceberg_configs.insert(
            iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_owned(),
            self.catalog_uri
                .clone()
                .with_context(|| "`catalog.uri` must be set in rest catalog".to_owned())?,
        );
        if let Some(warehouse_path) = &self.warehouse_path {
            iceberg_configs.insert(
                iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_owned(),
                warehouse_path.clone(),
            );
        }

        Ok(CatalogBuildPlan::NativeRest(iceberg_configs))
    }

    fn build_native_glue_catalog_props(&self) -> ConnectorResult<CatalogBuildPlan> {
        let mut iceberg_configs = HashMap::new();
        // glue
        if let Some(region) = self.glue_region() {
            iceberg_configs.insert(AWS_REGION_NAME.to_owned(), region.to_owned());
        }
        if let Some(access_key) = self.glue_access_key() {
            iceberg_configs.insert(AWS_ACCESS_KEY_ID.to_owned(), access_key.to_owned());
        }
        if let Some(secret_key) = self.glue_secret_key() {
            iceberg_configs.insert(AWS_SECRET_ACCESS_KEY.to_owned(), secret_key.to_owned());
        }
        // s3
        if let Some(region) = &self.s3_region {
            iceberg_configs.insert(S3_REGION.to_owned(), region.clone());
        }
        if let Some(endpoint) = &self.s3_endpoint {
            iceberg_configs.insert(S3_ENDPOINT.to_owned(), endpoint.clone());
        }
        if let Some(access_key) = &self.s3_access_key {
            iceberg_configs.insert(S3_ACCESS_KEY_ID.to_owned(), access_key.clone());
        }
        if let Some(secret_key) = &self.s3_secret_key {
            iceberg_configs.insert(S3_SECRET_ACCESS_KEY.to_owned(), secret_key.clone());
        }
        if let Some(role_arn) = &self.s3_iam_role_arn {
            iceberg_configs.insert(S3_ASSUME_ROLE_ARN.to_owned(), role_arn.clone());
        }
        iceberg_configs.insert(
            S3_PATH_STYLE_ACCESS.to_owned(),
            self.effective_s3_path_style_access().to_string(),
        );
        iceberg_configs.insert(
            iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE.to_owned(),
            self.warehouse_path
                .clone()
                .ok_or_else(|| anyhow!("`warehouse.path` must be set in glue catalog"))?,
        );
        if let Some(uri) = self.catalog_uri.as_deref() {
            iceberg_configs.insert(
                iceberg_catalog_glue::GLUE_CATALOG_PROP_URI.to_owned(),
                uri.to_owned(),
            );
        }

        Ok(CatalogBuildPlan::NativeGlue(iceberg_configs))
    }

    /// For both V1 and V2.
    fn build_jni_catalog_configs(
        &self,
        catalog_impl: JniCatalogImpl,
        java_catalog_props: &HashMap<String, String>,
    ) -> ConnectorResult<(HashMap<String, String>, HashMap<String, String>)> {
        let mut iceberg_configs = HashMap::new();
        let enable_config_load = self.enable_config_load();
        let file_io_props = {
            let catalog_type = catalog_impl.catalog_type();

            // Non-S3/Glue object-store backends only work with a REST catalog. This
            // function is only invoked for catalog_type in {hive, snowflake, jdbc, rest,
            // glue}, so the only accepted value here is "rest".
            let require_rest = |backend: &str| -> ConnectorResult<()> {
                if catalog_impl != JniCatalogImpl::Rest {
                    bail!("{} unsupported in {} catalog", backend, catalog_type);
                }
                Ok(())
            };

            if let Some(region) = &self.s3_region {
                // iceberg-rust
                iceberg_configs.insert(S3_REGION.to_owned(), region.clone());
            }

            if let Some(endpoint) = &self.s3_endpoint {
                // iceberg-rust
                iceberg_configs.insert(S3_ENDPOINT.to_owned(), endpoint.clone());
            }

            // iceberg-rust
            if let Some(access_key) = &self.s3_access_key {
                iceberg_configs.insert(S3_ACCESS_KEY_ID.to_owned(), access_key.clone());
            }
            if let Some(secret_key) = &self.s3_secret_key {
                iceberg_configs.insert(S3_SECRET_ACCESS_KEY.to_owned(), secret_key.clone());
            }
            if let Some(role_arn) = &self.s3_iam_role_arn {
                iceberg_configs.insert(S3_ASSUME_ROLE_ARN.to_owned(), role_arn.clone());
            }
            if let Some(gcs_credential) = &self.gcs_credential {
                iceberg_configs.insert(GCS_CREDENTIALS_JSON.to_owned(), gcs_credential.clone());
                require_rest("gcs")?;
            }

            if let (
                Some(azblob_account_name),
                Some(azblob_account_key),
                Some(azblob_endpoint_url),
            ) = (
                &self.azblob_account_name,
                &self.azblob_account_key,
                &self.azblob_endpoint_url,
            ) {
                iceberg_configs.insert(AZBLOB_ACCOUNT_NAME.to_owned(), azblob_account_name.clone());
                iceberg_configs.insert(AZBLOB_ACCOUNT_KEY.to_owned(), azblob_account_key.clone());
                iceberg_configs.insert(AZBLOB_ENDPOINT.to_owned(), azblob_endpoint_url.clone());

                require_rest("azblob")?;
            }

            // Validate adlsgen2 auth configuration before populating iceberg_configs.
            // Treat empty and whitespace-only strings as unset — serde surfaces
            // `adlsgen2.tenant_id = ''` (or a value with trailing `\n` from a copy-paste)
            // as `Some("...")` which would pass `is_some()` but break downstream auth.
            fn nonempty(v: &Option<String>) -> Option<&str> {
                v.as_deref().filter(|s| !s.trim().is_empty())
            }
            let sp_tenant = nonempty(&self.adlsgen2_tenant_id);
            let sp_client = nonempty(&self.adlsgen2_client_id);
            let sp_secret = nonempty(&self.adlsgen2_client_secret);
            let sp_authority = nonempty(&self.adlsgen2_authority_host);
            let sk_account_name = nonempty(&self.adlsgen2_account_name);
            let sk_account_key = nonempty(&self.adlsgen2_account_key);
            let any_sp_field = sp_tenant.is_some()
                || sp_client.is_some()
                || sp_secret.is_some()
                || sp_authority.is_some();
            let all_sp_required = sp_tenant.is_some() && sp_client.is_some() && sp_secret.is_some();

            if sk_account_key.is_some() && any_sp_field {
                bail!(
                    "adlsgen2: cannot configure both shared-key auth \
                     (adlsgen2.account_key) and service-principal auth \
                     (adlsgen2.tenant_id / adlsgen2.client_id / adlsgen2.client_secret / \
                     adlsgen2.authority_host) simultaneously. Specify exactly one auth mode."
                );
            }
            if any_sp_field && !all_sp_required {
                bail!(
                    "adlsgen2: service-principal auth requires all three of \
                     adlsgen2.tenant_id, adlsgen2.client_id, and adlsgen2.client_secret \
                     to be set. (adlsgen2.authority_host is optional and defaults to the \
                     public Azure AAD endpoint.)"
                );
            }
            // Defense in depth: reqsign POSTs the OAuth token request — carrying the
            // client_secret to this host. Require a bare https origin: no userinfo,
            // no query, no fragment, and no path beyond "/". The value itself is not
            // echoed into error messages in case a user pasted a secret by mistake.
            if let Some(host) = sp_authority {
                let parsed = Url::parse(host).map_err(|_| {
                    anyhow!(
                        "adlsgen2.authority_host does not parse as a URL ({} chars)",
                        host.len()
                    )
                })?;
                if parsed.scheme() != "https" {
                    bail!(
                        "adlsgen2.authority_host must use the https scheme, got {}",
                        parsed.scheme()
                    );
                }
                if !parsed.username().is_empty() || parsed.password().is_some() {
                    bail!("adlsgen2.authority_host must not contain userinfo");
                }
                if parsed.query().is_some() || parsed.fragment().is_some() {
                    bail!("adlsgen2.authority_host must not contain a query or fragment");
                }
                if !matches!(parsed.path(), "" | "/") {
                    bail!("adlsgen2.authority_host must not contain a path component");
                }
            }

            if let (Some(account_name), Some(account_key)) = (sk_account_name, sk_account_key) {
                iceberg_configs.insert(ADLS_ACCOUNT_NAME.to_owned(), account_name.to_owned());
                iceberg_configs.insert(ADLS_ACCOUNT_KEY.to_owned(), account_key.to_owned());
                require_rest("adlsgen2")?;
            }

            if let (Some(tenant_id), Some(client_id), Some(client_secret)) =
                (sp_tenant, sp_client, sp_secret)
            {
                iceberg_configs.insert(ADLS_TENANT_ID.to_owned(), tenant_id.to_owned());
                iceberg_configs.insert(ADLS_CLIENT_ID.to_owned(), client_id.to_owned());
                iceberg_configs.insert(ADLS_CLIENT_SECRET.to_owned(), client_secret.to_owned());
                // Strip trailing slash to prevent double slash
                let authority_host = sp_authority
                    .unwrap_or(ADLS_DEFAULT_AUTHORITY_HOST)
                    .trim_end_matches('/')
                    .to_owned();
                iceberg_configs.insert(ADLS_AUTHORITY_HOST.to_owned(), authority_host);
                require_rest("adlsgen2")?;
            }

            match &self.warehouse_path {
                Some(warehouse_path) => {
                    let (bucket, _) = {
                        let is_s3_tables = warehouse_path.starts_with("arn:aws:s3tables");
                        // Lakehouse Iceberg REST catalog federation uses bq:// prefix for BigQuery-managed Iceberg tables.
                        let is_bq_catalog_federation = warehouse_path.starts_with("bq://");
                        let url = Url::parse(warehouse_path);
                        if (url.is_err() || is_s3_tables || is_bq_catalog_federation)
                            && catalog_impl == JniCatalogImpl::Rest
                        {
                            // If the warehouse path is not a valid URL, it could be:
                            // - A warehouse name in REST catalog
                            // - An S3 Tables path (arn:aws:s3tables:...)
                            // - A Lakehouse path (bq://projects/...) for Google Cloud BigQuery integration
                            // We allow these to pass through for REST catalogs.
                            (None, None)
                        } else {
                            let url = url.with_context(|| {
                                format!("Invalid warehouse path: {}", warehouse_path)
                            })?;
                            let bucket = url
                                .host_str()
                                .with_context(|| {
                                    format!(
                                        "Invalid s3 path: {}, bucket is missing",
                                        warehouse_path
                                    )
                                })?
                                .to_owned();
                            let root = url.path().trim_start_matches('/').to_owned();
                            (Some(bucket), Some(root))
                        }
                    };

                    if let Some(bucket) = bucket {
                        iceberg_configs.insert("iceberg.table.io.bucket".to_owned(), bucket);
                    }
                }
                None => {
                    if catalog_impl != JniCatalogImpl::Rest {
                        bail!("`warehouse.path` must be set in {} catalog", catalog_type);
                    }
                }
            }
            iceberg_configs.insert(
                S3_DISABLE_CONFIG_LOAD.to_owned(),
                (!enable_config_load).to_string(),
            );

            iceberg_configs.insert(
                GCS_DISABLE_CONFIG_LOAD.to_owned(),
                (!enable_config_load).to_string(),
            );

            iceberg_configs.insert(
                S3_PATH_STYLE_ACCESS.to_owned(),
                self.effective_s3_path_style_access().to_string(),
            );

            iceberg_configs
        };

        // Prepare jni configs, for details please see https://iceberg.apache.org/docs/latest/aws/
        let mut java_catalog_configs = HashMap::new();
        {
            if let Some(uri) = self.catalog_uri.as_deref() {
                java_catalog_configs.insert("uri".to_owned(), uri.to_owned());
            }

            if let Some(warehouse_path) = &self.warehouse_path {
                java_catalog_configs.insert("warehouse".to_owned(), warehouse_path.clone());
            }
            java_catalog_configs.extend(java_catalog_props.clone());

            // Set io-impl: use custom io-impl if provided, otherwise default to S3FileIO
            let io_impl = self
                .catalog_io_impl
                .clone()
                .unwrap_or_else(|| "org.apache.iceberg.aws.s3.S3FileIO".to_owned());
            java_catalog_configs.insert("io-impl".to_owned(), io_impl);

            // suppress log of FileIO like: Unclosed FileIO instance created by...
            java_catalog_configs.insert("init-creation-stacktrace".to_owned(), "false".to_owned());

            if let Some(region) = &self.s3_region {
                java_catalog_configs.insert("client.region".to_owned(), region.clone());
            }
            if let Some(endpoint) = &self.s3_endpoint {
                java_catalog_configs.insert("s3.endpoint".to_owned(), endpoint.clone());
            }

            if let Some(access_key) = &self.s3_access_key {
                java_catalog_configs.insert("s3.access-key-id".to_owned(), access_key.clone());
            }
            if let Some(secret_key) = &self.s3_secret_key {
                java_catalog_configs.insert("s3.secret-access-key".to_owned(), secret_key.clone());
            }

            if let Some(path_style_access) = &self.s3_path_style_access {
                java_catalog_configs.insert(
                    "s3.path-style-access".to_owned(),
                    path_style_access.to_string(),
                );
            }

            let headers = self.headers()?;
            for (header_name, header_value) in headers {
                java_catalog_configs.insert(format!("header.{}", header_name), header_value);
            }

            match catalog_impl {
                JniCatalogImpl::Rest => {
                    // Handle security type for REST catalog (Iceberg 1.10+)
                    if let Some(security) = &self.catalog_security {
                        match security.to_lowercase().as_str() {
                            "google" => {
                                // Google AuthManager (Iceberg 1.10+) - uses Google ADC
                                java_catalog_configs.insert(
                                    "rest.auth.type".to_owned(),
                                    "org.apache.iceberg.gcp.auth.GoogleAuthManager".to_owned(),
                                );
                                // Set GCP auth scopes if provided
                                if let Some(gcp_auth_scopes) = &self.gcp_auth_scopes {
                                    java_catalog_configs.insert(
                                        "gcp.auth.scopes".to_owned(),
                                        gcp_auth_scopes.clone(),
                                    );
                                }
                            }
                            "oauth2" => {
                                // Standard OAuth2 authentication
                                if let Some(credential) = &self.catalog_credential {
                                    java_catalog_configs
                                        .insert("credential".to_owned(), credential.clone());
                                }
                                if let Some(token) = &self.catalog_token {
                                    java_catalog_configs.insert("token".to_owned(), token.clone());
                                }
                                if let Some(oauth2_server_uri) = &self.catalog_oauth2_server_uri {
                                    java_catalog_configs.insert(
                                        "oauth2-server-uri".to_owned(),
                                        oauth2_server_uri.clone(),
                                    );
                                }
                                if let Some(scope) = &self.catalog_scope {
                                    java_catalog_configs.insert("scope".to_owned(), scope.clone());
                                }
                            }
                            "none" | "" => {
                                // No authentication
                            }
                            _ => {
                                tracing::warn!(
                                    "Unknown catalog.security value: {}. Supported values: none, oauth2, google",
                                    security
                                );
                            }
                        }
                    } else {
                        // Legacy behavior: use individual OAuth2 properties if security type not specified
                        if let Some(credential) = &self.catalog_credential {
                            java_catalog_configs
                                .insert("credential".to_owned(), credential.clone());
                        }
                        if let Some(token) = &self.catalog_token {
                            java_catalog_configs.insert("token".to_owned(), token.clone());
                        }
                        if let Some(oauth2_server_uri) = &self.catalog_oauth2_server_uri {
                            java_catalog_configs
                                .insert("oauth2-server-uri".to_owned(), oauth2_server_uri.clone());
                        }
                        if let Some(scope) = &self.catalog_scope {
                            java_catalog_configs.insert("scope".to_owned(), scope.clone());
                        }
                    }
                    if let Some(rest_signing_region) = &self.rest_signing_region {
                        java_catalog_configs.insert(
                            "rest.signing-region".to_owned(),
                            rest_signing_region.clone(),
                        );
                    }
                    if let Some(rest_signing_name) = &self.rest_signing_name {
                        java_catalog_configs
                            .insert("rest.signing-name".to_owned(), rest_signing_name.clone());
                    }
                    if let Some(rest_sigv4_enabled) = self.rest_sigv4_enabled {
                        java_catalog_configs.insert(
                            "rest.sigv4-enabled".to_owned(),
                            rest_sigv4_enabled.to_string(),
                        );

                        if let Some(access_key) = &self.s3_access_key {
                            java_catalog_configs
                                .insert("rest.access-key-id".to_owned(), access_key.clone());
                        }

                        if let Some(secret_key) = &self.s3_secret_key {
                            java_catalog_configs
                                .insert("rest.secret-access-key".to_owned(), secret_key.clone());
                        }
                    }
                }
                JniCatalogImpl::Glue => {
                    let glue_access_key = self.glue_access_key();
                    let glue_secret_key = self.glue_secret_key();
                    let has_glue_credentials =
                        glue_access_key.is_some() && glue_secret_key.is_some();
                    let should_configure_glue_provider = !enable_config_load
                        || has_glue_credentials
                        || self.glue_iam_role_arn.is_some();

                    if should_configure_glue_provider {
                        java_catalog_configs.insert(
                            "client.credentials-provider".to_owned(),
                            "com.risingwave.connector.catalog.GlueCredentialProvider".to_owned(),
                        );
                        if let Some(region) = self.glue_region() {
                            java_catalog_configs.insert(
                                "client.credentials-provider.glue.region".to_owned(),
                                region.to_owned(),
                            );
                        }
                        if let Some(access_key) = glue_access_key {
                            java_catalog_configs.insert(
                                "client.credentials-provider.glue.access-key-id".to_owned(),
                                access_key.to_owned(),
                            );
                        }
                        if let Some(secret_key) = glue_secret_key {
                            java_catalog_configs.insert(
                                "client.credentials-provider.glue.secret-access-key".to_owned(),
                                secret_key.to_owned(),
                            );
                        }
                        if let Some(role_arn) = self.glue_iam_role_arn.as_deref() {
                            java_catalog_configs.insert(
                                "client.credentials-provider.glue.iam-role-arn".to_owned(),
                                role_arn.to_owned(),
                            );
                        }
                        if enable_config_load && !has_glue_credentials {
                            java_catalog_configs.insert(
                                "client.credentials-provider.glue.use-default-credential-chain"
                                    .to_owned(),
                                "true".to_owned(),
                            );
                        }
                    }

                    if let Some(region) = self.glue_region() {
                        java_catalog_configs.insert("client.region".to_owned(), region.to_owned());
                    }
                    let glue_endpoint = self.glue_endpoint().map(str::to_owned).or_else(|| {
                        self.glue_region()
                            .map(|region| format!("https://glue.{}.amazonaws.com", region))
                    });
                    if let Some(endpoint) = glue_endpoint {
                        java_catalog_configs.insert("glue.endpoint".to_owned(), endpoint);
                    }

                    if let Some(glue_id) = self.glue_id.as_deref() {
                        java_catalog_configs.insert("glue.id".to_owned(), glue_id.to_owned());
                    }
                    self.apply_java_s3_file_io_assume_role_configs(&mut java_catalog_configs);
                }
                JniCatalogImpl::Jdbc => {
                    self.apply_java_aws_client_assume_role_configs(&mut java_catalog_configs);
                }
                _ => {}
            }
        }

        Ok((file_io_props, java_catalog_configs))
    }

    fn apply_java_s3_file_io_assume_role_configs(
        &self,
        java_catalog_configs: &mut HashMap<String, String>,
    ) {
        if let Some(iam_role_arn) = &self.s3_iam_role_arn {
            java_catalog_configs.insert(
                "s3.client-factory-impl".to_owned(),
                "com.risingwave.connector.catalog.S3FileIOAssumeRoleAwsClientFactory".to_owned(),
            );
            java_catalog_configs.insert("s3.iam-role-arn".to_owned(), iam_role_arn.clone());
        }
    }

    fn apply_java_aws_client_assume_role_configs(
        &self,
        java_catalog_configs: &mut HashMap<String, String>,
    ) {
        if let Some(iam_role_arn) = &self.s3_iam_role_arn {
            java_catalog_configs.insert("client.assume-role.arn".to_owned(), iam_role_arn.clone());
            java_catalog_configs.insert(
                "client.factory".to_owned(),
                "org.apache.iceberg.aws.AssumeRoleAwsClientFactory".to_owned(),
            );
            if let Some(region) = &self.s3_region {
                java_catalog_configs.insert("client.assume-role.region".to_owned(), region.clone());
            }
        }
    }
}

/// Get a globally shared object cache keyed by table UUID to avoid reuse after drop & recreate.
pub(crate) async fn shared_object_cache(
    init_object_cache: Arc<ObjectCache>,
    table_uuid: Uuid,
) -> Arc<ObjectCache> {
    static CACHE: LazyLock<MokaCache<Uuid, Arc<ObjectCache>>> = LazyLock::new(|| {
        MokaCache::builder()
            .max_capacity(SHARED_OBJECT_CACHE_MAX_TABLES)
            .build()
    });

    CACHE
        .get_with(table_uuid, async { init_object_cache })
        .await
}

pub async fn rebuild_table_with_shared_cache(table: Table) -> Table {
    let table_uuid = table.metadata().uuid();
    let init_object_cache = table.object_cache();
    let object_cache = shared_object_cache(init_object_cache, table_uuid).await;
    table.with_object_cache(object_cache)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn test_common(catalog_type: &str) -> IcebergCommon {
        IcebergCommon {
            catalog_type: Some(catalog_type.to_owned()),
            s3_region: Some("ap-southeast-2".to_owned()),
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            s3_iam_role_arn: None,
            glue_access_key: None,
            glue_secret_key: None,
            glue_iam_role_arn: None,
            glue_region: None,
            glue_endpoint: None,
            glue_id: None,
            gcs_credential: None,
            azblob_account_name: None,
            azblob_account_key: None,
            azblob_endpoint_url: None,
            adlsgen2_account_name: None,
            adlsgen2_account_key: None,
            adlsgen2_endpoint: None,
            adlsgen2_tenant_id: None,
            adlsgen2_client_id: None,
            adlsgen2_client_secret: None,
            adlsgen2_authority_host: None,
            warehouse_path: Some("s3://bucket/warehouse".to_owned()),
            catalog_name: None,
            catalog_uri: None,
            catalog_credential: None,
            catalog_token: None,
            catalog_oauth2_server_uri: None,
            catalog_scope: None,
            rest_signing_region: None,
            rest_signing_name: None,
            rest_sigv4_enabled: None,
            s3_path_style_access: None,
            enable_config_load: None,
            hosted_catalog: None,
            catalog_header: None,
            vended_credentials: None,
            catalog_security: None,
            gcp_auth_scopes: None,
            catalog_io_impl: None,
        }
    }

    #[test]
    fn test_vended_rest_resolves_to_native_runtime_without_rewriting_catalog_type() {
        let common = IcebergCommon {
            vended_credentials: Some(true),
            ..test_common("rest")
        };

        assert_eq!(common.catalog_type(), "rest");
        assert_eq!(
            common.resolve_catalog_kind().unwrap(),
            IcebergCatalogKind::Rest(IcebergCatalogRuntime::NativeRust)
        );
    }

    #[test]
    fn test_rest_without_vended_credentials_resolves_to_jni_runtime() {
        let common = test_common("rest");

        assert_eq!(
            common.resolve_catalog_kind().unwrap(),
            IcebergCatalogKind::Rest(IcebergCatalogRuntime::JavaJni)
        );
    }

    #[test]
    fn test_s3_path_style_access_preserves_existing_default() {
        let common = test_common("storage");
        assert!(common.effective_s3_path_style_access());

        let common = IcebergCommon {
            s3_path_style_access: Some(false),
            ..common
        };
        assert!(!common.effective_s3_path_style_access());
    }

    #[test]
    fn test_mock_v3_resolves_to_mock_catalog_for_simulation_tests() {
        let common = test_common("mock_v3");

        assert_eq!(
            common.resolve_catalog_kind().unwrap(),
            IcebergCatalogKind::Mock
        );
    }

    #[test]
    fn test_extract_java_catalog_props_keeps_wire_options_flat() {
        let options = HashMap::from([
            ("catalog.type".to_owned(), "rest".to_owned()),
            ("catalog.uri".to_owned(), "http://localhost:8181".to_owned()),
            ("catalog.name".to_owned(), "demo".to_owned()),
            ("catalog.header".to_owned(), "x=y".to_owned()),
            (
                "catalog.rest.signing_region".to_owned(),
                "us-east-1".to_owned(),
            ),
            ("catalog.jdbc.user".to_owned(), "rw".to_owned()),
        ]);

        let java_props = iceberg_java_catalog_props_from_options(
            options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );

        assert_eq!(java_props.get("rest.signing_region").unwrap(), "us-east-1");
        assert_eq!(java_props.get("jdbc.user").unwrap(), "rw");
        assert!(!java_props.contains_key("type"));
        assert!(!java_props.contains_key("uri"));
        assert!(!java_props.contains_key("name"));
        assert!(!java_props.contains_key("header"));
    }

    #[test]
    fn test_glue_jni_catalog_uses_s3_assume_role_for_file_io() {
        let common = IcebergCommon {
            s3_iam_role_arn: Some("arn:aws:iam::123456789012:role/risingwave-s3".to_owned()),
            ..test_common("glue")
        };

        let (_, java_catalog_configs) = common
            .build_jni_catalog_configs(JniCatalogImpl::Glue, &HashMap::new())
            .unwrap();

        assert_eq!(
            java_catalog_configs.get("s3.client-factory-impl").unwrap(),
            "com.risingwave.connector.catalog.S3FileIOAssumeRoleAwsClientFactory"
        );
        assert_eq!(
            java_catalog_configs.get("s3.iam-role-arn").unwrap(),
            "arn:aws:iam::123456789012:role/risingwave-s3"
        );
        assert!(!java_catalog_configs.contains_key("client.factory"));
    }

    #[test]
    fn test_adlsgen2_service_principal_populates_file_io_configs_with_default_authority_host() {
        let common = test_adlsgen2_service_principal_common(None);

        let (file_io_props, _) = common
            .build_jni_catalog_configs(JniCatalogImpl::Rest, &HashMap::new())
            .unwrap();

        assert_eq!(file_io_props.get(ADLS_TENANT_ID).unwrap(), "tenant-uuid");
        assert_eq!(file_io_props.get(ADLS_CLIENT_ID).unwrap(), "client-uuid");
        assert_eq!(
            file_io_props.get(ADLS_CLIENT_SECRET).unwrap(),
            "secret-value"
        );
        assert_eq!(
            file_io_props.get(ADLS_AUTHORITY_HOST).unwrap(),
            ADLS_DEFAULT_AUTHORITY_HOST
        );
    }

    fn test_adlsgen2_service_principal_common(authority_host: Option<&str>) -> IcebergCommon {
        IcebergCommon {
            adlsgen2_account_name: Some("acct".to_owned()),
            adlsgen2_tenant_id: Some("tenant-uuid".to_owned()),
            adlsgen2_client_id: Some("client-uuid".to_owned()),
            adlsgen2_client_secret: Some("secret-value".to_owned()),
            adlsgen2_authority_host: authority_host.map(str::to_owned),
            warehouse_path: Some("abfss://wh@acct.dfs.core.windows.net/wh".to_owned()),
            ..test_common("rest")
        }
    }

    #[test]
    fn test_adlsgen2_service_principal_authority_host_override_is_respected() {
        let common =
            test_adlsgen2_service_principal_common(Some("https://login.microsoftonline.us"));

        let (file_io_props, _) = common
            .build_jni_catalog_configs(JniCatalogImpl::Rest, &HashMap::new())
            .unwrap();

        assert_eq!(
            file_io_props.get(ADLS_AUTHORITY_HOST).unwrap(),
            "https://login.microsoftonline.us"
        );
    }

    #[test]
    fn test_adlsgen2_authority_host_rejects_non_bare_https_origins() {
        let cases = [
            ("not a url", "does not parse as a URL"),
            (
                "http://login.microsoftonline.com",
                "must use the https scheme",
            ),
            (
                "https://user:pass@login.microsoftonline.com",
                "must not contain userinfo",
            ),
            (
                "https://login.microsoftonline.com?bar=baz",
                "must not contain a query or fragment",
            ),
            (
                "https://login.microsoftonline.com#frag",
                "must not contain a query or fragment",
            ),
            (
                "https://login.microsoftonline.com/foo",
                "must not contain a path component",
            ),
        ];
        for (authority_host, expected_error) in cases {
            let common = test_adlsgen2_service_principal_common(Some(authority_host));
            let err = common
                .build_jni_catalog_configs(JniCatalogImpl::Rest, &HashMap::new())
                .unwrap_err();
            assert!(
                format!("{:#}", err).contains(expected_error),
                "authority_host {authority_host:?}: expected error containing {expected_error:?}, got: {err:#}"
            );
        }
    }

    #[test]
    fn test_adlsgen2_authority_host_trailing_slash_is_normalized() {
        let common =
            test_adlsgen2_service_principal_common(Some("https://login.microsoftonline.us/"));

        let (file_io_props, _) = common
            .build_jni_catalog_configs(JniCatalogImpl::Rest, &HashMap::new())
            .unwrap();

        assert_eq!(
            file_io_props.get(ADLS_AUTHORITY_HOST).unwrap(),
            "https://login.microsoftonline.us"
        );
    }

    #[test]
    fn test_adlsgen2_rejects_mixing_shared_key_and_service_principal() {
        let common = IcebergCommon {
            adlsgen2_account_key: Some("shared-key".to_owned()),
            ..test_adlsgen2_service_principal_common(None)
        };

        let err = common
            .build_jni_catalog_configs(JniCatalogImpl::Rest, &HashMap::new())
            .unwrap_err();
        assert!(
            format!("{:#}", err).contains("exactly one auth mode"),
            "expected mutual-exclusion error, got: {err:#}"
        );
    }

    #[test]
    fn test_adlsgen2_rejects_partial_service_principal_config() {
        let common = IcebergCommon {
            adlsgen2_client_secret: None,
            ..test_adlsgen2_service_principal_common(None)
        };

        let err = common
            .build_jni_catalog_configs(JniCatalogImpl::Rest, &HashMap::new())
            .unwrap_err();
        assert!(
            format!("{:#}", err).contains("requires all three"),
            "expected partial-config error, got: {err:#}"
        );
    }

    #[test]
    fn test_iceberg_table_identifier_validation() {
        let valid_identifier = IcebergTableIdentifier {
            database_name: Some("valid_db".to_owned()),
            table_name: "test_table".to_owned(),
        };
        assert!(valid_identifier.validate().is_ok());

        let valid_underscore = IcebergTableIdentifier {
            database_name: Some("valid_db_name".to_owned()),
            table_name: "test_table".to_owned(),
        };
        assert!(valid_underscore.validate().is_ok());

        let no_database = IcebergTableIdentifier {
            database_name: None,
            table_name: "test_table".to_owned(),
        };
        assert!(no_database.validate().is_ok());

        let empty_part = IcebergTableIdentifier {
            database_name: Some("a..b".to_owned()),
            table_name: "test_table".to_owned(),
        };
        let result = empty_part.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("identifier parts must not be empty")
        );

        let leading_dot = IcebergTableIdentifier {
            database_name: None,
            table_name: ".test_table".to_owned(),
        };
        let result = leading_dot.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("identifier parts must not be empty")
        );
    }

    #[test]
    fn test_iceberg_table_identifier_dots_as_namespace_separators() {
        let table_ident = IcebergTableIdentifier {
            database_name: Some("general.zia.stats".to_owned()),
            table_name: "tagged_security_transactions".to_owned(),
        }
        .to_table_ident()
        .unwrap();
        let namespace: Vec<_> = table_ident
            .namespace()
            .as_ref()
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(namespace, vec!["general", "zia", "stats"]);
        assert_eq!(table_ident.name(), "tagged_security_transactions");

        let table_ident = IcebergTableIdentifier {
            database_name: Some("general".to_owned()),
            table_name: "zia.stats.tagged_security_transactions".to_owned(),
        }
        .to_table_ident()
        .unwrap();
        let namespace: Vec<_> = table_ident
            .namespace()
            .as_ref()
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(namespace, vec!["general", "zia", "stats"]);
        assert_eq!(table_ident.name(), "tagged_security_transactions");

        let table_ident = IcebergTableIdentifier {
            database_name: None,
            table_name: "general.zia.stats.tagged_security_transactions".to_owned(),
        }
        .to_table_ident()
        .unwrap();
        let namespace: Vec<_> = table_ident
            .namespace()
            .as_ref()
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(namespace, vec!["general", "zia", "stats"]);
        assert_eq!(table_ident.name(), "tagged_security_transactions");
    }
}
