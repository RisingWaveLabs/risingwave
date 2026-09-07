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

//! Definitions of catalog structs.
//!
//! The main struct is [`root_catalog::Catalog`], which is the root containing other catalog
//! structs. It is accessed via [`catalog_service::CatalogReader`] and
//! [`catalog_service::CatalogWriter`], which is held by [`crate::session::FrontendEnv`].

use risingwave_common::catalog::{
    ROW_ID_COLUMN_NAME, RW_RESERVED_COLUMN_NAME_PREFIX, is_system_schema,
};
use risingwave_common::error::code::PostgresErrorCode;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::user::grant_privilege::Object as PbGrantObject;
use thiserror::Error;

use crate::error::{ErrorCode, Result, RwError};
pub(crate) mod catalog_service;
pub mod purify;

pub(crate) mod connection_catalog;
pub(crate) mod database_catalog;
pub(crate) mod function_catalog;
pub(crate) mod index_catalog;
pub(crate) mod root_catalog;
pub(crate) mod schema_catalog;
pub(crate) mod source_catalog;
pub(crate) mod subscription_catalog;
pub(crate) mod system_catalog;
pub(crate) mod table_catalog;
pub(crate) mod view_catalog;

pub(crate) mod secret_catalog;

pub(crate) use catalog_service::CatalogReader;
pub use index_catalog::IndexCatalog;
pub use table_catalog::TableCatalog;

use crate::user::UserId;

pub(crate) type ConnectionId = risingwave_common::id::ConnectionId;
pub(crate) type SourceId = risingwave_common::id::SourceId;
pub(crate) type SinkId = risingwave_common::id::SinkId;
pub(crate) type SubscriptionId = risingwave_common::id::SubscriptionId;
pub(crate) type ViewId = risingwave_common::id::ViewId;
pub(crate) type DatabaseId = risingwave_common::catalog::DatabaseId;
pub(crate) type SchemaId = risingwave_common::catalog::SchemaId;
pub(crate) type TableId = risingwave_common::catalog::TableId;
pub(crate) type ColumnId = risingwave_common::catalog::ColumnId;
pub(crate) type FragmentId = risingwave_common::id::FragmentId;
pub(crate) type SecretId = risingwave_common::catalog::SecretId;

/// Check if the column name does not conflict with the internally reserved column name.
pub fn check_column_name_not_reserved(column_name: &str) -> Result<()> {
    if column_name.starts_with(ROW_ID_COLUMN_NAME) {
        return Err(ErrorCode::InternalError(format!(
            "column name prefixed with {:?} are reserved word.",
            ROW_ID_COLUMN_NAME
        ))
        .into());
    }

    if column_name.starts_with(RW_RESERVED_COLUMN_NAME_PREFIX) {
        return Err(ErrorCode::InternalError(format!(
            "column name prefixed with {:?} are reserved word.",
            RW_RESERVED_COLUMN_NAME_PREFIX
        ))
        .into());
    }

    if ["tableoid", "xmin", "cmin", "xmax", "cmax", "ctid"].contains(&column_name) {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "column name \"{column_name}\" conflicts with a system column name"
        ))
        .into());
    }

    Ok(())
}

/// Check if modifications happen to system catalog.
pub fn check_schema_writable(schema: &str) -> Result<()> {
    if is_system_schema(schema) {
        Err(ErrorCode::ProtocolError(format!(
            "permission denied to write on \"{}\", System catalog modifications are currently disallowed.",
            schema
        )).into())
    } else {
        Ok(())
    }
}

pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

// TODO(error-handling): provide more concrete error code for different object types.
#[derive(Error, Debug, thiserror_ext::Box)]
#[thiserror_ext(newtype(name = CatalogError, extra_provide = Self::provide_postgres_error_code))]
pub enum CatalogErrorInner {
    #[error("{object_type} not found: {name}")]
    NotFound {
        object_type: &'static str,
        name: String,
    },

    #[error(
        "{object_type} named {name} already exists{}",
        if *.under_creation { " and is still being created" } else { "" },
    )]
    Duplicated {
        object_type: &'static str,
        name: String,
        under_creation: bool, // only used for StreamingJob type and Subscription for now
    },
}

impl CatalogError {
    /// Provide the Postgres error code for the error.
    fn provide_postgres_error_code(&self, request: &mut std::error::Request<'_>) {
        match self.inner() {
            CatalogErrorInner::NotFound { object_type, .. } => {
                // `database` not found should map to SQLSTATE 3D000 (Invalid Catalog Name),
                // which is used by Postgres for non-existing database in startup.
                if *object_type == "database" {
                    request.provide_value(PostgresErrorCode::InvalidCatalogName);
                } else {
                    request.provide_value(PostgresErrorCode::UndefinedObject);
                }
            }
            CatalogErrorInner::Duplicated { .. } => {
                request.provide_value(PostgresErrorCode::DuplicateObject);
            }
        };
    }

    /// Construct a `not found` error.
    pub fn not_found(object_type: &'static str, name: impl Into<String>) -> Self {
        CatalogErrorInner::NotFound {
            object_type,
            name: name.into(),
        }
        .into()
    }

    /// Construct a `duplicated` error.
    pub fn duplicated(object_type: &'static str, name: impl Into<String>) -> Self {
        Self::duplicated_under_creation(object_type, name, false)
    }

    /// Construct a `duplicated` error with `under_creation` flag.
    pub fn duplicated_under_creation(
        object_type: &'static str,
        name: impl Into<String>,
        under_creation: bool,
    ) -> Self {
        CatalogErrorInner::Duplicated {
            object_type,
            name: name.into(),
            under_creation,
        }
        .into()
    }

    /// Whether the error is a `duplicated` error for the given object type.
    pub fn is_duplicated(&self, object_type: &'static str) -> bool {
        matches!(
            self.inner(),
            CatalogErrorInner::Duplicated { object_type: t, .. } if *t == object_type
        )
    }

    /// Whether the error is a `not found` error for the given object type.
    pub fn is_not_found(&self, object_type: &'static str) -> bool {
        matches!(
            self.inner(),
            CatalogErrorInner::NotFound { object_type: t, .. } if *t == object_type
        )
    }
}

impl From<CatalogError> for RwError {
    fn from(e: CatalogError) -> Self {
        ErrorCode::CatalogError(Box::new(e)).into()
    }
}

/// A trait for the catalog with owners, including relations (table, index, sink, etc.) and
/// function, connection.
///
/// This trait can be used to reduce code duplication and can be extended if needed in the future.
pub trait OwnedByUserCatalog {
    /// Returns the owner of the catalog.
    fn owner(&self) -> UserId;
}

impl OwnedByUserCatalog for SinkCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}

pub struct OwnedGrantObject {
    pub owner: UserId,
    pub object: PbGrantObject,
}
