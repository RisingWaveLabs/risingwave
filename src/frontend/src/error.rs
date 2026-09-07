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

use risingwave_batch::error::BatchError;
use risingwave_common::array::ArrayError;
use risingwave_common::error::{BoxedError, NoFunction, NotImplemented};
use risingwave_common::license::FeatureNotAvailable;
use risingwave_common::secret::SecretError;
use risingwave_common::session_config::SessionConfigError;
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::{RpcError, ToTonicStatus, TonicStatusWrapper};
use thiserror::Error;
use thiserror_ext::AsReport;
use tokio::task::JoinError;

use crate::expr::CastError;

/// The error type for the frontend crate, acting as the top-level error type for the
/// entire RisingWave project.
// TODO(error-handling): this is migrated from the `common` crate, and there could
// be some further refactoring to do:
// - Some variants are never constructed.
// - Some variants store a type-erased `BoxedError` to resolve the reverse dependency.
//   It's not necessary anymore as the error type is now defined at the top-level.
#[derive(Error, thiserror_ext::ReportDebug, thiserror_ext::Box, thiserror_ext::Macro)]
#[thiserror_ext(newtype(name = RwError, backtrace), macro(path = "crate::error"))]
pub enum ErrorCode {
    #[error("internal error: {0}")]
    InternalError(String),
    // TODO: unify with the above
    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("connector error: {0}")]
    ConnectorError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),
    // Tips: Use this only if it's intended to reject the query
    #[error("Not supported: {0}\nHINT: {1}")]
    NotSupported(String, String),
    #[error(transparent)]
    NoFunction(#[from] NoFunction),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Storage error: {0}")]
    StorageError(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("Expr error: {0}")]
    ExprError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    // TODO(error-handling): there's a limitation that `#[transparent]` can't be used with `#[backtrace]` if no `#[from]`
    // So we emulate a transparent error with "{0}" display here.
    #[error("{0}")]
    BatchError(
        #[source]
        #[backtrace]
        // `BatchError`
        BoxedError,
    ),
    #[error("Array error: {0}")]
    ArrayError(
        #[from]
        #[backtrace]
        ArrayError,
    ),
    #[cfg(feature = "datafusion")]
    #[error("DataFusion error: {0}")]
    DataFusionError(
        #[from]
        #[backtrace]
        datafusion_common::DataFusionError,
    ),
    #[error("Stream error: {0}")]
    StreamError(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    // TODO(error-handling): there's a limitation that `#[transparent]` can't be used with `#[backtrace]` if no `#[from]`
    // So we emulate a transparent error with "{0}" display here.
    #[error("{0}")]
    RpcError(
        #[source]
        #[backtrace]
        // `tonic::transport::Error`, `TonicStatusWrapper`, or `RpcError`
        BoxedError,
    ),
    // TODO: use a new type for bind error
    // TODO(error-handling): should prefer use error types than strings.
    #[error("Bind error: {0}")]
    BindError(#[message] String),
    // TODO: only keep this one
    #[error("Failed to bind expression: {expr}: {error}")]
    BindErrorRoot {
        expr: String,
        #[source]
        #[backtrace]
        error: BoxedError,
    },
    #[error(transparent)]
    CastError(
        #[from]
        #[backtrace]
        CastError,
    ),
    #[error("Catalog error: {0}")]
    CatalogError(
        #[source]
        #[backtrace]
        #[message]
        BoxedError,
    ),
    #[error("Protocol error: {0}")]
    ProtocolError(#[message] String),
    #[error("Scheduler error: {0}")]
    SchedulerError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error("Task not found")]
    TaskNotFound,
    #[error("Session not found")]
    SessionNotFound,
    #[error("Invalid reference: {0}")]
    InvalidReference(String),
    #[error("Item not found: {0}")]
    ItemNotFound(String),
    #[error("Duplicate Relation Name: {0}")]
    DuplicateRelationName(String),
    #[error("Invalid insert operation: {0}")]
    InsertViolation(String),
    #[error("Invalid input syntax: {0}")]
    InvalidInputSyntax(#[message] String),
    #[error("Cannot compare in memory: {0}")]
    MemComparableError(#[from] memcomparable::Error),
    #[error("Error while de/se values: {0}")]
    ValueEncodingError(
        #[from]
        #[backtrace]
        ValueEncodingError,
    ),
    #[error("Invalid value `{config_value}` for `{config_entry}`")]
    InvalidConfigValue {
        config_entry: String,
        config_value: String,
    },
    #[error("Invalid parameter value: {0}")]
    InvalidParameterValue(String),
    #[error("Sink error: {0}")]
    SinkError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error("Permission denied: {0}")]
    PermissionDenied(#[message] String),
    #[error("Failed to get or set session config: {0}")]
    SessionConfig(
        #[from]
        #[backtrace]
        SessionConfigError,
    ),
    #[error("Secret error: {0}")]
    SecretError(
        #[from]
        #[backtrace]
        SecretError,
    ),
    #[error("{0} has been deprecated, please use {1} instead.")]
    Deprecated(String, String),
    #[error(transparent)]
    FeatureNotAvailable(
        #[from]
        #[backtrace]
        FeatureNotAvailable,
    ),
}

/// The result type for the frontend crate.
pub type Result<T> = std::result::Result<T, RwError>;

impl From<TonicStatusWrapper> for RwError {
    fn from(status: TonicStatusWrapper) -> Self {
        use tonic::Code;

        let message = status.inner().message();

        // TODO(error-handling): `message` loses the source chain.
        match status.inner().code() {
            Code::InvalidArgument => ErrorCode::InvalidParameterValue(message.to_owned()),
            Code::NotFound | Code::AlreadyExists => ErrorCode::CatalogError(status.into()),
            Code::PermissionDenied => ErrorCode::PermissionDenied(message.to_owned()),
            Code::Cancelled => ErrorCode::SchedulerError(status.into()),
            _ => ErrorCode::RpcError(status.into()),
        }
        .into()
    }
}

impl From<RpcError> for RwError {
    fn from(r: RpcError) -> Self {
        match r {
            RpcError::GrpcStatus(status) => TonicStatusWrapper::into(*status),
            _ => ErrorCode::RpcError(r.into()).into(),
        }
    }
}

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}

impl From<ConnectorError> for RwError {
    fn from(e: ConnectorError) -> Self {
        ErrorCode::ConnectorError(e.into()).into()
    }
}

impl From<PbFieldNotFound> for RwError {
    fn from(err: PbFieldNotFound) -> Self {
        ErrorCode::InternalError(format!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
        .into()
    }
}

impl From<BatchError> for RwError {
    fn from(s: BatchError) -> Self {
        ErrorCode::BatchError(Box::new(s)).into()
    }
}

impl From<JoinError> for RwError {
    fn from(join_error: JoinError) -> Self {
        ErrorCode::Uncategorized(join_error.into()).into()
    }
}

// For errors without a concrete type, put them into `Uncategorized`.
impl From<BoxedError> for RwError {
    fn from(e: BoxedError) -> Self {
        // Show that the error is of `BoxedKind`, instead of `AdhocKind` which loses the sources.
        // This is essentially expanded from `anyhow::anyhow!(e)`.
        let e = anyhow::__private::kind::BoxedKind::anyhow_kind(&e).new(e);
        ErrorCode::Uncategorized(e).into()
    }
}

impl From<risingwave_sqlparser::parser::ParserError> for ErrorCode {
    fn from(e: risingwave_sqlparser::parser::ParserError) -> Self {
        ErrorCode::InvalidInputSyntax(e.to_report_string())
    }
}

impl From<RwError> for tonic::Status {
    fn from(err: RwError) -> Self {
        err.to_status(tonic::Code::Internal, "RwError")
    }
}
