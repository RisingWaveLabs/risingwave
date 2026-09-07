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

use std::fmt;
use std::io::Error as IoError;

use risingwave_common::error::code::PostgresErrorCode;
use thiserror::Error;

use crate::pg_server::BoxedError;
pub type PsqlResult<T> = std::result::Result<T, PsqlError>;

/// Error type used in pgwire crates.
#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("Failed to start a new session: {0}")]
    StartupError(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Invalid password")]
    PasswordError,

    #[error("Protocol violation: {0}")]
    ProtocolError(
        #[source]
        #[backtrace]
        ProtocolViolationError,
    ),

    #[error("Failed to run the query: {0}")]
    SimpleQueryError(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Failed to prepare the statement: {0}")]
    ExtendedPrepareError(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Failed to execute the statement: {0}")]
    ExtendedExecuteError(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error(transparent)]
    IoError(#[from] IoError),

    /// Uncategorized error for describe, bind.
    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        BoxedError,
    ),

    #[error("Panicked when handling the request: {0}
This is a bug. We would appreciate a bug report at:
  https://github.com/risingwavelabs/risingwave/issues/new?labels=type%2Fbug&template=bug_report.yml")]
    Panic(String),

    #[error("Unable to setup an SSL connection")]
    SslError(#[from] openssl::ssl::Error),

    #[error("terminating connection due to idle-in-transaction timeout")]
    IdleInTxnTimeout,

    #[error("Server throttled: {0}")]
    ServerThrottle(String),
}

#[derive(Debug)]
pub struct ProtocolViolationError(String);

impl fmt::Display for ProtocolViolationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ProtocolViolationError {
    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        request.provide_value(PostgresErrorCode::ProtocolViolation);
    }
}

impl PsqlError {
    pub fn protocol_error(message: impl Into<String>) -> Self {
        Self::ProtocolError(ProtocolViolationError(message.into()))
    }

    pub fn no_statement() -> Self {
        PsqlError::Uncategorized("No statement found".into())
    }

    pub fn no_portal() -> Self {
        PsqlError::Uncategorized("No portal found".into())
    }
}
