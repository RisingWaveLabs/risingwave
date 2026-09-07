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

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{HummockContextId, HummockSstableObjectId};
use risingwave_object_store::object::ObjectError;
use risingwave_rpc_client::error::ToTonicStatus;
use sea_orm::DbErr;
use thiserror::Error;

use crate::model::MetadataModelError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid hummock context {0}")]
    InvalidContext(HummockContextId),
    #[error("failed to access meta store")]
    MetaStore(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error(transparent)]
    ObjectStore(
        #[from]
        #[backtrace]
        ObjectError,
    ),
    #[error("compactor {0} is disconnected")]
    CompactorUnreachable(HummockContextId),
    #[error("compaction group error: {0}")]
    CompactionGroup(String),
    #[error("SST {0} is invalid")]
    InvalidSst(HummockSstableObjectId),
    #[error("invalid manual compaction option: {0}")]
    InvalidManualCompactionOption(String),
    #[error("invalid epoch range: {start_epoch}..={end_epoch}")]
    InvalidEpochRange { start_epoch: u64, end_epoch: u64 },
    #[error("time-travel version expired: table {table_id}, epoch {epoch}")]
    TimeTravelVersionExpired { table_id: TableId, epoch: u64 },
    #[error("time travel")]
    TimeTravel(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

impl Error {
    pub fn retryable(&self) -> bool {
        matches!(self, Error::MetaStore(_))
    }
}

impl From<MetadataModelError> for Error {
    fn from(err: MetadataModelError) -> Self {
        anyhow::anyhow!(err).into()
    }
}

impl From<sea_orm::DbErr> for Error {
    fn from(value: DbErr) -> Self {
        MetadataModelError::from(value).into()
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let code = match &err {
            Error::InvalidManualCompactionOption(_) | Error::InvalidEpochRange { .. } => {
                tonic::Code::InvalidArgument
            }
            Error::TimeTravelVersionExpired { .. } => tonic::Code::OutOfRange,
            _ => tonic::Code::Internal,
        };
        err.to_status(code, "hummock")
    }
}
