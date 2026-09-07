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

use bincode::error::{DecodeError, EncodeError};
use thiserror::Error;

use crate::RecordId;

pub type Result<T> = std::result::Result<T, TraceError>;

#[derive(Error, Debug)]
pub enum TraceError {
    #[error("failed to encode: {0}")]
    Encode(#[from] EncodeError),

    #[error("failed to decode: {0}")]
    Decode(#[from] DecodeError),

    #[error("failed to read or write {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid magic bytes, expected {expected}, found {found}")]
    MagicBytes { expected: u32, found: u32 },

    #[error("tried to close a non-existent record {0}")]
    FinRecord(RecordId),

    #[error("failed to create an iterator: {0}")]
    IterFailed(String),

    #[error("failed to get key: {0}")]
    GetFailed(String),

    #[error("failed to ingest: {0}")]
    IngestFailed(String),

    #[error("failed to sync: {0}")]
    SyncFailed(String),

    #[error("{0}")]
    Other(&'static str),

    #[error("failed to wait for the epoch non-blockingly")]
    TryWaitEpochFailed,

    #[error("failed to clear shared buffer")]
    ClearSharedBufferFailed,

    #[error("failed to flush")]
    FlushFailed,

    #[error("failed to try_flush")]
    TryFlushFailed,
}
