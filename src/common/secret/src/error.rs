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

pub use anyhow::anyhow;
use thiserror::Error;
use thiserror_ext::Construct;

use super::SecretId;

pub type SecretResult<T> = Result<T, SecretError>;

#[derive(Error, Debug, Construct)]
pub enum SecretError {
    #[error("secret not found: {0}")]
    ItemNotFound(SecretId),

    #[error("decode utf8 error: {0}")]
    DecodeUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("unspecified secret ref type: {0}")]
    UnspecifiedRefType(SecretId),

    #[error("failed to encrypt or decrypt the secret")]
    AesError,

    #[error("ser/de proto message error: {0}")]
    ProtoError(#[from] bincode::Error),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}
