// Copyright 2025 RisingWave Labs
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

use super::hba::HbaConfig;
use super::*;

/// The section [`frontend`] in `risingwave.toml`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct FrontendConfig {
    /// Total memory constraints for running queries.
    #[serde(default = "default::frontend::max_total_query_size_bytes")]
    pub max_total_query_size_bytes: u64,

    /// A query of size under this threshold will never be rejected due to memory constraints.
    #[serde(default = "default::frontend::min_single_query_size_bytes")]
    pub min_single_query_size_bytes: u64,

    /// A query of size exceeding this threshold will always be rejected due to memory constraints.
    #[serde(default = "default::frontend::max_single_query_size_bytes")]
    pub max_single_query_size_bytes: u64,

    /// Flush threshold in bytes for streaming pgwire responses. A value of `0` flushes after every
    /// row. A single pgwire message can exceed this threshold because messages are encoded
    /// atomically.
    #[serde(default = "default::frontend::stream_flush_threshold_bytes")]
    pub stream_flush_threshold_bytes: usize,

    /// Host-based authentication configuration
    #[serde(default = "HbaConfig::default")]
    pub hba_config: HbaConfig,

    /// Maximum allowed clock skew in milliseconds for `WebSocket` ingest init authentication.
    #[serde(default = "default::frontend::webhook_auth_max_clock_skew_ms")]
    pub webhook_auth_max_clock_skew_ms: u64,

    /// Whether to allow local filesystem connectors such as `posix_fs` and `fs`.
    /// Disabled by default in release builds because these connectors can access files on the
    /// frontend host. Enabled by default in debug builds for local development.
    #[serde(default = "default::frontend::unsafe_enable_local_fs_connector")]
    pub unsafe_enable_local_fs_connector: bool,

    /// Whether to allow the experimental `oracle-cdc` connector.
    /// Disabled by default while Oracle CDC support is incomplete and not yet public.
    #[serde(default = "default::frontend::unsafe_enable_oracle_cdc")]
    pub unsafe_enable_oracle_cdc: bool,
}

pub mod default {

    pub mod frontend {
        pub fn max_total_query_size_bytes() -> u64 {
            1024 * 1024 * 1024
        }

        pub fn min_single_query_size_bytes() -> u64 {
            1024 * 1024
        }

        pub fn max_single_query_size_bytes() -> u64 {
            1024 * 1024 * 1024
        }

        pub fn stream_flush_threshold_bytes() -> usize {
            64 * 1024
        }

        pub fn webhook_auth_max_clock_skew_ms() -> u64 {
            300_000
        }

        pub fn unsafe_enable_local_fs_connector() -> bool {
            cfg!(debug_assertions)
        }

        pub fn unsafe_enable_oracle_cdc() -> bool {
            false
        }
    }
}
