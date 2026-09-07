// Copyright 2026 RisingWave Labs
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

//! Manager for the Iceberg pk-index sink path. Owns per-sink commit coordinators that
//! drive iceberg `commit_epoch` ahead of hummock `commit_epoch` and persist
//! exactly-once state via `pending_sink_state`.
//!
//! This is intentionally separate from [`crate::manager::sink_coordination`]
//! (which serves V1/V2 sinks via gRPC). Future responsibilities such as
//! per-sink compaction will live alongside the per-sink commit coordinator here.

mod committed_epoch;
mod coordinator;
mod manager;

use std::collections::BTreeMap;

use anyhow::anyhow;
pub use manager::IcebergPkIndexSinkManager;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::iceberg::{ENABLE_PK_INDEX, IcebergConfig};
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::catalog::PbSink;

/// Returns true if the given sink properties identify a Iceberg pk-index sink
/// (i.e. an iceberg sink with `enable_pk_index = 'true'`).
pub fn is_iceberg_pk_index_sink(properties: &BTreeMap<String, String>) -> bool {
    let connector_match = properties
        .get(UPSTREAM_SOURCE_KEY)
        .map(|v| v.eq_ignore_ascii_case("iceberg"))
        .unwrap_or(false);
    let pk_index_enabled = properties
        .get(ENABLE_PK_INDEX)
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    connector_match && pk_index_enabled
}

/// Build an [`IcebergConfig`] from a [`PbSink`], filling secret refs along the
/// way. Used at CREATE SINK time and during recovery to (re-)register the
/// commit coordinator.
pub fn build_iceberg_config(pb_sink: &PbSink) -> anyhow::Result<IcebergConfig> {
    let properties: BTreeMap<String, String> = pb_sink.properties.clone().into_iter().collect();
    let secret_refs: BTreeMap<_, _> = pb_sink.secret_refs.clone().into_iter().collect();
    let with_secrets = LocalSecretManager::global()
        .fill_secrets(properties, secret_refs)
        .map_err(|e| anyhow!(e).context("fill secrets for iceberg"))?;
    IcebergConfig::from_btreemap(with_secrets)
        .map_err(|e| anyhow!(e).context("parse iceberg config"))
}
