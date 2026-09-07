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

use std::marker::PhantomData;

use anyhow::Context;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::cdc::external::DebeziumOffset;
use crate::source::cdc::external::postgres::PostgresOffset;
use crate::source::cdc::{CdcSourceType, CdcSourceTypeTrait, Mysql, Postgres, SqlServer};
use crate::source::{SplitId, SplitMetaData};

/// The base states of a CDC split, which will be persisted to checkpoint.
/// CDC source only has single split, so we use the `source_id` to identify the split.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct CdcSplitBase {
    pub split_id: u32,
    pub start_offset: Option<String>,
    pub snapshot_done: bool,
}

impl CdcSplitBase {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        Self {
            split_id,
            start_offset,
            snapshot_done: false,
        }
    }
}

trait CdcSplitTrait: Send + Sync {
    fn split_id(&self) -> u32;
    fn start_offset(&self) -> &Option<String>;
    fn is_snapshot_done(&self) -> bool;
    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()>;

    // MySQL and MongoDB shares the same logic to extract the snapshot flag
    fn extract_snapshot_flag(&self, start_offset: &str) -> ConnectorResult<bool> {
        // if snapshot_done is already true, it won't be changed
        let mut snapshot_done = self.is_snapshot_done();
        if snapshot_done {
            return Ok(snapshot_done);
        }

        let dbz_offset: DebeziumOffset = serde_json::from_str(start_offset).with_context(|| {
            format!(
                "invalid cdc offset: {}, split: {}",
                start_offset,
                self.split_id()
            )
        })?;

        // heartbeat event should not update the `snapshot_done` flag
        if !dbz_offset.is_heartbeat {
            snapshot_done = match dbz_offset.source_offset.snapshot {
                Some(val) => !val,
                None => true,
            };
        }
        Ok(snapshot_done)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MySqlCdcSplit {
    pub inner: CdcSplitBase,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PostgresCdcSplit {
    pub inner: CdcSplitBase,
    // the hostname and port of a node that holding shard tables (for Citus)
    pub server_addr: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MongoDbCdcSplit {
    pub inner: CdcSplitBase,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct SqlServerCdcSplit {
    pub inner: CdcSplitBase,
}

impl MySqlCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self { inner: split }
    }

    /// Extract MySQL CDC binlog offset (file sequence and position) from the offset JSON string.
    ///
    /// MySQL binlog offset format:
    /// ```json
    /// {
    ///   "sourcePartition": { "server": "..." },
    ///   "sourceOffset": {
    ///     "file": "binlog.000123",
    ///     "pos": 456789,
    ///     ...
    ///   }
    /// }
    /// ```
    ///
    /// Returns `Some((file_seq, position))` where:
    /// - `file_seq`: the numeric part of binlog filename (e.g., 123 from "binlog.000123")
    /// - `position`: the byte offset within the binlog file
    pub fn mysql_binlog_offset(&self) -> Option<(u64, u64)> {
        let offset_str = self.inner.start_offset.as_ref()?;
        let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
        let source_offset = offset.get("sourceOffset")?;

        let file = source_offset.get("file")?.as_str()?;
        let pos = source_offset.get("pos")?.as_u64()?;

        let file_seq = extract_binlog_file_seq(file)?;

        Some((file_seq, pos))
    }
}

impl CdcSplitTrait for MySqlCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // if snapshot_done is already true, it won't be updated
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

impl PostgresCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>, server_addr: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self {
            inner: split,
            server_addr,
        }
    }

    /// Extract PostgreSQL LSN value from the offset JSON string.
    ///
    /// This function parses the offset JSON and extracts the LSN value from the sourceOffset.lsn field.
    /// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
    pub fn pg_lsn(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
        let source_offset = offset.get("sourceOffset")?;
        let lsn = source_offset.get("lsn")?;
        lsn.as_u64()
    }

    /// Extract PostgreSQL `lsn_commit` from the current offset (last commit position).
    pub fn pg_lsn_commit(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_postgres_lsn_commit_from_offset_str(offset_str)
    }

    /// Extract PostgreSQL `lsn_proc` from the current offset (last completely processed position).
    pub fn pg_lsn_proc(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_postgres_lsn_proc_from_offset_str(offset_str)
    }
}

impl CdcSplitTrait for PostgresCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        let new_snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;

        // Monotonicity guard for streaming-phase Postgres CDC offsets.
        //
        // Debezium's task-level auto-restart (BaseSourceTask) reloads
        // effectiveOffset from ConfigurableOffsetBackingStore, which lags
        // dbz's in-memory position by one barrier + async commit cycle.
        // After a broken pipe, dbz re-emits older WAL events whose LSNs
        // are smaller than the freshest entry we already persisted.
        // Without this guard those stale LSNs would overwrite the split
        // state, the store would then be pushed backwards via the next
        // barrier, and a subsequent restart would load the stale value.
        //
        // We guard both `lsn_commit` and `lsn_proc` because dbz consumes
        // them for two different things on resume:
        //   - `lsn_commit` is what `validateLogPosition` checks against
        //     PG slot.restart_lsn; regression here causes a fatal
        //     `DebeziumException` ("change stream ... no longer available").
        //   - `lsn_proc` is the start position for `startStreaming` and
        //     the key `WalPositionLocator` uses to deduplicate replayed
        //     events on restart; regression here can lead to silent
        //     data loss / a hung resume if PG has already advanced past
        //     the rewound value.
        //
        // The per-message `lsn` field is intentionally NOT used as a
        // monotonicity key: under concurrent overlapping transactions
        // PG streams events in commit order, so the per-event physical
        // lsn can legitimately move backwards when a long-running
        // earlier transaction commits later than a shorter newer one.
        // Only `lsn_commit` and `lsn_proc` are monotonic under correct
        // dbz behavior.
        //
        // If both offsets are parseable as streaming Postgres offsets, use
        // the same `(lsn_commit, lsn_proc)` ordering as snapshot backfill
        // deduplication. Snapshot / heartbeat / incomplete offsets are not
        // comparable and pass through.
        if let (Some(old_offset), Some(new_offset)) = (
            self.inner
                .start_offset
                .as_deref()
                .and_then(parse_postgres_offset_from_offset_str),
            parse_postgres_offset_from_offset_str(&last_seen_offset),
        ) && new_offset < old_offset
        {
            tracing::warn!(
                split_id = self.inner.split_id,
                ?old_offset,
                ?new_offset,
                "Rejecting backward Postgres CDC offset update; \
                 keeping current state to prevent state-table regression."
            );
            return Ok(());
        }

        self.inner.snapshot_done = new_snapshot_done;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }

    fn extract_snapshot_flag(&self, start_offset: &str) -> ConnectorResult<bool> {
        // if snapshot_done is already true, it won't be changed
        let mut snapshot_done = self.is_snapshot_done();
        if snapshot_done {
            return Ok(snapshot_done);
        }

        let dbz_offset: DebeziumOffset = serde_json::from_str(start_offset).with_context(|| {
            format!(
                "invalid postgres offset: {}, split: {}",
                start_offset, self.inner.split_id
            )
        })?;

        // heartbeat event should not update the `snapshot_done` flag
        if !dbz_offset.is_heartbeat {
            snapshot_done = dbz_offset
                .source_offset
                .last_snapshot_record
                .unwrap_or(false);
        }
        Ok(snapshot_done)
    }
}

impl MongoDbCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self { inner: split }
    }
}

impl CdcSplitTrait for MongoDbCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // if snapshot_done is already true, it will remain true
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

impl SqlServerCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self { inner: split }
    }

    /// Extract SQL Server `change_lsn` value from the offset JSON string.
    pub fn sql_server_change_lsn(&self) -> Option<u128> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_sql_server_change_lsn_from_offset_str(offset_str)
    }

    /// Extract SQL Server `commit_lsn` value from the offset JSON string.
    pub fn sql_server_commit_lsn(&self) -> Option<u128> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_sql_server_commit_lsn_from_offset_str(offset_str)
    }
}

impl CdcSplitTrait for SqlServerCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // if snapshot_done is already true, it will remain true
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

/// We use this struct to wrap the specific split, which act as an interface to other modules
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct DebeziumCdcSplit<T: CdcSourceTypeTrait> {
    pub mysql_split: Option<MySqlCdcSplit>,

    #[serde(rename = "pg_split")] // backward compatibility
    pub postgres_split: Option<PostgresCdcSplit>,
    pub citus_split: Option<PostgresCdcSplit>,
    pub mongodb_split: Option<MongoDbCdcSplit>,
    pub sql_server_split: Option<SqlServerCdcSplit>,

    #[serde(skip)]
    pub _phantom: PhantomData<T>,
}

macro_rules! dispatch_cdc_split_inner {
    ($dbz_split:expr, $as_type:tt, {$({$cdc_source_type:tt, $cdc_source_split:tt}),*}, $body:expr) => {
        match T::source_type() {
            $(
                CdcSourceType::$cdc_source_type => {
                    $crate::paste! {
                        $dbz_split.[<$cdc_source_split>]
                            .[<as_ $as_type>]()
                            .expect(concat!(stringify!([<$cdc_source_type:lower>]), " split must exist"))
                            .$body
                    }
                }
            )*
            CdcSourceType::Unspecified => {
                unreachable!("invalid debezium split");
            }
        }
    }
}

// call corresponding split method of the specific cdc source type
macro_rules! dispatch_cdc_split {
    ($dbz_split:expr, $as_type:tt, $body:expr) => {
        dispatch_cdc_split_inner!($dbz_split, $as_type, {
            {Mysql, mysql_split},
            {Postgres, postgres_split},
            {Citus, citus_split},
            {Mongodb, mongodb_split},
            {SqlServer, sql_server_split}
        }, $body)
    }
}

impl<T: CdcSourceTypeTrait> SplitMetaData for DebeziumCdcSplit<T> {
    fn id(&self) -> SplitId {
        format!("{}", self.split_id()).into()
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.update_offset_inner(last_seen_offset)
    }
}

impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
    pub fn new(split_id: u32, start_offset: Option<String>, server_addr: Option<String>) -> Self {
        let mut ret = Self {
            mysql_split: None,
            postgres_split: None,
            citus_split: None,
            mongodb_split: None,
            sql_server_split: None,
            _phantom: PhantomData,
        };
        match T::source_type() {
            CdcSourceType::Mysql => {
                let split = MySqlCdcSplit::new(split_id, start_offset);
                ret.mysql_split = Some(split);
            }
            CdcSourceType::Postgres => {
                let split = PostgresCdcSplit::new(split_id, start_offset, None);
                ret.postgres_split = Some(split);
            }
            CdcSourceType::Citus => {
                let split = PostgresCdcSplit::new(split_id, start_offset, server_addr);
                ret.citus_split = Some(split);
            }
            CdcSourceType::Mongodb => {
                let split = MongoDbCdcSplit::new(split_id, start_offset);
                ret.mongodb_split = Some(split);
            }
            CdcSourceType::SqlServer => {
                let split = SqlServerCdcSplit::new(split_id, start_offset);
                ret.sql_server_split = Some(split);
            }
            CdcSourceType::Unspecified => {
                unreachable!("invalid debezium split")
            }
        }
        ret
    }

    pub fn split_id(&self) -> u32 {
        dispatch_cdc_split!(self, ref, split_id())
    }

    pub fn start_offset(&self) -> &Option<String> {
        dispatch_cdc_split!(self, ref, start_offset())
    }

    pub fn snapshot_done(&self) -> bool {
        dispatch_cdc_split!(self, ref, is_snapshot_done())
    }

    pub fn update_offset_inner(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        dispatch_cdc_split!(self, mut, update_offset(last_seen_offset)?);
        Ok(())
    }
}

impl DebeziumCdcSplit<Postgres> {
    /// Extract PostgreSQL LSN value from the current split offset.
    ///
    /// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
    pub fn pg_lsn(&self) -> Option<u64> {
        self.postgres_split.as_ref()?.pg_lsn()
    }
}

impl DebeziumCdcSplit<Mysql> {
    /// Extract MySQL CDC binlog offset (file sequence and position) from the current split offset.
    ///
    /// Returns `Some((file_seq, position))` where:
    /// - `file_seq`: the numeric part of binlog filename (e.g., 123 from "binlog.000123")
    /// - `position`: the byte offset within the binlog file
    pub fn mysql_binlog_offset(&self) -> Option<(u64, u64)> {
        self.mysql_split.as_ref()?.mysql_binlog_offset()
    }
}

impl DebeziumCdcSplit<SqlServer> {
    /// Extract SQL Server CDC `change_lsn` from the current split offset.
    pub fn sql_server_change_lsn(&self) -> Option<u128> {
        self.sql_server_split.as_ref()?.sql_server_change_lsn()
    }

    /// Extract SQL Server CDC `commit_lsn` from the current split offset.
    pub fn sql_server_commit_lsn(&self) -> Option<u128> {
        self.sql_server_split.as_ref()?.sql_server_commit_lsn()
    }
}

/// Extract the numeric sequence from a MySQL binlog file name `<basename>.<sequence>`.
///
/// The basename is configurable (`binlog`, `mysql-bin`, `mysql-bin-changelog` on RDS/Aurora, ...),
/// so we take the number after the last `.` rather than assuming a fixed prefix.
pub fn extract_binlog_file_seq(file_name: &str) -> Option<u64> {
    file_name.rsplit('.').next()?.parse::<u64>().ok()
}

/// Extract PostgreSQL LSN value from a CDC offset JSON string.
///
/// This is a standalone helper function that can be used when you only have the offset string
/// (e.g., in callbacks) and don't have access to the Split object.
///
/// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
pub fn extract_postgres_lsn_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn")?;
    lsn.as_u64()
}

fn parse_postgres_offset_from_offset_str(offset_str: &str) -> Option<PostgresOffset> {
    PostgresOffset::parse_debezium_offset(offset_str).ok()
}

/// Extract PostgreSQL `lsn_commit` from a CDC offset JSON string.
///
/// `lsn_commit` is the WAL position of the last COMMIT message dbz has
/// processed. It is the value dbz flushes to PG (`flushLsn`) to advance
/// `confirmed_flush_lsn`, and the value `validateLogPosition` compares
/// against `slot.restart_lsn` on restart. Must be monotonic across
/// restarts -- a regression here can cause a fatal `DebeziumException`
/// ("change stream ... no longer available").
pub fn extract_postgres_lsn_commit_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn_commit")?;
    lsn.as_u64()
}

/// Extract PostgreSQL `lsn_proc` from a CDC offset JSON string.
///
/// `lsn_proc` is the WAL position of the last message dbz has fully
/// processed. On restart dbz uses it as the start position passed to
/// `startStreaming`, and as the dedup key for `WalPositionLocator` to
/// filter replayed events. A regression can cause silent data loss or
/// a hung resume if PG has already advanced past the rewound value.
pub fn extract_postgres_lsn_proc_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn_proc")?;
    lsn.as_u64()
}

/// Parse SQL Server LSN string (`XXXXXXXX:XXXXXXXX:XXXX`) into a comparable integer.
pub fn parse_sql_server_lsn_str(lsn: &str) -> Option<u128> {
    let mut parts = lsn.split(':');
    let part0 = u32::from_str_radix(parts.next()?, 16).ok()? as u128;
    let part1 = u32::from_str_radix(parts.next()?, 16).ok()? as u128;
    let part2 = u16::from_str_radix(parts.next()?, 16).ok()? as u128;
    if parts.next().is_some() {
        return None;
    }

    Some((part0 << 48) | (part1 << 16) | part2)
}

/// Extract SQL Server `change_lsn` from a CDC offset JSON string.
pub fn extract_sql_server_change_lsn_from_offset_str(offset_str: &str) -> Option<u128> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("change_lsn")?.as_str()?;
    parse_sql_server_lsn_str(lsn)
}

/// Extract SQL Server `commit_lsn` from a CDC offset JSON string.
pub fn extract_sql_server_commit_lsn_from_offset_str(offset_str: &str) -> Option<u128> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("commit_lsn")?.as_str()?;
    parse_sql_server_lsn_str(lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql_server_lsn_str() {
        let lsn = "00000027:00000ac0:0002";
        let parsed = parse_sql_server_lsn_str(lsn).unwrap();
        let expected = ((0x00000027_u128) << 48) | ((0x00000ac0_u128) << 16) | (0x0002_u128);
        assert_eq!(parsed, expected);
    }

    /// Build a streaming-phase PG offset JSON. Sets `lsn`, `lsn_proc`,
    /// and `lsn_commit` all to the same value -- this is the common
    /// shape for the guard's basic monotonic tests.
    fn pg_streaming_offset_json(lsn: u64) -> String {
        pg_streaming_offset_json_full(lsn, lsn, lsn)
    }

    /// Build a streaming-phase PG offset JSON with independent control
    /// over `lsn`, `lsn_proc`, and `lsn_commit`. Used to exercise
    /// transaction-interleave and asymmetric-regression scenarios.
    fn pg_streaming_offset_json_full(lsn: u64, lsn_proc: u64, lsn_commit: u64) -> String {
        format!(
            r#"{{
                "sourcePartition": {{"server": "RW_CDC_1001"}},
                "sourceOffset": {{
                    "last_snapshot_record": false,
                    "lsn": {lsn},
                    "lsn_proc": {lsn_proc},
                    "lsn_commit": {lsn_commit},
                    "txId": 12345,
                    "ts_usec": 1700000000000000
                }},
                "isHeartbeat": false
            }}"#
        )
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_rejects_backward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)), None);

        // Push a backward LSN -- must be rejected, current offset kept.
        split
            .update_offset(pg_streaming_offset_json(150))
            .expect("update_offset must not error on rejection");
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_forward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)), None);

        split.update_offset(pg_streaming_offset_json(250)).unwrap();
        assert_eq!(split.pg_lsn(), Some(250));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_equal_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)), None);

        // Equal is allowed (no regression).
        split.update_offset(pg_streaming_offset_json(200)).unwrap();
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_snapshot_phase_bypasses_guard() {
        let snapshot_offset = r#"{
            "sourcePartition": {"server": "RW_CDC_1001"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 150,
                "txId": 12345,
                "ts_usec": 1700000000000000
            },
            "isHeartbeat": false
        }"#;
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)), None);

        // Snapshot / incomplete offsets do not have a comparable streaming
        // `lsn_proc`, so they pass through even when the plain `lsn` is lower.
        split.update_offset(snapshot_offset.to_owned()).unwrap();
        assert_eq!(split.pg_lsn(), Some(150));
    }

    #[test]
    fn test_postgres_offset_lsn_commit_regression_rejected() {
        // `lsn_commit` regressing alone must trigger reject -- this is
        // the field validateLogPosition checks, regression here is the
        // catastrophic case.
        let initial = pg_streaming_offset_json_full(200, 200, 200);
        let mut split = PostgresCdcSplit::new(1, Some(initial), None);

        // lsn_commit 200 -> 150, but lsn / lsn_proc stay at 200.
        let new = pg_streaming_offset_json_full(200, 200, 150);
        split.update_offset(new).unwrap();

        assert_eq!(
            split.pg_lsn_commit(),
            Some(200),
            "lsn_commit must not regress"
        );
    }

    #[test]
    fn test_postgres_offset_lsn_proc_regression_rejected() {
        // `lsn_proc` regressing alone must trigger reject -- this is the
        // field dbz uses as the startStreaming resume position, and
        // WalPositionLocator's dedup key on restart.
        let initial = pg_streaming_offset_json_full(200, 200, 200);
        let mut split = PostgresCdcSplit::new(1, Some(initial), None);

        // lsn_proc 200 -> 150, but lsn / lsn_commit stay at 200.
        let new = pg_streaming_offset_json_full(200, 150, 200);
        split.update_offset(new).unwrap();

        assert_eq!(split.pg_lsn_proc(), Some(200), "lsn_proc must not regress");
    }

    #[test]
    fn test_postgres_offset_interleaved_tx_lsn_proc_drop_allowed_by_commit_lsn() {
        // Concurrent overlapping transactions under PG logical
        // replication produce a state-table chunk where the per-event
        // `lsn` and `lsn_proc` move backwards while `lsn_commit`
        // strictly advances (because PG streams events in commit
        // order, and the earlier-begin transaction's INSERT has a
        // smaller physical LSN even though it commits later).
        //
        // Use the same Postgres offset ordering as #22503: compare
        // `lsn_commit` first, then `lsn_proc`. Since the commit LSN
        // advances, this update is not a regression.
        let initial = pg_streaming_offset_json_full(33_967_592, 33_967_592, 33_893_392);
        let mut split = PostgresCdcSplit::new(1, Some(initial), None);

        let new = pg_streaming_offset_json_full(33_918_336, 33_918_336, 33_969_832);
        split.update_offset(new).unwrap();

        assert_eq!(split.pg_lsn_commit(), Some(33_969_832));
        assert_eq!(split.pg_lsn_proc(), Some(33_918_336));
    }

    #[test]
    fn test_extract_binlog_file_seq() {
        // default MySQL 8.0 basename
        assert_eq!(extract_binlog_file_seq("binlog.000123"), Some(123));
        // `--log-bin=mysql-bin`
        assert_eq!(extract_binlog_file_seq("mysql-bin.000123"), Some(123));
        // `<hostname>-bin` on older versions
        assert_eq!(extract_binlog_file_seq("my-host-bin.000001"), Some(1));
        // RDS / Aurora MySQL
        assert_eq!(
            extract_binlog_file_seq("mysql-bin-changelog.037568"),
            Some(37568)
        );
        // invalid suffix
        assert_eq!(extract_binlog_file_seq("binlog.index"), None);
        assert_eq!(extract_binlog_file_seq("no-extension"), None);
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let offset = r#"{
            "sourcePartition": {"server": "test"},
            "sourceOffset": {
                "file": "mysql-bin-changelog.037568",
                "pos": 12345
            },
            "isHeartbeat": false
        }"#;
        let split = MySqlCdcSplit::new(1, Some(offset.to_owned()));
        assert_eq!(split.mysql_binlog_offset(), Some((37568, 12345)));
    }

    #[test]
    fn test_extract_sql_server_lsn_from_offset_str() {
        let offset = r#"{
            "sourcePartition": {"server":"RW_CDC_1001"},
            "sourceOffset": {
                "change_lsn":"00000027:00000ac0:0001",
                "commit_lsn":"00000027:00000ac0:0002"
            },
            "isHeartbeat": false
        }"#;

        let change_lsn = extract_sql_server_change_lsn_from_offset_str(offset).unwrap();
        let commit_lsn = extract_sql_server_commit_lsn_from_offset_str(offset).unwrap();
        assert!(change_lsn < commit_lsn);
    }
}
