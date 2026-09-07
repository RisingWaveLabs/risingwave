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

use risingwave_common::types::{DatumRef, ScalarRefImpl, Timestamptz};
use risingwave_pb::connector_service::{CdcMessage, SourceType, cdc_message};

use crate::source::SourceMeta;
use crate::source::base::SourceMessage;

#[derive(Clone, Debug)]
pub enum CdcMessageType {
    Unspecified,
    Heartbeat,
    Data,
    TransactionMeta,
    SchemaChange,
}

impl From<cdc_message::CdcMessageType> for CdcMessageType {
    fn from(msg_type: cdc_message::CdcMessageType) -> Self {
        match msg_type {
            cdc_message::CdcMessageType::Data => CdcMessageType::Data,
            cdc_message::CdcMessageType::Heartbeat => CdcMessageType::Heartbeat,
            cdc_message::CdcMessageType::TransactionMeta => CdcMessageType::TransactionMeta,
            cdc_message::CdcMessageType::SchemaChange => CdcMessageType::SchemaChange,
            cdc_message::CdcMessageType::Unspecified => CdcMessageType::Unspecified,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DebeziumCdcMeta {
    /// The CDC source type of this message.
    ///
    /// This is required to disambiguate the parsing logic for `full_table_name` when it only
    /// contains one dot (e.g. `db.table` in MySQL vs `schema.table` in Postgres/SQL Server).
    pub source_type: SourceType,

    /// The end index (exclusive) of the database name in `full_table_name`.
    ///
    /// For `db.schema.table` or `db.table`, this is the index of the first `.`.
    db_name_end: usize,

    /// The start index of the routing table identifier in `full_table_name`.
    ///
    /// - For Postgres/SQL Server Debezium topics: `db.schema.table` → points to `schema.table`
    /// - For MySQL Debezium topics: `db.table` → `0` (keep `db.table` as the routing key)
    table_name_start: usize,

    pub full_table_name: String,
    // extracted from `payload.source.ts_ms`, the time that the change event was made in the database
    pub source_ts_ms: i64,
    pub msg_type: CdcMessageType,
}

impl DebeziumCdcMeta {
    // These `extract_xxx` methods are used to support the `INCLUDE TIMESTAMP/DATABASE_NAME/TABLE_NAME` feature
    pub fn extract_timestamp(&self) -> DatumRef<'_> {
        // Yield NULL rather than panicking on an out-of-range upstream `source.ts_ms`.
        Timestamptz::from_millis(self.source_ts_ms).map(ScalarRefImpl::Timestamptz)
    }

    pub fn extract_database_name(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(
            &self.full_table_name.as_str()[0..self.db_name_end],
        ))
    }

    pub fn extract_table_name(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(
            &self.full_table_name.as_str()[self.table_name_start..],
        ))
    }

    /// Extract the *object* table name (the last part) from `full_table_name`.
    ///
    /// - `db.schema.table` → `table`
    /// - `db.table` → `table`
    pub fn extract_table_name_only(&self) -> DatumRef<'_> {
        let s = self.full_table_name.as_str();
        let table = s.rsplit_once('.').map(|(_, t)| t).unwrap_or(s);
        Some(ScalarRefImpl::Utf8(table))
    }

    /// Derive indices for slicing `full_table_name`.
    ///
    /// # Background
    ///
    /// Debezium sends `full_table_name` in different formats depending on the CDC source type:
    ///
    /// - **MySQL**: `"database.table"` (1 dot, 2 parts)
    ///   - Example: `"risedev.orders"`
    ///   - User provides: `"risedev.orders"` (same format)
    ///   - Result: Keep entire string → `prefix_len` = 0
    ///
    /// - **Postgres**: `"database.schema.table"` (2 dots, 3 parts)
    ///   - Example: `"mydb.public.users"`
    ///   - User provides: `"public.users"` (database already in source config)
    ///   - Result: Skip "mydb." → `prefix_len` = 5
    ///
    /// - **SQL Server**: `"database.schema.table"` (2 dots, 3 parts)
    ///   - Example: `"mydb.dbo.orders"`
    ///   - User provides: `"dbo.orders"` (database already in source config)
    ///   - Result: Skip "mydb." → `prefix_len` = 5
    ///
    /// # Why the difference?
    ///
    /// - **MySQL** has no schema concept, so the "table name" IS `database.table`
    /// - **Postgres/SQL Server** have schemas, and the database is specified in the source,
    ///   so the "table name" is just `schema.table`
    fn derive_name_indices_from_full_table_name(
        full_table_name: &str,
        source_type: SourceType,
    ) -> (usize, usize) {
        let Some(first_dot) = full_table_name.find('.') else {
            // No dot found (should not happen in practice for valid CDC messages)
            return (0, 0);
        };

        let has_second_dot = full_table_name[first_dot + 1..].find('.').is_some();
        match source_type {
            // MySQL/MongoDB routing key keeps the full identifier (`db.table` / `db.collection`).
            SourceType::Mysql | SourceType::Mongodb => (first_dot, 0),

            // Postgres/Citus/SQL Server routing key is `schema.table` if database is present.
            // If `full_table_name` only contains one dot (e.g. `schema.table`), we can still route
            // correctly, but we can't derive the database name from it.
            SourceType::Postgres | SourceType::Citus | SourceType::SqlServer => {
                if has_second_dot {
                    (first_dot, first_dot + 1)
                } else {
                    (0, 0)
                }
            }

            // Fall back to the old heuristic:
            // - 2 dots: `db.schema.table` → strip db for routing
            // - 1 dot: treat as MySQL-style `db.table`
            SourceType::Unspecified => {
                if has_second_dot {
                    (first_dot, first_dot + 1)
                } else {
                    (first_dot, 0)
                }
            }
        }
    }

    pub fn new(
        full_table_name: String,
        source_ts_ms: i64,
        msg_type: cdc_message::CdcMessageType,
        source_type: SourceType,
    ) -> Self {
        let (db_name_end, table_name_start) =
            Self::derive_name_indices_from_full_table_name(&full_table_name, source_type);
        Self {
            source_type,
            db_name_end,
            table_name_start,
            full_table_name,
            source_ts_ms,
            msg_type: msg_type.into(),
        }
    }
}

impl From<CdcMessage> for SourceMessage {
    fn from(message: CdcMessage) -> Self {
        let msg_type = message.get_msg_type().expect("invalid message type");
        let source_type = message.get_source_type().unwrap_or(SourceType::Unspecified);
        SourceMessage {
            key: if message.key.is_empty() {
                None // only data message has key
            } else {
                Some(message.key.as_bytes().to_vec())
            },
            payload: if message.payload.is_empty() {
                None // heartbeat message
            } else {
                Some(message.payload.as_bytes().to_vec())
            },
            offset: message.offset,
            split_id: message.partition.into(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                message.full_table_name,
                message.source_ts_ms,
                msg_type,
                source_type,
            )),
        }
    }
}
