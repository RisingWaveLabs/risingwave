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

use crate::sink::catalog::SinkId;

#[derive(Debug, Clone)]
pub struct IcebergCommittedSnapshot {
    pub branch: String,
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    /// Inclusive file sequence number boundary for this snapshot. Iceberg V1
    /// does not support file sequence numbers.
    pub max_file_sequence_number: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IcebergSinkCompactionUpdate {
    // runtime event information
    pub sink_id: SinkId,
    pub force_compaction: bool,
    pub observed_snapshot: IcebergCommittedSnapshot,
}
