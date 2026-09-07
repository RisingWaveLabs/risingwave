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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta, StateTableInfoDelta};

use crate::version::{HummockVersion, HummockVersionDelta, HummockVersionStateTableInfo};
use crate::{HummockVersionId, INVALID_VERSION_ID};

#[derive(Clone, Debug)]
pub struct FrontendHummockVersion {
    pub id: HummockVersionId,
    pub state_table_info: HummockVersionStateTableInfo,
}

impl FrontendHummockVersion {
    pub fn from_version(version: &HummockVersion) -> Self {
        Self {
            id: version.id,
            state_table_info: version.state_table_info.clone(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersion {
        #[expect(deprecated)]
        PbHummockVersion {
            id: self.id,
            levels: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            table_watermarks: Default::default(),
            table_change_logs: Default::default(),
            state_table_info: self.state_table_info.info().clone(),
            vector_indexes: Default::default(),
        }
    }

    pub fn from_protobuf(value: PbHummockVersion) -> Self {
        Self {
            id: value.id,
            state_table_info: HummockVersionStateTableInfo::from_protobuf(&value.state_table_info),
        }
    }

    pub fn apply_delta(&mut self, delta: FrontendHummockVersionDelta) {
        if self.id != INVALID_VERSION_ID {
            assert_eq!(self.id, delta.prev_id);
        }
        self.id = delta.id;
        self.state_table_info
            .apply_delta(&delta.state_table_info_delta, &delta.removed_table_id);
    }
}

pub struct FrontendHummockVersionDelta {
    pub prev_id: HummockVersionId,
    pub id: HummockVersionId,
    pub removed_table_id: HashSet<TableId>,
    pub state_table_info_delta: HashMap<TableId, StateTableInfoDelta>,
}

impl FrontendHummockVersionDelta {
    pub fn from_delta(delta: &HummockVersionDelta) -> Self {
        Self {
            prev_id: delta.prev_id,
            id: delta.id,
            removed_table_id: delta.removed_table_ids.clone(),
            state_table_info_delta: delta.state_table_info_delta.clone(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        #[expect(deprecated)]
        PbHummockVersionDelta {
            id: self.id,
            prev_id: self.prev_id,
            group_deltas: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            trivial_move: false,
            new_table_watermarks: Default::default(),
            removed_table_ids: self.removed_table_id.iter().copied().collect(),
            change_log_delta: Default::default(),
            state_table_info_delta: self.state_table_info_delta.clone(),
            vector_index_delta: Default::default(),
        }
    }

    pub fn from_protobuf(delta: PbHummockVersionDelta) -> Self {
        Self {
            prev_id: delta.prev_id,
            id: delta.id,
            removed_table_id: delta.removed_table_ids.into_iter().collect(),
            state_table_info_delta: delta
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| ((*table_id), *delta))
                .collect(),
        }
    }
}
