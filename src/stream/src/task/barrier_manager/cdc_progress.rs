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

use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;

use crate::task::barrier_manager::LocalBarrierEvent::ReportCdcTableBackfillProgress;
use crate::task::{ActorId, FragmentId, LocalBarrierManager};

#[derive(Debug, Clone, Copy)]
pub(crate) enum CdcTableBackfillState {
    Update {
        fragment_id: FragmentId,
        split_id_start_inclusive: i64,
        split_id_end_inclusive: i64,
        generation: u64,
    },
    Finish {
        fragment_id: FragmentId,
        split_id_start_inclusive: i64,
        split_id_end_inclusive: i64,
        generation: u64,
    },
    Rows {
        fragment_id: FragmentId,
        backfilled_row_count: u64,
        estimated_row_count: Option<u64>,
        done: bool,
    },
}

impl CdcTableBackfillState {
    pub fn to_pb(self, actor_id: ActorId, epoch: u64) -> PbCdcTableBackfillProgress {
        match self {
            CdcTableBackfillState::Update {
                fragment_id,
                split_id_start_inclusive,
                split_id_end_inclusive,
                generation,
            } => PbCdcTableBackfillProgress {
                actor_id,
                epoch,
                done: false,
                split_id_start_inclusive,
                split_id_end_inclusive,
                generation,
                fragment_id,
                backfilled_row_count: None,
                estimated_row_count: None,
            },
            CdcTableBackfillState::Finish {
                fragment_id,
                split_id_start_inclusive,
                split_id_end_inclusive,
                generation,
            } => PbCdcTableBackfillProgress {
                actor_id,
                epoch,
                done: true,
                split_id_start_inclusive,
                split_id_end_inclusive,
                generation,
                fragment_id,
                backfilled_row_count: None,
                estimated_row_count: None,
            },
            CdcTableBackfillState::Rows {
                fragment_id,
                backfilled_row_count,
                estimated_row_count,
                done,
            } => PbCdcTableBackfillProgress {
                actor_id,
                epoch,
                done,
                fragment_id,
                backfilled_row_count: Some(backfilled_row_count),
                estimated_row_count,
                ..Default::default()
            },
        }
    }
}

pub struct CdcProgressReporter {
    barrier_manager: LocalBarrierManager,
}

impl CdcProgressReporter {
    pub fn new(barrier_manager: LocalBarrierManager) -> Self {
        Self { barrier_manager }
    }

    pub fn update(
        &self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        epoch: EpochPair,
        generation: u64,
        split_id_range: (i64, i64),
    ) {
        self.barrier_manager.update_cdc_backfill_progress(
            actor_id,
            epoch,
            CdcTableBackfillState::Update {
                fragment_id,
                split_id_start_inclusive: split_id_range.0,
                split_id_end_inclusive: split_id_range.1,
                generation,
            },
        );
    }

    pub fn finish(
        &self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        epoch: EpochPair,
        generation: u64,
        split_id_range: (i64, i64),
    ) {
        self.barrier_manager.update_cdc_backfill_progress(
            actor_id,
            epoch,
            CdcTableBackfillState::Finish {
                fragment_id,
                split_id_start_inclusive: split_id_range.0,
                split_id_end_inclusive: split_id_range.1,
                generation,
            },
        );
    }

    pub fn update_rows(
        &self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        epoch: EpochPair,
        backfilled_row_count: u64,
        estimated_row_count: Option<u64>,
    ) {
        self.barrier_manager.update_cdc_backfill_progress(
            actor_id,
            epoch,
            CdcTableBackfillState::Rows {
                fragment_id,
                backfilled_row_count,
                estimated_row_count,
                done: false,
            },
        );
    }

    pub fn finish_rows(
        &self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        epoch: EpochPair,
        backfilled_row_count: u64,
        estimated_row_count: Option<u64>,
    ) {
        self.barrier_manager.update_cdc_backfill_progress(
            actor_id,
            epoch,
            CdcTableBackfillState::Rows {
                fragment_id,
                backfilled_row_count,
                estimated_row_count,
                done: true,
            },
        );
    }
}

impl LocalBarrierManager {
    fn update_cdc_backfill_progress(
        &self,
        actor_id: ActorId,
        epoch: EpochPair,
        state: CdcTableBackfillState,
    ) {
        self.send_event(ReportCdcTableBackfillProgress {
            actor_id,
            epoch,
            state,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CdcTableBackfillState;

    #[test]
    fn test_progress_to_pb_row_fields() {
        let split_progress = CdcTableBackfillState::Update {
            fragment_id: 1.into(),
            split_id_start_inclusive: 2,
            split_id_end_inclusive: 3,
            generation: 4,
        }
        .to_pb(5.into(), 6);
        assert_eq!(split_progress.backfilled_row_count, None);
        assert_eq!(split_progress.estimated_row_count, None);

        let row_progress = CdcTableBackfillState::Rows {
            fragment_id: 7.into(),
            backfilled_row_count: 80,
            estimated_row_count: Some(100),
            done: true,
        }
        .to_pb(8.into(), 9);
        assert_eq!(row_progress.backfilled_row_count, Some(80));
        assert_eq!(row_progress.estimated_row_count, Some(100));
        assert!(row_progress.done);
    }
}
