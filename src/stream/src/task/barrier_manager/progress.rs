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

use std::fmt::{Display, Formatter};

use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::id::FragmentId;
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;

use crate::executor::ActorContext;
use crate::task::barrier_manager::LocalBarrierEvent::ReportCreateProgress;
use crate::task::barrier_worker::managed_state::PartialGraphState;
use crate::task::cdc_progress::CdcTableBackfillState;
use crate::task::{ActorId, LocalBarrierManager};

type ConsumedEpoch = u64;
type ConsumedRows = u64;
type BufferedRows = u64;

#[derive(Debug, Clone, Copy)]
pub(crate) enum BackfillState {
    ConsumingUpstreamTableOrSource(ConsumedEpoch, ConsumedRows, BufferedRows),
    DoneConsumingUpstreamTableOrSource(ConsumedRows, BufferedRows),
}

impl BackfillState {
    pub fn to_pb(self, fragment_id: FragmentId, actor_id: ActorId) -> PbCreateMviewProgress {
        let (done, consumed_epoch, consumed_rows, buffered_rows) = match self {
            BackfillState::ConsumingUpstreamTableOrSource(
                consumed_epoch,
                consumed_rows,
                buffered_rows,
            ) => (false, consumed_epoch, consumed_rows, buffered_rows),
            BackfillState::DoneConsumingUpstreamTableOrSource(consumed_rows, buffered_rows) => {
                (true, 0, consumed_rows, buffered_rows)
            }
        };
        PbCreateMviewProgress {
            backfill_actor_id: actor_id,
            done,
            consumed_epoch,
            consumed_rows,
            buffered_rows,
            fragment_id,
        }
    }
}

impl Display for BackfillState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillState::ConsumingUpstreamTableOrSource(epoch, rows, buffered) => {
                write!(
                    f,
                    "ConsumingUpstreamTable(epoch: {}, rows: {}, buffered: {})",
                    epoch, rows, buffered
                )
            }
            BackfillState::DoneConsumingUpstreamTableOrSource(rows, buffered) => {
                write!(
                    f,
                    "DoneConsumingUpstreamTable(rows: {}, buffered: {})",
                    rows, buffered
                )
            }
        }
    }
}

impl PartialGraphState {
    pub(crate) fn update_create_mview_progress(
        &mut self,
        epoch: EpochPair,
        fragment_id: FragmentId,
        actor: ActorId,
        state: BackfillState,
    ) {
        if let Some((prev_fragment_id, _)) = self
            .graph_state
            .create_mview_progress
            .entry(epoch.curr)
            .or_default()
            .insert(actor, (fragment_id, state))
        {
            assert_eq!(prev_fragment_id, fragment_id)
        }
    }

    pub(crate) fn update_cdc_table_backfill_progress(
        &mut self,
        epoch: EpochPair,
        actor: ActorId,
        state: CdcTableBackfillState,
    ) {
        if let Some(actor_state) = self.actor_states.get(&actor)
            && actor_state.inflight_barriers.contains(&epoch.prev)
        {
            self.graph_state
                .cdc_table_backfill_progress
                .entry(epoch.curr)
                .or_default()
                .insert(actor, state);
        } else {
            warn!(?epoch, %actor, ?state, "ignore CDC table backfill progress");
        }
    }
}

impl LocalBarrierManager {
    fn update_create_mview_progress(
        &self,
        epoch: EpochPair,
        fragment_id: FragmentId,
        actor: ActorId,
        state: BackfillState,
    ) {
        self.send_event(ReportCreateProgress {
            epoch,
            fragment_id,
            actor,
            state,
        })
    }
}

/// The progress held by the backfill executors to report to the local barrier manager.
///
/// Progress can be computed by
/// `total_rows_consumed` / `total_rows_upstream`.
/// This yields the (approximate) percentage of rows we are done backfilling.
///
/// For `total_rows_consumed`, the progress is tracked in the following way:
/// 1. Fetching the row count from our state table.
///    This number is the total number, NOT incremental.
///    This is done per actor.
/// 2. Refreshing this number on the meta side, on every barrier.
///    This is done by just summing up all the row counts from the actors.
///
/// For `total_rows_upstream`,
/// this is fetched from `HummockVersion`'s statistics (`TableStats::total_key_count`).
///
/// This is computed per `HummockVersion`, which is updated whenever a checkpoint is committed.
/// The `total_key_count` figure just counts the number of storage keys.
/// For example, if a key is inserted and then deleted,
/// it results two storage entries in `LSMt`, so count=2.
/// Only after compaction, the count will drop back to 0.
///
/// So the total count could be more pessimistic, than actual progress.
///
/// It is fine for this number not to be precise,
/// since we don't rely on it to update the status of a stream job internally.
///
/// TODO(kwannoel): Perhaps it is possible to get total key count of the replicated state table
/// for arrangement backfill. We can use that to estimate the progress as well, and avoid recording
/// `row_count` state for it.
pub struct CreateMviewProgressReporter {
    barrier_manager: LocalBarrierManager,

    fragment_id: FragmentId,

    /// The id of the actor containing the backfill executors.
    backfill_actor_id: ActorId,

    state: Option<BackfillState>,
}

impl CreateMviewProgressReporter {
    pub fn new(
        barrier_manager: LocalBarrierManager,
        fragment_id: FragmentId,
        backfill_actor_id: ActorId,
    ) -> Self {
        Self {
            barrier_manager,
            fragment_id,
            backfill_actor_id,
            state: None,
        }
    }

    #[cfg(test)]
    pub fn for_test(barrier_manager: LocalBarrierManager) -> Self {
        Self::new(barrier_manager, 0.into(), 0.into())
    }

    pub fn actor_id(&self) -> ActorId {
        self.backfill_actor_id
    }

    fn update_inner(&mut self, epoch: EpochPair, state: BackfillState) {
        self.state = Some(state);
        self.barrier_manager.update_create_mview_progress(
            epoch,
            self.fragment_id,
            self.backfill_actor_id,
            state,
        );
    }

    /// Update the progress to `ConsumingUpstream(consumed_epoch, consumed_rows)`. The epoch must be
    /// monotonically increasing.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    /// `current_consumed_rows` is an accumulated value.
    pub fn update(
        &mut self,
        epoch: EpochPair,
        consumed_epoch: ConsumedEpoch,
        current_consumed_rows: ConsumedRows,
    ) {
        self.update_with_buffered_rows(epoch, consumed_epoch, current_consumed_rows, 0);
    }

    /// Update the progress with buffered rows information.
    /// This is used by locality backfill to report precise progress including buffered data.
    pub fn update_with_buffered_rows(
        &mut self,
        epoch: EpochPair,
        consumed_epoch: ConsumedEpoch,
        current_consumed_rows: ConsumedRows,
        buffered_rows: BufferedRows,
    ) {
        match self.state {
            Some(BackfillState::ConsumingUpstreamTableOrSource(last, last_consumed_rows, _)) => {
                assert!(
                    last <= consumed_epoch,
                    "last_epoch: {:#?} must be greater than consumed epoch: {:#?}",
                    last,
                    consumed_epoch
                );
                assert!(last_consumed_rows <= current_consumed_rows);
            }
            Some(state) => {
                panic!(
                    "should not update consuming progress at invalid state: {:?}",
                    state
                )
            }
            None => {}
        };
        tracing::debug!(
            actor_id = %self.backfill_actor_id,
            ?epoch,
            consumed_epoch,
            current_consumed_rows,
            buffered_rows,
            "progress update"
        );
        self.update_inner(
            epoch,
            BackfillState::ConsumingUpstreamTableOrSource(
                consumed_epoch,
                current_consumed_rows,
                buffered_rows,
            ),
        );
    }

    /// The difference from [`Self::update`] (MV backfill) is that we
    /// don't care `ConsumedEpoch` here.
    pub fn update_for_source_backfill(
        &mut self,
        epoch: EpochPair,
        current_consumed_rows: ConsumedRows,
    ) {
        match self.state {
            Some(BackfillState::ConsumingUpstreamTableOrSource(
                dummy_last_epoch,
                _last_consumed_rows,
                _,
            )) => {
                debug_assert_eq!(dummy_last_epoch, 0);
            }
            Some(state) => {
                panic!(
                    "should not update consuming progress at invalid state: {:?}",
                    state
                )
            }
            None => {}
        };
        self.update_inner(
            epoch,
            // fill a dummy ConsumedEpoch and no buffered rows
            BackfillState::ConsumingUpstreamTableOrSource(0, current_consumed_rows, 0),
        );
    }

    /// Finish the progress. If the progress is already finished, then perform no-op.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    pub fn finish(&mut self, epoch: EpochPair, current_consumed_rows: ConsumedRows) {
        self.finish_with_buffered_rows(epoch, current_consumed_rows, 0);
    }

    /// Finish the progress with buffered rows information.
    /// This is used by locality backfill to report any remaining buffered rows at completion.
    pub fn finish_with_buffered_rows(
        &mut self,
        epoch: EpochPair,
        current_consumed_rows: ConsumedRows,
        buffered_rows: BufferedRows,
    ) {
        if let Some(BackfillState::DoneConsumingUpstreamTableOrSource(_, _)) = self.state {
            return;
        }
        tracing::debug!(
            actor_id = %self.backfill_actor_id,
            ?epoch,
            current_consumed_rows,
            buffered_rows,
            "progress finish"
        );
        // When finishing, report the consumed rows and buffered rows separately
        self.update_inner(
            epoch,
            BackfillState::DoneConsumingUpstreamTableOrSource(current_consumed_rows, buffered_rows),
        );
    }
}

impl LocalBarrierManager {
    /// Create a struct for reporting the progress of creating mview. The backfill executors should
    /// report the progress of barrier rearranging continuously using this. The updated progress
    /// will be collected by the local barrier manager and reported to the meta service in this
    /// epoch.
    ///
    /// When all backfill executors of the creating mview finish, the creation progress will be done at
    /// frontend and the mview will be exposed to the user.
    pub(crate) fn register_create_mview_progress(
        &self,
        actor_ctx: &ActorContext,
    ) -> CreateMviewProgressReporter {
        let fragment_id = actor_ctx.fragment_id;
        let backfill_actor_id = actor_ctx.id;
        trace!(%fragment_id, %backfill_actor_id, "register create mview progress");
        CreateMviewProgressReporter::new(self.clone(), fragment_id, backfill_actor_id)
    }
}
