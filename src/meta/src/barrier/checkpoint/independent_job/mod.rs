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

use std::collections::{HashMap, HashSet};
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::id::FragmentId;
use risingwave_pb::stream_plan::barrier::PbBarrierKind;

pub(crate) mod batch_refresh_job;
pub(crate) mod creating_job;

pub(crate) use batch_refresh_job::{
    BatchRefreshJobCheckpointControl, BatchRefreshJobTriggerContext, BatchRefreshLogicalFragments,
    BatchRefreshRenderResult,
};
pub(crate) use creating_job::CreatingStreamingJobControl;

use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::NotifierStarter;
use crate::barrier::partial_graph::{CollectedBarrier, PartialGraphManager};
use crate::barrier::{BackfillProgress, BarrierKind, FragmentBackfillProgress, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;

/// Build a fake `BarrierInfo` for independent partial-graph barriers.
///
/// Shared by both `CreatingStreamingJobControl` and `BatchRefreshJobCheckpointControl`.
fn new_fake_barrier(
    prev_epoch_fake_physical_time: &mut u64,
    pending_non_checkpoint_barriers: &mut Vec<u64>,
    kind: PbBarrierKind,
) -> BarrierInfo {
    let prev_epoch = TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
    *prev_epoch_fake_physical_time += 1;
    let curr_epoch = TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
    let kind = match kind {
        PbBarrierKind::Unspecified => unreachable!(),
        PbBarrierKind::Initial => {
            assert!(pending_non_checkpoint_barriers.is_empty());
            BarrierKind::Initial
        }
        PbBarrierKind::Barrier => {
            pending_non_checkpoint_barriers.push(prev_epoch.value().0);
            BarrierKind::Barrier
        }
        PbBarrierKind::Checkpoint => {
            pending_non_checkpoint_barriers.push(prev_epoch.value().0);
            BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
        }
    };
    BarrierInfo {
        prev_epoch,
        curr_epoch,
        kind,
    }
}

// ── Enum unifying independent checkpoint job types ──────────────────────────

/// A streaming job that checkpoints independently from the database's main graph,
/// using its own partial graph.
pub(crate) enum IndependentCheckpointJobControl {
    CreatingStreamingJob(CreatingStreamingJobControl),
    BatchRefresh(BatchRefreshJobCheckpointControl),
}

impl IndependentCheckpointJobControl {
    pub(crate) fn gen_backfill_progress(&self) -> Option<BackfillProgress> {
        match self {
            Self::CreatingStreamingJob(j) => Some(j.gen_backfill_progress()),
            Self::BatchRefresh(j) => j.gen_backfill_progress(),
        }
    }

    /// Collect a barrier and return whether a checkpoint should be forced in the next barrier.
    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        match self {
            Self::CreatingStreamingJob(j) => j.collect(collected_barrier),
            Self::BatchRefresh(j) => j.collect(collected_barrier),
        }
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match self {
            Self::CreatingStreamingJob(j) => j.gen_fragment_backfill_progress(),
            Self::BatchRefresh(j) => j.gen_fragment_backfill_progress(),
        }
    }

    pub(crate) fn pinned_upstream_tables(&self) -> HashSet<TableId> {
        match self {
            Self::CreatingStreamingJob(j) => j.pinned_upstream_tables(),
            Self::BatchRefresh(j) => j.pinned_upstream_tables(),
        }
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match self {
            Self::CreatingStreamingJob(j) => j.fragment_infos(),
            Self::BatchRefresh(j) => j.fragment_infos(),
        }
    }

    pub(crate) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        epoch: u64,
    ) {
        match self {
            Self::CreatingStreamingJob(j) => j.ack_completed(partial_graph_manager, epoch),
            Self::BatchRefresh(j) => j.ack_completed(partial_graph_manager, epoch),
        }
    }

    pub(crate) fn on_partial_graph_reset(self) {
        match self {
            Self::CreatingStreamingJob(j) => j.on_partial_graph_reset(),
            Self::BatchRefresh(j) => j.on_partial_graph_reset(),
        }
    }

    pub(crate) fn drop(
        &mut self,
        notifier: Option<&mut NotifierStarter>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        match self {
            Self::CreatingStreamingJob(j) => j.drop(notifier, partial_graph_manager),
            Self::BatchRefresh(j) => j.drop(notifier, partial_graph_manager),
        }
    }

    /// Reset during database recovery.
    ///
    /// Returns `true` if the partial graph was already resetting (from a prior drop),
    /// meaning caller should not issue a new reset request.
    pub(crate) fn reset(self) -> bool {
        match self {
            Self::CreatingStreamingJob(j) => j.reset(),
            Self::BatchRefresh(j) => j.reset(),
        }
    }
}
