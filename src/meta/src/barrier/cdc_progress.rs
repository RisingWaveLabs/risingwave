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

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use risingwave_common::id::JobId;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_connector::source::CdcTableSnapshotSplitRaw;
use risingwave_connector::source::cdc::external::CDC_TABLE_SPLIT_ID_START;
use risingwave_connector::source::cdc::{
    INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID, INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
};
use risingwave_meta_model::{FragmentId, cdc_table_snapshot_split};
use risingwave_pb::id::ActorId;
use risingwave_pb::source::PbCdcTableSnapshotSplits;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

use crate::MetaResult;
use crate::stream::cdc::{
    CdcTableSnapshotSplits, assign_cdc_table_snapshot_splits, single_merged_split,
};

#[derive(Debug)]
enum CdcBackfillStatus {
    Backfilling(CdcBackfillProgress),
    PreCompleted,
    Completed,
}

#[derive(Debug)]
struct CdcBackfillProgress {
    splits: Vec<CdcTableSnapshotSplitRaw>,
    /// The number of splits that has completed backfill.
    split_backfilled_count: u64,
    /// The number of splits that has completed backfill and synchronized CDC offset.
    split_completed_count: u64,
    /// The generation of split assignment.
    split_assignment_generation: u64,
}

#[derive(Debug, Default)]
struct CdcBackfillRowProgress {
    backfilled_row_count: u64,
    estimated_row_count: Option<u64>,
}

#[derive(Debug)]
pub struct CdcProgress {
    /// The total number of splits, immutable.
    pub split_total_count: u64,
    /// The number of splits that has completed backfill.
    pub split_backfilled_count: u64,
    /// The number of splits that has completed backfill and synchronized CDC offset.
    pub split_completed_count: u64,
    /// Only set for non-parallelized CDC backfill.
    pub backfilled_row_count: Option<u64>,
    /// Best-effort estimate from upstream database statistics.
    pub estimated_row_count: Option<u64>,
}

impl CdcTableBackfillTracker {
    pub async fn mark_complete_job(txn: &impl ConnectionTrait, job_id: JobId) -> MetaResult<()> {
        // Rewrite the first split as [inf, inf].
        let bound = OwnedRow::new(vec![None]).value_serialize();
        cdc_table_snapshot_split::Entity::update_many()
            .col_expr(
                cdc_table_snapshot_split::Column::IsBackfillFinished,
                Expr::value(1),
            )
            .col_expr(
                cdc_table_snapshot_split::Column::Left,
                Expr::value(bound.clone()),
            )
            .col_expr(cdc_table_snapshot_split::Column::Right, Expr::value(bound))
            .filter(cdc_table_snapshot_split::Column::TableId.eq(job_id))
            .filter(cdc_table_snapshot_split::Column::SplitId.eq(CDC_TABLE_SPLIT_ID_START))
            .exec(txn)
            .await?;
        // Keep only the first split.
        cdc_table_snapshot_split::Entity::delete_many()
            .filter(cdc_table_snapshot_split::Column::TableId.eq(job_id))
            .filter(cdc_table_snapshot_split::Column::SplitId.gt(CDC_TABLE_SPLIT_ID_START))
            .exec(txn)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct CdcTableBackfillTracker {
    status: CdcBackfillStatus,
    row_progress: Option<CdcBackfillRowProgress>,
    cdc_scan_fragment_id: FragmentId,
    next_generation: u64,
}

impl CdcTableBackfillTracker {
    fn new_inner(cdc_scan_fragment_id: FragmentId, splits: CdcTableSnapshotSplits) -> Self {
        let status = match splits {
            CdcTableSnapshotSplits::Backfilling(splits) => {
                CdcBackfillStatus::Backfilling(CdcBackfillProgress {
                    splits,
                    split_backfilled_count: 0,
                    split_completed_count: 0,
                    split_assignment_generation: INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
                })
            }
            CdcTableSnapshotSplits::Finished => CdcBackfillStatus::Completed,
        };
        Self {
            status,
            row_progress: None,
            cdc_scan_fragment_id,
            next_generation: INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID + 1,
        }
    }

    pub fn restore(cdc_scan_fragment_id: FragmentId, splits: CdcTableSnapshotSplits) -> Self {
        Self::new_inner(cdc_scan_fragment_id, splits)
    }

    pub fn new(cdc_scan_fragment_id: FragmentId, splits: Vec<CdcTableSnapshotSplitRaw>) -> Self {
        Self::new_inner(
            cdc_scan_fragment_id,
            CdcTableSnapshotSplits::Backfilling(splits),
        )
    }

    pub fn new_non_parallelized(cdc_scan_fragment_id: FragmentId) -> Self {
        Self {
            status: CdcBackfillStatus::Backfilling(CdcBackfillProgress {
                splits: vec![single_merged_split()],
                split_backfilled_count: 0,
                split_completed_count: 0,
                split_assignment_generation: INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
            }),
            row_progress: Some(CdcBackfillRowProgress::default()),
            cdc_scan_fragment_id,
            next_generation: INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
        }
    }

    pub fn is_parallelized(&self) -> bool {
        self.row_progress.is_none()
    }

    pub fn cdc_scan_fragment_id(&self) -> FragmentId {
        self.cdc_scan_fragment_id
    }

    pub fn update_split_progress(&mut self, progress: &PbCdcTableBackfillProgress) {
        tracing::debug!(?progress, "Complete split.");
        let current_progress = match &mut self.status {
            CdcBackfillStatus::Backfilling(progress) => progress,
            CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => {
                return;
            }
        };
        assert_ne!(
            progress.generation,
            INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID
        );
        if current_progress.split_assignment_generation == progress.generation {
            current_progress.split_backfilled_count +=
                (1 + progress.split_id_end_inclusive - progress.split_id_start_inclusive) as u64;
            if progress.done {
                current_progress.split_completed_count += (1 + progress.split_id_end_inclusive
                    - progress.split_id_start_inclusive)
                    as u64;
                if current_progress.split_completed_count == current_progress.splits.len() as u64 {
                    self.status = CdcBackfillStatus::PreCompleted;
                }
            }
        }
    }

    pub fn update_row_progress(&mut self, progress: &PbCdcTableBackfillProgress) {
        let Some(backfilled_row_count) = progress.backfilled_row_count else {
            return;
        };
        let Some(row_progress) = &mut self.row_progress else {
            return;
        };
        row_progress.backfilled_row_count =
            row_progress.backfilled_row_count.max(backfilled_row_count);
        if row_progress.estimated_row_count.is_none() {
            row_progress.estimated_row_count = progress.estimated_row_count;
        }
        if progress.done {
            self.status = CdcBackfillStatus::Completed;
        }
    }

    pub fn reassign_splits(
        &mut self,
        actor_ids: HashSet<ActorId>,
    ) -> MetaResult<HashMap<ActorId, PbCdcTableSnapshotSplits>> {
        assert!(self.is_parallelized());
        let generation = self.next_generation;
        self.next_generation += 1;
        let splits = match &mut self.status {
            CdcBackfillStatus::Backfilling(progress) => {
                progress.split_backfilled_count = 0;
                progress.split_completed_count = 0;
                progress.split_assignment_generation = generation;
                progress.splits.as_slice()
            }
            CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => {
                static SINGLE_SPLIT: LazyLock<CdcTableSnapshotSplitRaw> =
                    LazyLock::new(single_merged_split);
                core::slice::from_ref(&*SINGLE_SPLIT)
            }
        };
        assign_cdc_table_snapshot_splits(actor_ids, splits, generation)
    }

    pub fn gen_cdc_progress(&self) -> CdcProgress {
        let (split_total_count, split_backfilled_count, split_completed_count) = match &self.status
        {
            CdcBackfillStatus::Backfilling(progress) => (
                progress.splits.len() as _,
                progress.split_backfilled_count,
                progress.split_completed_count,
            ),
            CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => (1, 1, 1),
        };
        let row_progress = self.row_progress.as_ref();
        CdcProgress {
            split_total_count,
            split_backfilled_count,
            split_completed_count,
            backfilled_row_count: row_progress.map(|progress| progress.backfilled_row_count),
            estimated_row_count: row_progress.and_then(|progress| progress.estimated_row_count),
        }
    }

    pub fn take_pre_completed(&mut self) -> bool {
        if let CdcBackfillStatus::PreCompleted = &self.status {
            self.status = CdcBackfillStatus::Completed;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {

    use risingwave_connector::source::CdcTableSnapshotSplitRaw;
    use risingwave_pb::stream_service::barrier_complete_response::CdcTableBackfillProgress;

    use crate::barrier::cdc_progress::{
        CdcBackfillProgress, CdcBackfillStatus, CdcTableBackfillTracker,
    };

    impl CdcTableBackfillTracker {
        fn progress(&self) -> &CdcBackfillProgress {
            if let CdcBackfillStatus::Backfilling(progress) = &self.status {
                progress
            } else {
                unreachable!()
            }
        }
    }

    #[tokio::test]
    async fn test_generation() {
        let split_count = 10u64;
        let mut tracker = CdcTableBackfillTracker::new(
            233.into(),
            (0..split_count)
                .map(|split_id| CdcTableSnapshotSplitRaw {
                    split_id: split_id as _,
                    left_bound_inclusive: vec![],
                    right_bound_exclusive: vec![],
                })
                .collect(),
        );
        assert_eq!(tracker.next_generation, 2);
        tracker
            .reassign_splits([1.into()].into_iter().collect())
            .unwrap();
        let generation = tracker.progress().split_assignment_generation;
        assert_eq!(generation, 2);
        assert_init_state(&tracker, split_count);
        let cdc_table_backfill_progress = vec![
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 1,
                split_id_end_inclusive: 2,
                generation,
                fragment_id: 12.into(),
                ..Default::default()
            },
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 5,
                split_id_end_inclusive: 10,
                generation,
                fragment_id: 11.into(),
                ..Default::default()
            },
        ];
        for progress in &cdc_table_backfill_progress {
            tracker.update_split_progress(progress);
        }
        assert_eq!(tracker.progress().split_completed_count, 8);

        // Reset generation.
        tracker
            .reassign_splits([1.into()].into_iter().collect())
            .unwrap();
        let generation = tracker.progress().split_assignment_generation;
        assert_eq!(generation, 3);
        assert_init_state(&tracker, split_count);
        let cdc_table_backfill_progress = CdcTableBackfillProgress {
            done: true,
            split_id_start_inclusive: 3,
            split_id_end_inclusive: 4,
            // Expired generation.
            generation: generation - 1,
            fragment_id: 13.into(),
            ..Default::default()
        };
        tracker.update_split_progress(&cdc_table_backfill_progress);
        assert_init_state(&tracker, split_count);
        assert_eq!(tracker.progress().split_completed_count, 0);

        let cdc_table_backfill_progress = [
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 1,
                split_id_end_inclusive: 2,
                generation,
                fragment_id: 12.into(),
                ..Default::default()
            },
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 5,
                split_id_end_inclusive: 10,
                generation,
                fragment_id: 11.into(),
                ..Default::default()
            },
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 3,
                split_id_end_inclusive: 4,
                generation,
                fragment_id: 13.into(),
                ..Default::default()
            },
        ];
        for progress in &cdc_table_backfill_progress {
            tracker.update_split_progress(progress);
        }
        assert!(tracker.take_pre_completed());
    }

    #[test]
    fn test_non_parallelized_row_progress() {
        let mut tracker = CdcTableBackfillTracker::new_non_parallelized(233.into());
        assert!(!tracker.is_parallelized());

        tracker.update_row_progress(&CdcTableBackfillProgress {
            fragment_id: 233.into(),
            backfilled_row_count: Some(40),
            estimated_row_count: Some(100),
            ..Default::default()
        });
        let progress = tracker.gen_cdc_progress();
        assert_eq!(progress.split_total_count, 1);
        assert_eq!(progress.split_backfilled_count, 0);
        assert_eq!(progress.split_completed_count, 0);
        assert_eq!(progress.backfilled_row_count, Some(40));
        assert_eq!(progress.estimated_row_count, Some(100));

        // Retried or stale reports must not move the numerator backwards or
        // replace the estimate captured by the first successful query.
        tracker.update_row_progress(&CdcTableBackfillProgress {
            fragment_id: 233.into(),
            backfilled_row_count: Some(20),
            estimated_row_count: Some(200),
            ..Default::default()
        });
        let progress = tracker.gen_cdc_progress();
        assert_eq!(progress.backfilled_row_count, Some(40));
        assert_eq!(progress.estimated_row_count, Some(100));

        tracker.update_row_progress(&CdcTableBackfillProgress {
            fragment_id: 233.into(),
            done: true,
            backfilled_row_count: Some(110),
            estimated_row_count: Some(100),
            ..Default::default()
        });
        let progress = tracker.gen_cdc_progress();
        assert_eq!(progress.split_backfilled_count, 1);
        assert_eq!(progress.split_completed_count, 1);
        assert_eq!(progress.backfilled_row_count, Some(110));
        assert_eq!(progress.estimated_row_count, Some(100));
        assert!(!tracker.take_pre_completed());
    }

    fn assert_init_state(tracker: &CdcTableBackfillTracker, split_count: u64) {
        let CdcBackfillStatus::Backfilling(progress) = &tracker.status else {
            unreachable!()
        };
        assert_eq!(progress.splits.len() as u64, split_count);
        assert_eq!(progress.split_completed_count, 0);
        assert_eq!(progress.split_backfilled_count, 0);
    }
}
