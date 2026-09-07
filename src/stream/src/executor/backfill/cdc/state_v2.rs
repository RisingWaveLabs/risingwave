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

use std::ops::Range;

use anyhow::anyhow;
use risingwave_common::row;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{JsonbVal, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::cdc::external::CdcOffset;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

#[derive(Debug, Default)]
pub struct CdcStateRecord {
    pub current_pk_pos: Option<OwnedRow>,
    pub is_finished: bool,
    pub row_count: i64,
    pub cdc_offset_low: Option<CdcOffset>,
    pub cdc_offset_high: Option<CdcOffset>,
}

/// V1 state schema: | `split_id` | `backfill_finished` | `row_count` |
/// V2 state schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset_low` | `cdc_offset_high` |
/// V3 state schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset_low` | `cdc_offset_high` | `pk...` |
pub struct ParallelizedCdcBackfillState<S: StateStore> {
    state_table: StateTable<S>,
    layout: ParallelizedCdcBackfillStateLayout,
}

#[derive(Debug, Clone, Copy)]
enum ParallelizedCdcBackfillStateLayout {
    Legacy,
    WithCdcOffsets,
    WithSnapshotCursor { pk_len: usize },
}

struct StateFieldIndices {
    finished: usize,
    row_count: usize,
    cdc_offset_low: Option<usize>,
    cdc_offset_high: Option<usize>,
    snapshot_cursor: Option<Range<usize>>,
}

impl StateFieldIndices {
    const fn new(
        finished: usize,
        row_count: usize,
        cdc_offset_low: Option<usize>,
        cdc_offset_high: Option<usize>,
        snapshot_cursor: Option<Range<usize>>,
    ) -> Self {
        Self {
            finished,
            row_count,
            cdc_offset_low,
            cdc_offset_high,
            snapshot_cursor,
        }
    }
}

impl ParallelizedCdcBackfillStateLayout {
    fn from_state_len(state_len: usize, pk_len: usize) -> Self {
        match state_len {
            3 => Self::Legacy,
            5 => Self::WithCdcOffsets,
            state_len if state_len == pk_len + 5 => Self::WithSnapshotCursor { pk_len },
            _ => panic!("unsupported parallel CDC backfill state length: {state_len}"),
        }
    }

    fn state_len(self) -> usize {
        match self {
            Self::Legacy => 3,
            Self::WithCdcOffsets => 5,
            Self::WithSnapshotCursor { pk_len } => pk_len + 5,
        }
    }

    fn field_indices(self) -> StateFieldIndices {
        const FINISHED: usize = 1;
        const ROW_COUNT: usize = 2;
        const CDC_OFFSET_LOW: usize = 3;
        const CDC_OFFSET_HIGH: usize = 4;
        const SNAPSHOT_CURSOR_START: usize = 5;

        match self {
            Self::Legacy => StateFieldIndices::new(FINISHED, ROW_COUNT, None, None, None),
            Self::WithCdcOffsets => StateFieldIndices::new(
                FINISHED,
                ROW_COUNT,
                Some(CDC_OFFSET_LOW),
                Some(CDC_OFFSET_HIGH),
                None,
            ),
            Self::WithSnapshotCursor { pk_len } => StateFieldIndices::new(
                FINISHED,
                ROW_COUNT,
                Some(CDC_OFFSET_LOW),
                Some(CDC_OFFSET_HIGH),
                Some(SNAPSHOT_CURSOR_START..SNAPSHOT_CURSOR_START + pk_len),
            ),
        }
    }
}

impl<S: StateStore> ParallelizedCdcBackfillState<S> {
    pub fn new(state_table: StateTable<S>, pk_len: usize) -> Self {
        let layout = ParallelizedCdcBackfillStateLayout::from_state_len(
            state_table.get_data_types().len(),
            pk_len,
        );
        Self {
            state_table,
            layout,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    /// Restore the backfill state from storage
    pub async fn restore_state(&mut self, split_id: i64) -> StreamExecutorResult<CdcStateRecord> {
        let key = Some(split_id);
        match self
            .state_table
            .get_row(row::once(key.map(ScalarImpl::from)))
            .await?
        {
            Some(row) => {
                tracing::info!("restored cdc backfill state: {:?}", row);
                let state = row.into_inner().into_vec();
                let field_indices = self.layout.field_indices();
                let current_pk_pos = if let Some(snapshot_cursor) = field_indices.snapshot_cursor {
                    let pk = state[snapshot_cursor].to_vec();

                    // snapshot cursor can only be all None or all Some
                    if pk.iter().all(Option::is_none) {
                        None
                    } else if pk.iter().all(Option::is_some) {
                        Some(OwnedRow::new(pk))
                    } else {
                        return Err(anyhow!(
                            "invalid backfill state: partially null snapshot cursor"
                        )
                        .into());
                    }
                } else {
                    None
                };
                let Some(ScalarImpl::Bool(is_finished)) = state[field_indices.finished] else {
                    return Err(anyhow!("invalid backfill state: backfill_finished").into());
                };
                let Some(ScalarImpl::Int64(row_count)) = state[field_indices.row_count] else {
                    return Err(anyhow!("invalid backfill state: row_count").into());
                };
                let parse_offset = |idx: Option<usize>,
                                    field_name: &str|
                 -> StreamExecutorResult<Option<CdcOffset>> {
                    let Some(idx) = idx else {
                        return Ok(None);
                    };

                    match state[idx] {
                        Some(ScalarImpl::Jsonb(ref jsonb)) => {
                            Ok(serde_json::from_value(jsonb.clone().take()).unwrap())
                        }
                        None => Ok(None),
                        _ => Err(anyhow!("invalid backfill state: {field_name}").into()),
                    }
                };
                let cdc_offset_low = parse_offset(field_indices.cdc_offset_low, "cdc_offset_low")?;
                let cdc_offset_high =
                    parse_offset(field_indices.cdc_offset_high, "cdc_offset_high")?;

                Ok(CdcStateRecord {
                    current_pk_pos,
                    is_finished,
                    row_count,
                    cdc_offset_low,
                    cdc_offset_high,
                })
            }
            None => Ok(CdcStateRecord::default()),
        }
    }

    /// Modify the state of the corresponding split
    pub async fn mutate_state(
        &mut self,
        split_id: i64,
        current_pk_pos: Option<OwnedRow>,
        is_finished: bool,
        row_count: u64,
        cdc_offset_low: Option<CdcOffset>,
        cdc_offset_high: Option<CdcOffset>,
    ) -> StreamExecutorResult<()> {
        // schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset_low` | `cdc_offset_high` | `pk...` |
        let mut state = vec![None; self.layout.state_len()];
        let split_id = Some(ScalarImpl::from(split_id));
        state[0].clone_from(&split_id);
        let field_indices = self.layout.field_indices();

        if let (Some(snapshot_cursor), Some(current_pk_pos)) =
            (field_indices.snapshot_cursor, current_pk_pos)
        {
            state[snapshot_cursor].clone_from_slice(current_pk_pos.as_inner());
        }

        state[field_indices.finished] = Some(is_finished.into());
        state[field_indices.row_count] = Some((row_count as i64).into());

        if let (Some(low_idx), Some(high_idx)) =
            (field_indices.cdc_offset_low, field_indices.cdc_offset_high)
        {
            state[low_idx] = cdc_offset_low.map(|cdc_offset| {
                let json = serde_json::to_value(cdc_offset)
                    .expect("CDC low offset must be serializable as JSON");
                ScalarImpl::Jsonb(JsonbVal::from(json))
            });

            state[high_idx] = cdc_offset_high.map(|cdc_offset| {
                let json = serde_json::to_value(cdc_offset)
                    .expect("CDC high offset must be serializable as JSON");
                ScalarImpl::Jsonb(JsonbVal::from(json))
            });
        }

        match self.state_table.get_row(row::once(split_id)).await? {
            Some(prev_row) => {
                self.state_table.update(prev_row, state.as_slice());
            }
            None => {
                self.state_table.insert(state.as_slice());
            }
        }
        Ok(())
    }

    pub async fn init_state_if_absent(&mut self, split_id: i64) -> StreamExecutorResult<()> {
        let key = Some(ScalarImpl::from(split_id));

        if self
            .state_table
            .get_row(row::once(key.clone()))
            .await?
            .is_none()
        {
            let state_len = self.layout.state_len();
            let mut state = vec![None; state_len];

            state[0] = key;
            let field_indices = self.layout.field_indices();
            state[field_indices.finished] = Some(false.into());
            state[field_indices.row_count] = Some(0_i64.into());

            self.state_table.insert(state.as_slice());
        }

        Ok(())
    }

    /// Persist the state to storage
    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table
            .commit_assert_no_update_vnode_bitmap(new_epoch)
            .await
    }

    pub fn is_legacy_state(&self) -> bool {
        matches!(self.layout, ParallelizedCdcBackfillStateLayout::Legacy)
    }
}
