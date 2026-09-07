// Copyright 2023 RisingWave Labs
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

use anyhow::anyhow;
use risingwave_common::id::TableId;
use risingwave_common::row;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{Datum, JsonbVal, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::cdc::external::CdcOffset;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

#[derive(Debug, Default)]
pub struct CdcStateRecord {
    pub current_pk_pos: Option<OwnedRow>,
    pub is_finished: bool,
    /// The last cdc offset that has been consumed by the cdc backfill executor
    pub last_cdc_offset: Option<CdcOffset>,
    pub row_count: i64,
}

/// state schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
pub struct CdcBackfillState<S: StateStore> {
    /// Id of the backfilling table, will be the key of the state
    split_id: String,
    state_table: StateTable<S>,

    cached_state: Vec<Datum>,
}

impl<S: StateStore> CdcBackfillState<S> {
    pub fn new(table_id: TableId, state_table: StateTable<S>, state_len: usize) -> Self {
        Self {
            split_id: table_id.to_string(),
            state_table,
            cached_state: vec![None; state_len],
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    /// Restore the backfill state from storage
    pub async fn restore_state(&mut self) -> StreamExecutorResult<CdcStateRecord> {
        let key = Some(self.split_id.clone());
        match self
            .state_table
            .get_row(row::once(key.map(ScalarImpl::from)))
            .await?
        {
            Some(row) => {
                tracing::info!("restored cdc backfill state: {:?}", row);
                self.cached_state = row.into_inner().into_vec();
                let state = self.cached_state.as_slice();
                let state_len = state.len();
                // schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
                let row_count = match state[state_len - 2] {
                    Some(ScalarImpl::Int64(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: row_count").into()),
                };
                let is_finished = match state[state_len - 3] {
                    Some(ScalarImpl::Bool(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: backfill_finished").into()),
                };
                let cdc_offset = match state[state_len - 1] {
                    Some(ScalarImpl::Jsonb(ref jsonb)) => {
                        serde_json::from_value(jsonb.clone().take()).unwrap()
                    }
                    None if is_finished => None,
                    None => {
                        return Err(anyhow!(
                            "invalid backfill state: unfinished row has null cdc_offset"
                        )
                        .into());
                    }
                    _ => return Err(anyhow!("invalid backfill state: cdc_offset").into()),
                };

                let current_pk_pos = state[1..state_len - 3].to_vec();
                Ok(CdcStateRecord {
                    current_pk_pos: Some(OwnedRow::new(current_pk_pos)),
                    is_finished,
                    last_cdc_offset: cdc_offset,
                    row_count,
                })
            }
            None => Ok(CdcStateRecord::default()),
        }
    }

    /// Modify the state of the corresponding split (currently only supports single split)
    pub async fn mutate_state(
        &mut self,
        current_pk_pos: Option<OwnedRow>,
        last_cdc_offset: Option<CdcOffset>,
        row_count: u64,
        is_finished: bool,
    ) -> StreamExecutorResult<()> {
        // schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
        let state = self.cached_state.as_mut_slice();
        let split_id = Some(ScalarImpl::from(self.split_id.clone()));
        let state_len = state.len();
        state[0].clone_from(&split_id);
        if let Some(current_pk_pos) = &current_pk_pos {
            state[1..=current_pk_pos.len()].clone_from_slice(current_pk_pos.as_inner());
        }
        state[state_len - 3] = Some(is_finished.into());
        state[state_len - 2] = Some((row_count as i64).into());
        state[state_len - 1] = last_cdc_offset.clone().map(|cdc_offset| {
            let json = serde_json::to_value(cdc_offset).unwrap();
            ScalarImpl::Jsonb(JsonbVal::from(json))
        });

        match self.state_table.get_row(row::once(split_id)).await? {
            Some(prev_row) => {
                self.state_table
                    .update(prev_row, self.cached_state.as_slice());
            }
            None => {
                self.state_table.insert(self.cached_state.as_slice());
            }
        }
        Ok(())
    }

    /// Persist the state to storage
    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table
            .commit_assert_no_update_vnode_bitmap(new_epoch)
            .await
    }
}
