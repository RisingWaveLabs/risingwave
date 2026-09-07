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

use std::collections::{HashMap, VecDeque};
use std::ops::RangeBounds;

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::{PbEpochNewChangeLog, PbSstableInfo, PbTableChangeLog};
use tracing::warn;

use crate::HummockObjectId;
use crate::sstable_info::SstableInfo;
use crate::version::ObjectIdReader;

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeLogCommon<T>(
    // older log at the front
    VecDeque<EpochNewChangeLogCommon<T>>,
);

impl<T> TableChangeLogCommon<T> {
    pub fn new(logs: impl IntoIterator<Item = EpochNewChangeLogCommon<T>>) -> Self {
        let logs = logs.into_iter().collect::<VecDeque<_>>();
        debug_assert!(logs.iter().flat_map(|log| log.epochs()).is_sorted());
        Self(logs)
    }

    pub fn iter(&self) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut EpochNewChangeLogCommon<T>> {
        self.0.iter_mut()
    }

    pub fn first(&self) -> Option<&EpochNewChangeLogCommon<T>> {
        self.0.front()
    }

    pub fn last(&self) -> Option<&EpochNewChangeLogCommon<T>> {
        self.0.back()
    }

    pub fn get(&self, index: usize) -> Option<&EpochNewChangeLogCommon<T>> {
        self.0.get(index)
    }

    pub fn range(
        &self,
        range: impl RangeBounds<usize>,
    ) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> + '_ {
        self.0.range(range)
    }

    pub fn add_change_log(&mut self, new_change_log: EpochNewChangeLogCommon<T>) {
        if let Some(prev_log) = self.0.back() {
            assert!(prev_log.checkpoint_epoch < new_change_log.first_epoch());
        }
        self.0.push_back(new_change_log);
    }

    pub fn epochs(&self) -> impl Iterator<Item = u64> + '_ {
        self.0
            .iter()
            .flat_map(|epoch_change_log| epoch_change_log.epochs())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn binary_search_by_checkpoint_epoch(&self, epoch: u64) -> Result<usize, usize> {
        self.0
            .binary_search_by_key(&epoch, |log| log.checkpoint_epoch)
    }
}

impl<T> IntoIterator for TableChangeLogCommon<T> {
    type Item = EpochNewChangeLogCommon<T>;

    type IntoIter = impl Iterator<Item = EpochNewChangeLogCommon<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub type TableChangeLog = TableChangeLogCommon<SstableInfo>;
pub type TableChangeLogs = HashMap<TableId, TableChangeLog>;

impl TableChangeLog {
    pub fn get_object_ids(&self) -> impl Iterator<Item = HummockObjectId> + '_ {
        self.0.iter().flat_map(|c| {
            c.old_value
                .iter()
                .chain(c.new_value.iter())
                .map(|t| HummockObjectId::Sstable(t.object_id()))
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EpochNewChangeLogCommon<T> {
    pub new_value: Vec<T>,
    pub old_value: Vec<T>,
    // epochs are sorted in ascending order
    pub non_checkpoint_epochs: Vec<u64>,
    pub checkpoint_epoch: u64,
}

impl EpochNewChangeLog {
    pub fn change_log_ssts(&self) -> impl Iterator<Item = &SstableInfo> + '_ {
        self.new_value.iter().chain(self.old_value.iter())
    }
}

pub(crate) fn resolve_pb_log_epochs(epochs: &Vec<u64>) -> (Vec<u64>, u64) {
    (
        Vec::from(&epochs[0..(epochs.len() - 1)]),
        *epochs.last().expect("non-empty"),
    )
}

impl<T> EpochNewChangeLogCommon<T> {
    pub fn epochs(&self) -> impl Iterator<Item = u64> + '_ {
        self.non_checkpoint_epochs
            .iter()
            .cloned()
            .chain([self.checkpoint_epoch])
    }

    pub fn first_epoch(&self) -> u64 {
        self.non_checkpoint_epochs
            .first()
            .cloned()
            .unwrap_or(self.checkpoint_epoch)
    }
}

pub type EpochNewChangeLog = EpochNewChangeLogCommon<SstableInfo>;

impl<T> From<&EpochNewChangeLogCommon<T>> for PbEpochNewChangeLog
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(val: &EpochNewChangeLogCommon<T>) -> Self {
        Self {
            new_value: val.new_value.iter().map(|a| a.into()).collect(),
            old_value: val.old_value.iter().map(|a| a.into()).collect(),
            epochs: val.epochs().collect(),
        }
    }
}

impl<T> From<&PbEpochNewChangeLog> for EpochNewChangeLogCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(value: &PbEpochNewChangeLog) -> Self {
        let (non_checkpoint_epochs, checkpoint_epoch) = resolve_pb_log_epochs(&value.epochs);
        Self {
            new_value: value.new_value.iter().map(|a| a.into()).collect(),
            old_value: value.old_value.iter().map(|a| a.into()).collect(),
            non_checkpoint_epochs,
            checkpoint_epoch,
        }
    }
}

impl<T> From<EpochNewChangeLogCommon<T>> for PbEpochNewChangeLog
where
    PbSstableInfo: From<T>,
{
    fn from(val: EpochNewChangeLogCommon<T>) -> Self {
        Self {
            epochs: val.epochs().collect(),
            new_value: val.new_value.into_iter().map(|a| a.into()).collect(),
            old_value: val.old_value.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl<T> From<PbEpochNewChangeLog> for EpochNewChangeLogCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(value: PbEpochNewChangeLog) -> Self {
        let (non_checkpoint_epochs, checkpoint_epoch) = resolve_pb_log_epochs(&value.epochs);
        Self {
            new_value: value.new_value.into_iter().map(|a| a.into()).collect(),
            old_value: value.old_value.into_iter().map(|a| a.into()).collect(),
            non_checkpoint_epochs,
            checkpoint_epoch,
        }
    }
}

impl<T> TableChangeLogCommon<T> {
    pub fn filter_epoch(
        &self,
        (min_epoch, max_epoch): (u64, u64),
    ) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> + '_ {
        assert!(
            min_epoch <= max_epoch,
            "invalid epoch range: {min_epoch}..={max_epoch}"
        );
        let start = self
            .0
            .partition_point(|epoch_change_log| epoch_change_log.checkpoint_epoch < min_epoch);
        let end = self
            .0
            .partition_point(|epoch_change_log| epoch_change_log.first_epoch() <= max_epoch);
        self.0.range(start..end)
    }

    /// Get the `next_epoch` of the given `epoch`
    /// Return:
    ///     - Ok(Some(`next_epoch`)): the `next_epoch` of `epoch`
    ///     - Ok(None): `next_epoch` of `epoch` is not added to change log yet
    ///     - Err(()): `epoch` is not an existing or to exist one
    #[expect(clippy::result_unit_err)]
    pub fn next_epoch(&self, epoch: u64) -> Result<Option<u64>, ()> {
        let start = self
            .0
            .partition_point(|epoch_change_log| epoch_change_log.checkpoint_epoch < epoch);
        debug_assert!(
            self.0
                .range(start..)
                .flat_map(|epoch_change_log| epoch_change_log.epochs())
                .is_sorted()
        );
        let mut later_epochs = self
            .0
            .range(start..)
            .flat_map(|epoch_change_log| epoch_change_log.epochs())
            .skip_while(|log_epoch| *log_epoch < epoch);
        if let Some(first_epoch) = later_epochs.next() {
            assert!(
                first_epoch >= epoch,
                "first_epoch {} < epoch {}",
                first_epoch,
                epoch
            );
            if first_epoch != epoch {
                return Err(());
            }
            if let Some(next_epoch) = later_epochs.next() {
                assert!(
                    next_epoch > epoch,
                    "next_epoch {} not exceed epoch {}",
                    next_epoch,
                    epoch
                );
                Ok(Some(next_epoch))
            } else {
                // `epoch` is latest
                Ok(None)
            }
        } else {
            // all epochs are less than `epoch`
            Ok(None)
        }
    }

    pub fn truncate(&mut self, truncate_epoch: u64) {
        while let Some(change_log) = self.0.front()
            && change_log.checkpoint_epoch < truncate_epoch
        {
            let _change_log = self.0.pop_front().expect("non-empty");
        }
        if let Some(first_log) = self.0.front_mut() {
            first_log
                .non_checkpoint_epochs
                .retain(|epoch| *epoch >= truncate_epoch);
        }
    }
}

impl<T> TableChangeLogCommon<T>
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    pub fn to_protobuf(&self) -> PbTableChangeLog {
        PbTableChangeLog {
            change_logs: self.0.iter().map(|a| a.into()).collect(),
        }
    }
}

impl<T> TableChangeLogCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(val.change_logs.iter().map(|a| a.into()).collect())
    }
}

impl<T> TableChangeLogCommon<T>
where
    T: From<PbSstableInfo>,
{
    pub fn from_protobuf_owned(val: PbTableChangeLog) -> Self {
        Self(val.change_logs.into_iter().map(|a| a.into()).collect())
    }
}

pub fn build_table_change_log_delta<'a>(
    old_value_ssts: impl Iterator<Item = SstableInfo>,
    new_value_ssts: impl Iterator<Item = &'a SstableInfo>,
    epochs: &Vec<u64>,
    log_store_table_ids: impl Iterator<Item = TableId>,
) -> HashMap<TableId, EpochNewChangeLog> {
    let mut table_change_log: HashMap<_, _> = log_store_table_ids
        .map(|table_id| {
            let (non_checkpoint_epochs, checkpoint_epoch) = resolve_pb_log_epochs(epochs);
            (
                table_id,
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs,
                    checkpoint_epoch,
                },
            )
        })
        .collect();
    for sst in old_value_ssts {
        for table_id in &sst.table_ids {
            match table_change_log.get_mut(table_id) {
                Some(log) => {
                    log.old_value.push(sst.clone());
                }
                None => {
                    warn!(%table_id, ?sst, "old value sst contains non-log-store table");
                }
            }
        }
    }
    for sst in new_value_ssts {
        for table_id in &sst.table_ids {
            if let Some(log) = table_change_log.get_mut(table_id) {
                log.new_value.push(sst.clone());
            }
        }
    }
    table_change_log
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::change_log::{EpochNewChangeLog, TableChangeLogCommon};
    use crate::sstable_info::SstableInfo;

    #[test]
    fn test_filter_epoch() {
        let table_change_log = TableChangeLogCommon::<SstableInfo>::new([
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![],
                checkpoint_epoch: 2,
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![3],
                checkpoint_epoch: 4,
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![],
                checkpoint_epoch: 6,
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![8],
                checkpoint_epoch: 10,
            },
        ]);

        let epochs = (1..=11).collect_vec();
        for i in 0..epochs.len() {
            for j in i..epochs.len() {
                let min_epoch = epochs[i];
                let max_epoch = epochs[j];
                let expected = table_change_log
                    .0
                    .iter()
                    .filter(|log| {
                        min_epoch <= log.checkpoint_epoch && log.first_epoch() <= max_epoch
                    })
                    .cloned()
                    .collect_vec();
                let actual = table_change_log
                    .filter_epoch((min_epoch, max_epoch))
                    .cloned()
                    .collect_vec();
                assert_eq!(expected, actual, "{:?}", (min_epoch, max_epoch));
            }
        }

        let existing_epochs = table_change_log.epochs().collect_vec();
        assert!(existing_epochs.is_sorted());
        for &epoch in &epochs {
            let expected = match existing_epochs
                .iter()
                .position(|existing_epoch| *existing_epoch >= epoch)
            {
                None => {
                    // all existing epochs are less than epoch
                    Ok(None)
                }
                Some(i) => {
                    let this_epoch = existing_epochs[i];
                    assert!(this_epoch >= epoch);
                    if this_epoch == epoch {
                        if i + 1 == existing_epochs.len() {
                            // epoch is the latest epoch
                            Ok(None)
                        } else {
                            Ok(Some(existing_epochs[i + 1]))
                        }
                    } else {
                        // epoch not a existing epoch
                        Err(())
                    }
                }
            };
            assert_eq!(expected, table_change_log.next_epoch(epoch));
        }
    }

    #[test]
    fn test_truncate() {
        let mut table_change_log = TableChangeLogCommon::<SstableInfo>::new([
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![],
                checkpoint_epoch: 1,
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![],
                checkpoint_epoch: 2,
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![3],
                checkpoint_epoch: 4,
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                non_checkpoint_epochs: vec![],
                checkpoint_epoch: 5,
            },
        ]);
        let origin_table_change_log = table_change_log.clone();
        for truncate_epoch in 0..6 {
            table_change_log.truncate(truncate_epoch);
            let expected_table_change_log = TableChangeLogCommon(
                origin_table_change_log
                    .0
                    .iter()
                    .filter_map(|epoch_change_log| {
                        let mut epoch_change_log = epoch_change_log.clone();
                        epoch_change_log
                            .non_checkpoint_epochs
                            .retain(|epoch| *epoch >= truncate_epoch);
                        if epoch_change_log.non_checkpoint_epochs.is_empty()
                            && epoch_change_log.checkpoint_epoch < truncate_epoch
                        {
                            None
                        } else {
                            Some(epoch_change_log)
                        }
                    })
                    .collect(),
            );
            assert_eq!(expected_table_change_log, table_change_log);
        }
    }
}
