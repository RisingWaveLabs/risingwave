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

use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::Arc;

use fail::fail_point;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use crate::hummock::iterator::{
    DirectionEnum, HummockIterator, HummockIteratorDirection, ValueMeta,
};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, IteratorStatsGuard, SstableIteratorType, SstableStoreRef};
use crate::monitor::StoreLocalStatistic;

fn smallest_key(sstable_info: &SstableInfo) -> &[u8] {
    &sstable_info.key_range.left
}

fn largest_key(sstable_info: &SstableInfo) -> &[u8] {
    &sstable_info.key_range.right
}

/// Served as the concrete implementation of `ConcatIterator` and `BackwardConcatIterator`.
pub struct ConcatIteratorInner<TI: SstableIteratorType> {
    /// The iterator of the current table.
    sstable_iter: Option<TI>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping `sstable_infos`.
    sstable_infos: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    read_options: Arc<SstableIteratorReadOptions>,
}

impl<TI: SstableIteratorType> ConcatIteratorInner<TI> {
    /// Caller should make sure that `sstable_infos` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        sstable_infos: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            sstable_infos,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            read_options,
        }
    }

    /// Seeks to a table, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(
        &mut self,
        idx: usize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        if idx >= self.sstable_infos.len() {
            if let Some(old_iter) = self.sstable_iter.take() {
                old_iter.collect_local_statistic(&mut self.stats);
            }
            self.cur_idx = self.sstable_infos.len();
        } else {
            let table = self
                .sstable_store
                .sstable(&self.sstable_infos[idx], &mut self.stats)
                .await?;
            let sstable_iter = TI::create(
                table,
                self.sstable_store.clone(),
                self.read_options.clone(),
                &self.sstable_infos[idx],
            );

            let mut pending = IteratorStatsGuard::new(sstable_iter, &mut self.stats);
            if let Some(key) = seek_key {
                pending.iter_mut().seek(key).await?;
            } else {
                pending.iter_mut().rewind().await?;
            }
            fail_point!("concat_iter_after_seek_before_init_complete", |_| Err(
                crate::hummock::HummockError::meta_error(
                    "test error after concat iterator seek before initialization completes"
                )
            ));

            let sstable_iter = pending.handoff();
            if let Some(old_iter) = self.sstable_iter.take() {
                old_iter.collect_local_statistic(&mut self.stats);
            }
            self.sstable_iter = Some(sstable_iter);
            self.cur_idx = idx;
        }
        Ok(())
    }
}

impl<TI: SstableIteratorType> HummockIterator for ConcatIteratorInner<TI> {
    type Direction = TI::Direction;

    async fn next(&mut self) -> HummockResult<()> {
        let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");
        sstable_iter.next().await?;

        if sstable_iter.is_valid() {
            Ok(())
        } else {
            // seek to next table
            let mut table_idx = self.cur_idx + 1;
            while !self.is_valid() && table_idx < self.sstable_infos.len() {
                self.seek_idx(table_idx, None).await?;
                table_idx += 1;
            }
            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.as_ref().is_some_and(|i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await?;
        let mut table_idx = 1;
        while !self.is_valid() && table_idx < self.sstable_infos.len() {
            // Seek to next table
            self.seek_idx(table_idx, None).await?;
            table_idx += 1;
        }
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        let mut table_idx = self
            .sstable_infos
            .partition_point(|table| match Self::Direction::direction() {
                DirectionEnum::Forward => {
                    let ord = FullKey::decode(smallest_key(table)).cmp(&key);

                    ord == Less || ord == Equal
                }
                DirectionEnum::Backward => {
                    let ord = FullKey::decode(largest_key(table)).cmp(&key);
                    ord == Greater || (ord == Equal && !table.key_range.right_exclusive)
                }
            })
            .saturating_sub(1); // considering the boundary of 0

        self.seek_idx(table_idx, Some(key)).await?;
        table_idx += 1;
        while !self.is_valid() && table_idx < self.sstable_infos.len() {
            // Seek to next table
            self.seek_idx(table_idx, None).await?;
            table_idx += 1;
        }
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats);
        if let Some(iter) = &self.sstable_iter {
            iter.collect_local_statistic(stats);
        }
    }

    fn value_meta(&self) -> ValueMeta {
        self.sstable_iter
            .as_ref()
            .expect("no table iter")
            .value_meta()
    }
}
