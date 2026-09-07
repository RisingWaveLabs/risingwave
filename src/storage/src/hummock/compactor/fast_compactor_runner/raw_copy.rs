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

use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::key::FullKey;

use crate::hummock::{BlockMeta, TableHolder};

/// V2-only planner for the current fast raw-copy path.
///
/// Fast compaction copies physical block bytes and per-block filter bytes. Keep
/// those physical layout dependencies behind this planner instead of exposing
/// them through the normal logical metadata accessor.
pub(super) struct RawCopyPlanner<'a> {
    sstable: &'a TableHolder,
}

impl<'a> RawCopyPlanner<'a> {
    pub(super) fn for_current_v2(sstable: &'a TableHolder) -> Self {
        Self { sstable }
    }

    pub(super) fn block_count(&self) -> usize {
        self.sstable.meta.block_metas.len()
    }

    pub(super) fn block_metas_from(&self, block_idx: usize) -> &'a [BlockMeta] {
        &self.sstable.meta.block_metas[block_idx..]
    }

    pub(super) fn raw_block_meta(&self, block_idx: usize) -> BlockMeta {
        self.sstable.meta.block_metas[block_idx].clone()
    }

    pub(super) fn raw_block_filter(&self, block_idx: usize) -> Vec<u8> {
        self.sstable.filter_reader.get_block_raw_filter(block_idx)
    }

    pub(super) fn block_smallest_key(&self, block_idx: usize) -> &'a [u8] {
        self.sstable.meta.block_metas[block_idx]
            .smallest_key
            .as_ref()
    }

    pub(super) fn block_largest_key(&self, block_idx: usize) -> &'a [u8] {
        if block_idx + 1 < self.block_count() {
            self.block_smallest_key(block_idx + 1)
        } else {
            self.sstable_largest_key()
        }
    }

    pub(super) fn raw_block_exclusive_largest_key(&self, next_block_idx: usize) -> Vec<u8> {
        if next_block_idx < self.block_count() {
            let mut largest_key = FullKey::decode(self.block_smallest_key(next_block_idx));
            // Do not include this key because it is the smallest key of next block.
            largest_key.epoch_with_gap = EpochWithGap::new_max_epoch();
            largest_key.encode()
        } else {
            self.sstable.meta.largest_key.clone()
        }
    }

    fn sstable_largest_key(&self) -> &'a [u8] {
        self.sstable.meta.largest_key.as_ref()
    }

    pub(super) fn sstable_largest_key_vec(&self) -> Vec<u8> {
        self.sstable.meta.largest_key.clone()
    }
}
