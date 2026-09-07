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

use std::sync::LazyLock;

use indexmap::IndexMap;
use indexmap::map::Entry;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk, StreamChunkBuilder};
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::{Row, RowExt as _};
use risingwave_common::types::DataType;

use crate::consistency::consistency_panic;

/// Behavior when inconsistency is detected when aggregating changes to [`ChangeBuffer`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InconsistencyBehavior {
    Panic,
    Warn,
    Tolerate,
}

impl InconsistencyBehavior {
    /// Report an inconsistency.
    #[track_caller]
    pub fn report(self, msg: &str) {
        match self {
            InconsistencyBehavior::Panic => consistency_panic!("{}", msg),
            InconsistencyBehavior::Warn => {
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);

                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::warn!(suppressed_count, "{}", msg);
                }
            }
            InconsistencyBehavior::Tolerate => {}
        }
    }
}

mod private {
    pub trait Key: Eq + std::hash::Hash {}
    impl<K> Key for K where K: Eq + std::hash::Hash {}

    pub trait Row: Eq {}
    impl<R> Row for R where R: Eq {}
}

/// The accumulated change of a key, with each side in an [`Option`] so that state
/// transitions can take one side out in place. `(None, None)` entries are removed eagerly.
#[derive(Debug)]
struct Slot<R> {
    old: Option<R>,
    new: Option<R>,
}

/// A buffer that accumulates changes and produce compacted changes.
#[derive(Debug)]
pub struct ChangeBuffer<K, R> {
    // We use an `IndexMap` to preserve the original order of the changes as much as possible.
    buffer: IndexMap<K, Slot<R>>,
    ib: InconsistencyBehavior,
}

impl<K, R> ChangeBuffer<K, R>
where
    K: private::Key,
    R: private::Row,
{
    /// Apply an insertion of a row with the given key.
    pub fn insert(&mut self, key: K, new_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Slot {
                    old: None,
                    new: Some(new_row),
                });
            }
            Entry::Occupied(mut e) => {
                let slot = e.get_mut();
                if slot.new.is_some() {
                    self.ib.report("inconsistent changes: double-inserting");
                }
                slot.new = Some(new_row);
            }
        }
    }

    /// Apply a deletion of a row with the given key.
    pub fn delete(&mut self, key: K, old_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Slot {
                    old: Some(old_row),
                    new: None,
                });
            }
            Entry::Occupied(mut e) => {
                let slot = e.get_mut();
                if slot.new.take().is_some() {
                    if slot.old.is_none() {
                        // The previous `Insert` is fully cancelled by this deletion.
                        // FIXME: though preserving the order well,
                        // this is not performant compared to `swap_remove`
                        e.shift_remove();
                    }
                } else {
                    self.ib.report("inconsistent changes: double-deleting");
                    slot.old = Some(old_row);
                }
            }
        }
    }

    /// Apply an update of a row with the given key.
    pub fn update(&mut self, key: K, old_row: R, new_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Slot {
                    old: Some(old_row),
                    new: Some(new_row),
                });
            }
            Entry::Occupied(mut e) => {
                let slot = e.get_mut();
                if slot.new.is_some() {
                    slot.new = Some(new_row);
                } else {
                    self.ib.report("inconsistent changes: update after delete");
                    slot.old = Some(old_row);
                    slot.new = Some(new_row);
                }
            }
        }
    }

    /// Apply a change record, with the key extracted by the given function.
    ///
    /// For `Record::Update`, inconsistency is reported if the old key and the new key are different.
    /// Further behavior is determined by the `InconsistencyBehavior`.
    pub fn apply_record(&mut self, record: Record<R>, key_fn: impl Fn(&R) -> K) {
        match record {
            Record::Insert { new_row } => self.insert(key_fn(&new_row), new_row),
            Record::Delete { old_row } => self.delete(key_fn(&old_row), old_row),
            Record::Update { old_row, new_row } => {
                let old_key = key_fn(&old_row);
                let new_key = key_fn(&new_row);

                // As long as `ib` is not `Panic`, we still gracefully handle the mismatched key.
                if old_key != new_key {
                    self.ib
                        .report("inconsistent changes: mismatched key in update");
                    self.delete(old_key, old_row);
                    self.insert(new_key, new_row);
                } else {
                    self.update(old_key, old_row, new_row);
                }
            }
        }
    }

    /// Apply an `Op` of a row with the given key.
    pub fn apply_op_row(&mut self, op: Op, key: K, row: R) {
        match op {
            Op::Insert | Op::UpdateInsert => self.insert(key, row),
            Op::Delete | Op::UpdateDelete => self.delete(key, row),
        }
    }

    /// Consume the buffer and produce a list of change records.
    ///
    /// No-op updates are filtered out.
    pub fn into_records(self) -> impl Iterator<Item = Record<R>> {
        self.buffer
            .into_values()
            .filter_map(|slot| match (slot.old, slot.new) {
                (None, Some(new_row)) => Some(Record::Insert { new_row }),
                (Some(old_row), None) => Some(Record::Delete { old_row }),
                (Some(old_row), Some(new_row)) => {
                    (old_row != new_row).then(|| Record::Update { old_row, new_row })
                }
                (None, None) => unreachable!("empty slot should have been removed"),
            })
    }
}

impl<K, R> Default for ChangeBuffer<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, R> ChangeBuffer<K, R> {
    /// Create a new `ChangeBuffer` that panics on inconsistency.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new `ChangeBuffer` with the given capacity that panics on inconsistency.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: IndexMap::with_capacity(capacity),
            ib: InconsistencyBehavior::Panic,
        }
    }

    /// Set the inconsistency behavior.
    pub fn with_inconsistency_behavior(mut self, ib: InconsistencyBehavior) -> Self {
        self.ib = ib;
        self
    }

    /// Get the number of keys that have pending changes in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// The kind of output for [`ChangeBuffer::into_chunk`] and [`ChangeBuffer::into_chunks`].
/// Can be [`UPSERT`] or [`RETRACT`].
pub type OutputKind = bool;
pub mod output_kind {
    use super::OutputKind;
    /// For updates, only keep the new row with `Insert` operation.
    ///
    /// The output chunk can only be used in streams with `StreamKind::Upsert`. Refer to it for
    /// more details.
    pub const UPSERT: OutputKind = true;
    /// For updates, keep both the old and new row with `UpdateDelete` and `UpdateInsert` operation.
    pub const RETRACT: OutputKind = false;
}
use output_kind::*;

impl<K, R> ChangeBuffer<K, R>
where
    K: private::Key,
    R: private::Row + Row,
{
    /// Consume the buffer and produce a single compacted chunk.
    pub fn into_chunk<const KIND: OutputKind>(
        self,
        data_types: Vec<DataType>,
    ) -> Option<StreamChunk> {
        let mut builder = StreamChunkBuilder::unlimited(data_types, Some(self.buffer.len()));
        for record in self.into_records() {
            let record = match KIND {
                UPSERT => record.into_upsert(),
                RETRACT => record,
            };
            let none = builder.append_record(record);
            debug_assert!(none.is_none());
        }
        builder.take()
    }

    /// Consume the buffer and produce a single compacted chunk, with the given new key indices.
    ///
    /// The key must be a superset of the old key to ensure uniqueness. Otherwise, changes to the
    /// same key might be written multiple times so that the correct ordering of the operations cannot
    /// be guaranteed.
    pub fn into_chunk_with_key(
        self,
        data_types: Vec<DataType>,
        key_indices: &[usize],
    ) -> Option<StreamChunk> {
        let mut builder = StreamChunkBuilder::unlimited(data_types, Some(self.buffer.len()));
        for record in self.into_records() {
            macro_rules! append_record {
                ($record:expr) => {
                    let none = builder.append_record($record);
                    debug_assert!(none.is_none());
                };
            }

            if let Record::Update { old_row, new_row } = &record
                && !Row::eq(&old_row.project(key_indices), new_row.project(key_indices))
            {
                append_record!(Record::Delete { old_row });
                append_record!(Record::Insert { new_row });
            } else {
                append_record!(record);
            }
        }
        builder.take()
    }

    /// Consume the buffer and produce a list of compacted chunks with the given size at most.
    pub fn into_chunks<const KIND: OutputKind>(
        self,
        data_types: Vec<DataType>,
        chunk_size: usize,
    ) -> Vec<StreamChunk> {
        let mut res = Vec::new();
        let mut builder = StreamChunkBuilder::new(chunk_size, data_types);
        for record in self.into_records() {
            let record = match KIND {
                UPSERT => record.into_upsert(),
                RETRACT => record,
            };
            if let Some(chunk) = builder.append_record(record) {
                res.push(chunk);
            }
        }
        res.extend(builder.take());
        res
    }
}
