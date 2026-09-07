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

//! Pure, I/O-free bookkeeping for the position-delete merger's in-memory
//! "staging version": the current delete state of each data file in this
//! actor's shard. Seeded once at init and appended to on every flush; the
//! handler owns all iceberg I/O and calls into here for state transitions.

use hashbrown::HashMap;
use iceberg::delete_vector::DeleteVector;
use iceberg::spec::DataFile;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::ScalarRefImpl;

#[derive(Default)]
pub struct StagedEntry {
    current_file: Option<DataFile>,
    delete_vector: Option<DeleteVector>,
}

pub struct WritePlan {
    pub merged: DeleteVector,
    pub overwrite: Option<DataFile>,
}

impl StagedEntry {
    pub fn needs_load(&self) -> bool {
        self.delete_vector.is_none() && self.current_file.is_some()
    }

    pub fn set_loaded_delete_vector(&mut self, dv: DeleteVector) {
        self.delete_vector = Some(dv);
    }

    pub fn plan_write(&self, new_positions: DeleteVector) -> WritePlan {
        let mut merged = new_positions;
        if let Some(existing) = &self.delete_vector {
            merged |= existing;
        }
        WritePlan {
            merged,
            overwrite: self.current_file.clone(),
        }
    }

    pub fn record_written(&mut self, new_file: DataFile, merged: DeleteVector) {
        self.current_file = Some(new_file);
        self.delete_vector = Some(merged);
    }

    pub fn current_file(&self) -> Option<&DataFile> {
        self.current_file.as_ref()
    }
}

pub struct StagingVersion {
    staged: HashMap<String, StagedEntry>,
    vnode_bitmap: Option<Bitmap>,
}

/// Compute the vnode of a data file path the same way the `file_path`-keyed
/// `HashShard` dispatcher does
fn vnode_of(data_file_path: &str, vnode_count: usize) -> VirtualNode {
    let row = risingwave_common::row::once(Some(ScalarRefImpl::Utf8(data_file_path)));
    VirtualNode::compute_row(row, &[0], vnode_count)
}

impl StagingVersion {
    pub fn new(vnode_bitmap: Option<Bitmap>) -> Self {
        Self {
            staged: HashMap::new(),
            vnode_bitmap,
        }
    }

    pub fn owns(&self, data_file_path: &str) -> bool {
        match &self.vnode_bitmap {
            None => true,
            Some(bitmap) => {
                let vnode = vnode_of(data_file_path, bitmap.len());
                bitmap.is_set(vnode.to_index())
            }
        }
    }

    pub fn seed(&mut self, data_file_path: String, existing_delete_file: DataFile) {
        self.staged.insert(
            data_file_path,
            StagedEntry {
                current_file: Some(existing_delete_file),
                delete_vector: None,
            },
        );
    }

    /// Get (creating a default) the entry for `data_file_path`.
    pub fn entry_mut(&mut self, data_file_path: &str) -> &mut StagedEntry {
        self.staged.entry_ref(data_file_path).or_default()
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    use super::*;

    fn dv(positions: &[u64]) -> DeleteVector {
        let mut v = DeleteVector::default();
        for p in positions {
            v.insert(*p);
        }
        v
    }

    fn positions(v: &DeleteVector) -> Vec<u64> {
        v.iter().collect()
    }

    fn delete_file(path: &str, referenced: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Puffin)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .referenced_data_file(Some(referenced.to_owned()))
            .build()
            .expect("delete file build")
    }

    #[test]
    fn owns_without_bitmap_is_true() {
        let s = StagingVersion::new(None);
        assert!(s.owns("data/a.parquet"));
    }

    #[test]
    fn owns_filters_by_bitmap() {
        // Build a bitmap that owns exactly the vnode of "data/a.parquet".
        let vnode_count = 256;
        let idx = super::vnode_of("data/a.parquet", vnode_count).to_index();
        let mut builder = risingwave_common::bitmap::BitmapBuilder::zeroed(vnode_count);
        builder.set(idx, true);
        let s = StagingVersion::new(Some(builder.finish()));
        assert!(s.owns("data/a.parquet"));
        // A path hashing to a different vnode is not owned (find one).
        let other = (0..)
            .map(|i| format!("data/other-{i}.parquet"))
            .find(|p| super::vnode_of(p, vnode_count).to_index() != idx)
            .unwrap();
        assert!(!s.owns(&other));
    }

    #[test]
    fn plan_write_on_fresh_path_has_no_overwrite() {
        let mut s = StagingVersion::new(None);
        let entry = s.entry_mut("data/a.parquet");
        assert!(!entry.needs_load());
        let plan = entry.plan_write(dv(&[0, 3]));
        assert_eq!(positions(&plan.merged), vec![0, 3]);
        assert!(plan.overwrite.is_none());
    }

    #[test]
    fn seed_then_plan_write_merges_and_sets_overwrite() {
        let mut s = StagingVersion::new(None);
        s.seed(
            "data/a.parquet".to_owned(),
            delete_file("dv/old.puff", "data/a.parquet"),
        );
        let entry = s.entry_mut("data/a.parquet");
        assert!(entry.needs_load()); // current_file set, dv not loaded yet
        entry.set_loaded_delete_vector(dv(&[0, 5]));
        let plan = entry.plan_write(dv(&[3, 5]));
        assert_eq!(positions(&plan.merged), vec![0, 3, 5]);
        assert_eq!(plan.overwrite.unwrap().file_path(), "dv/old.puff");
    }

    #[test]
    fn record_written_becomes_next_overwrite_target() {
        let mut s = StagingVersion::new(None);
        let entry = s.entry_mut("data/a.parquet");
        let plan = entry.plan_write(dv(&[0]));
        entry.record_written(delete_file("dv/e2.puff", "data/a.parquet"), plan.merged);
        // Next epoch: existing dv is resident, overwrite target is the file we just wrote.
        let entry = s.entry_mut("data/a.parquet");
        assert!(!entry.needs_load());
        let plan = entry.plan_write(dv(&[2]));
        assert_eq!(positions(&plan.merged), vec![0, 2]);
        assert_eq!(plan.overwrite.unwrap().file_path(), "dv/e2.puff");
    }
}
