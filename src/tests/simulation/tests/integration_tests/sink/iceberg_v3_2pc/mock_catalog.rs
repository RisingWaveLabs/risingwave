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

//! Mock `iceberg::Catalog` for V3 2PC fault injection tests.
//!
//! Mock only the two methods V3 coordinator actually calls (`load_table`,
//! `update_table`); all other trait methods panic.
//!
//! Tracks data files at the file_path level (not row level) — PK correctness
//! is V3 writer's responsibility, not within this spec's scope.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{Snapshot, TableMetadata, TableMetadataRef};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind as IcebergErrorKind, Namespace, NamespaceIdent,
    Result as IcebergResult, Runtime, TableCommit, TableCreation, TableIdent, TableUpdate,
};
use rand::Rng;
use tokio::time::sleep;

static ICEBERG_RUNTIME: LazyLock<rw_tokio::runtime::Runtime> = LazyLock::new(|| {
    rw_tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("failed to build the Iceberg test runtime")
});

/// Snapshot data tracked by the mock (file-level only).
#[derive(Debug, Default, Clone)]
pub struct MockSnapshotData {
    pub data_files_added: Vec<String>,       // file_path identifiers
    pub data_files_overwritten: Vec<String>, // file_path identifiers
}

/// Event characters tracked in `err_events`:
/// - 'I' = iceberg update_table succeeded
/// - 'i' = iceberg update_table failed (F9 or F11 injection)
/// - 'K' = kill triggered (test code calls push_kill_event)
/// - 'r' = retry due to duplicate snapshot_id (mock detected V3 re-attempted same snapshot)
///
/// (Note: 'P'/'C'/'p'/'c' are conceptual; they would be tracked in the real
/// coordinator path. Since mock only sees iceberg-level events, we track
/// only 'I'/'i'/'K'/'r' here.)
pub struct MockIcebergV3CatalogInner {
    /// Committed snapshots, keyed by snapshot_id.
    pub snapshots: BTreeMap<i64, MockSnapshotData>,
    /// Current table metadata — replaced after each successful update_table.
    pub current_metadata: TableMetadata,
    /// Table identifier this mock answers for.
    pub table_ident: TableIdent,

    /// Error injection probabilities, scaled to [0, u32::MAX].
    pub err_rate_catalog_load: u32,
    pub err_rate_txn_commit: u32,

    /// Event sequence (see character legend above).
    pub err_events: Vec<char>,

    /// Snapshot IDs ever committed (for duplicate detection).
    pub seen_snapshot_ids: HashSet<i64>,

    /// All file_path strings ever added (for cross-snapshot duplicate detection).
    pub all_added_file_paths: HashSet<String>,

    /// Index in `err_events` of the most recent 'K' event (for after-kill queries).
    pub last_kill_event_idx: Option<usize>,
}

/// Mock iceberg Catalog. Shared via `Arc<Mutex<_>>` for concurrent access from
/// V3 coordinator and test code.
#[derive(Clone)]
pub struct MockIcebergV3Catalog {
    inner: Arc<Mutex<MockIcebergV3CatalogInner>>,
}

impl fmt::Debug for MockIcebergV3Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockIcebergV3Catalog")
            .field("snapshot_count", &self.inner().snapshots.len())
            .finish()
    }
}

impl MockIcebergV3Catalog {
    pub fn new(table_ident: TableIdent, initial_metadata: TableMetadata) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockIcebergV3CatalogInner {
                snapshots: BTreeMap::new(),
                current_metadata: initial_metadata,
                table_ident,
                err_rate_catalog_load: 0,
                err_rate_txn_commit: 0,
                err_events: Vec::with_capacity(8192),
                seen_snapshot_ids: HashSet::new(),
                all_added_file_paths: HashSet::new(),
                last_kill_event_idx: None,
            })),
        }
    }

    fn inner(&self) -> MutexGuard<'_, MockIcebergV3CatalogInner> {
        self.inner.lock().unwrap()
    }

    /// Set catalog load failure rate in [0.0, 1.0].
    pub fn set_err_rate_catalog_load(&self, rate: f64) {
        self.inner().err_rate_catalog_load = (u32::MAX as f64 * rate) as u32;
    }

    /// Set iceberg commit conflict rate in [0.0, 1.0].
    pub fn set_err_rate_txn_commit(&self, rate: f64) {
        self.inner().err_rate_txn_commit = (u32::MAX as f64 * rate) as u32;
    }

    /// Test-side hook: record a 'K' event before killing a node.
    pub fn push_kill_event(&self) {
        let mut inner = self.inner();
        let idx = inner.err_events.len();
        inner.err_events.push('K');
        inner.last_kill_event_idx = Some(idx);
    }

    // -------- Assertion / wait helpers --------

    /// Total snapshots successfully committed.
    pub fn committed_snapshot_count(&self) -> usize {
        self.inner().snapshots.len()
    }

    /// Sum of `data_files_added` across all snapshots.
    pub fn total_data_files_added(&self) -> usize {
        self.inner()
            .snapshots
            .values()
            .map(|d| d.data_files_added.len())
            .sum()
    }

    /// Sum of `data_files_overwritten` across all snapshots.
    pub fn total_data_files_overwritten(&self) -> usize {
        self.inner()
            .snapshots
            .values()
            .map(|d| d.data_files_overwritten.len())
            .sum()
    }

    /// `added - overwritten`, the current effective file count.
    pub fn effective_data_file_count(&self) -> usize {
        self.total_data_files_added()
            .saturating_sub(self.total_data_files_overwritten())
    }

    /// Returns true iff every `file_path` ever added is unique across the
    /// history. Violation indicates a spurious double-commit (V3 bug).
    pub fn check_no_duplicate_file_path_in_history(&self) -> Result<()> {
        let inner = self.inner();
        let mut counts: HashMap<&String, usize> = HashMap::new();
        for data in inner.snapshots.values() {
            for path in &data.data_files_added {
                *counts.entry(path).or_default() += 1;
            }
        }
        for (path, count) in counts {
            if count > 1 {
                return Err(anyhow::anyhow!(
                    "duplicate file_path {} appeared in {} snapshots",
                    path,
                    count
                ));
            }
        }
        Ok(())
    }

    /// Count of 'I' events strictly after the most recent 'K' event.
    /// If no 'K' has been recorded, returns the total 'I' count.
    pub fn count_iceberg_writes_after_kill(&self) -> usize {
        let inner = self.inner();
        let start = inner.last_kill_event_idx.map(|i| i + 1).unwrap_or(0);
        inner.err_events[start..]
            .iter()
            .filter(|&&e| e == 'I')
            .count()
    }

    pub fn count_events(&self, c: char) -> usize {
        self.inner().err_events.iter().filter(|&&e| e == c).count()
    }

    /// Wait (with 50ms polling) until at least one event of char `c` is recorded.
    pub async fn wait_for_event(&self, c: char) -> Result<()> {
        loop {
            if self.inner().err_events.contains(&c) {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait (with 50ms polling) until the recorded count of event `c` reaches
    /// at least `target`. Use this when callers need to observe an *increment*
    /// past a baseline (e.g. one more `'I'` than was already there).
    pub async fn wait_for_event_count(&self, c: char, target: usize) -> Result<()> {
        loop {
            if self.count_events(c) >= target {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait (with 50ms polling) until at least one `'I'` event is recorded
    /// strictly after the most recent `'K'` (kill) marker.
    pub async fn wait_for_iceberg_write_after_kill(&self) -> Result<()> {
        loop {
            if self.count_iceberg_writes_after_kill() > 0 {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until effective file count is at least `min_files` and stable for
    /// 5 consecutive 1-second polls; error if it ever exceeds `max_files`.
    pub async fn wait_for_data_files_stable(
        &self,
        min_files: usize,
        max_files: usize,
    ) -> Result<()> {
        let mut consecutive_matches = 0;
        let mut last = 0usize;
        loop {
            sleep(Duration::from_secs(1)).await;
            let curr = self.effective_data_file_count();
            if curr > max_files {
                return Err(anyhow::anyhow!(
                    "effective file count {} exceeded max {}",
                    curr,
                    max_files
                ));
            }
            if curr >= min_files && curr == last {
                consecutive_matches += 1;
                if consecutive_matches >= 5 {
                    return Ok(());
                }
            } else {
                consecutive_matches = 0;
            }
            last = curr;
        }
    }

    pub fn has_consecutive_event_pattern(&self, pattern: &str) -> bool {
        let trace = self.get_event_trace();
        trace.contains(pattern)
    }

    pub fn has_event_pattern_after_kill(&self, pattern: &str) -> bool {
        let inner = self.inner();
        let start = inner.last_kill_event_idx.map(|i| i + 1).unwrap_or(0);
        let trace: String = inner.err_events[start..].iter().collect();
        trace.contains(pattern)
    }

    pub fn get_event_trace(&self) -> String {
        self.inner().err_events.iter().collect()
    }

    // -------- internal helpers --------

    fn build_table_with_metadata(&self, metadata: TableMetadata) -> IcebergResult<Table> {
        let file_io = FileIO::new_with_memory();
        let ident = self.inner().table_ident.clone();
        // This mock uses `Table` only as a metadata container, but `TableBuilder` requires an
        // Iceberg runtime. Simulation runs on madsim-tokio while Iceberg uses crates.io Tokio,
        // so `Runtime::try_current()` cannot see the simulator runtime. This real Tokio handle
        // is a test-only shim; do not use it to spawn work because the current-thread runtime is
        // not driven by the test.
        Table::builder()
            .metadata(TableMetadataRef::from(metadata))
            .identifier(ident)
            .file_io(file_io)
            .runtime(Runtime::new(&ICEBERG_RUNTIME))
            .build()
            .map_err(to_unexpected)
    }
}

fn to_unexpected(e: impl std::fmt::Display) -> IcebergError {
    IcebergError::new(IcebergErrorKind::Unexpected, e.to_string())
}

#[async_trait]
impl Catalog for MockIcebergV3Catalog {
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        // F9: probability-based catalog load failure
        let rate = self.inner().err_rate_catalog_load;
        if rate > 0 && rand::thread_rng().gen_ratio(rate, u32::MAX) {
            self.inner().err_events.push('i');
            return Err(IcebergError::new(
                IcebergErrorKind::Unexpected,
                "mock F9: catalog load failed",
            ));
        }

        let current_metadata = {
            let inner = self.inner();
            if &inner.table_ident != table {
                return Err(IcebergError::new(
                    IcebergErrorKind::TableNotFound,
                    format!("mock catalog does not know table {}", table),
                ));
            }
            inner.current_metadata.clone()
        };
        self.build_table_with_metadata(current_metadata)
    }

    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        // F11: probability-based iceberg txn commit conflict
        let rate = self.inner().err_rate_txn_commit;
        if rate > 0 && rand::thread_rng().gen_ratio(rate, u32::MAX) {
            self.inner().err_events.push('i');
            return Err(IcebergError::new(
                IcebergErrorKind::Unexpected,
                "mock F11: concurrent commit conflict",
            ));
        }

        // Parse the updates: collect the new snapshot's id + the data files
        // (added vs overwritten). V3 packs everything in one AddSnapshot update.
        let updates = commit.take_updates();
        let mut new_snapshot_id: Option<i64> = None;
        let mut new_snapshot: Option<Snapshot> = None;
        let mut data_files_added: Vec<String> = Vec::new();
        let mut data_files_overwritten: Vec<String> = Vec::new();

        for update in &updates {
            if let TableUpdate::AddSnapshot { snapshot } = update {
                new_snapshot_id = Some(snapshot.snapshot_id());
                new_snapshot = Some(snapshot.clone());
                // Note: the snapshot's summary may have "added-data-files",
                // "deleted-data-files" properties; we use those to count files.
                // Actual file_path strings come from the manifest_list, which
                // we don't read in mock. Instead we use the snapshot summary
                // properties or, failing that, assume one file per snapshot.
                let summary = snapshot.summary();
                let added_count: usize = summary
                    .additional_properties
                    .get("added-data-files")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let removed_count: usize = summary
                    .additional_properties
                    .get("deleted-data-files")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                // Use snapshot_id + per-file index as synthetic file_path.
                // This is a mock convention; sufficient for duplicate detection.
                for i in 0..added_count {
                    data_files_added.push(format!(
                        "mock://snap-{}-add-{}",
                        snapshot.snapshot_id(),
                        i
                    ));
                }
                for i in 0..removed_count {
                    data_files_overwritten.push(format!(
                        "mock://snap-{}-rm-{}",
                        snapshot.snapshot_id(),
                        i
                    ));
                }
            }
        }

        let snapshot_id = new_snapshot_id.ok_or_else(|| {
            IcebergError::new(
                IcebergErrorKind::Unexpected,
                "mock: TableCommit contained no AddSnapshot update",
            )
        })?;

        let mut inner = self.inner();

        // Duplicate snapshot_id detection — V3 should have skipped via idempotency check.
        if !inner.seen_snapshot_ids.insert(snapshot_id) {
            // V3 hit retry path. Record 'r' and return current table unchanged.
            inner.err_events.push('r');
            let current = inner.current_metadata.clone();
            drop(inner);
            return self.build_table_with_metadata(current);
        }

        // Cross-snapshot file_path uniqueness — track but don't enforce here
        // (the check_no_duplicate_file_path_in_history method does this).
        for path in &data_files_added {
            inner.all_added_file_paths.insert(path.clone());
        }

        // Append the new snapshot to current_metadata using the builder.
        let new_metadata = inner
            .current_metadata
            .clone()
            .into_builder(None)
            .add_snapshot(new_snapshot.unwrap())
            .map_err(to_unexpected)?
            .build()
            .map_err(to_unexpected)?
            .metadata;

        inner.current_metadata = new_metadata.clone();
        inner.snapshots.insert(
            snapshot_id,
            MockSnapshotData {
                data_files_added,
                data_files_overwritten,
            },
        );
        inner.err_events.push('I');

        drop(inner);

        self.build_table_with_metadata(new_metadata)
    }

    // ---- unimplemented stubs: panic if V3 paths unexpectedly call these ----

    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        unimplemented!("mock V3 catalog: list_namespaces should not be called by V3")
    }

    async fn create_namespace(
        &self,
        _ns: &NamespaceIdent,
        _props: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        unimplemented!("mock V3 catalog: create_namespace should not be called by V3")
    }

    async fn get_namespace(&self, _ns: &NamespaceIdent) -> IcebergResult<Namespace> {
        unimplemented!("mock V3 catalog: get_namespace should not be called by V3")
    }

    async fn namespace_exists(&self, _ns: &NamespaceIdent) -> IcebergResult<bool> {
        unimplemented!("mock V3 catalog: namespace_exists should not be called by V3")
    }

    async fn update_namespace(
        &self,
        _ns: &NamespaceIdent,
        _props: HashMap<String, String>,
    ) -> IcebergResult<()> {
        unimplemented!("mock V3 catalog: update_namespace should not be called by V3")
    }

    async fn drop_namespace(&self, _ns: &NamespaceIdent) -> IcebergResult<()> {
        unimplemented!("mock V3 catalog: drop_namespace should not be called by V3")
    }

    async fn list_tables(&self, _ns: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        unimplemented!("mock V3 catalog: list_tables should not be called by V3")
    }

    async fn create_table(&self, _ns: &NamespaceIdent, _c: TableCreation) -> IcebergResult<Table> {
        unimplemented!("mock V3 catalog: create_table should not be called by V3")
    }

    async fn drop_table(&self, _t: &TableIdent) -> IcebergResult<()> {
        unimplemented!("mock V3 catalog: drop_table should not be called by V3")
    }

    async fn purge_table(&self, _t: &TableIdent) -> IcebergResult<()> {
        unimplemented!("mock V3 catalog: purge_table should not be called by V3")
    }

    async fn table_exists(&self, _t: &TableIdent) -> IcebergResult<bool> {
        unimplemented!("mock V3 catalog: table_exists should not be called by V3")
    }

    async fn rename_table(&self, _s: &TableIdent, _d: &TableIdent) -> IcebergResult<()> {
        unimplemented!("mock V3 catalog: rename_table should not be called by V3")
    }

    async fn register_table(&self, _t: &TableIdent, _loc: String) -> IcebergResult<Table> {
        unimplemented!("mock V3 catalog: register_table should not be called by V3")
    }
}
