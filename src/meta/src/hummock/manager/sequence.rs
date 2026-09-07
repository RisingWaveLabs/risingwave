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

use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::sync::LazyLock;

use parking_lot::Mutex as ParkingMutex;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockRawObjectId, HummockSstableId};
use risingwave_meta_model::hummock_sequence;
use risingwave_meta_model::hummock_sequence::{
    COMPACTION_GROUP_ID, COMPACTION_TASK_ID, META_BACKUP_ID, SSTABLE_OBJECT_ID,
};
use risingwave_meta_model::prelude::HummockSequence;
use risingwave_pb::id::TypedId;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, TransactionTrait};
use tokio::sync::Mutex;

use crate::hummock::error::Result;
use crate::manager::MetaSrvEnv;

static SEQ_INIT: LazyLock<HashMap<String, i64>> = LazyLock::new(|| {
    maplit::hashmap! {
        COMPACTION_TASK_ID.into() => 1,
        COMPACTION_GROUP_ID.into() => StaticCompactionGroupId::End.as_i64_id() + 1,
        SSTABLE_OBJECT_ID.into() => 1,
        META_BACKUP_ID.into() => 1,
    }
});

/// A half-open range `[next, end)` of prefetched sequence values.
///
/// Invariant: `next <= end` always holds after construction / `reset`.
/// `pop` advances `next` towards `end`; once `next == end` the range is empty.
#[derive(Default, Debug, PartialEq, Eq)]
struct PrefetchedRange {
    next: u64,
    end: u64,
}

impl PrefetchedRange {
    fn is_empty(&self) -> bool {
        self.next >= self.end
    }

    fn pop(&mut self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }

        let value = self.next;
        self.next += 1;
        Some(value)
    }

    fn reset(&mut self, start: u64, count: u32) {
        self.next = start;
        self.end = start + u64::from(count);
    }

    #[cfg(test)]
    fn set_bounds(&mut self, next: u64, end: u64) {
        self.next = next;
        self.end = end;
    }

    #[cfg(test)]
    fn bounds(&self) -> (u64, u64) {
        (self.next, self.end)
    }
}

/// Stores a prefetched half-open interval `[next, end)` and serializes refills so concurrent
/// callers share the same refill instead of wasting a newly allocated range.
#[derive(Default, Debug)]
pub(crate) struct PrefetchedSequence {
    range: ParkingMutex<PrefetchedRange>,
    refill_lock: Mutex<()>,
}

impl PrefetchedSequence {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the next unique value from the prefetched range, refilling from the backend
    /// when the range is exhausted.
    ///
    /// - `refill_count`: how many values to request from the backend in one batch when refilling.
    ///   Clamped to at least 1 so that a caller with zero remaining budget still obtains a value
    ///   (the caller may have already committed to needing one id before checking the budget).
    /// - `refill`: an async closure `|count| -> Result<start>` that allocates `count` sequential
    ///   values from the persistent sequence and returns the start of the allocated range.
    ///
    /// Concurrent callers are serialized by `refill_lock` so that only one refill is in-flight
    /// at a time; others wait and then consume from the freshly filled range.
    pub(crate) async fn next<F, Fut>(&self, refill_count: u32, refill: F) -> Result<u64>
    where
        F: FnOnce(u32) -> Fut,
        Fut: Future<Output = Result<u64>>,
    {
        // Clamp to 1: even when the caller's budget is exhausted, it still needs exactly one id.
        let refill_count = refill_count.max(1);
        if let Some(value) = self.try_pop() {
            return Ok(value);
        }

        let _guard = self.refill_lock.lock().await;
        if let Some(value) = self.try_pop() {
            return Ok(value);
        }

        let start = refill(refill_count).await?;
        let mut range = self.range.lock();
        debug_assert!(range.is_empty());
        range.reset(start, refill_count);
        Ok(range
            .pop()
            .expect("a freshly refilled sequence must return one value"))
    }

    fn try_pop(&self) -> Option<u64> {
        self.range.lock().pop()
    }

    #[cfg(test)]
    pub(crate) fn set_test_bounds(&self, next: u64, end: u64) {
        self.range.lock().set_bounds(next, end);
    }

    #[cfg(test)]
    pub(crate) fn test_bounds(&self) -> (u64, u64) {
        self.range.lock().bounds()
    }
}

pub struct SequenceGenerator {
    db: Mutex<DatabaseConnection>,
}

impl SequenceGenerator {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db: Mutex::new(db) }
    }

    /// Returns start, indicates range [start, start + num).
    ///
    /// Despite being a serial function, its infrequent invocation allows for acceptable performance.
    ///
    /// If num is 0, the next seq is returned just like num is 1, but caller must not use this seq.
    pub async fn next_interval(&self, ident: &str, num: u32) -> Result<u64> {
        // TODO: add pre-allocation if necessary
        let guard = self.db.lock().await;
        let txn = guard.begin().await?;
        let model: Option<hummock_sequence::Model> =
            hummock_sequence::Entity::find_by_id(ident.to_owned())
                .one(&txn)
                .await?;
        let start_seq = match model {
            None => {
                let init: u64 = SEQ_INIT
                    .get(ident)
                    .copied()
                    .unwrap_or_else(|| panic!("seq {ident} not found"))
                    as u64;
                let active_model = hummock_sequence::ActiveModel {
                    name: ActiveValue::set(ident.into()),
                    seq: ActiveValue::set(init.checked_add(num as _).unwrap().try_into().unwrap()),
                };
                HummockSequence::insert(active_model).exec(&txn).await?;
                init
            }
            Some(model) => {
                let start_seq: u64 = model.seq as u64;
                if num > 0 {
                    let mut active_model: hummock_sequence::ActiveModel = model.into();
                    active_model.seq = ActiveValue::set(
                        start_seq.checked_add(num as _).unwrap().try_into().unwrap(),
                    );
                    HummockSequence::update(active_model).exec(&txn).await?;
                }
                start_seq
            }
        };
        if num > 0 {
            txn.commit().await?;
        }
        Ok(start_seq)
    }
}

pub async fn next_compaction_task_id(env: &MetaSrvEnv) -> Result<u64> {
    env.hummock_seq.next_interval(COMPACTION_TASK_ID, 1).await
}

pub async fn next_meta_backup_id(env: &MetaSrvEnv) -> Result<u64> {
    env.hummock_seq.next_interval(META_BACKUP_ID, 1).await
}

pub async fn next_compaction_group_id(env: &MetaSrvEnv) -> Result<CompactionGroupId> {
    Ok(env
        .hummock_seq
        .next_interval(COMPACTION_GROUP_ID, 1)
        .await?
        .into())
}

pub async fn next_sstable_id(
    env: &MetaSrvEnv,
    num: impl TryInto<u32> + Display + Copy,
) -> Result<HummockSstableId> {
    next_unique_id(env, num).await
}

pub async fn next_raw_object_id(
    env: &MetaSrvEnv,
    num: impl TryInto<u32> + Display + Copy,
) -> Result<HummockRawObjectId> {
    next_unique_id(env, num).await
}

async fn next_unique_id<const C: usize>(
    env: &MetaSrvEnv,
    num: impl TryInto<u32> + Display + Copy,
) -> Result<TypedId<C, u64>> {
    let num: u32 = num
        .try_into()
        .unwrap_or_else(|_| panic!("failed to convert {num} into u32"));
    env.hummock_seq
        .next_interval(SSTABLE_OBJECT_ID, num)
        .await
        .map(Into::into)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use itertools::Itertools;

    use crate::controller::SqlMetaStore;
    use crate::hummock::manager::sequence::{
        COMPACTION_TASK_ID, PrefetchedSequence, SequenceGenerator,
    };

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_seq_gen() {
        let store = SqlMetaStore::for_test().await;
        let conn = store.conn.clone();
        let s = SequenceGenerator::new(conn);
        assert_eq!(1, s.next_interval(COMPACTION_TASK_ID, 1).await.unwrap());
        assert_eq!(2, s.next_interval(COMPACTION_TASK_ID, 10).await.unwrap());
        assert_eq!(12, s.next_interval(COMPACTION_TASK_ID, 10).await.unwrap());
    }

    #[test]
    fn test_prefetched_sequence_reuses_cached_range() {
        let seq = PrefetchedSequence::new();
        seq.set_test_bounds(100, 103);

        assert_eq!(
            futures::executor::block_on(seq.next(16, |_| async {
                panic!("refill should not be called when cached values exist")
            }))
            .unwrap(),
            100
        );
        assert_eq!(
            futures::executor::block_on(seq.next(16, |_| async {
                panic!("refill should not be called when cached values exist")
            }))
            .unwrap(),
            101
        );
        assert_eq!(
            futures::executor::block_on(seq.next(16, |_| async {
                panic!("refill should not be called when cached values exist")
            }))
            .unwrap(),
            102
        );
        assert_eq!(seq.test_bounds(), (103, 103));
    }

    #[tokio::test]
    async fn test_prefetched_sequence_refills_in_batch_when_empty() {
        let seq = PrefetchedSequence::new();
        let next_start = Arc::new(AtomicU64::new(100));

        let first = seq
            .next(4, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap();
        assert_eq!(first, 100);
        assert_eq!(seq.test_bounds(), (101, 104));

        assert_eq!(
            seq.next(4, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap(),
            101
        );
        assert_eq!(
            seq.next(4, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap(),
            102
        );
        assert_eq!(
            seq.next(4, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap(),
            103
        );
        assert_eq!(
            seq.next(4, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap(),
            104
        );
        assert_eq!(seq.test_bounds(), (105, 108));
    }

    #[tokio::test]
    async fn test_prefetched_sequence_respects_refill_capacity() {
        let seq = PrefetchedSequence::new();
        let next_start = Arc::new(AtomicU64::new(100));

        let first = seq
            .next(2, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap();
        assert_eq!(first, 100);
        assert_eq!(
            seq.next(2, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap(),
            101
        );
        assert_eq!(seq.test_bounds(), (102, 102));

        assert_eq!(
            seq.next(1, {
                let next_start = next_start.clone();
                move |count| async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
            })
            .await
            .unwrap(),
            102
        );
        assert_eq!(seq.test_bounds(), (103, 103));
    }

    #[test]
    fn test_prefetched_sequence_pop_does_not_overflow_bounds() {
        let seq = PrefetchedSequence::new();
        seq.set_test_bounds(10, 13);

        assert_eq!(
            futures::executor::block_on(seq.next(4, |_| async {
                panic!("refill should not be called when cached values exist")
            }))
            .unwrap(),
            10
        );
        assert_eq!(
            futures::executor::block_on(seq.next(4, |_| async {
                panic!("refill should not be called when cached values exist")
            }))
            .unwrap(),
            11
        );
        assert_eq!(
            futures::executor::block_on(seq.next(4, |_| async {
                panic!("refill should not be called when cached values exist")
            }))
            .unwrap(),
            12
        );
        assert_eq!(seq.test_bounds(), (13, 13));
    }

    #[tokio::test]
    async fn test_prefetched_sequence_shares_refill_without_gaps() {
        let seq = Arc::new(PrefetchedSequence::new());
        let next_start = Arc::new(AtomicU64::new(100));
        let refill_calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(64);
        for _ in 0..64 {
            let seq = seq.clone();
            let next_start = next_start.clone();
            let refill_calls = refill_calls.clone();
            handles.push(tokio::spawn(async move {
                seq.next(4, move |count| {
                    refill_calls.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(next_start.fetch_add(u64::from(count), Ordering::SeqCst)) }
                })
                .await
                .unwrap()
            }));
        }

        let mut ids = Vec::with_capacity(64);
        for handle in handles {
            ids.push(handle.await.unwrap());
        }

        assert_eq!(ids.len(), 64);
        assert_eq!(ids.iter().copied().collect::<HashSet<_>>().len(), 64);

        let mut sorted_ids = ids.iter().copied().collect_vec();
        sorted_ids.sort_unstable();
        assert_eq!(sorted_ids.first().copied(), Some(100));
        assert!(
            sorted_ids
                .windows(2)
                .all(|window| window[1] == window[0] + 1),
            "ids should form a contiguous range: {:?}",
            sorted_ids
        );
        assert_eq!(refill_calls.load(Ordering::SeqCst), 16);
        assert_eq!(seq.test_bounds(), (164, 164));
    }

    #[tokio::test]
    async fn test_prefetched_sequence_propagates_refill_error() {
        use crate::hummock::error::Error;

        let seq = PrefetchedSequence::new();

        // First call: refill fails → caller sees the error.
        let err = seq
            .next(4, |_count| async {
                Err(Error::Internal(anyhow::anyhow!("db failure")))
            })
            .await;
        assert!(err.is_err(), "refill error should propagate to caller");

        // The range must still be empty so a subsequent call retries the refill
        // instead of returning stale data.
        assert_eq!(seq.test_bounds(), (0, 0));

        // Second call: refill succeeds → caller gets a valid value, proving retry works.
        let val = seq
            .next(4, |_count| async move { Ok(200u64) })
            .await
            .unwrap();
        assert_eq!(val, 200);
        assert_eq!(seq.test_bounds(), (201, 204));
    }
}
