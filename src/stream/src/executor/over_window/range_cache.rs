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

use std::collections::BTreeMap;
use std::ops::{Bound, RangeInclusive};

use risingwave_common::config::streaming::OverWindowCachePolicy as CachePolicy;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::Sentinelled;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedBTreeMap;
use risingwave_expr::window_function::StateKey;
use static_assertions::const_assert;

pub(super) type CacheKey = Sentinelled<StateKey>;

/// Range cache for one over window partition.
/// The cache entries can be:
///
/// - `(Normal)*`
/// - `Smallest, (Normal)*, Largest`
/// - `(Normal)+, Largest`
/// - `Smallest, (Normal)+`
///
/// This means it's impossible to only have one sentinel in the cache without any normal entry,
/// and, each of the two types of sentinel can only appear once. Also, since sentinels are either
/// smallest or largest, they always appear at the beginning or the end of the cache.
#[derive(Clone, Debug, Default)]
pub(super) struct PartitionCache {
    inner: EstimatedBTreeMap<CacheKey, OwnedRow>,
}

impl PartitionCache {
    /// Create a new empty partition cache without sentinel values.
    pub fn new_without_sentinels() -> Self {
        Self {
            inner: EstimatedBTreeMap::new(),
        }
    }

    /// Create a new empty partition cache with sentinel values.
    pub fn new() -> Self {
        let mut cache = Self {
            inner: EstimatedBTreeMap::new(),
        };
        cache.insert(CacheKey::Smallest, OwnedRow::empty());
        cache.insert(CacheKey::Largest, OwnedRow::empty());
        cache
    }

    /// Get access to the inner `BTreeMap` for cursor operations.
    pub fn inner(&self) -> &BTreeMap<CacheKey, OwnedRow> {
        self.inner.inner()
    }

    /// Insert a key-value pair into the cache.
    pub fn insert(&mut self, key: CacheKey, value: OwnedRow) -> Option<OwnedRow> {
        self.inner.insert(key, value)
    }

    /// Remove a key from the cache.
    pub fn remove(&mut self, key: &CacheKey) -> Option<OwnedRow> {
        self.inner.remove(key)
    }

    /// Get the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get the first key-value pair in the cache.
    pub fn first_key_value(&self) -> Option<(&CacheKey, &OwnedRow)> {
        self.inner.first_key_value()
    }

    /// Get the last key-value pair in the cache.
    pub fn last_key_value(&self) -> Option<(&CacheKey, &OwnedRow)> {
        self.inner.last_key_value()
    }

    /// Retain entries in the given range, removing others.
    /// Returns `(left_removed, right_removed)` where sentinels are filtered out.
    /// Sentinels are preserved in the cache.
    fn retain_range(
        &mut self,
        range: RangeInclusive<&CacheKey>,
    ) -> (BTreeMap<CacheKey, OwnedRow>, BTreeMap<CacheKey, OwnedRow>) {
        // Check if we had sentinels before the operation
        let had_smallest = self.inner.inner().contains_key(&CacheKey::Smallest);
        let had_largest = self.inner.inner().contains_key(&CacheKey::Largest);

        let (left_removed, right_removed) = self.inner.retain_range(range);

        // Restore sentinels if they were present before
        if had_smallest {
            self.inner.insert(CacheKey::Smallest, OwnedRow::empty());
        }
        if had_largest {
            self.inner.insert(CacheKey::Largest, OwnedRow::empty());
        }

        // Filter out sentinels from the returned maps
        let left_removed = left_removed
            .into_iter()
            .filter(|(k, _)| k.is_normal())
            .collect();
        let right_removed = right_removed
            .into_iter()
            .filter(|(k, _)| k.is_normal())
            .collect();

        (left_removed, right_removed)
    }

    /// Get the number of cached `Sentinel::Normal` entries.
    pub fn normal_len(&self) -> usize {
        let len = self.inner().len();
        if len <= 1 {
            debug_assert!(
                self.inner()
                    .first_key_value()
                    .map(|(k, _)| k.is_normal())
                    .unwrap_or(true)
            );
            return len;
        }
        // len >= 2
        let cache_inner = self.inner();
        let sentinels = [
            // sentinels only appear at the beginning and/or the end
            cache_inner.first_key_value().unwrap().0.is_sentinel(),
            cache_inner.last_key_value().unwrap().0.is_sentinel(),
        ];
        len - sentinels.into_iter().filter(|x| *x).count()
    }

    /// Get the first normal key in the cache, if any.
    pub fn first_normal_key(&self) -> Option<&StateKey> {
        self.inner()
            .iter()
            .find(|(k, _)| k.is_normal())
            .map(|(k, _)| k.as_normal_expect())
    }

    /// Get the last normal key in the cache, if any.
    pub fn last_normal_key(&self) -> Option<&StateKey> {
        self.inner()
            .iter()
            .rev()
            .find(|(k, _)| k.is_normal())
            .map(|(k, _)| k.as_normal_expect())
    }

    /// Whether the leftmost entry is a sentinel.
    pub fn left_is_sentinel(&self) -> bool {
        self.first_key_value()
            .map(|(k, _)| k.is_sentinel())
            .unwrap_or(false)
    }

    /// Whether the rightmost entry is a sentinel.
    pub fn right_is_sentinel(&self) -> bool {
        self.last_key_value()
            .map(|(k, _)| k.is_sentinel())
            .unwrap_or(false)
    }

    /// Shrink the partition cache based on the given policy and recently accessed range.
    pub fn shrink(
        &mut self,
        deduped_part_key: &OwnedRow,
        cache_policy: CachePolicy,
        recently_accessed_range: RangeInclusive<StateKey>,
    ) {
        const MAGIC_CACHE_SIZE: usize = 1024;
        const MAGIC_JITTER_PREVENTION: usize = MAGIC_CACHE_SIZE / 8;

        // The cache can have zero entry (not even sentinels), e.g., after all rows of the
        // fully-cached partition are deleted. The `Recent` branch below can't handle this.
        if self.is_empty() {
            return;
        }

        tracing::trace!(
            partition=?deduped_part_key,
            cache_policy=?cache_policy,
            recently_accessed_range=?recently_accessed_range,
            "find the range to retain in the range cache"
        );

        let (start, end) = match cache_policy {
            CachePolicy::Full => {
                // evict nothing if the policy is to cache full partition
                return;
            }
            CachePolicy::Recent => {
                let (sk_start, sk_end) = recently_accessed_range.into_inner();
                let (ck_start, ck_end) = (CacheKey::from(sk_start), CacheKey::from(sk_end));

                // find the cursor just before `ck_start`
                let mut cursor = self.inner().upper_bound(Bound::Excluded(&ck_start));
                for _ in 0..MAGIC_JITTER_PREVENTION {
                    if cursor.prev().is_none() {
                        // already at the beginning
                        break;
                    }
                }
                let start = cursor
                    .peek_prev()
                    .map(|(k, _)| k)
                    .unwrap_or_else(|| self.first_key_value().unwrap().0)
                    .clone();

                // find the cursor just after `ck_end`
                let mut cursor = self.inner().lower_bound(Bound::Excluded(&ck_end));
                for _ in 0..MAGIC_JITTER_PREVENTION {
                    if cursor.next().is_none() {
                        // already at the end
                        break;
                    }
                }
                let end = cursor
                    .peek_next()
                    .map(|(k, _)| k)
                    .unwrap_or_else(|| self.last_key_value().unwrap().0)
                    .clone();

                (start, end)
            }
            CachePolicy::RecentFirstN => {
                if self.len() <= MAGIC_CACHE_SIZE {
                    // no need to evict if cache len <= N
                    return;
                } else {
                    let (sk_start, _sk_end) = recently_accessed_range.into_inner();
                    let ck_start = CacheKey::from(sk_start);

                    let mut capacity_remain = MAGIC_CACHE_SIZE; // precision is not important here, code simplicity is the first
                    const_assert!(MAGIC_JITTER_PREVENTION < MAGIC_CACHE_SIZE);

                    // find the cursor just before `ck_start`
                    let cursor_just_before_ck_start =
                        self.inner().upper_bound(Bound::Excluded(&ck_start));

                    let mut cursor = cursor_just_before_ck_start.clone();
                    // go back for at most `MAGIC_JITTER_PREVENTION` entries
                    for _ in 0..MAGIC_JITTER_PREVENTION {
                        if cursor.prev().is_none() {
                            // already at the beginning
                            break;
                        }
                        capacity_remain -= 1;
                    }
                    let start = cursor
                        .peek_prev()
                        .map(|(k, _)| k)
                        .unwrap_or_else(|| self.first_key_value().unwrap().0)
                        .clone();

                    let mut cursor = cursor_just_before_ck_start;
                    // go forward for at most `capacity_remain` entries
                    for _ in 0..capacity_remain {
                        if cursor.next().is_none() {
                            // already at the end
                            break;
                        }
                    }
                    let end = cursor
                        .peek_next()
                        .map(|(k, _)| k)
                        .unwrap_or_else(|| self.last_key_value().unwrap().0)
                        .clone();

                    (start, end)
                }
            }
            CachePolicy::RecentLastN => {
                if self.len() <= MAGIC_CACHE_SIZE {
                    // no need to evict if cache len <= N
                    return;
                } else {
                    let (_sk_start, sk_end) = recently_accessed_range.into_inner();
                    let ck_end = CacheKey::from(sk_end);

                    let mut capacity_remain = MAGIC_CACHE_SIZE; // precision is not important here, code simplicity is the first
                    const_assert!(MAGIC_JITTER_PREVENTION < MAGIC_CACHE_SIZE);

                    // find the cursor just after `ck_end`
                    let cursor_just_after_ck_end =
                        self.inner().lower_bound(Bound::Excluded(&ck_end));

                    let mut cursor = cursor_just_after_ck_end.clone();
                    // go forward for at most `MAGIC_JITTER_PREVENTION` entries
                    for _ in 0..MAGIC_JITTER_PREVENTION {
                        if cursor.next().is_none() {
                            // already at the end
                            break;
                        }
                        capacity_remain -= 1;
                    }
                    let end = cursor
                        .peek_next()
                        .map(|(k, _)| k)
                        .unwrap_or_else(|| self.last_key_value().unwrap().0)
                        .clone();

                    let mut cursor = cursor_just_after_ck_end;
                    // go back for at most `capacity_remain` entries
                    for _ in 0..capacity_remain {
                        if cursor.prev().is_none() {
                            // already at the beginning
                            break;
                        }
                    }
                    let start = cursor
                        .peek_prev()
                        .map(|(k, _)| k)
                        .unwrap_or_else(|| self.first_key_value().unwrap().0)
                        .clone();

                    (start, end)
                }
            }
        };

        tracing::trace!(
            partition=?deduped_part_key,
            retain_range=?(&start..=&end),
            "retain range in the range cache"
        );

        let (left_removed, right_removed) = self.retain_range(&start..=&end);
        if self.is_empty() {
            if !left_removed.is_empty() || !right_removed.is_empty() {
                self.insert(CacheKey::Smallest, OwnedRow::empty());
                self.insert(CacheKey::Largest, OwnedRow::empty());
            }
        } else {
            if !left_removed.is_empty() {
                self.insert(CacheKey::Smallest, OwnedRow::empty());
            }
            if !right_removed.is_empty() {
                self.insert(CacheKey::Largest, OwnedRow::empty());
            }
        }
    }
}

impl EstimateSize for PartitionCache {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DefaultOrdered, ScalarImpl};
    use risingwave_common::util::memcmp_encoding::encode_value;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::window_function::StateKey;

    use super::*;

    fn create_test_state_key(value: i32) -> StateKey {
        let row = OwnedRow::new(vec![Some(ScalarImpl::Int32(value))]);
        StateKey {
            order_key: encode_value(Some(ScalarImpl::Int32(value)), OrderType::ascending())
                .unwrap(),
            pk: DefaultOrdered::new(row),
        }
    }

    fn create_test_cache_key(value: i32) -> CacheKey {
        CacheKey::from(create_test_state_key(value))
    }

    fn create_test_row(value: i32) -> OwnedRow {
        OwnedRow::new(vec![Some(ScalarImpl::Int32(value))])
    }

    #[test]
    fn test_partition_cache_new() {
        let cache = PartitionCache::new_without_sentinels();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_partition_cache_new_with_sentinels() {
        let cache = PartitionCache::new();
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 2);

        // Should have smallest and largest sentinels
        let first = cache.first_key_value().unwrap();
        let last = cache.last_key_value().unwrap();

        assert_eq!(*first.0, CacheKey::Smallest);
        assert_eq!(*last.0, CacheKey::Largest);
    }

    #[test]
    fn test_partition_cache_insert_and_remove() {
        let mut cache = PartitionCache::new_without_sentinels();
        let key = create_test_cache_key(1);
        let value = create_test_row(100);

        // Insert
        assert!(cache.insert(key.clone(), value.clone()).is_none());
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        // Remove
        let removed = cache.remove(&key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), value);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_partition_cache_first_last_key_value() {
        let mut cache = PartitionCache::new_without_sentinels();

        // Empty cache
        assert!(cache.first_key_value().is_none());
        assert!(cache.last_key_value().is_none());

        // Add some entries
        cache.insert(create_test_cache_key(2), create_test_row(200));
        cache.insert(create_test_cache_key(1), create_test_row(100));
        cache.insert(create_test_cache_key(3), create_test_row(300));

        let first = cache.first_key_value().unwrap();
        let last = cache.last_key_value().unwrap();

        // BTreeMap should order by key
        assert_eq!(*first.0, create_test_cache_key(1));
        assert_eq!(*first.1, create_test_row(100));

        assert_eq!(*last.0, create_test_cache_key(3));
        assert_eq!(*last.1, create_test_row(300));
    }

    #[test]
    fn test_partition_cache_retain_range() {
        let mut cache = PartitionCache::new();

        // Add some entries
        for i in 1..=5 {
            cache.insert(create_test_cache_key(i), create_test_row(i * 100));
        }

        assert_eq!(cache.len(), 7); // 5 normal entries + 2 sentinels

        // Retain range [2, 4]
        let start = create_test_cache_key(2);
        let end = create_test_cache_key(4);
        let (left_removed, right_removed) = cache.retain_range(&start..=&end);

        // Should have removed key 1 on the left and key 5 on the right
        assert_eq!(left_removed.len(), 1);
        assert_eq!(right_removed.len(), 1);
        assert!(left_removed.contains_key(&create_test_cache_key(1)));
        assert!(right_removed.contains_key(&create_test_cache_key(5)));

        // Cache should now contain keys 2, 3, 4 plus sentinels
        assert_eq!(cache.len(), 5);
        for i in 2..=4 {
            let key = create_test_cache_key(i);
            assert!(cache.inner.iter().any(|(k, _)| *k == key));
        }
    }

    #[test]
    fn test_partition_cache_shrink_full_policy() {
        let mut cache = PartitionCache::new();

        // Add many entries
        for i in 1..=10 {
            cache.insert(create_test_cache_key(i), create_test_row(i * 100));
        }

        let initial_len = cache.len();
        let deduped_part_key = OwnedRow::empty();
        let recently_accessed_range = create_test_state_key(3)..=create_test_state_key(7);

        // Full policy should not shrink anything
        cache.shrink(
            &deduped_part_key,
            CachePolicy::Full,
            recently_accessed_range,
        );

        assert_eq!(cache.len(), initial_len);
    }

    #[test]
    fn test_partition_cache_shrink_recent_policy() {
        let mut cache = PartitionCache::new();

        // Add entries
        for i in 1..=10 {
            cache.insert(create_test_cache_key(i), create_test_row(i * 100));
        }

        let deduped_part_key = OwnedRow::empty();
        let recently_accessed_range = create_test_state_key(4)..=create_test_state_key(6);

        // Recent policy should keep entries around the accessed range
        cache.shrink(
            &deduped_part_key,
            CachePolicy::Recent,
            recently_accessed_range,
        );

        // Cache should still contain the accessed range and some nearby entries
        let remaining_keys: Vec<_> = cache
            .inner
            .iter()
            .filter_map(|(k, _)| match k {
                CacheKey::Normal(state_key) => Some(state_key),
                _ => None,
            })
            .collect();

        // Should contain at least the accessed range
        for i in 4..=6 {
            let target_key = create_test_state_key(i);
            assert!(
                remaining_keys
                    .iter()
                    .any(|k| k.order_key == target_key.order_key)
            );
        }
    }

    #[test]
    fn test_partition_cache_shrink_with_small_cache() {
        let mut cache = PartitionCache::new();

        // Add only a few entries (less than MAGIC_CACHE_SIZE)
        for i in 1..=5 {
            cache.insert(create_test_cache_key(i), create_test_row(i * 100));
        }

        let initial_len = cache.len();
        let deduped_part_key = OwnedRow::empty();
        let recently_accessed_range = create_test_state_key(2)..=create_test_state_key(4);

        // RecentFirstN and RecentLastN should not shrink small caches
        cache.shrink(
            &deduped_part_key,
            CachePolicy::RecentFirstN,
            recently_accessed_range.clone(),
        );
        assert_eq!(cache.len(), initial_len);

        cache.shrink(
            &deduped_part_key,
            CachePolicy::RecentLastN,
            recently_accessed_range,
        );
        assert_eq!(cache.len(), initial_len);
    }

    #[test]
    fn test_partition_cache_estimate_size() {
        let cache = PartitionCache::new_without_sentinels();
        let size_without_sentinels = cache.estimated_heap_size();

        let mut cache = PartitionCache::new();
        let size_with_sentinels = cache.estimated_heap_size();

        // Size should increase when adding entries
        assert!(size_with_sentinels >= size_without_sentinels);

        cache.insert(create_test_cache_key(1), create_test_row(100));
        let size_with_entry = cache.estimated_heap_size();

        assert!(size_with_entry > size_with_sentinels);
    }
}
