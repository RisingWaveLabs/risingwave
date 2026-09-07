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

use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};
use std::ops::{Deref, DerefMut};

use risingwave_hummock_sdk::key::FullKey;

use crate::hummock::HummockResult;
use crate::hummock::iterator::{
    DirectionEnum, HummockIterator, HummockIteratorDirection, ValueMeta,
};
use crate::hummock::value::HummockValue;
use crate::monitor::StoreLocalStatistic;

pub struct Node<I: HummockIterator> {
    iter: I,
}

impl<I: HummockIterator> Eq for Node<I> where Self: PartialEq {}
impl<I: HummockIterator> PartialOrd for Node<I>
where
    Self: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Implement `Ord` for unordered iter node. Only compare the key.
impl<I: HummockIterator> Ord for Node<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.

        match I::Direction::direction() {
            DirectionEnum::Forward => other.iter.key().cmp(&self.iter.key()),
            DirectionEnum::Backward => self.iter.key().cmp(&other.iter.key()),
        }
    }
}

impl<I: HummockIterator> PartialEq for Node<I> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

/// Iterates on multiple iterators, a.k.a. `MergeIterator`.
pub struct MergeIterator<I: HummockIterator> {
    /// Invalid or non-initialized iterators.
    unused_iters: LinkedList<Node<I>>,

    /// The heap for merge sort.
    heap: BinaryHeap<Node<I>>,

    /// Statistics collected from iterators discarded after an error.
    retired_stats: Option<Box<StoreLocalStatistic>>,
}

impl<I: HummockIterator> MergeIterator<I> {
    fn collect_local_statistic_impl(&self, stats: &mut StoreLocalStatistic) {
        if let Some(retired_stats) = &self.retired_stats {
            stats.add(retired_stats);
        }
        for node in &self.heap {
            node.iter.collect_local_statistic(stats);
        }
        for node in &self.unused_iters {
            node.iter.collect_local_statistic(stats);
        }
    }
}

impl<I: HummockIterator> MergeIterator<I> {
    pub fn new(iterators: impl IntoIterator<Item = I>) -> Self {
        Self::create(iterators)
    }

    pub fn for_compactor(iterators: impl IntoIterator<Item = I>) -> Self {
        Self::create(iterators)
    }

    fn create(iterators: impl IntoIterator<Item = I>) -> Self {
        Self {
            unused_iters: iterators.into_iter().map(|iter| Node { iter }).collect(),
            heap: BinaryHeap::new(),
            retired_stats: None,
        }
    }
}

impl<I: HummockIterator> MergeIterator<I>
where
    Node<I>: Ord,
{
    /// Moves all iterators from the `heap` to the linked list.
    fn reset_heap(&mut self) {
        self.unused_iters.extend(self.heap.drain());
    }

    /// After some iterators in `unused_iterators` are sought or rewound, calls this function
    /// to construct a new heap using the valid ones.
    fn build_heap(&mut self) {
        assert!(self.heap.is_empty());

        self.heap = self
            .unused_iters
            .extract_if(|i| i.iter.is_valid())
            .collect();
    }
}

/// This is a wrapper for the `PeekMut` of heap.
///
/// Several panics due to future cancellation are caused by calling `drop` on the `PeekMut` when
/// futures holding the `PeekMut` are cancelled and dropped. Dropping a `PeekMut` will accidentally
/// cause a comparison between the top node and the node below, and may call `key()` for top node
/// iterators that are in some intermediate inconsistent states.
///
/// When a `PeekMut` is wrapped by this guard, when the guard is dropped, `PeekMut::pop` will be
/// called on the `PeekMut`, and the popped node will be added to the linked list that collects the
/// unused nodes. In this way, when the future holding the guard is dropped, the `PeekMut` will not
/// be called `drop`, and there will not be unexpected `key()` called for heap comparison.
///
/// In normal usage, when we finished using the `PeekMut`, we should explicitly call `guard.used()`
/// in every branch carefully. When we want to pop the `PeekMut`, we can simply call `guard.pop()`.
struct PeekMutGuard<'a, T: Ord> {
    peek: Option<PeekMut<'a, T>>,
    unused: &'a mut LinkedList<T>,
}

impl<'a, T: Ord> PeekMutGuard<'a, T> {
    /// Call `peek_mut` on the top of heap and return a guard over the `PeekMut` if the heap is not
    /// empty.
    fn peek_mut(heap: &'a mut BinaryHeap<T>, unused: &'a mut LinkedList<T>) -> Option<Self> {
        heap.peek_mut().map(|peek| Self {
            peek: Some(peek),
            unused,
        })
    }

    /// Call `pop` on the `PeekMut`.
    fn pop(mut self) -> T {
        PeekMut::pop(self.peek.take().expect("should not be None"))
    }

    /// Mark finish using the `PeekMut`. `drop` will be called on the `PeekMut` directly.
    fn used(mut self) {
        self.peek.take().expect("should not be None");
    }
}

impl<T: Ord> Deref for PeekMutGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.peek.as_ref().expect("should not be None")
    }
}

impl<T: Ord> DerefMut for PeekMutGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.peek.as_mut().expect("should not be None")
    }
}

impl<T: Ord> Drop for PeekMutGuard<'_, T> {
    /// When the guard is dropped, if `pop` or `used` is not called before it is dropped, we will
    /// call `PeekMut::pop` on the `PeekMut` and recycle the node to the unused list.
    fn drop(&mut self) {
        if let Some(peek) = self.peek.take() {
            tracing::debug!(
                "PeekMut are dropped without used. May be caused by future cancellation"
            );
            let top = PeekMut::pop(peek);
            self.unused.push_back(top);
        }
    }
}

impl<I: HummockIterator> HummockIterator for MergeIterator<I>
where
    Node<I>: Ord,
{
    type Direction = I::Direction;

    async fn next(&mut self) -> HummockResult<()> {
        let mut node =
            PeekMutGuard::peek_mut(&mut self.heap, &mut self.unused_iters).expect("no inner iter");

        // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
        // return. Once the iterator enters an invalid state, we should remove it from heap
        // before returning.

        match node.iter.next().await {
            Ok(_) => {}
            Err(e) => {
                // If the iterator returns error, we should clear the heap, so that this
                // iterator becomes invalid. Collect their work before dropping them, but do not
                // retain the failed/current heap iterators for a later seek or rewind.
                let node = node.pop();
                let retired_stats = self
                    .retired_stats
                    .get_or_insert_with(|| Box::new(StoreLocalStatistic::default()));
                node.iter.collect_local_statistic(retired_stats);
                for node in self.heap.drain() {
                    node.iter.collect_local_statistic(retired_stats);
                }
                return Err(e);
            }
        }

        if !node.iter.is_valid() {
            // Put back to `unused_iters`
            let node = node.pop();
            self.unused_iters.push_back(node);
        } else {
            // This will update the heap top.
            node.used();
        }

        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.heap.peek().expect("no inner iter").iter.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").iter.value()
    }

    fn is_valid(&self) -> bool {
        self.heap.peek().is_some_and(|n| n.iter.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.rewind()))
            .await?;
        self.build_heap();
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.seek(key)))
            .await?;
        self.build_heap();
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.collect_local_statistic_impl(stats);
    }

    fn value_meta(&self) -> ValueMeta {
        self.heap.peek().expect("no inner iter").iter.value_meta()
    }
}
