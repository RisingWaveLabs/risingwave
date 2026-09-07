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

use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use prometheus::core::{
    Atomic, AtomicF64, AtomicI64, AtomicU64, Collector, Desc, GenericCounter, GenericLocalCounter,
    MetricVec, MetricVecBuilder,
};
use prometheus::local::{LocalHistogram, LocalIntCounter};
use prometheus::proto::MetricFamily;
use prometheus::{Gauge, Histogram, IntCounter, IntGauge};
use thiserror_ext::AsReport;
use tracing::warn;

#[macro_export]
macro_rules! register_guarded_histogram_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        $crate::register_guarded_histogram_vec_with_registry! {
            {prometheus::histogram_opts!($NAME, $HELP)},
            $LABELS_NAMES,
            $REGISTRY
        }
    }};
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $BUCKETS:expr, $REGISTRY:expr $(,)?) => {{
        $crate::register_guarded_histogram_vec_with_registry! {
            {prometheus::histogram_opts!($NAME, $HELP, $BUCKETS)},
            $LABELS_NAMES,
            $REGISTRY
        }
    }};
    ($HOPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::HistogramVec::new($HOPTS, $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_histogram_builder(inner);
            let label_guarded = $crate::LabelGuardedHistogramVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_gauge_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::GaugeVec::new(prometheus::opts!($NAME, $HELP), $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_gauge_builder(inner);
            let label_guarded = $crate::LabelGuardedGaugeVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_int_gauge_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::IntGaugeVec::new(prometheus::opts!($NAME, $HELP), $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_gauge_builder(inner);
            let label_guarded = $crate::LabelGuardedIntGaugeVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_uint_gauge_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::core::GenericGaugeVec::<prometheus::core::AtomicU64>::new(
            prometheus::opts!($NAME, $HELP),
            $LABELS_NAMES,
        );
        inner.and_then(|inner| {
            let inner = $crate::__extract_gauge_builder(inner);
            let label_guarded = $crate::LabelGuardedUintGaugeVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

#[macro_export]
macro_rules! register_guarded_int_counter_vec_with_registry {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let inner = prometheus::IntCounterVec::new(prometheus::opts!($NAME, $HELP), $LABELS_NAMES);
        inner.and_then(|inner| {
            let inner = $crate::__extract_counter_builder(inner);
            let label_guarded = $crate::LabelGuardedIntCounterVec::new(inner, { $LABELS_NAMES });
            let result = ($REGISTRY).register(Box::new(label_guarded.clone()));
            result.map(move |()| label_guarded)
        })
    }};
}

// put TAITs in a separate module to avoid "non-defining opaque type use in defining scope"
mod tait {
    use prometheus::core::{
        Atomic, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec, MetricVec,
        MetricVecBuilder,
    };
    use prometheus::{Histogram, HistogramVec};

    pub type VecBuilderOfCounter<P: Atomic> = impl MetricVecBuilder<M = GenericCounter<P>>;
    pub type VecBuilderOfGauge<P: Atomic> = impl MetricVecBuilder<M = GenericGauge<P>>;
    pub type VecBuilderOfHistogram = impl MetricVecBuilder<M = Histogram>;

    #[define_opaque(VecBuilderOfCounter)]
    pub fn __extract_counter_builder<P: Atomic>(
        vec: GenericCounterVec<P>,
    ) -> MetricVec<VecBuilderOfCounter<P>> {
        vec
    }

    #[define_opaque(VecBuilderOfGauge)]
    pub fn __extract_gauge_builder<P: Atomic>(
        vec: GenericGaugeVec<P>,
    ) -> MetricVec<VecBuilderOfGauge<P>> {
        vec
    }

    #[define_opaque(VecBuilderOfHistogram)]
    pub fn __extract_histogram_builder(vec: HistogramVec) -> MetricVec<VecBuilderOfHistogram> {
        vec
    }
}
pub use tait::*;

use crate::UintGauge;

pub type LabelGuardedHistogramVec = LabelGuardedMetricVec<VecBuilderOfHistogram>;
pub type LabelGuardedIntCounterVec = LabelGuardedMetricVec<VecBuilderOfCounter<AtomicU64>>;
pub type LabelGuardedIntGaugeVec = LabelGuardedMetricVec<VecBuilderOfGauge<AtomicI64>>;
pub type LabelGuardedUintGaugeVec = LabelGuardedMetricVec<VecBuilderOfGauge<AtomicU64>>;
pub type LabelGuardedGaugeVec = LabelGuardedMetricVec<VecBuilderOfGauge<AtomicF64>>;

pub type LabelGuardedHistogram = LabelGuardedMetric<Histogram>;
pub type LabelGuardedIntCounter = LabelGuardedMetric<IntCounter>;
pub type LabelGuardedIntGauge = LabelGuardedMetric<IntGauge>;
pub type LabelGuardedUintGauge = LabelGuardedMetric<UintGauge>;
pub type LabelGuardedGauge = LabelGuardedMetric<Gauge>;

pub type LabelGuardedLocalHistogram = LabelGuardedMetric<LocalHistogram>;
pub type LabelGuardedLocalIntCounter = LabelGuardedMetric<LocalIntCounter>;

fn gen_test_label<const N: usize>() -> [&'static str; N] {
    const TEST_LABELS: [&str; 5] = ["test1", "test2", "test3", "test4", "test5"];
    (0..N)
        .map(|i| TEST_LABELS[i])
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

#[derive(Default)]
struct LabelGuardedMetricsInfo {
    labeled_metrics_count: HashMap<Box<[String]>, usize>,
    uncollected_removed_labels: HashSet<Box<[String]>>,
}

impl LabelGuardedMetricsInfo {
    fn register_new_label<V: AsRef<str>>(mutex: &Arc<Mutex<Self>>, labels: &[V]) -> LabelGuard {
        let mut guard = mutex.lock();
        let label_string = labels
            .iter()
            .map(|label| label.as_ref().to_owned())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        guard.uncollected_removed_labels.remove(&label_string);
        *guard
            .labeled_metrics_count
            .entry(label_string.clone())
            .or_insert(0) += 1;
        LabelGuard {
            labels: label_string,
            info: mutex.clone(),
        }
    }
}

/// An RAII metrics vec with labels.
///
/// `LabelGuardedMetricVec` enhances the [`MetricVec`] to ensure the set of labels to be
/// correctly removed from the Prometheus client once being dropped. This is useful for metrics
/// that are associated with an object that can be dropped, such as streaming jobs, fragments,
/// actors, batch tasks, etc.
///
/// When a set labels is dropped, it will record it in the `uncollected_removed_labels` set.
/// Once the metrics has been collected, it will finally remove the metrics of the labels.
///
/// See also [`LabelGuardedMetricsInfo`] and [`LabelGuard::drop`].
///
/// # Arguments
///
/// * `T` - The type of the raw metrics vec.
/// * `N` - The number of labels.
#[derive(Clone)]
pub struct LabelGuardedMetricVec<T: MetricVecBuilder> {
    inner: MetricVec<T>,
    info: Arc<Mutex<LabelGuardedMetricsInfo>>,
    labels: Box<[&'static str]>,
}

impl<T: MetricVecBuilder> Debug for LabelGuardedMetricVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("LabelGuardedMetricVec<{}>", type_name::<T>()).as_str())
            .field("label", &self.labels)
            .finish()
    }
}

impl<T: MetricVecBuilder> Collector for LabelGuardedMetricVec<T> {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut guard = self.info.lock();
        let ret = self.inner.collect();
        for labels in guard.uncollected_removed_labels.drain() {
            if let Err(e) = self.inner.remove_label_values(&labels) {
                warn!(
                    error = %e.as_report(),
                    "err when delete metrics of {:?} of labels {:?}",
                    self.inner.desc().first().expect("should have desc").fq_name,
                    self.labels,
                );
            }
        }
        ret
    }
}

impl<T: MetricVecBuilder> LabelGuardedMetricVec<T> {
    pub fn new(inner: MetricVec<T>, labels: &[&'static str]) -> Self {
        Self {
            inner,
            info: Default::default(),
            labels: labels.to_vec().into_boxed_slice(),
        }
    }

    /// This is similar to the `with_label_values` of the raw metrics vec.
    /// We need to pay special attention that, unless for some special purpose,
    /// we should not drop the returned `LabelGuardedMetric` immediately after
    /// using it, such as `metrics.with_guarded_label_values(...).inc();`,
    /// because after dropped the label will be regarded as not used any more,
    /// and the internal raw metrics will be removed and reset.
    ///
    /// Instead, we should store the returned `LabelGuardedMetric` in a scope with longer
    /// lifetime so that the labels can be regarded as being used in its whole life scope.
    /// This is also the recommended way to use the raw metrics vec.
    ///
    /// A short-lived guard is safe for a gauge that is always updated with `set`: collection
    /// observes the value before removing a dropped label, and the next `set` replaces the value
    /// completely. Counters and histograms must retain the guard for their intended lifetime so
    /// that values are not reset between collections.
    pub fn with_guarded_label_values<V: AsRef<str> + std::fmt::Debug>(
        &self,
        labels: &[V],
    ) -> LabelGuardedMetric<T::M> {
        let guard = LabelGuardedMetricsInfo::register_new_label(&self.info, labels);
        let inner = self.inner.with_label_values(labels);
        LabelGuardedMetric {
            inner,
            _guard: Arc::new(guard),
        }
    }

    pub fn with_test_label<const N: usize>(&self) -> LabelGuardedMetric<T::M> {
        let labels = gen_test_label::<N>();
        self.with_guarded_label_values(&labels)
    }
}

impl LabelGuardedIntCounterVec {
    pub fn test_int_counter_vec<const N: usize>() -> Self {
        let registry = prometheus::Registry::new();
        let labels = gen_test_label::<N>();
        register_guarded_int_counter_vec_with_registry!("test", "test", &labels, &registry).unwrap()
    }
}

impl LabelGuardedIntGaugeVec {
    pub fn test_int_gauge_vec<const N: usize>() -> Self {
        let registry = prometheus::Registry::new();
        let labels = gen_test_label::<N>();
        register_guarded_int_gauge_vec_with_registry!("test", "test", &labels, &registry).unwrap()
    }
}

impl LabelGuardedGaugeVec {
    pub fn test_gauge_vec<const N: usize>() -> Self {
        let registry = prometheus::Registry::new();
        let labels = gen_test_label::<N>();
        register_guarded_gauge_vec_with_registry!("test", "test", &labels, &registry).unwrap()
    }
}

impl LabelGuardedHistogramVec {
    pub fn test_histogram_vec<const N: usize>() -> Self {
        let registry = prometheus::Registry::new();
        let labels = gen_test_label::<N>();
        register_guarded_histogram_vec_with_registry!("test", "test", &labels, &registry).unwrap()
    }
}

#[derive(Clone)]
struct LabelGuard {
    labels: Box<[String]>,
    info: Arc<Mutex<LabelGuardedMetricsInfo>>,
}

impl Drop for LabelGuard {
    fn drop(&mut self) {
        let mut guard = self.info.lock();
        let count = guard.labeled_metrics_count.get_mut(&self.labels).expect(
            "should exist because the current existing dropping one means the count is not zero",
        );
        *count -= 1;
        if *count == 0 {
            guard
                .labeled_metrics_count
                .remove(&self.labels)
                .expect("should exist");
            guard.uncollected_removed_labels.insert(self.labels.clone());
        }
    }
}

#[derive(Clone)]
pub struct LabelGuardedMetric<T> {
    inner: T,
    _guard: Arc<LabelGuard>,
}

impl<T> Debug for LabelGuardedMetric<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabelGuardedMetric").finish()
    }
}

impl<T> Deref for LabelGuardedMetric<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl LabelGuardedHistogram {
    pub fn test_histogram<const N: usize>() -> Self {
        LabelGuardedHistogramVec::test_histogram_vec::<N>().with_test_label::<N>()
    }
}

impl LabelGuardedIntCounter {
    pub fn test_int_counter<const N: usize>() -> Self {
        LabelGuardedIntCounterVec::test_int_counter_vec::<N>().with_test_label::<N>()
    }
}

impl LabelGuardedIntGauge {
    pub fn test_int_gauge<const N: usize>() -> Self {
        LabelGuardedIntGaugeVec::test_int_gauge_vec::<N>().with_test_label::<N>()
    }
}

impl LabelGuardedGauge {
    pub fn test_gauge<const N: usize>() -> Self {
        LabelGuardedGaugeVec::test_gauge_vec::<N>().with_test_label::<N>()
    }
}

pub type LazyLabelGuardedMetrics<T: MetricVecBuilder> =
    LazyLock<LabelGuardedMetric<T::M>, impl FnOnce() -> LabelGuardedMetric<T::M>>;

impl<T: MetricVecBuilder> LabelGuardedMetricVec<T> {
    #[define_opaque(LazyLabelGuardedMetrics)]
    pub fn lazy_guarded_metrics(self, labels: Vec<String>) -> LazyLabelGuardedMetrics<T> {
        LazyLock::new(move || self.with_guarded_label_values(&labels))
    }
}

pub type LazyLabelGuardedHistogram = LazyLabelGuardedMetrics<VecBuilderOfHistogram>;
pub type LazyLabelGuardedIntCounter = LazyLabelGuardedMetrics<VecBuilderOfCounter<AtomicU64>>;
pub type LazyLabelGuardedIntGauge = LazyLabelGuardedMetrics<VecBuilderOfGauge<AtomicI64>>;
pub type LazyLabelGuardedUintGauge = LazyLabelGuardedMetrics<VecBuilderOfGauge<AtomicU64>>;
pub type LazyLabelGuardedGauge = LazyLabelGuardedMetrics<VecBuilderOfGauge<AtomicF64>>;

pub trait MetricWithLocal {
    type Local;
    fn local(&self) -> Self::Local;
}

impl MetricWithLocal for Histogram {
    type Local = LocalHistogram;

    fn local(&self) -> Self::Local {
        self.local()
    }
}

impl<P: Atomic> MetricWithLocal for GenericCounter<P> {
    type Local = GenericLocalCounter<P>;

    fn local(&self) -> Self::Local {
        self.local()
    }
}

impl<T: MetricWithLocal> LabelGuardedMetric<T> {
    pub fn local(&self) -> LabelGuardedMetric<T::Local> {
        LabelGuardedMetric {
            inner: self.inner.local(),
            _guard: self._guard.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;

    use crate::{LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec};

    #[test]
    fn test_label_guarded_metrics_drop() {
        let vec = LabelGuardedIntCounterVec::test_int_counter_vec::<3>();
        let m1_1 = vec.with_guarded_label_values(&["1", "2", "3"]);
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        let m1_2 = vec.with_guarded_label_values(&["1", "2", "3"]);
        let m1_3 = m1_2.clone();
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        let m2 = vec.with_guarded_label_values(&["2", "2", "3"]);
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        drop(m1_3);
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        drop(m2);
        assert_eq!(2, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        drop(m1_1);
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        drop(m1_2);
        assert_eq!(1, vec.collect().pop().unwrap().get_metric().len());
        assert_eq!(0, vec.collect().pop().unwrap().get_metric().len());
    }

    #[test]
    fn test_short_lived_guarded_gauge() {
        let vec = LabelGuardedIntGaugeVec::test_int_gauge_vec::<1>();

        vec.with_guarded_label_values(&["1"]).set(7);
        vec.with_guarded_label_values(&["1"]).set(11);
        let collected = vec.collect().pop().unwrap();
        assert_eq!(
            11.0,
            collected.get_metric()[0]
                .get_gauge()
                .as_ref()
                .unwrap()
                .value()
        );
        assert!(vec.collect().pop().unwrap().get_metric().is_empty());

        vec.with_guarded_label_values(&["1"]).set(13);
        let collected = vec.collect().pop().unwrap();
        assert_eq!(
            13.0,
            collected.get_metric()[0]
                .get_gauge()
                .as_ref()
                .unwrap()
                .value()
        );
    }
}
