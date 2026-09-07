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

use prometheus::core::{Collector, MetricVec, MetricVecBuilder};
use prometheus::{HistogramVec, IntCounterVec};

use crate::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
    LabelGuardedMetric, LabelGuardedMetricVec, LazyLabelGuardedMetrics, MetricLevel,
};

/// For all `Relabeled*Vec` below,
/// - when `metric_level` <= `relabel_threshold`, they behave exactly the same as their inner
///   metric.
/// - when `metric_level` > `relabel_threshold`, the first `relabel_num` labels are rewrite to "" when
///   calling `with_label_values`. That's means the metric vec is aggregated into a single metric.
///
/// These wrapper classes add a `metric_level` field to corresponding metric.
/// We could have use one single struct to represent all `MetricVec<T: MetricVecBuilder>`, rather
/// than specializing them one by one. However, that's undoable because prometheus crate doesn't
/// export `MetricVecBuilder` implementation like `HistogramVecBuilder`.
///
/// ## Note
///
/// CAUTION! Relabelling might cause expected result!
///
/// For counters (including histogram because it uses counters internally), it's usually natural
/// to sum up the count from multiple labels.
///
/// For the rest (such as Gauge), the semantics becomes "any/last of the recorded value". Please be
/// cautious.
#[derive(Clone, Debug)]
pub struct RelabeledMetricVec<M> {
    relabel_threshold: MetricLevel,
    metric_level: MetricLevel,
    metric: M,

    /// The first `relabel_num` labels will be relabeled to empty string
    ///
    /// For example, if `relabel_num` is 1, and the input labels are `["actor_id",
    /// "fragment_id", "table_id"]`, when threshold is reached, the label values will be
    /// `["", "<original_fragment_id>", "<original_table_id>"]`.
    relabel_num: usize,
}

impl<M> RelabeledMetricVec<M> {
    pub fn with_metric_level(
        metric_level: MetricLevel,
        metric: M,
        relabel_threshold: MetricLevel,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
            relabel_num: usize::MAX,
        }
    }

    pub fn with_metric_level_relabel_n(
        metric_level: MetricLevel,
        metric: M,
        relabel_threshold: MetricLevel,
        relabel_num: usize,
    ) -> Self {
        Self {
            relabel_threshold,
            metric_level,
            metric,
            relabel_num,
        }
    }

    fn relabel_impl<V: AsRef<str> + std::fmt::Debug>(&self, vals: &[V]) -> Option<Vec<String>> {
        if self.metric_level > self.relabel_threshold {
            // relabel first n labels to empty string
            let mut relabeled_vals = vals
                .iter()
                .map(|v| v.as_ref().to_owned())
                .collect::<Vec<_>>();
            for label in relabeled_vals.iter_mut().take(self.relabel_num) {
                *label = String::new();
            }
            Some(relabeled_vals)
        } else {
            None
        }
    }
}

#[easy_ext::ext(MetricVecRelabelExt)]
impl<M> M
where
    M: Sized,
{
    /// Equivalent to [`RelabeledMetricVec::with_metric_level`].
    pub fn relabel(
        self,
        metric_level: MetricLevel,
        relabel_threshold: MetricLevel,
    ) -> RelabeledMetricVec<M> {
        RelabeledMetricVec::with_metric_level(metric_level, self, relabel_threshold)
    }

    /// Equivalent to [`RelabeledMetricVec::with_metric_level_relabel_n`].
    pub fn relabel_n(
        self,
        metric_level: MetricLevel,
        relabel_threshold: MetricLevel,
        relabel_num: usize,
    ) -> RelabeledMetricVec<M> {
        RelabeledMetricVec::with_metric_level_relabel_n(
            metric_level,
            self,
            relabel_threshold,
            relabel_num,
        )
    }

    /// Equivalent to [`RelabeledMetricVec::with_metric_level_relabel_n`] with `metric_level` set to
    /// `MetricLevel::Debug` and `relabel_num` set to 1.
    pub fn relabel_debug_1(self, relabel_threshold: MetricLevel) -> RelabeledMetricVec<M> {
        RelabeledMetricVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            self,
            relabel_threshold,
            1,
        )
    }
}

impl<T: MetricVecBuilder> RelabeledMetricVec<MetricVec<T>> {
    pub fn with_label_values<V: AsRef<str> + std::fmt::Debug>(&self, vals: &[V]) -> T::M {
        if let Some(relabeled_vals) = self.relabel_impl(vals) {
            return self.metric.with_label_values(&relabeled_vals);
        }
        self.metric.with_label_values(vals)
    }

    pub fn reset(&self) {
        self.metric.reset();
    }
}

impl<T: MetricVecBuilder> RelabeledMetricVec<LabelGuardedMetricVec<T>> {
    pub fn with_guarded_label_values<V: AsRef<str> + std::fmt::Debug>(
        &self,
        vals: &[V],
    ) -> LabelGuardedMetric<T::M> {
        if let Some(relabeled_vals) = self.relabel_impl(vals) {
            return self.metric.with_guarded_label_values(&relabeled_vals);
        }
        self.metric.with_guarded_label_values(vals)
    }

    pub fn lazy_guarded_metrics(&self, labels: Vec<String>) -> LazyLabelGuardedMetrics<T> {
        if let Some(relabeled_vals) = self.relabel_impl(labels.as_slice()) {
            return self.metric.clone().lazy_guarded_metrics(relabeled_vals);
        }
        self.metric.clone().lazy_guarded_metrics(labels)
    }
}

impl<T: Collector> Collector for RelabeledMetricVec<T> {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.metric.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.metric.collect()
    }
}

pub type RelabeledCounterVec = RelabeledMetricVec<IntCounterVec>;
pub type RelabeledHistogramVec = RelabeledMetricVec<HistogramVec>;

pub type RelabeledGuardedHistogramVec = RelabeledMetricVec<LabelGuardedHistogramVec>;
pub type RelabeledGuardedIntCounterVec = RelabeledMetricVec<LabelGuardedIntCounterVec>;

/// CAUTION! Relabelling a Gauge might cause expected result!
///
/// See [`RelabeledMetricVec`] for details.
pub type RelabeledGuardedIntGaugeVec = RelabeledMetricVec<LabelGuardedIntGaugeVec>;
