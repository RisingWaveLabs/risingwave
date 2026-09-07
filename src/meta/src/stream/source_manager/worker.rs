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

use std::future::Future;
use std::time::Instant;

use anyhow::Context;
use await_tree::InstrumentAwait;
use parking_lot::Mutex;
use risingwave_connector::WithPropertiesExt;
#[cfg(not(debug_assertions))]
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::AnySplitEnumerator;
use risingwave_connector::source::base::ConnectorProperties;

use super::*;

const MAX_FAIL_CNT: u32 = 10;

// The key used to load `SplitImpl` directly from source properties.
// When this key is present, the enumerator will only return the given ones
// instead of fetching them from the external source.
// Only valid in debug builds - will return an error in release builds.
const DEBUG_SPLITS_KEY: &str = "debug_splits";

/// Latest splits discovered by a source worker, shared between the worker and its handle.
/// The internal lock never escapes, so no holder can keep it across an await point.
#[derive(Clone, Default)]
pub struct SharedSplits(Arc<Mutex<Option<BTreeMap<SplitId, SplitImpl>>>>);

impl SharedSplits {
    fn publish(&self, splits: impl IntoIterator<Item = SplitImpl>) {
        *self.0.lock() = Some(
            splits
                .into_iter()
                .map(|split| (split.id(), split))
                .collect(),
        );
    }

    fn snapshot(&self) -> Option<BTreeMap<SplitId, SplitImpl>> {
        self.0.lock().clone()
    }

    pub(super) fn take(&self) -> Option<BTreeMap<SplitId, SplitImpl>> {
        self.0.lock().take()
    }

    pub(super) fn is_unset(&self) -> bool {
        self.0.lock().is_none()
    }
}

/// `ConnectorSourceWorker` keeps fetching the latest split metadata from the external source service ([`Self::tick`]),
/// and maintains it in `current_splits`.
pub struct ConnectorSourceWorker {
    source_id: SourceId,
    source_name: String,
    current_splits: SharedSplits,
    // XXX: box or arc?
    enumerator: Box<dyn AnySplitEnumerator>,
    period: Duration,
    metrics: Arc<MetaMetrics>,
    connector_properties: ConnectorProperties,
    fail_cnt: u32,
    // Held for the worker's lifetime: dropping a guarded handle resets its series on the next scrape.
    source_is_up: LabelGuardedIntGauge,
    tick_duration: LabelGuardedHistogram,
    monitor_error_count: LabelGuardedIntCounter,

    debug_splits: Option<Vec<SplitImpl>>,
}

fn extract_prop_from_existing_source(source: &Source) -> ConnectorResult<ConnectorProperties> {
    let options_with_secret =
        WithOptionsSecResolved::new(source.with_properties.clone(), source.secret_refs.clone());
    let mut properties = ConnectorProperties::extract(options_with_secret, false)?;
    properties.init_from_pb_source(source);
    Ok(properties)
}
fn extract_prop_from_new_source(source: &Source) -> ConnectorResult<ConnectorProperties> {
    let options_with_secret = WithOptionsSecResolved::new(
        {
            let mut with_properties = source.with_properties.clone();
            let _removed = with_properties.remove(DEBUG_SPLITS_KEY);

            #[cfg(not(debug_assertions))]
            {
                if _removed.is_some() {
                    return Err(ConnectorError::from(anyhow::anyhow!(
                        "`debug_splits` is not allowed in release mode"
                    )));
                }
            }

            with_properties
        },
        source.secret_refs.clone(),
    );
    let mut properties = ConnectorProperties::extract(options_with_secret, true)?;
    properties.init_from_pb_source(source);
    Ok(properties)
}

/// Used to create a new [`ConnectorSourceWorkerHandle`] for a new source.
///
/// It will call [`ConnectorSourceWorker::tick()`] to fetch split metadata once before returning.
pub async fn create_source_worker(
    source: &Source,
    metrics: Arc<MetaMetrics>,
    await_tree_reg: await_tree::Registry,
) -> MetaResult<ConnectorSourceWorkerHandle> {
    tracing::info!("spawning new watcher for source {}", source.id);

    let splits = SharedSplits::default();
    let current_splits_ref = splits.clone();

    let connector_properties = extract_prop_from_new_source(source)?;
    let enable_scale_in = connector_properties.enable_drop_split();
    let enable_adaptive_splits = connector_properties.enable_adaptive_splits();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
    let sync_call_timeout = source
        .with_properties
        .get_sync_call_timeout()
        .unwrap_or(DEFAULT_SOURCE_TICK_TIMEOUT);
    let handle = {
        let mut worker = ConnectorSourceWorker::create(
            source,
            connector_properties,
            DEFAULT_SOURCE_WORKER_TICK_INTERVAL,
            current_splits_ref.clone(),
            metrics,
        )
        .await?;

        // if fail to fetch meta info, will refuse to create source
        tokio::time::timeout(sync_call_timeout, worker.tick())
            .await
            .with_context(|| {
                format!(
                    "failed to fetch meta info for source {}, timeout {:?}",
                    source.id, DEFAULT_SOURCE_TICK_TIMEOUT
                )
            })??;

        let root = format!(
            "ConnectorSourceWorker(source_id={}, name={})",
            source.id, source.name
        );
        tokio::spawn(
            await_tree_reg
                .register_derived_root(root)
                .instrument(async move { worker.run(command_rx).await }),
        )
    };
    Ok(ConnectorSourceWorkerHandle {
        handle,
        command_tx,
        splits,
        enable_drop_split: enable_scale_in,
        enable_adaptive_splits,
    })
}

/// Used on startup ([`SourceManager::new`]). Failed sources will not block meta startup.
pub fn create_source_worker_async(
    source: Source,
    managed_sources: &mut HashMap<SourceId, ConnectorSourceWorkerHandle>,
    metrics: Arc<MetaMetrics>,
    await_tree_reg: await_tree::Registry,
) -> MetaResult<()> {
    tracing::info!("spawning new watcher for source {}", source.id);

    let splits = SharedSplits::default();
    let current_splits_ref = splits.clone();
    let source_id = source.id;
    let source_name = source.name.clone();

    let connector_properties = extract_prop_from_existing_source(&source)?;

    let enable_drop_split = connector_properties.enable_drop_split();
    let enable_adaptive_splits = connector_properties.enable_adaptive_splits();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
    let run_fut = async move {
        let mut ticker = time::interval(DEFAULT_SOURCE_WORKER_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut worker = loop {
            ticker.tick().await;

            match ConnectorSourceWorker::create(
                &source,
                connector_properties.clone(),
                DEFAULT_SOURCE_WORKER_TICK_INTERVAL,
                current_splits_ref.clone(),
                metrics.clone(),
            )
            .await
            {
                Ok(worker) => {
                    break worker;
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "failed to create source worker");
                }
            }
        };

        worker.run(command_rx).await
    };
    let root = format!("ConnectorSourceWorker(source_id={source_id}, name={source_name})");
    let handle = tokio::spawn(
        await_tree_reg
            .register_derived_root(root)
            .instrument(run_fut),
    );

    managed_sources.insert(
        source_id,
        ConnectorSourceWorkerHandle {
            handle,
            command_tx,
            splits,
            enable_drop_split,
            enable_adaptive_splits,
        },
    );
    Ok(())
}

const DEFAULT_SOURCE_WORKER_TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Interval at which a still-running (in-flight) source worker tick emits a progressive warning.
const TICK_INFLIGHT_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Drive a source worker `tick` future to completion, recording its duration in `tick_duration`
/// and emitting a `warn!` every [`TICK_INFLIGHT_WARN_INTERVAL`] while it is still pending.
/// The future is never cancelled or timed out, so `tick()` semantics are unchanged.
async fn run_tick_with_progressive_warn<F>(
    source_id: SourceId,
    source_name: String,
    tick_duration: LabelGuardedHistogram,
    fut: F,
) -> F::Output
where
    F: Future,
{
    let start = Instant::now();
    let mut fut = std::pin::pin!(fut);
    let mut warn_interval = time::interval(TICK_INFLIGHT_WARN_INTERVAL);
    // Consume the immediate first tick so the first warning fires only after one full interval.
    warn_interval.tick().await;
    loop {
        select! {
            biased;
            output = &mut fut => {
                tick_duration.observe(start.elapsed().as_secs_f64());
                return output;
            }
            _ = warn_interval.tick() => {
                let elapsed_secs = start.elapsed().as_secs();
                tracing::warn!(
                    source_id = %source_id,
                    source_name = %source_name,
                    "source worker tick has been running for {elapsed_secs}s; \
                     source split state and SourceManager may be blocked",
                );
            }
        }
    }
}

impl ConnectorSourceWorker {
    /// Recreate the `SplitEnumerator` to establish a new connection to the external source service.
    async fn refresh(&mut self) -> MetaResult<()> {
        let enumerator = self
            .connector_properties
            .clone()
            .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                metrics: self.metrics.source_enumerator_metrics.clone(),
                info: SourceEnumeratorInfo {
                    source_id: self.source_id,
                },
            }))
            .await
            .context("failed to create SplitEnumerator")?;
        self.enumerator = enumerator;
        self.fail_cnt = 0;
        tracing::info!("refreshed source enumerator: {}", self.source_name);
        Ok(())
    }

    /// On creation, connection to the external source service will be established, but `splits`
    /// will not be updated until `tick` is called.
    pub async fn create(
        source: &Source,
        connector_properties: ConnectorProperties,
        period: Duration,
        splits: SharedSplits,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let enumerator = connector_properties
            .clone()
            .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                metrics: metrics.source_enumerator_metrics.clone(),
                info: SourceEnumeratorInfo {
                    source_id: source.id,
                },
            }))
            .await
            .context("failed to create SplitEnumerator")?;

        let source_id_str = source.id.to_string();
        let metric_labels = [source_id_str.as_str(), source.name.as_str()];
        let source_is_up = metrics
            .source_is_up
            .with_guarded_label_values(&metric_labels);
        let tick_duration = metrics
            .source_worker_tick_duration_seconds
            .with_guarded_label_values(&metric_labels);
        let monitor_error_count = metrics
            .source_enumerator_monitor_error_count
            .with_guarded_label_values(&metric_labels);

        Ok(Self {
            source_id: source.id,
            source_name: source.name.clone(),
            current_splits: splits,
            enumerator,
            period,
            metrics,
            connector_properties,
            fail_cnt: 0,
            source_is_up,
            tick_duration,
            monitor_error_count,
            debug_splits: {
                let debug_splits = source.with_properties.get(DEBUG_SPLITS_KEY);
                #[cfg(not(debug_assertions))]
                {
                    if debug_splits.is_some() {
                        return Err(ConnectorError::from(anyhow::anyhow!(
                            "`debug_splits` is not allowed in release mode"
                        ))
                        .into());
                    }
                    None
                }

                #[cfg(debug_assertions)]
                {
                    use risingwave_common::types::JsonbVal;
                    if let Some(debug_splits) = debug_splits {
                        let mut splits = Vec::new();
                        let debug_splits_value =
                            jsonbb::serde_json::from_str::<serde_json::Value>(debug_splits)
                                .context("failed to parse split impl")?;
                        for split_impl_value in debug_splits_value.as_array().unwrap() {
                            splits.push(SplitImpl::restore_from_json(JsonbVal::from(
                                split_impl_value.clone(),
                            ))?);
                        }
                        Some(splits)
                    } else {
                        None
                    }
                }
            },
        })
    }

    pub async fn run(&mut self, mut command_rx: UnboundedReceiver<SourceWorkerCommand>) {
        let mut interval = time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            select! {
                biased;
                cmd = command_rx.borrow_mut().recv() => {
                    if let Some(cmd) = cmd {
                        match cmd {
                            SourceWorkerCommand::Tick(tx) => {
                                let _ = tx.send(self.run_tick().await);
                            }
                            SourceWorkerCommand::DropFragments(fragment_ids) => {
                                if let Err(e) = self.drop_fragments(fragment_ids).await {
                                    // when error happens, we just log it and ignore
                                    tracing::warn!(error = %e.as_report(), "failed to drop fragments");
                                }
                            }
                            SourceWorkerCommand::FinishBackfill(fragment_ids) => {
                                if let Err(e) = self.finish_backfill(fragment_ids).await {
                                    // when error happens, we just log it and ignore
                                    tracing::warn!(error = %e.as_report(), "failed to finish backfill");
                                }
                            }
                            SourceWorkerCommand::UpdateProps(new_props) => {
                                self.connector_properties = new_props;
                                if let Err(e) = self.refresh().await {
                                    tracing::error!(error = %e.as_report(), "failed to refresh the connector source worker");
                                }
                                tracing::debug!("source {} worker properties updated", self.source_name);
                            }
                            SourceWorkerCommand::Terminate => {
                                return;
                            }
                        }
                    }
                }
                _ = interval.tick() => {
                    if self.fail_cnt > MAX_FAIL_CNT
                        && let Err(e) = self.refresh().await {
                            tracing::error!(error = %e.as_report(), "failed to refresh the connector source worker");
                        }
                    if let Err(e) = self.run_tick().await {
                        tracing::error!(error = %e.as_report(), "connector source worker tick failed");
                    }
                }
            }
        }
    }

    /// Run one [`Self::tick`] with await-tree instrumentation, duration metric, and in-flight warnings.
    async fn run_tick(&mut self) -> MetaResult<()> {
        // Cloning the guarded histogram keeps its label guard alive.
        let source_id = self.source_id;
        let source_name = self.source_name.clone();
        let tick_duration = self.tick_duration.clone();
        let tick_fut = self.tick().instrument_await("tick");
        run_tick_with_progressive_warn(source_id, source_name, tick_duration, tick_fut).await
    }

    /// Uses [`risingwave_connector::source::SplitEnumerator`] to fetch the latest split metadata from the external source service.
    async fn tick(&mut self) -> MetaResult<()> {
        let source_is_up = |res: i64| {
            self.source_is_up.set(res);
        };

        let splits = {
            if let Some(debug_splits) = &self.debug_splits {
                debug_splits.clone()
            } else {
                self.enumerator
                    .list_splits()
                    .instrument_await("list_splits")
                    .await
                    .inspect_err(|_| {
                        source_is_up(0);
                        self.fail_cnt += 1;
                    })?
            }
        };

        self.fail_cnt = 0;
        self.current_splits.publish(splits);

        // Call enumerator's `on_tick` method for monitoring tasks. `on_tick` may do unbounded
        // network I/O, so it must not run under `current_splits` (#26275). For CDC, `list_splits`
        // never fails, so `on_tick` is the sole driver of `source_is_up` (Ok -> 1, Err -> 0).
        match self.enumerator.on_tick().instrument_await("on_tick").await {
            Ok(()) => source_is_up(1),
            Err(e) => {
                tracing::error!(
                    "Failed to execute enumerator `on_tick` for source {}: {}",
                    self.source_id,
                    e.as_report()
                );
                source_is_up(0);
                self.monitor_error_count.inc();
            }
        }

        Ok(())
    }

    async fn drop_fragments(&mut self, fragment_ids: Vec<FragmentId>) -> MetaResult<()> {
        self.enumerator.on_drop_fragments(fragment_ids).await?;
        Ok(())
    }

    async fn finish_backfill(&mut self, fragment_ids: Vec<FragmentId>) -> MetaResult<()> {
        self.enumerator.on_finish_backfill(fragment_ids).await?;
        Ok(())
    }
}

/// Handle for a running [`ConnectorSourceWorker`].
pub struct ConnectorSourceWorkerHandle {
    #[expect(dead_code)]
    handle: JoinHandle<()>,
    command_tx: UnboundedSender<SourceWorkerCommand>,
    pub splits: SharedSplits,
    pub enable_drop_split: bool,
    pub enable_adaptive_splits: bool,
}

impl ConnectorSourceWorkerHandle {
    pub fn get_enable_adaptive_splits(&self) -> bool {
        self.enable_adaptive_splits
    }

    pub fn discovered_splits(&self, source_id: SourceId) -> MetaResult<Option<DiscoveredSplits>> {
        let Some(discovered_splits) = self.splits.snapshot() else {
            tracing::info!(
                "The discover loop for source {} is not ready yet; we'll wait for the next run",
                source_id
            );
            return Ok(None);
        };
        if discovered_splits.is_empty() {
            tracing::warn!("Empty splits discovered for source {}", source_id);
        }

        if self.enable_adaptive_splits {
            debug_assert!(self.enable_drop_split);
            debug_assert!(discovered_splits.len() == 1);
            let template = discovered_splits.into_values().next().unwrap();
            Ok(Some(DiscoveredSplits::Adaptive(template)))
        } else {
            Ok(Some(DiscoveredSplits::Fixed(discovered_splits)))
        }
    }

    fn send_command(&self, command: SourceWorkerCommand) -> MetaResult<()> {
        let cmd_str = format!("{:?}", command);
        self.command_tx
            .send(command)
            .with_context(|| format!("failed to send {cmd_str} command to source worker"))?;
        Ok(())
    }

    /// Force [`ConnectorSourceWorker::tick()`] to be called.
    pub async fn force_tick(&self) -> MetaResult<()> {
        let (tx, rx) = oneshot::channel();
        self.send_command(SourceWorkerCommand::Tick(tx))?;
        rx.await
            .context("failed to receive tick command response from source worker")?
            .context("source worker tick failed")?;
        Ok(())
    }

    pub fn drop_fragments(&self, fragment_ids: Vec<FragmentId>) {
        tracing::debug!("drop_fragments: {:?}", fragment_ids);
        if let Err(e) = self.send_command(SourceWorkerCommand::DropFragments(fragment_ids)) {
            // ignore drop fragment error, just log it
            tracing::warn!(error = %e.as_report(), "failed to drop fragments");
        }
    }

    pub fn finish_backfill(&self, fragment_ids: Vec<FragmentId>) {
        tracing::debug!("finish_backfill: {:?}", fragment_ids);
        if let Err(e) = self.send_command(SourceWorkerCommand::FinishBackfill(fragment_ids)) {
            // ignore error, just log it
            tracing::warn!(error = %e.as_report(), "failed to finish backfill");
        }
    }

    pub fn update_props(&self, new_props: ConnectorProperties) {
        if let Err(e) = self.send_command(SourceWorkerCommand::UpdateProps(new_props)) {
            // ignore update props error, just log it
            tracing::warn!(error = %e.as_report(), "failed to update source worker properties");
        }
    }

    pub fn terminate(&self, dropped_fragments: Option<BTreeSet<FragmentId>>) {
        tracing::debug!("terminate: {:?}", dropped_fragments);
        if let Some(dropped_fragments) = dropped_fragments {
            self.drop_fragments(dropped_fragments.into_iter().collect());
        }
        if let Err(e) = self.send_command(SourceWorkerCommand::Terminate) {
            // ignore terminate error, just log it
            tracing::warn!(error = %e.as_report(), "failed to terminate source worker");
        }
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub enum SourceWorkerCommand {
    /// Sync command to force [`ConnectorSourceWorker::tick()`] to be called.
    Tick(#[educe(Debug(ignore))] oneshot::Sender<MetaResult<()>>),
    /// Async command to drop a fragment.
    DropFragments(Vec<FragmentId>),
    /// Async command to finish backfill.
    FinishBackfill(Vec<FragmentId>),
    /// Terminate the worker task.
    Terminate,
    /// Update the properties of the source worker.
    UpdateProps(ConnectorProperties),
}
