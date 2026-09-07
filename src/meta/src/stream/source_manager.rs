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

mod split_assignment;
mod worker;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::id::ObjectId;
use risingwave_common::metrics::{
    LabelGuardedHistogram, LabelGuardedIntCounter, LabelGuardedIntGauge,
};
use risingwave_common::panic_if_debug;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::{
    AnySplitEnumerator, ConnectorProperties, SourceEnumeratorContext, SourceEnumeratorInfo,
    SplitId, SplitImpl, SplitMetaData,
};
use risingwave_meta_model::SourceId;
use risingwave_pb::catalog::Source;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
pub use split_assignment::{SplitDiffOptions, SplitState, align_splits, reassign_splits};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, MutexGuard, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};
pub use worker::create_source_worker;
use worker::{ConnectorSourceWorkerHandle, create_source_worker_async};

use crate::barrier::{BarrierScheduler, Command, ReplaceStreamJobPlan};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::model::{ActorId, FragmentId, StreamJobFragments};
use crate::rpc::metrics::MetaMetrics;
use crate::{MetaError, MetaResult};

pub type SourceManagerRef = Arc<SourceManager>;
/// Actor-level split assignment, used inside the barrier worker after actor rendering.
pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;

/// Source-level split assignment: maps source ID to the discovered splits enum.
/// Unlike [`SplitAssignment`], this does not include fragment- or actor-level assignment.
/// The [`DiscoveredSplits`] enum is preserved all the way through to the barrier
/// command, and only resolved to concrete per-fragment, per-actor splits when the
/// barrier worker performs the actual split assignment.
pub type SourceSplitAssignment = HashMap<SourceId, DiscoveredSplits>;

/// Discovered splits from the source enumerator.
///
/// For adaptive splits, the single template split is returned and must be expanded
/// to per-actor splits after actor rendering inside the barrier worker.
#[derive(Debug, Clone)]
pub enum DiscoveredSplits {
    /// Fixed splits discovered from the external source.
    Fixed(BTreeMap<Arc<str>, SplitImpl>),
    /// A single adaptive split template that needs to be expanded per actor.
    /// The `fill_adaptive_split` function should be called after actor rendering.
    Adaptive(SplitImpl),
}

/// Describes how splits should be resolved for a replace stream job.
///
/// This is determined during Phase 1 (before the barrier command) and stored in
/// [`ReplaceStreamJobPlan`](crate::barrier::ReplaceStreamJobPlan).
/// The barrier worker uses this to decide the split resolution strategy in Phase 2.
#[derive(Debug, Clone)]
pub enum ReplaceJobSplitPlan {
    /// Fresh discovered splits. Used when the replace job has no existing downstream
    /// consumers (i.e., `replace_upstream` is empty), or the job is not a source.
    /// Resolved via [`SourceManager::resolve_fragment_to_actor_splits`] in Phase 2.
    Discovered(SourceSplitAssignment),
    /// Splits need to be aligned with the previous source fragment in Phase 2.
    /// This happens when replacing a source that has existing downstream consumers.
    /// Resolved via [`SourceManager::resolve_replace_source_splits`] in Phase 2.
    ///
    /// Contains the no-shuffle mapping needed for split alignment.
    AlignFromPrevious,
}

// ALTER CONNECTOR parameters, specifying the new parameters to be set for each job_id (source_id/sink_id)
pub type ConnectorPropsChange = HashMap<ObjectId, HashMap<String, String>>;

const DEFAULT_SOURCE_TICK_TIMEOUT: Duration = Duration::from_secs(10);

/// `SourceManager` keeps fetching the latest split metadata from the external source services ([`worker::ConnectorSourceWorker::tick`]),
/// and sends a split assignment command if split changes detected ([`Self::tick`]).
pub struct SourceManager {
    pub paused: Mutex<()>,
    barrier_scheduler: BarrierScheduler,
    core: Mutex<SourceManagerCore>,
    pub metrics: Arc<MetaMetrics>,
}
pub struct SourceManagerCore {
    metadata_manager: MetadataManager,

    /// Managed source loops
    managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    /// Fragments associated with each source
    source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// `source_id` -> `(fragment_id, upstream_fragment_id)`
    backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,

    env: MetaSrvEnv,
}

pub struct SourceManagerRunningInfo {
    pub source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    pub backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
}

impl SourceManagerCore {
    fn new(
        metadata_manager: MetadataManager,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            env,
        }
    }

    /// Updates states after all kinds of source change.
    pub fn apply_source_change(&mut self, source_change: SourceChange) {
        let mut added_source_fragments = Default::default();
        let mut added_backfill_fragments = Default::default();
        let mut finished_backfill_fragments = Default::default();
        let mut fragment_replacements = Default::default();
        let mut dropped_source_fragments = Default::default();
        let mut dropped_source_ids = Default::default();
        let mut recreate_source_id_map_new_props: Vec<(SourceId, HashMap<String, String>)> =
            Default::default();

        match source_change {
            SourceChange::CreateJob {
                added_source_fragments: added_source_fragments_,
                added_backfill_fragments: added_backfill_fragments_,
            } => {
                added_source_fragments = added_source_fragments_;
                added_backfill_fragments = added_backfill_fragments_;
            }
            SourceChange::CreateJobFinished {
                finished_backfill_fragments: finished_backfill_fragments_,
            } => {
                finished_backfill_fragments = finished_backfill_fragments_;
            }

            SourceChange::DropMv {
                dropped_source_fragments: dropped_source_fragments_,
            } => {
                dropped_source_fragments = dropped_source_fragments_;
            }
            SourceChange::ReplaceJob {
                dropped_source_fragments: dropped_source_fragments_,
                added_source_fragments: added_source_fragments_,
                fragment_replacements: fragment_replacements_,
            } => {
                dropped_source_fragments = dropped_source_fragments_;
                added_source_fragments = added_source_fragments_;
                fragment_replacements = fragment_replacements_;
            }
            SourceChange::DropSource {
                dropped_source_ids: dropped_source_ids_,
            } => {
                dropped_source_ids = dropped_source_ids_;
            }

            SourceChange::UpdateSourceProps {
                source_id_map_new_props,
            } => {
                for (source_id, new_props) in source_id_map_new_props {
                    recreate_source_id_map_new_props.push((source_id, new_props));
                }
            }
        }

        for source_id in dropped_source_ids {
            let dropped_fragments = self.source_fragments.remove(&source_id);

            if let Some(handle) = self.managed_sources.remove(&source_id) {
                handle.terminate(dropped_fragments);
            }
            if let Some(_fragments) = self.backfill_fragments.remove(&source_id) {
                // TODO: enable this assertion after we implemented cleanup for backfill fragments
                // debug_assert!(
                //     fragments.is_empty(),
                //     "when dropping source, there should be no backfill fragments, got: {:?}",
                //     fragments
                // );
            }
        }

        for (source_id, fragments) in added_source_fragments {
            self.source_fragments
                .entry(source_id)
                .or_default()
                .extend(fragments);
        }

        for (source_id, fragments) in added_backfill_fragments {
            self.backfill_fragments
                .entry(source_id)
                .or_default()
                .extend(fragments);
        }

        for (source_id, fragments) in finished_backfill_fragments {
            let handle = self.managed_sources.get(&source_id).unwrap_or_else(|| {
                panic!(
                    "source {} not found when adding backfill fragments {:?}",
                    source_id, fragments
                );
            });
            handle.finish_backfill(fragments.iter().map(|(id, _up_id)| *id).collect());
        }

        for (source_id, fragment_ids) in dropped_source_fragments {
            self.drop_source_fragments(Some(source_id), fragment_ids);
        }

        for (old_fragment_id, new_fragment_id) in fragment_replacements {
            // TODO: add source_id to the fragment_replacements to avoid iterating all sources
            self.drop_source_fragments(None, BTreeSet::from([old_fragment_id]));

            for fragment_ids in self.backfill_fragments.values_mut() {
                let mut new_backfill_fragment_ids = fragment_ids.clone();
                for (fragment_id, upstream_fragment_id) in fragment_ids.iter() {
                    assert_ne!(
                        fragment_id, upstream_fragment_id,
                        "backfill fragment should not be replaced"
                    );
                    if *upstream_fragment_id == old_fragment_id {
                        new_backfill_fragment_ids.remove(&(*fragment_id, *upstream_fragment_id));
                        new_backfill_fragment_ids.insert((*fragment_id, new_fragment_id));
                    }
                }
                *fragment_ids = new_backfill_fragment_ids;
            }
        }

        for (source_id, new_props) in recreate_source_id_map_new_props {
            if let Some(handle) = self.managed_sources.get_mut(&source_id) {
                // the update here should not involve fragments change and split change
                // Or we need to drop and recreate the source worker instead of updating inplace
                let props_wrapper =
                    WithOptionsSecResolved::without_secrets(new_props.into_iter().collect());
                let props = ConnectorProperties::extract(props_wrapper, false).unwrap(); // already checked when sending barrier
                handle.update_props(props);
                tracing::info!("update source {source_id} properties in source manager");
            } else {
                tracing::info!("job id {source_id} is not registered in source manager");
            }
        }
    }

    fn drop_source_fragments(
        &mut self,
        source_id: Option<SourceId>,
        dropped_fragment_ids: BTreeSet<FragmentId>,
    ) {
        if let Some(source_id) = source_id {
            if let Entry::Occupied(mut entry) = self.source_fragments.entry(source_id) {
                let mut dropped_ids = vec![];
                let managed_fragment_ids = entry.get_mut();
                for fragment_id in &dropped_fragment_ids {
                    managed_fragment_ids.remove(fragment_id);
                    dropped_ids.push(*fragment_id);
                }
                if let Some(handle) = self.managed_sources.get(&source_id) {
                    handle.drop_fragments(dropped_ids);
                } else {
                    panic_if_debug!(
                        "source {source_id} not found when dropping fragment {dropped_ids:?}",
                    );
                }
                if managed_fragment_ids.is_empty() {
                    entry.remove();
                }
            }
        } else {
            for (source_id, fragment_ids) in &mut self.source_fragments {
                let mut dropped_ids = vec![];
                for fragment_id in &dropped_fragment_ids {
                    if fragment_ids.remove(fragment_id) {
                        dropped_ids.push(*fragment_id);
                    }
                }
                if !dropped_ids.is_empty() {
                    if let Some(handle) = self.managed_sources.get(source_id) {
                        handle.drop_fragments(dropped_ids);
                    } else {
                        panic_if_debug!(
                            "source {source_id} not found when dropping fragment {dropped_ids:?}",
                        );
                    }
                }
            }
        }
    }
}

impl SourceManager {
    const DEFAULT_SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);

    pub async fn new(
        barrier_scheduler: BarrierScheduler,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
        env: MetaSrvEnv,
    ) -> MetaResult<Self> {
        let mut managed_sources = HashMap::new();
        {
            let sources = metadata_manager.list_sources().await?;
            for source in sources {
                create_source_worker_async(
                    source,
                    &mut managed_sources,
                    metrics.clone(),
                    env.await_tree_reg().clone(),
                )?
            }
        }

        let source_fragments = metadata_manager
            .catalog_controller
            .load_source_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids.into_iter().map(|id| id as _).collect(),
                )
            })
            .collect();
        let backfill_fragments = metadata_manager
            .catalog_controller
            .load_backfill_fragment_ids()
            .await?;

        let core = Mutex::new(SourceManagerCore::new(
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            env,
        ));

        Ok(Self {
            barrier_scheduler,
            core,
            paused: Mutex::new(()),
            metrics,
        })
    }

    pub async fn validate_source_once(
        &self,
        source_id: SourceId,
        new_source_props: WithOptionsSecResolved,
    ) -> MetaResult<()> {
        let props = ConnectorProperties::extract(new_source_props, false).unwrap();

        {
            let mut enumerator = props
                .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                    metrics: self.metrics.source_enumerator_metrics.clone(),
                    info: SourceEnumeratorInfo { source_id },
                }))
                .await
                .context("failed to create SplitEnumerator")?;

            validate_enumerator_once(&mut *enumerator).await?;
        }
        Ok(())
    }

    /// For replacing job (alter table/source, create sink into table).
    #[await_tree::instrument]
    pub async fn handle_replace_job(
        &self,
        dropped_job_fragments: &StreamJobFragments,
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        replace_plan: &ReplaceStreamJobPlan,
    ) {
        // Extract the fragments that include source operators.
        let dropped_source_fragments = dropped_job_fragments.stream_source_fragments();

        self.apply_source_change(SourceChange::ReplaceJob {
            dropped_source_fragments,
            added_source_fragments,
            fragment_replacements: replace_plan.fragment_replacements(),
        })
        .await;
    }

    /// Updates states after all kinds of source change.
    /// e.g., split change (`post_collect` barrier) or scaling (`post_apply_reschedule`).
    #[await_tree::instrument("apply_source_change({source_change})")]
    pub async fn apply_source_change(&self, source_change: SourceChange) {
        let need_force_tick = matches!(source_change, SourceChange::UpdateSourceProps { .. });
        let updated_source_ids = if let SourceChange::UpdateSourceProps {
            ref source_id_map_new_props,
        } = source_change
        {
            source_id_map_new_props.keys().cloned().collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        {
            let mut core = self.core.lock().await;
            core.apply_source_change(source_change);
        }

        // Force tick for updated source workers
        if need_force_tick {
            self.force_tick_updated_sources(updated_source_ids).await;
        }
    }

    /// create and register connector worker for source.
    #[await_tree::instrument("register_source({})", source.name)]
    pub async fn register_source(&self, source: &Source) -> MetaResult<()> {
        tracing::debug!("register_source: {}", source.get_id());
        let mut core = self.core.lock().await;
        let source_id = source.get_id();
        if core.managed_sources.contains_key(&source_id) {
            tracing::warn!("source {} already registered", source_id);
            return Ok(());
        }

        let handle = create_source_worker(
            source,
            self.metrics.clone(),
            core.env.await_tree_reg().clone(),
        )
        .await
        .context("failed to create source worker")?;

        core.managed_sources.insert(source_id, handle);

        Ok(())
    }

    /// register connector worker for source.
    pub async fn register_source_with_handle(
        &self,
        source_id: SourceId,
        handle: ConnectorSourceWorkerHandle,
    ) {
        let mut core = self.core.lock().await;
        if core.managed_sources.contains_key(&source_id) {
            tracing::warn!("source {} already registered", source_id);
            return;
        }

        core.managed_sources.insert(source_id, handle);
    }

    pub async fn get_running_info(&self) -> SourceManagerRunningInfo {
        let core = self.core.lock().await;

        SourceManagerRunningInfo {
            source_fragments: core.source_fragments.clone(),
            backfill_fragments: core.backfill_fragments.clone(),
        }
    }

    /// Checks whether the external source metadata has changed, and sends a split assignment command
    /// if it has.
    ///
    /// This is also how a newly created `SourceExecutor` is initialized.
    /// (force `tick` in `Self::create_source_worker`)
    ///
    /// The command will first updates `SourceExecutor`'s splits, and finally calls `Self::apply_source_change`
    /// to update states in `SourceManager`.
    async fn tick(&self) -> MetaResult<()> {
        let split_states = {
            let core_guard = self.core.lock().await;
            core_guard.reassign_splits().await?
        };

        for (database_id, split_state) in split_states {
            if !split_state.split_assignment.is_empty() {
                let command = Command::SourceChangeSplit(split_state);
                tracing::info!(command = ?command, "pushing down split assignment command");
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn run(&self) -> MetaResult<()> {
        let mut ticker = time::interval(Self::DEFAULT_SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _pause_guard = self.paused.lock().await;
            if let Err(e) = self.tick().await {
                tracing::error!(
                    error = %e.as_report(),
                    "source manager tick failed",
                );
            }
        }
    }

    /// Pause the tick loop in source manager until the returned guard is dropped.
    pub async fn pause_tick(&self) -> MutexGuard<'_, ()> {
        tracing::debug!("pausing tick lock in source manager");
        self.paused.lock().await
    }

    /// Force tick for specific updated source workers after properties update.
    async fn force_tick_updated_sources(&self, updated_source_ids: Vec<SourceId>) {
        let core = self.core.lock().await;
        for source_id in updated_source_ids {
            if let Some(handle) = core.managed_sources.get(&source_id) {
                tracing::info!("forcing tick for updated source {}", source_id);
                if let Err(e) = handle.force_tick().await {
                    tracing::warn!(
                        error = %e.as_report(),
                        "failed to force tick for source {} after properties update",
                        source_id
                    );
                }
            } else {
                tracing::warn!(
                    "source {} not found when trying to force tick after update",
                    source_id
                );
            }
        }
    }

    /// Reset source split assignments by clearing the cached split state
    /// and triggering re-discovery. This is an UNSAFE operation that may
    /// cause data duplication or loss depending on the connector.
    pub async fn reset_source_splits(&self, source_id: SourceId) -> MetaResult<()> {
        tracing::warn!(
            %source_id,
            "UNSAFE: Resetting source splits - clearing cached state and triggering re-discovery"
        );

        let core = self.core.lock().await;
        if let Some(handle) = core.managed_sources.get(&source_id) {
            // Clear the cached splits to force re-discovery
            let prev_splits = handle.splits.take();
            tracing::info!(
                %source_id,
                prev_splits = ?prev_splits.as_ref().map(|s| s.len()),
                "Clearing cached splits"
            );

            // Force a tick to re-discover splits
            tracing::info!(
                %source_id,
                "Triggering split re-discovery via force_tick"
            );
            handle.force_tick().await.with_context(|| {
                format!(
                    "failed to force tick for source {} after split reset",
                    source_id
                )
            })?;

            tracing::info!(
                %source_id,
                "Split reset completed - new splits will be assigned on next tick"
            );
            Ok(())
        } else {
            Err(anyhow::anyhow!("source {} not found in source manager", source_id).into())
        }
    }

    /// Validate split offsets before injecting them.
    /// Returns `Ok(applied_split_ids)` if all validations pass, otherwise returns an error.
    ///
    /// Validations performed:
    /// 1. Source exists in source manager
    /// 2. All requested split IDs exist in the source's current splits (runtime assignment)
    pub async fn validate_inject_source_offsets(
        &self,
        source_id: SourceId,
        split_offsets: &HashMap<String, String>,
    ) -> MetaResult<Vec<String>> {
        let (fragment_ids, env) = {
            let core = self.core.lock().await;

            // Check if source exists
            let _ = core.managed_sources.get(&source_id).ok_or_else(|| {
                MetaError::invalid_parameter(format!(
                    "source {} not found in source manager",
                    source_id
                ))
            })?;

            let mut ids = Vec::new();
            if let Some(src_frags) = core.source_fragments.get(&source_id) {
                ids.extend(src_frags.iter().copied());
            }
            if let Some(backfill_frags) = core.backfill_fragments.get(&source_id) {
                ids.extend(
                    backfill_frags
                        .iter()
                        .flat_map(|(id, upstream)| [*id, *upstream]),
                );
            }
            (ids, core.env.clone())
        };

        if fragment_ids.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "source {} has no running fragments",
                source_id
            )));
        }

        let guard = env.shared_actor_infos().read_guard();
        let mut assigned_split_ids = HashSet::new();
        for fragment_id in fragment_ids {
            if let Some(fragment) = guard.get_fragment(fragment_id) {
                for actor in fragment.actors.values() {
                    for split in &actor.splits {
                        assigned_split_ids.insert(split.id().to_string());
                    }
                }
            }
        }

        // Validate all requested split IDs exist
        let mut invalid_splits = Vec::new();
        for split_id in split_offsets.keys() {
            if !assigned_split_ids.contains(split_id) {
                invalid_splits.push(split_id.clone());
            }
        }

        if !invalid_splits.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "invalid split IDs for source {}: {:?}. Valid splits are: {:?}",
                source_id,
                invalid_splits,
                assigned_split_ids.iter().collect::<Vec<_>>()
            )));
        }

        tracing::info!(
            source_id = %source_id,
            num_splits = split_offsets.len(),
            "Validated inject source offsets request"
        );

        Ok(split_offsets.keys().cloned().collect())
    }
}

async fn validate_enumerator_once(enumerator: &mut dyn AnySplitEnumerator) -> MetaResult<()> {
    let _ = tokio::time::timeout(DEFAULT_SOURCE_TICK_TIMEOUT, enumerator.list_splits())
        .await
        .context("failed to list splits")??;
    Ok(())
}

#[derive(strum::Display, Debug)]
pub enum SourceChange {
    /// `CREATE SOURCE` (shared), or `CREATE MV`.
    /// This is applied after the job is successfully created (`post_collect` barrier).
    CreateJob {
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        /// (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`))
        added_backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    },
    UpdateSourceProps {
        // the new properties to be set for each source_id
        // and the props should not affect split assignment and fragments
        source_id_map_new_props: HashMap<SourceId, HashMap<String, String>>,
    },
    /// `CREATE SOURCE` (shared), or `CREATE MV` is _finished_ (backfill is done).
    /// This is applied after `wait_streaming_job_finished`.
    /// XXX: Should we merge `CreateJob` into this?
    CreateJobFinished {
        /// (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`))
        finished_backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    },
    /// `DROP SOURCE` or `DROP MV`
    DropSource { dropped_source_ids: Vec<SourceId> },
    DropMv {
        // FIXME: we should consider source backfill fragments here for MV on shared source.
        dropped_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    },
    ReplaceJob {
        dropped_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        fragment_replacements: HashMap<FragmentId, FragmentId>,
    },
}

pub fn build_actor_connector_splits(
    splits: &HashMap<ActorId, Vec<SplitImpl>>,
) -> HashMap<ActorId, ConnectorSplits> {
    splits
        .iter()
        .map(|(&actor_id, splits)| {
            (
                actor_id,
                ConnectorSplits {
                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                },
            )
        })
        .collect()
}

pub fn build_actor_split_impls(
    actor_splits: &HashMap<ActorId, ConnectorSplits>,
) -> HashMap<ActorId, Vec<SplitImpl>> {
    actor_splits
        .iter()
        .map(|(actor_id, ConnectorSplits { splits })| {
            (
                *actor_id,
                splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .collect(),
            )
        })
        .collect()
}
