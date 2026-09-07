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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::future;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::hash::{ActorMapping, VnodeBitmapExt};
use risingwave_connector::source::{SplitId, SplitMetaData};
use risingwave_meta_model::{
    StreamingParallelism, WorkerId, fragment, fragment_relation, object, streaming_job,
};
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::stream_plan::{PbDispatchOutputMapping, PbDispatcher};
use sea_orm::{ConnectionTrait, JoinType, QuerySelect, RelationTrait};
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::barrier::{Command, Reschedule, RescheduleContext, ReschedulePlan};
use crate::controller::scale::{
    FragmentRenderMap, LoadedFragmentContext, NoShuffleEnsemble,
    find_fragment_no_shuffle_dags_detailed, load_fragment_context, load_fragment_context_for_jobs,
};
use crate::error::bail_invalid_parameter;
use crate::manager::{ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv, MetadataManager};
use crate::model::{ActorId, FragmentId, StreamActor, StreamActorWithDispatchers};
use crate::stream::{GlobalStreamManager, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerReschedule {
    pub worker_actor_diff: BTreeMap<WorkerId, isize>,
}

use risingwave_common::id::JobId;
use risingwave_meta_model::DispatcherType;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::prelude::{Fragment, FragmentRelation, StreamingJob};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, TransactionTrait,
};

use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::utils::{
    StreamingJobExtraInfo, compose_dispatchers, get_streaming_job_extra_info,
};

pub type ScaleControllerRef = Arc<ScaleController>;

// Field order is intentional: derived `Ord` is used to canonicalize actor
// layout for no-op detection, so new fields must preserve meaningful
// comparison order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NormalizedActor {
    worker_id: WorkerId,
    vnode_bitmap: Option<Vec<usize>>,
    splits: Vec<SplitId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedFragmentLayout {
    distribution_type: DistributionType,
    actors: Vec<NormalizedActor>,
}

fn normalize_actor_info(info: &InflightActorInfo) -> NormalizedActor {
    let vnode_bitmap = info.vnode_bitmap.as_ref().map(|bitmap| {
        bitmap
            .iter_vnodes()
            .map(|vnode| vnode.to_index())
            .collect_vec()
    });
    let mut splits = info.splits.iter().map(SplitMetaData::id).collect_vec();
    splits.sort_unstable();
    NormalizedActor {
        worker_id: info.worker_id,
        vnode_bitmap,
        splits,
    }
}

fn build_normalized_fragment_layout(
    fragment_info: &InflightFragmentInfo,
) -> NormalizedFragmentLayout {
    // Actor ids are intentionally erased here. For no-op detection we only care
    // about the semantic layout carried by worker placement, vnode ownership,
    // and split assignment.
    let mut actors = fragment_info
        .actors
        .values()
        .map(normalize_actor_info)
        .collect_vec();
    actors.sort_unstable();

    NormalizedFragmentLayout {
        distribution_type: fragment_info.distribution_type,
        actors,
    }
}

/// Compare the rendered (preview) layout against the current in-flight layout
/// to decide whether a reschedule is a no-op.
///
/// Dispatcher layout is intentionally not compared here. On the reschedule path
/// it is a deterministic function of the fragment actor layout together with the
/// stable fragment-relation metadata held in `RescheduleContext`, so fragment-level
/// equality already implies dispatcher equality.
///
/// Only fragments present in `render_result` are compared. This is intentional:
/// this function sits on the reschedule path, which only re-renders fragments
/// that were loaded into the `RescheduleContext`. Fragment creation or deletion
/// is handled by separate DDL / recovery paths, not by reschedule, so fragments
/// outside the render set are irrelevant here. In other words, this is a subset
/// check over the rendered fragments, not a full bidirectional equality check.
pub(crate) fn rendered_layout_matches_current(
    render_result: &FragmentRenderMap,
    all_prev_fragments: &HashMap<FragmentId, &InflightFragmentInfo>,
) -> MetaResult<bool> {
    let all_rendered_fragments: HashMap<_, _> = render_result
        .values()
        .flat_map(|jobs| jobs.values())
        .flatten()
        .map(|(fragment_id, info)| (*fragment_id, info))
        .collect();

    for (fragment_id, rendered_fragment) in &all_rendered_fragments {
        let Some(prev_fragment) = all_prev_fragments.get(fragment_id).copied() else {
            return Err(MetaError::from(anyhow!(
                "previous fragment info for {} not found",
                fragment_id
            )));
        };

        let rendered_layout = build_normalized_fragment_layout(rendered_fragment);
        let current_layout = build_normalized_fragment_layout(prev_fragment);

        if rendered_layout != current_layout {
            return Ok(false);
        }
    }

    Ok(true)
}

pub struct ScaleController {
    pub metadata_manager: MetadataManager,

    pub source_manager: SourceManagerRef,

    pub env: MetaSrvEnv,

    /// We will acquire lock during DDL to prevent scaling operations on jobs that are in the creating state.
    /// e.g., a MV cannot be rescheduled during foreground backfill.
    pub reschedule_lock: RwLock<()>,
}

impl ScaleController {
    pub fn new(
        metadata_manager: &MetadataManager,
        source_manager: SourceManagerRef,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            metadata_manager: metadata_manager.clone(),
            source_manager,
            env,
            reschedule_lock: RwLock::new(()),
        }
    }

    pub async fn resolve_related_no_shuffle_jobs(
        &self,
        jobs: &[JobId],
    ) -> MetaResult<HashSet<JobId>> {
        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragment_ids: Vec<_> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::JobId.is_in(jobs.to_vec()))
            .into_tuple()
            .all(&txn)
            .await?;
        let ensembles = find_fragment_no_shuffle_dags_detailed(&txn, &fragment_ids).await?;
        let related_fragments = ensembles
            .iter()
            .flat_map(|ensemble| ensemble.fragments())
            .collect_vec();

        let job_ids: Vec<_> = Fragment::find()
            .select_only()
            .column(fragment::Column::JobId)
            .filter(fragment::Column::FragmentId.is_in(related_fragments))
            .into_tuple()
            .all(&txn)
            .await?;

        let job_ids = job_ids.into_iter().collect();

        Ok(job_ids)
    }

    pub async fn reschedule_inplace(
        &self,
        policy: HashMap<JobId, ReschedulePolicy>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        let inner = self.metadata_manager.catalog_controller.inner.write().await;
        let txn = inner.db.begin().await?;

        for (table_id, target) in &policy {
            let streaming_job = StreamingJob::find_by_id(*table_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?;

            let max_parallelism = streaming_job.max_parallelism;

            let mut streaming_job = streaming_job.into_active_model();

            match &target {
                ReschedulePolicy::Parallelism(p) | ReschedulePolicy::Both(p, _) => {
                    if let StreamingParallelism::Fixed(n) = p.parallelism
                        && n > max_parallelism as usize
                    {
                        bail!(format!(
                            "specified parallelism {n} should not exceed max parallelism {max_parallelism}"
                        ));
                    }

                    streaming_job.parallelism = Set(p.parallelism.clone());
                    if matches!(p.parallelism, StreamingParallelism::Fixed(_)) {
                        streaming_job.adaptive_parallelism_strategy = Set(None);
                    } else {
                        streaming_job.adaptive_parallelism_strategy =
                            Set(p.adaptive_parallelism_strategy.clone());
                    }
                }
                _ => {}
            }

            match &target {
                ReschedulePolicy::ResourceGroup(r) | ReschedulePolicy::Both(_, r) => {
                    streaming_job.specific_resource_group = Set(r.resource_group.clone());
                }
                _ => {}
            }

            StreamingJob::update(streaming_job).exec(&txn).await?;
        }

        let job_ids: HashSet<JobId> = policy.keys().copied().collect();
        let commands = build_reschedule_intent_for_jobs(&txn, job_ids).await?;

        txn.commit().await?;

        Ok(commands)
    }

    pub async fn reschedule_backfill_parallelism_inplace(
        &self,
        policy: HashMap<JobId, Option<ParallelismPolicy>>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        if policy.is_empty() {
            return Ok(HashMap::new());
        }

        let inner = self.metadata_manager.catalog_controller.inner.write().await;
        let txn = inner.db.begin().await?;

        for (table_id, policy) in &policy {
            let streaming_job = StreamingJob::find_by_id(*table_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?;

            let max_parallelism = streaming_job.max_parallelism;

            let mut streaming_job = streaming_job.into_active_model();

            if let Some(ParallelismPolicy {
                parallelism: StreamingParallelism::Fixed(n),
                ..
            }) = policy
                && *n > max_parallelism as usize
            {
                bail!(format!(
                    "specified backfill parallelism {n} should not exceed max parallelism {max_parallelism}"
                ));
            }

            if let Some(policy) = policy {
                streaming_job.backfill_parallelism = Set(Some(policy.parallelism.clone()));
                if matches!(policy.parallelism, StreamingParallelism::Fixed(_)) {
                    streaming_job.backfill_adaptive_parallelism_strategy = Set(None);
                } else {
                    streaming_job.backfill_adaptive_parallelism_strategy =
                        Set(policy.adaptive_parallelism_strategy.clone());
                }
            } else {
                streaming_job.backfill_parallelism = Set(None);
                streaming_job.backfill_adaptive_parallelism_strategy = Set(None);
            }
            streaming_job.update(&txn).await?;
        }

        let jobs = policy.keys().copied().collect();

        let command = build_reschedule_intent_for_jobs(&txn, jobs).await?;

        txn.commit().await?;

        Ok(command)
    }

    pub async fn reschedule_fragment_inplace(
        &self,
        policy: HashMap<risingwave_meta_model::FragmentId, Option<StreamingParallelism>>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        if policy.is_empty() {
            return Ok(HashMap::new());
        }

        let inner = self.metadata_manager.catalog_controller.inner.write().await;
        let txn = inner.db.begin().await?;

        let fragment_id_list = policy.keys().copied().collect_vec();

        let existing_fragment_ids: HashSet<_> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::FragmentId.is_in(fragment_id_list.clone()))
            .into_tuple::<risingwave_meta_model::FragmentId>()
            .all(&txn)
            .await?
            .into_iter()
            .collect();

        if let Some(missing_fragment_id) = fragment_id_list
            .iter()
            .find(|fragment_id| !existing_fragment_ids.contains(*fragment_id))
        {
            return Err(MetaError::catalog_id_not_found(
                "fragment",
                *missing_fragment_id,
            ));
        }

        let mut target_ensembles = vec![];

        for ensemble in find_fragment_no_shuffle_dags_detailed(&txn, &fragment_id_list).await? {
            let entry_fragment_ids = ensemble.entry_fragments().collect_vec();

            let desired_parallelism = match entry_fragment_ids
                .iter()
                .filter_map(|fragment_id| policy.get(fragment_id).cloned())
                .dedup()
                .collect_vec()
                .as_slice()
            {
                [] => {
                    bail_invalid_parameter!(
                        "none of the entry fragments {:?} were included in the reschedule request; \
                         provide at least one entry fragment id",
                        entry_fragment_ids
                    );
                }
                [parallelism] => parallelism.clone(),
                parallelisms => {
                    bail!(
                        "conflicting reschedule policies for fragments in the same no-shuffle ensemble: {:?}",
                        parallelisms
                    );
                }
            };

            let fragments = Fragment::find()
                .filter(fragment::Column::FragmentId.is_in(entry_fragment_ids))
                .all(&txn)
                .await?;

            debug_assert!(
                fragments
                    .iter()
                    .map(|fragment| fragment.parallelism.as_ref())
                    .all_equal(),
                "entry fragments in the same ensemble should share the same parallelism"
            );

            let current_parallelism = fragments
                .first()
                .and_then(|fragment| fragment.parallelism.clone());

            if current_parallelism == desired_parallelism {
                continue;
            }

            for fragment in fragments {
                let mut fragment = fragment.into_active_model();
                fragment.parallelism = Set(desired_parallelism.clone());
                Fragment::update(fragment).exec(&txn).await?;
            }

            target_ensembles.push(ensemble);
        }

        if target_ensembles.is_empty() {
            txn.commit().await?;
            return Ok(HashMap::new());
        }

        let target_fragment_ids: HashSet<FragmentId> = target_ensembles
            .iter()
            .flat_map(|ensemble| ensemble.component_fragments())
            .collect();
        let commands = build_reschedule_intent_for_fragments(&txn, target_fragment_ids).await?;

        txn.commit().await?;

        Ok(commands)
    }

    async fn rerender(&self, jobs: HashSet<JobId>) -> MetaResult<HashMap<DatabaseId, Command>> {
        if jobs.is_empty() {
            return Ok(HashMap::new());
        }

        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;
        let commands = build_reschedule_intent_for_jobs(&txn, jobs).await?;
        txn.commit().await?;
        Ok(commands)
    }
}

async fn build_reschedule_intent_for_jobs(
    txn: &impl ConnectionTrait,
    job_ids: HashSet<JobId>,
) -> MetaResult<HashMap<DatabaseId, Command>> {
    if job_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let job_id_list = job_ids.iter().copied().collect_vec();
    let database_jobs: Vec<(DatabaseId, JobId)> = StreamingJob::find()
        .select_only()
        .column(object::Column::DatabaseId)
        .column(streaming_job::Column::JobId)
        .join(JoinType::LeftJoin, streaming_job::Relation::Object.def())
        .filter(streaming_job::Column::JobId.is_in(job_id_list.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    if database_jobs.len() != job_ids.len() {
        let returned_jobs: HashSet<JobId> =
            database_jobs.iter().map(|(_, job_id)| *job_id).collect();
        let missing = job_ids.difference(&returned_jobs).copied().collect_vec();
        return Err(MetaError::catalog_id_not_found(
            "streaming job",
            format!("{missing:?}"),
        ));
    }

    let reschedule_context = load_reschedule_context_for_jobs(txn, job_ids).await?;
    if reschedule_context.is_empty() {
        return Ok(HashMap::new());
    }

    let commands = reschedule_context
        .into_database_contexts()
        .into_iter()
        .map(|(database_id, context)| {
            (
                database_id,
                Command::RescheduleIntent {
                    context,
                    reschedule_plan: None,
                },
            )
        })
        .collect();

    Ok(commands)
}

async fn build_reschedule_intent_for_fragments(
    txn: &impl ConnectionTrait,
    fragment_ids: HashSet<FragmentId>,
) -> MetaResult<HashMap<DatabaseId, Command>> {
    if fragment_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let fragment_id_list = fragment_ids.iter().copied().collect_vec();
    let fragment_databases: Vec<(FragmentId, DatabaseId)> = Fragment::find()
        .select_only()
        .column(fragment::Column::FragmentId)
        .column(object::Column::DatabaseId)
        .join(JoinType::LeftJoin, fragment::Relation::Object.def())
        .filter(fragment::Column::FragmentId.is_in(fragment_id_list.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    if fragment_databases.len() != fragment_ids.len() {
        let returned: HashSet<FragmentId> = fragment_databases
            .iter()
            .map(|(fragment_id, _)| *fragment_id)
            .collect();
        let missing = fragment_ids.difference(&returned).copied().collect_vec();
        return Err(MetaError::catalog_id_not_found(
            "fragment",
            format!("{missing:?}"),
        ));
    }

    let ensembles = find_fragment_no_shuffle_dags_detailed(txn, &fragment_id_list).await?;
    let reschedule_context = load_reschedule_context_for_ensembles(txn, ensembles).await?;
    if reschedule_context.is_empty() {
        return Ok(HashMap::new());
    }

    let commands = reschedule_context
        .into_database_contexts()
        .into_iter()
        .map(|(database_id, context)| {
            (
                database_id,
                Command::RescheduleIntent {
                    context,
                    reschedule_plan: None,
                },
            )
        })
        .collect();

    Ok(commands)
}

async fn load_reschedule_context_for_jobs(
    txn: &impl ConnectionTrait,
    job_ids: HashSet<JobId>,
) -> MetaResult<RescheduleContext> {
    let loaded = load_fragment_context_for_jobs(txn, job_ids).await?;
    build_reschedule_context_from_loaded(txn, loaded).await
}

async fn load_reschedule_context_for_ensembles(
    txn: &impl ConnectionTrait,
    ensembles: Vec<NoShuffleEnsemble>,
) -> MetaResult<RescheduleContext> {
    let loaded = load_fragment_context(txn, ensembles).await?;
    build_reschedule_context_from_loaded(txn, loaded).await
}

async fn build_reschedule_context_from_loaded(
    txn: &impl ConnectionTrait,
    loaded: LoadedFragmentContext,
) -> MetaResult<RescheduleContext> {
    if loaded.is_empty() {
        return Ok(RescheduleContext::empty());
    }

    let job_ids = loaded.job_map.keys().copied().collect_vec();
    let job_extra_info = get_streaming_job_extra_info(txn, job_ids).await?;

    let fragment_ids = loaded
        .job_fragments
        .values()
        .flat_map(|fragments| fragments.keys().copied())
        .collect_vec();

    let upstreams: Vec<(FragmentId, FragmentId, DispatcherType)> = FragmentRelation::find()
        .select_only()
        .columns([
            fragment_relation::Column::TargetFragmentId,
            fragment_relation::Column::SourceFragmentId,
            fragment_relation::Column::DispatcherType,
        ])
        .filter(fragment_relation::Column::TargetFragmentId.is_in(fragment_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let mut upstream_fragments = HashMap::new();
    for (fragment, upstream, dispatcher) in upstreams {
        upstream_fragments
            .entry(fragment as FragmentId)
            .or_insert(HashMap::new())
            .insert(upstream as FragmentId, dispatcher);
    }

    let downstreams = FragmentRelation::find()
        .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids.clone()))
        .all(txn)
        .await?;

    let mut downstream_fragments = HashMap::new();
    let mut downstream_relations = HashMap::new();
    for relation in downstreams {
        let source_fragment_id = relation.source_fragment_id as FragmentId;
        let target_fragment_id = relation.target_fragment_id as FragmentId;
        downstream_fragments
            .entry(source_fragment_id)
            .or_insert(HashMap::new())
            .insert(target_fragment_id, relation.dispatcher_type);
        downstream_relations.insert((source_fragment_id, target_fragment_id), relation);
    }

    Ok(RescheduleContext {
        loaded,
        job_extra_info,
        upstream_fragments,
        downstream_fragments,
        downstream_relations,
    })
}

/// Build a `Reschedule` by diffing the previously materialized fragment state against
/// the newly rendered actor layout.
///
/// This function assumes a full rebuild (no kept actors) and produces:
/// - actor additions/removals and vnode bitmap updates
/// - dispatcher updates for upstream/downstream fragments
/// - updated split assignments for source actors
///
/// `upstream_fragments`/`downstream_fragments` describe neighbor fragments and dispatcher types,
/// while `all_actor_dispatchers` contains the new dispatcher list for each actor. `job_extra_info`
/// supplies job-level context for building new actors.
fn diff_fragment(
    prev_fragment_info: &InflightFragmentInfo,
    curr_actors: &HashMap<ActorId, InflightActorInfo>,
    upstream_fragments: HashMap<FragmentId, DispatcherType>,
    downstream_fragments: HashMap<FragmentId, DispatcherType>,
    all_actor_dispatchers: HashMap<ActorId, Vec<PbDispatcher>>,
    job_extra_info: Option<&StreamingJobExtraInfo>,
) -> MetaResult<Reschedule> {
    let prev_ids: HashSet<_> = prev_fragment_info.actors.keys().cloned().collect();
    let curr_ids: HashSet<_> = curr_actors.keys().cloned().collect();

    let removed_actors: HashSet<_> = &prev_ids - &curr_ids;
    let added_actor_ids: HashSet<_> = &curr_ids - &prev_ids;
    let kept_ids: HashSet<_> = prev_ids.intersection(&curr_ids).cloned().collect();
    debug_assert!(
        kept_ids.is_empty(),
        "kept actors found in scale; expected full rebuild, prev={prev_ids:?}, curr={curr_ids:?}, kept={kept_ids:?}"
    );

    let mut added_actors = HashMap::new();
    for &actor_id in &added_actor_ids {
        let InflightActorInfo { worker_id, .. } = curr_actors
            .get(&actor_id)
            .ok_or_else(|| anyhow!("BUG: Worker not found for new actor {}", actor_id))?;

        added_actors
            .entry(*worker_id)
            .or_insert_with(Vec::new)
            .push(actor_id);
    }

    let mut vnode_bitmap_updates = HashMap::new();
    for actor_id in kept_ids {
        let prev_actor = &prev_fragment_info.actors[&actor_id];
        let curr_actor = &curr_actors[&actor_id];

        // Check if the vnode distribution has changed.
        if prev_actor.vnode_bitmap != curr_actor.vnode_bitmap
            && let Some(bitmap) = curr_actor.vnode_bitmap.clone()
        {
            vnode_bitmap_updates.insert(actor_id, bitmap);
        }
    }

    let upstream_dispatcher_mapping =
        if let DistributionType::Hash = prev_fragment_info.distribution_type {
            let actor_mapping = curr_actors
                .iter()
                .map(
                    |(
                        actor_id,
                        InflightActorInfo {
                            worker_id: _,
                            vnode_bitmap,
                            ..
                        },
                    )| { (*actor_id, vnode_bitmap.clone().unwrap()) },
                )
                .collect();
            Some(ActorMapping::from_bitmaps(&actor_mapping))
        } else {
            None
        };

    let upstream_fragment_dispatcher_ids = upstream_fragments
        .iter()
        .filter(|&(_, dispatcher_type)| *dispatcher_type != DispatcherType::NoShuffle)
        .map(|(upstream_fragment, _)| (*upstream_fragment, prev_fragment_info.fragment_id))
        .collect();

    let downstream_fragment_ids = downstream_fragments
        .iter()
        .filter(|&(_, dispatcher_type)| *dispatcher_type != DispatcherType::NoShuffle)
        .map(|(fragment_id, _)| *fragment_id)
        .collect();

    let extra_info = job_extra_info.cloned().unwrap_or_default();
    let expr_context = extra_info.stream_context().to_expr_context();
    let job_definition = extra_info.job_definition;
    let config_override = extra_info.config_override;

    let newly_created_actors: HashMap<ActorId, (StreamActorWithDispatchers, WorkerId)> =
        added_actor_ids
            .iter()
            .map(|actor_id| {
                let actor = StreamActor {
                    actor_id: *actor_id,
                    fragment_id: prev_fragment_info.fragment_id,
                    vnode_bitmap: curr_actors[actor_id].vnode_bitmap.clone(),
                    mview_definition: job_definition.clone(),
                    expr_context: Some(expr_context.clone()),
                    config_override: config_override.clone(),
                };
                (
                    *actor_id,
                    (
                        (
                            actor,
                            all_actor_dispatchers
                                .get(actor_id)
                                .cloned()
                                .unwrap_or_default(),
                        ),
                        curr_actors[actor_id].worker_id,
                    ),
                )
            })
            .collect();

    let actor_splits = curr_actors
        .iter()
        .map(|(&actor_id, info)| (actor_id, info.splits.clone()))
        .collect();

    let reschedule = Reschedule {
        added_actors,
        removed_actors,
        vnode_bitmap_updates,
        upstream_fragment_dispatcher_ids,
        upstream_dispatcher_mapping,
        downstream_fragment_ids,
        actor_splits,
        newly_created_actors,
    };

    Ok(reschedule)
}

/// Build executable reschedule plans from the rendered fragment layout.
///
/// Callers are expected to run the no-op layout check before invoking this
/// function so command construction stays focused on plan materialization.
pub(crate) fn build_reschedule_commands(
    render_result: FragmentRenderMap,
    context: RescheduleContext,
    all_prev_fragments: HashMap<FragmentId, &InflightFragmentInfo>,
) -> MetaResult<HashMap<DatabaseId, ReschedulePlan>> {
    if render_result.is_empty() {
        return Ok(HashMap::new());
    }

    let RescheduleContext {
        job_extra_info,
        upstream_fragments: mut all_upstream_fragments,
        downstream_fragments: mut all_downstream_fragments,
        mut downstream_relations,
        ..
    } = context;

    let fragment_ids = render_result
        .values()
        .flat_map(|jobs| jobs.values())
        .flatten()
        .map(|(fragment_id, _)| *fragment_id)
        .collect_vec();

    let all_related_fragment_ids: HashSet<_> = fragment_ids
        .iter()
        .copied()
        .chain(all_upstream_fragments.values().flatten().map(|(id, _)| *id))
        .chain(
            all_downstream_fragments
                .values()
                .flatten()
                .map(|(id, _)| *id),
        )
        .collect();

    for fragment_id in all_related_fragment_ids {
        if !all_prev_fragments.contains_key(&fragment_id) {
            return Err(MetaError::from(anyhow!(
                "previous fragment info for {fragment_id} not found"
            )));
        }
    }

    let all_rendered_fragments: HashMap<_, _> = render_result
        .values()
        .flat_map(|jobs| jobs.values())
        .flatten()
        .map(|(fragment_id, info)| (*fragment_id, info))
        .collect();

    let mut commands = HashMap::new();

    for (database_id, jobs) in &render_result {
        let mut all_fragment_actors = HashMap::new();
        let mut reschedules = HashMap::new();

        for (job_id, fragment_id, fragment_info) in jobs.iter().flat_map(|(job_id, fragments)| {
            fragments
                .iter()
                .map(move |(fragment_id, info)| (job_id, fragment_id, info))
        }) {
            let InflightFragmentInfo {
                distribution_type,
                actors,
                ..
            } = fragment_info;

            let upstream_fragments = all_upstream_fragments
                .remove(&(*fragment_id as FragmentId))
                .unwrap_or_default();
            let downstream_fragments = all_downstream_fragments
                .remove(&(*fragment_id as FragmentId))
                .unwrap_or_default();

            let fragment_actors: HashMap<_, _> = upstream_fragments
                .keys()
                .copied()
                .chain(downstream_fragments.keys().copied())
                .map(|fragment_id| {
                    all_prev_fragments
                        .get(&fragment_id)
                        .map(|fragment| {
                            (
                                fragment_id,
                                fragment.actors.keys().copied().collect::<HashSet<_>>(),
                            )
                        })
                        .ok_or_else(|| {
                            MetaError::from(anyhow!(
                                "fragment {} not found in previous state",
                                fragment_id
                            ))
                        })
                })
                .collect::<MetaResult<_>>()?;

            all_fragment_actors.extend(fragment_actors);

            let source_fragment_actors = actors
                .iter()
                .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                .collect();

            let mut all_actor_dispatchers: HashMap<_, Vec<_>> = HashMap::new();

            for downstream_fragment_id in downstream_fragments.keys() {
                let target_fragment_actors =
                    match all_rendered_fragments.get(downstream_fragment_id) {
                        None => {
                            let external_fragment = all_prev_fragments
                                .get(downstream_fragment_id)
                                .ok_or_else(|| {
                                    MetaError::from(anyhow!(
                                        "fragment {} not found in previous state",
                                        downstream_fragment_id
                                    ))
                                })?;

                            external_fragment
                                .actors
                                .iter()
                                .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                                .collect()
                        }
                        Some(downstream_rendered) => downstream_rendered
                            .actors
                            .iter()
                            .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                            .collect(),
                    };

                let target_fragment_distribution = *distribution_type;

                let fragment_relation::Model {
                    source_fragment_id: _,
                    target_fragment_id: _,
                    dispatcher_type,
                    dist_key_indices,
                    output_indices,
                    output_type_mapping,
                } = downstream_relations
                    .remove(&(
                        *fragment_id as FragmentId,
                        *downstream_fragment_id as FragmentId,
                    ))
                    .ok_or_else(|| {
                        MetaError::from(anyhow!(
                            "downstream relation missing for {} -> {}",
                            fragment_id,
                            downstream_fragment_id
                        ))
                    })?;

                let pb_mapping = PbDispatchOutputMapping {
                    indices: output_indices.into_u32_array(),
                    types: output_type_mapping.unwrap_or_default().to_protobuf(),
                };

                let (dispatchers, _) = compose_dispatchers(
                    *distribution_type,
                    &source_fragment_actors,
                    *downstream_fragment_id,
                    target_fragment_distribution,
                    &target_fragment_actors,
                    dispatcher_type,
                    dist_key_indices.into_u32_array(),
                    pb_mapping,
                );

                for (actor_id, dispatcher) in dispatchers {
                    all_actor_dispatchers
                        .entry(actor_id)
                        .or_default()
                        .push(dispatcher);
                }
            }

            let prev_fragment = all_prev_fragments.get(&{ *fragment_id }).ok_or_else(|| {
                MetaError::from(anyhow!(
                    "fragment {} not found in previous state",
                    fragment_id
                ))
            })?;

            let reschedule = diff_fragment(
                prev_fragment,
                actors,
                upstream_fragments,
                downstream_fragments,
                all_actor_dispatchers,
                job_extra_info.get(job_id),
            )?;

            reschedules.insert(*fragment_id as FragmentId, reschedule);
        }

        let command = ReschedulePlan {
            reschedules,
            fragment_actors: all_fragment_actors,
        };

        debug_assert!(
            command
                .reschedules
                .values()
                .all(|reschedule| reschedule.vnode_bitmap_updates.is_empty()),
            "reschedule plan carries vnode_bitmap_updates, expected full rebuild"
        );

        commands.insert(*database_id, command);
    }

    Ok(commands)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::FragmentTypeMask;
    use risingwave_connector::source::SplitImpl;
    use risingwave_connector::source::test_source::TestSourceSplit;

    use super::*;

    fn actor(worker_id: impl Into<WorkerId>, vnode_bitmap: Option<Bitmap>) -> InflightActorInfo {
        InflightActorInfo {
            worker_id: worker_id.into(),
            vnode_bitmap,
            splits: vec![],
        }
    }

    fn actor_with_splits(
        worker_id: impl Into<WorkerId>,
        vnode_bitmap: Option<Bitmap>,
        splits: Vec<SplitImpl>,
    ) -> InflightActorInfo {
        InflightActorInfo {
            worker_id: worker_id.into(),
            vnode_bitmap,
            splits,
        }
    }

    fn test_split(id: &str, offset: &str) -> SplitImpl {
        SplitImpl::Test(TestSourceSplit {
            id: id.into(),
            properties: HashMap::new(),
            offset: offset.to_owned(),
        })
    }

    fn fragment(
        fragment_id: FragmentId,
        actors: impl IntoIterator<Item = (ActorId, InflightActorInfo)>,
    ) -> InflightFragmentInfo {
        InflightFragmentInfo {
            fragment_id,
            distribution_type: DistributionType::Hash,
            fragment_type_mask: FragmentTypeMask::empty(),
            vnode_count: 8,
            nodes: Default::default(),
            actors: actors.into_iter().collect(),
            state_table_ids: HashSet::new(),
        }
    }

    fn render_result(fragments: Vec<(FragmentId, InflightFragmentInfo)>) -> FragmentRenderMap {
        HashMap::from([(
            DatabaseId::new(1),
            HashMap::from([(
                JobId::new(1),
                fragments.into_iter().collect::<HashMap<_, _>>(),
            )]),
        )])
    }

    #[test]
    fn rendered_layout_matches_current_ignores_actor_ids_across_fragments() {
        let current_source = fragment(
            FragmentId::new(1),
            [
                (
                    ActorId::new(101),
                    actor(1, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                ),
                (
                    ActorId::new(102),
                    actor(2, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                ),
            ],
        );
        let current_target = fragment(
            FragmentId::new(2),
            [
                (
                    ActorId::new(201),
                    actor(3, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                ),
                (
                    ActorId::new(202),
                    actor(4, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                ),
            ],
        );

        let rendered = render_result(vec![
            (
                FragmentId::new(1),
                fragment(
                    FragmentId::new(1),
                    [
                        (
                            ActorId::new(1001),
                            actor(1, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                        ),
                        (
                            ActorId::new(1002),
                            actor(2, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                        ),
                    ],
                ),
            ),
            (
                FragmentId::new(2),
                fragment(
                    FragmentId::new(2),
                    [
                        (
                            ActorId::new(2001),
                            actor(3, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                        ),
                        (
                            ActorId::new(2002),
                            actor(4, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                        ),
                    ],
                ),
            ),
        ]);

        let prev = HashMap::from([
            (FragmentId::new(1), &current_source),
            (FragmentId::new(2), &current_target),
        ]);

        assert!(rendered_layout_matches_current(&rendered, &prev,).unwrap());
    }

    #[test]
    fn rendered_layout_matches_current_detects_target_fragment_layout_changes() {
        let current_source = fragment(
            FragmentId::new(1),
            [
                (
                    ActorId::new(101),
                    actor(1, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                ),
                (
                    ActorId::new(102),
                    actor(2, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                ),
            ],
        );
        let current_target = fragment(
            FragmentId::new(2),
            [
                (
                    ActorId::new(201),
                    actor(3, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                ),
                (
                    ActorId::new(202),
                    actor(4, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                ),
            ],
        );

        let rendered = render_result(vec![
            (
                FragmentId::new(1),
                fragment(
                    FragmentId::new(1),
                    [
                        (
                            ActorId::new(1001),
                            actor(1, Some(Bitmap::from_indices(8, [0, 1, 2, 3]))),
                        ),
                        (
                            ActorId::new(1002),
                            actor(2, Some(Bitmap::from_indices(8, [4, 5, 6, 7]))),
                        ),
                    ],
                ),
            ),
            (
                FragmentId::new(2),
                fragment(
                    FragmentId::new(2),
                    [
                        (
                            ActorId::new(2001),
                            actor(3, Some(Bitmap::from_indices(8, [0, 1, 2, 3, 4, 5]))),
                        ),
                        (
                            ActorId::new(2002),
                            actor(4, Some(Bitmap::from_indices(8, [6, 7]))),
                        ),
                    ],
                ),
            ),
        ]);

        let prev = HashMap::from([
            (FragmentId::new(1), &current_source),
            (FragmentId::new(2), &current_target),
        ]);

        assert!(!rendered_layout_matches_current(&rendered, &prev,).unwrap());
    }

    #[test]
    fn rendered_layout_matches_current_ignores_split_offsets() {
        let fragment_id = FragmentId::new(1);
        let prev_fragment = fragment(
            fragment_id,
            [(
                ActorId::new(101),
                actor_with_splits(
                    1,
                    Some(Bitmap::from_indices(8, [0, 1, 2, 3])),
                    vec![test_split("split-0", "100")],
                ),
            )],
        );
        let rendered = render_result(vec![(
            fragment_id,
            fragment(
                fragment_id,
                [(
                    ActorId::new(1001),
                    actor_with_splits(
                        1,
                        Some(Bitmap::from_indices(8, [0, 1, 2, 3])),
                        vec![test_split("split-0", "200")],
                    ),
                )],
            ),
        )]);
        let prev = HashMap::from([(fragment_id, &prev_fragment)]);

        assert!(rendered_layout_matches_current(&rendered, &prev,).unwrap());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelismPolicy {
    pub parallelism: StreamingParallelism,
    pub adaptive_parallelism_strategy: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResourceGroupPolicy {
    pub resource_group: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ReschedulePolicy {
    Parallelism(ParallelismPolicy),
    ResourceGroup(ResourceGroupPolicy),
    Both(ParallelismPolicy, ResourceGroupPolicy),
}

impl GlobalStreamManager {
    #[await_tree::instrument("acquire_reschedule_read_guard")]
    pub async fn reschedule_lock_read_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.scale_controller.reschedule_lock.read().await
    }

    #[await_tree::instrument("acquire_reschedule_write_guard")]
    pub async fn reschedule_lock_write_guard(&self) -> RwLockWriteGuard<'_, ()> {
        self.scale_controller.reschedule_lock.write().await
    }

    /// When new worker nodes joined, or the parallelism of existing worker nodes changed,
    /// examines if there are any jobs can be scaled, and scales them if found.
    ///
    /// This method will iterate over all `CREATED` jobs, and can be repeatedly called.
    ///
    /// Returns
    /// - `Ok(false)` if no jobs can be scaled;
    /// - `Ok(true)` if some jobs are scaled, and it is possible that there are more jobs can be scaled.
    async fn trigger_parallelism_control(&self) -> MetaResult<bool> {
        tracing::info!("trigger parallelism control");

        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let creating_streaming_jobs = self.metadata_manager.list_creating_jobs().await?;

        let blocked_jobs = self
            .metadata_manager
            .collect_reschedule_blocked_jobs_for_creating_jobs(&creating_streaming_jobs, true)
            .await?;
        let has_blocked_jobs = !blocked_jobs.is_empty();

        let database_objects: HashMap<risingwave_meta_model::DatabaseId, Vec<JobId>> = self
            .metadata_manager
            .catalog_controller
            .list_streaming_job_with_database()
            .await?;

        let job_ids = database_objects
            .iter()
            .flat_map(|(database_id, job_ids)| {
                job_ids
                    .iter()
                    .enumerate()
                    .map(move |(idx, job_id)| (idx, database_id, job_id))
            })
            .sorted_by(|(idx_a, database_a, _), (idx_b, database_b, _)| {
                idx_a.cmp(idx_b).then(database_a.cmp(database_b))
            })
            .map(|(_, database_id, job_id)| (*database_id, *job_id))
            .filter(|(_, job_id)| !blocked_jobs.contains(job_id))
            .collect_vec();

        if job_ids.is_empty() {
            tracing::info!("no streaming jobs for scaling, maybe an empty cluster");
            // Retry periodically while some jobs are temporarily blocked by creating
            // unreschedulable backfill jobs. This allows us to scale them
            // automatically once the creating jobs finish.
            return Ok(has_blocked_jobs);
        }

        let active_workers =
            ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone()).await?;

        tracing::info!(
            "trigger parallelism control for jobs: {:#?}, workers {:#?}",
            job_ids,
            active_workers.current()
        );

        let batch_size = match self.env.opts.parallelism_control_batch_size {
            0 => job_ids.len(),
            n => n,
        };

        tracing::info!(
            "total {} streaming jobs, batch size {}, schedulable worker ids: {:?}",
            job_ids.len(),
            batch_size,
            active_workers.current()
        );

        let batches: Vec<_> = job_ids
            .into_iter()
            .chunks(batch_size)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect();

        for batch in batches {
            let jobs = batch.iter().map(|(_, job_id)| *job_id).collect();

            let commands = self.scale_controller.rerender(jobs).await?;

            let futures = commands.into_iter().map(|(database_id, command)| {
                let barrier_scheduler = self.barrier_scheduler.clone();
                async move { barrier_scheduler.run_command(database_id, command).await }
            });

            let _results = future::try_join_all(futures).await?;
        }

        Ok(has_blocked_jobs)
    }

    /// Handles notification of worker node activation and deletion, and triggers parallelism control.
    async fn run(&self, mut shutdown_rx: Receiver<()>) {
        tracing::info!("starting automatic parallelism control monitor");

        let check_period =
            Duration::from_secs(self.env.opts.parallelism_control_trigger_period_sec);

        let mut ticker = tokio::time::interval_at(
            Instant::now()
                + Duration::from_secs(self.env.opts.parallelism_control_trigger_first_delay_sec),
            check_period,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();

        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx);

        // waiting for the first tick
        ticker.tick().await;

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .expect("list active streaming compute nodes");

        let mut worker_cache: BTreeMap<_, _> = worker_nodes
            .into_iter()
            .map(|worker| (worker.id, worker))
            .collect();

        let mut should_trigger = false;

        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown_rx => {
                    tracing::info!("Stream manager is stopped");
                    break;
                }

                _ = ticker.tick(), if should_trigger => {
                    let include_workers = worker_cache.keys().copied().collect_vec();

                    if include_workers.is_empty() {
                        tracing::debug!("no available worker nodes");
                        should_trigger = false;
                        continue;
                    }

                    match self.trigger_parallelism_control().await {
                        Ok(cont) => {
                            should_trigger = cont;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "Failed to trigger scale out, waiting for next tick to retry after {}s", ticker.period().as_secs());
                            ticker.reset();
                        }
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.expect("local notification channel closed in loop of stream manager");

                    // Only maintain the cache for streaming compute nodes.
                    let worker_is_streaming_compute = |worker: &WorkerNode| {
                        worker.get_type() == Ok(WorkerType::ComputeNode)
                            && worker.property.as_ref().unwrap().is_streaming
                    };

                    match notification {
                        // TODO(#25478): add a local notification path for relevant session
                        // parameter updates and trigger a control round here when the cluster
                        // defaults for stream/backfill parallelism change.
                        LocalNotification::SystemParamsChange(_) => {}
                        LocalNotification::WorkerNodeActivated(worker) => {
                            if !worker_is_streaming_compute(&worker) {
                                continue;
                            }

                            tracing::info!(worker = %worker.id, "worker activated notification received");

                            let prev_worker = worker_cache.insert(worker.id, worker.clone());

                            match prev_worker {
                                Some(prev_worker) if prev_worker.compute_node_parallelism() != worker.compute_node_parallelism()  => {
                                    tracing::info!(worker = %worker.id, "worker parallelism changed");
                                    should_trigger = true;
                                }
                                Some(prev_worker) if prev_worker.resource_group() != worker.resource_group()  => {
                                    tracing::info!(worker = %worker.id, "worker label changed");
                                    should_trigger = true;
                                }
                                None => {
                                    tracing::info!(worker = %worker.id, "new worker joined");
                                    should_trigger = true;
                                }
                                _ => {}
                            }
                        }

                        // Since our logic for handling passive scale-in is within the barrier manager,
                        // there’s not much we can do here. All we can do is proactively remove the entries from our cache.
                        LocalNotification::WorkerNodeDeleted(worker) => {
                            if !worker_is_streaming_compute(&worker) {
                                continue;
                            }

                            match worker_cache.remove(&worker.id) {
                                Some(prev_worker) => {
                                    tracing::info!(worker = %prev_worker.id, "worker removed from stream manager cache");
                                }
                                None => {
                                    tracing::warn!(worker = %worker.id, "worker not found in stream manager cache, but it was removed");
                                }
                            }
                        }

                        LocalNotification::StreamingJobBackfillFinished(job_id) => {
                            tracing::debug!(job_id = %job_id, "received backfill finished notification");
                            if let Err(e) = self.apply_post_backfill_parallelism(job_id).await {
                                tracing::warn!(job_id = %job_id, error = %e.as_report(), "failed to restore parallelism after backfill");
                                // Retry in the next periodic tick. This avoids triggering
                                // unnecessary full control passes when restore succeeds.
                                should_trigger = true;
                            }
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    /// Restores a streaming job's parallelism to its target value after backfill completes.
    async fn apply_post_backfill_parallelism(&self, job_id: JobId) -> MetaResult<()> {
        // Fetch both the target parallelism (final desired state) and the backfill parallelism
        // (temporary parallelism used during backfill phase) from the catalog.
        let Some((
            target,
            target_adaptive_parallelism_strategy,
            backfill_parallelism,
            backfill_adaptive_parallelism_strategy,
        )) = self
            .metadata_manager
            .catalog_controller
            .get_job_parallelisms(job_id)
            .await?
        else {
            // The job may have been dropped before this notification is processed.
            // Treat it as a benign no-op so we don't trigger unnecessary retries.
            tracing::debug!(
                job_id = %job_id,
                "streaming job not found when applying post-backfill parallelism, skip"
            );
            return Ok(());
        };

        // Determine if we need to reschedule based on the backfill configuration.
        match backfill_parallelism {
            Some(backfill_parallelism)
                if backfill_parallelism == target
                    && backfill_adaptive_parallelism_strategy
                        == target_adaptive_parallelism_strategy =>
            {
                // Backfill parallelism matches target - no reschedule needed since the job
                // is already running at the desired parallelism.
                tracing::debug!(
                    job_id = %job_id,
                    ?backfill_parallelism,
                    ?target,
                    "backfill parallelism equals job parallelism, skip reschedule"
                );
                return Ok(());
            }
            Some(_) => {
                // Backfill parallelism differs from target - proceed to restore target parallelism.
            }
            None => {
                // No backfill parallelism was configured, meaning the job was created without
                // a special backfill override. No reschedule is necessary.
                tracing::debug!(
                    job_id = %job_id,
                    ?target,
                    "no backfill parallelism configured, skip post-backfill reschedule"
                );
                return Ok(());
            }
        }

        // Reschedule the job to restore its target parallelism.
        tracing::info!(
            job_id = %job_id,
            ?target,
            ?backfill_parallelism,
            "restoring parallelism after backfill via reschedule"
        );
        let policy = ReschedulePolicy::Parallelism(ParallelismPolicy {
            parallelism: target,
            adaptive_parallelism_strategy: target_adaptive_parallelism_strategy,
        });
        if let Err(e) = self.reschedule_streaming_job(job_id, policy, false).await {
            tracing::warn!(job_id = %job_id, error = %e.as_report(), "reschedule after backfill failed");
            return Err(e);
        }

        tracing::info!(job_id = %job_id, "parallelism reschedule after backfill submitted");
        Ok(())
    }

    pub fn start_auto_parallelism_monitor(
        self: Arc<Self>,
    ) -> (JoinHandle<()>, oneshot::Sender<()>) {
        tracing::info!("Automatic parallelism scale-out is enabled for streaming jobs");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }
}
