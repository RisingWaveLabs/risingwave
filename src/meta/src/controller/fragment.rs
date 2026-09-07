// Copyright 2023 RisingWave Labs
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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::TryStreamExt;
use itertools::{Either, Itertools};
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
use risingwave_common::hash::{VnodeCount, VnodeCountCompat};
use risingwave_common::id::JobId;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::system_param::adaptive_parallelism_strategy::parse_strategy;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
use risingwave_connector::source::SplitImpl;
use risingwave_connector::source::cdc::CdcScanOptions;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{
    Fragment as FragmentModel, FragmentRelation, FragmentSplits, Sink, StreamingJob,
};
use risingwave_meta_model::{
    ActorId, ConnectorSplits, DatabaseId, DispatcherType, ExprContext, FragmentId, I32Array,
    JobStatus, ObjectId, SchemaId, SinkId, SourceId, StreamNode, StreamingParallelism, TableId,
    TableIdArray, VnodeBitmap, WorkerId, database, fragment, fragment_relation, fragment_splits,
    object, sink, source, streaming_job, table,
};
use risingwave_meta_model_migration::{ExprTrait, OnConflict, SimpleExpr};
use risingwave_pb::catalog::PbTable;
use risingwave_pb::common::PbActorLocation;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::meta::table_fragments::{PbActorStatus, PbState};
use risingwave_pb::meta::{FragmentDistribution, PbFragmentWorkerSlotMapping};
use risingwave_pb::source::{ConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    PbDispatchOutputMapping, PbDispatcherType, PbStreamNode, PbStreamScanType, SinkLogStoreType,
    StreamScanType,
};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, FromQueryResult, JoinType,
    PaginatorTrait, QueryFilter, QuerySelect, RelationTrait, StreamTrait, TransactionTrait,
};
use serde::{Deserialize, Serialize};

use crate::barrier::{SharedActorInfos, SharedFragmentInfo, SnapshotBackfillInfo};
use crate::controller::catalog::{CatalogController, CatalogControllerInner};
use crate::controller::scale::{
    FragmentRenderMap, LoadedFragmentContext, NoShuffleEnsemble, RenderedGraph,
    find_fragment_no_shuffle_dags_detailed, load_fragment_context_for_jobs,
    render_actor_assignments, resolve_streaming_job_definition,
};
use crate::controller::utils::{
    FragmentDesc, PartialActorLocation, PartialFragmentStateTables, compose_dispatchers,
    get_sink_fragment_by_ids, has_table_been_migrated, rebuild_fragment_mapping,
    resolve_no_shuffle_actor_mapping,
};
use crate::error::MetaError;
use crate::manager::{ActiveStreamingWorkerNodes, LocalNotification, NotificationManager};
use crate::model::{
    DownstreamFragmentRelation, Fragment, FragmentActorDispatchers, FragmentDownstreamRelation,
    StreamActor, StreamContext, StreamJobFragments, StreamingJobModelContextExt as _,
};
use crate::rpc::ddl_controller::build_upstream_sink_info;
use crate::stream::UpstreamSinkInfo;
use crate::{MetaResult, model};

/// Some information of running (inflight) actors.
#[derive(Debug)]
pub struct InflightActorInfo {
    pub worker_id: WorkerId,
    pub vnode_bitmap: Option<Bitmap>,
    pub splits: Vec<SplitImpl>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ActorInfo {
    pub actor_id: ActorId,
    pub fragment_id: FragmentId,
    pub splits: ConnectorSplits,
    pub worker_id: WorkerId,
    pub vnode_bitmap: Option<VnodeBitmap>,
    pub expr_context: ExprContext,
    pub config_override: Arc<str>,
}

#[derive(Debug)]
struct FragmentDescRow {
    fragment_id: FragmentId,
    job_id: JobId,
    fragment_type_mask: i32,
    distribution_type: DistributionType,
    state_table_ids: TableIdArray,
    vnode_count: i32,
    parallelism_override: Option<StreamingParallelism>,
    stream_node: Option<StreamNode>,
}

#[derive(Debug)]
pub struct InflightFragmentInfo {
    pub fragment_id: FragmentId,
    pub distribution_type: DistributionType,
    pub fragment_type_mask: FragmentTypeMask,
    pub vnode_count: usize,
    pub nodes: PbStreamNode,
    pub actors: HashMap<ActorId, InflightActorInfo>,
    pub state_table_ids: HashSet<TableId>,
}

#[derive(Clone, Debug)]
pub struct FragmentServingInfo {
    /// The query-visible result table for this job, if it has one.
    /// Sink/source jobs have no serving target.
    pub result_table_id: Option<TableId>,
    pub distribution_type: FragmentDistributionType,
    pub vnode_count: usize,
}

#[easy_ext::ext(FragmentTypeMaskExt)]
pub impl FragmentTypeMask {
    /// Matches fragments whose creation progress cannot be recovered after a failure.
    fn contains_non_recoverable_fragment() -> SimpleExpr {
        Self::intersects(FragmentTypeFlag::Values)
    }

    fn intersects(flag: FragmentTypeFlag) -> SimpleExpr {
        Expr::col(fragment::Column::FragmentTypeMask)
            .bit_and(Expr::value(flag as i32))
            .ne(0)
    }

    fn intersects_any(flags: impl IntoIterator<Item = FragmentTypeFlag>) -> SimpleExpr {
        Expr::col(fragment::Column::FragmentTypeMask)
            .bit_and(Expr::value(FragmentTypeFlag::raw_flag(flags) as i32))
            .ne(0)
    }

    fn disjoint(flag: FragmentTypeFlag) -> SimpleExpr {
        Expr::col(fragment::Column::FragmentTypeMask)
            .bit_and(Expr::value(flag as i32))
            .eq(0)
    }
}

#[derive(Clone, Debug, FromQueryResult, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")] // for dashboard
pub struct StreamingJobInfo {
    pub job_id: JobId,
    pub obj_type: ObjectType,
    pub name: String,
    pub job_status: JobStatus,
    pub parallelism: StreamingParallelism,
    pub adaptive_parallelism_strategy: Option<String>,
    // These backfill fields are only used to derive the effective parallelism shown for
    // in-progress jobs. They are skipped in JSON responses so existing dashboard payloads keep
    // the same schema.
    #[serde(skip)]
    pub backfill_parallelism: Option<StreamingParallelism>,
    #[serde(skip)]
    pub backfill_adaptive_parallelism_strategy: Option<String>,
    pub max_parallelism: i32,
    pub resource_group: String,
    pub config_override: String,
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
}

impl NotificationManager {
    /// Notify frontend about streaming worker slot mapping changes.
    ///
    /// This only sends streaming mapping updates to frontends. Serving mapping
    /// notifications are decoupled and driven by fragment model changes (insert/delete)
    /// instead of barrier-driven actor set changes.
    pub(crate) fn notify_streaming_fragment_mapping(
        &self,
        operation: NotificationOperation,
        fragment_mappings: Vec<PbFragmentWorkerSlotMapping>,
    ) {
        if fragment_mappings.is_empty() {
            return;
        }
        // notify all fragment mappings to frontend.
        for fragment_mapping in fragment_mappings {
            self.notify_frontend_without_version(
                operation,
                NotificationInfo::StreamingWorkerSlotMapping(fragment_mapping),
            );
        }
    }

    /// Notify the serving module about fragment mapping changes.
    ///
    /// This should be called when fragments are inserted into or deleted from the meta store,
    /// so that serving vnode mappings are kept in sync with the fragment model.
    pub(crate) fn notify_serving_fragment_mapping_update(
        &self,
        fragment_ids: Vec<crate::model::FragmentId>,
    ) {
        if fragment_ids.is_empty() {
            return;
        }
        self.notify_local_subscribers(LocalNotification::ServingFragmentMappingsUpsert(
            fragment_ids,
        ));
    }

    /// Notify the serving module about fragment mapping deletions.
    ///
    /// This should be called when fragments are deleted from the meta store,
    /// so that serving vnode mappings are cleaned up.
    pub(crate) fn notify_serving_fragment_mapping_delete(
        &self,
        fragment_ids: Vec<crate::model::FragmentId>,
    ) {
        if fragment_ids.is_empty() {
            return;
        }
        self.notify_local_subscribers(LocalNotification::ServingFragmentMappingsDelete(
            fragment_ids,
        ));
    }
}

impl CatalogControllerInner {
    /// Returns distribution type, vnode count and query-visible result table for all fragments.
    ///
    /// Reads directly from the persistent catalog rather than from the in-memory
    /// `shared_actor_infos`. This is critical because the serving vnode mapping
    /// must be available even before recovery has populated `shared_actor_infos`.
    pub async fn fragment_serving_infos(
        &self,
    ) -> MetaResult<HashMap<FragmentId, FragmentServingInfo>> {
        let query = FragmentModel::find().select_only().columns([
            fragment::Column::FragmentId,
            fragment::Column::JobId,
            fragment::Column::DistributionType,
            fragment::Column::VnodeCount,
            fragment::Column::StateTableIds,
        ]);
        let fragments: Vec<(FragmentId, JobId, DistributionType, i32, TableIdArray)> =
            query.into_tuple().all(&self.db).await?;

        Ok(fragments
            .into_iter()
            .map(
                |(fragment_id, job_id, distribution_type, vnode_count, state_table_ids)| {
                    // Table/MV/index result tables reuse the job id and appear only in their
                    // owning fragment; sink/source jobs therefore have no matching state table.
                    let result_table_id = job_id.as_mv_table_id();
                    (
                        fragment_id,
                        FragmentServingInfo {
                            result_table_id: state_table_ids
                                .0
                                .contains(&result_table_id)
                                .then_some(result_table_id),
                            distribution_type: PbFragmentDistributionType::from(distribution_type),
                            vnode_count: vnode_count as usize,
                        },
                    )
                },
            )
            .collect())
    }
}

impl CatalogController {
    pub fn prepare_fragment_models_from_fragments(
        job_id: JobId,
        fragments: impl Iterator<Item = &Fragment>,
    ) -> MetaResult<Vec<fragment::Model>> {
        fragments
            .map(|fragment| Self::prepare_fragment_model_for_new_job(job_id, fragment))
            .try_collect()
    }

    pub fn prepare_fragment_model_for_new_job(
        job_id: JobId,
        fragment: &Fragment,
    ) -> MetaResult<fragment::Model> {
        let vnode_count = fragment.vnode_count();
        let Fragment {
            fragment_id: pb_fragment_id,
            fragment_type_mask: pb_fragment_type_mask,
            distribution_type: pb_distribution_type,
            state_table_ids: pb_state_table_ids,
            nodes,
            ..
        } = fragment;

        let state_table_ids = pb_state_table_ids.clone().into();

        let fragment_parallelism = nodes
            .node_body
            .as_ref()
            .and_then(|body| match body {
                NodeBody::StreamCdcScan(node) => Some(node),
                _ => None,
            })
            .and_then(|node| node.options.as_ref())
            .map(CdcScanOptions::from_proto)
            .filter(|opts| opts.is_parallelized_backfill())
            .map(|opts| StreamingParallelism::Fixed(opts.backfill_parallelism as usize));

        let stream_node = StreamNode::from(nodes);

        let distribution_type = PbFragmentDistributionType::try_from(*pb_distribution_type)
            .unwrap()
            .into();

        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: *pb_fragment_id as _,
            job_id,
            fragment_type_mask: (*pb_fragment_type_mask).into(),
            distribution_type,
            stream_node,
            state_table_ids,
            upstream_fragment_id: Default::default(),
            vnode_count: vnode_count as _,
            parallelism: fragment_parallelism,
        };

        Ok(fragment)
    }

    #[expect(clippy::type_complexity)]
    fn compose_table_fragments(
        job_id: JobId,
        state: PbState,
        ctx: StreamContext,
        fragments: Vec<(fragment::Model, Vec<ActorInfo>)>,
        max_parallelism: usize,
        job_definition: Option<String>,
    ) -> MetaResult<(
        StreamJobFragments,
        HashMap<FragmentId, Vec<StreamActor>>,
        HashMap<crate::model::ActorId, PbActorStatus>,
    )> {
        let mut pb_fragments = BTreeMap::new();
        let mut fragment_actors = HashMap::new();
        let mut all_actor_status = HashMap::new();

        for (fragment, actors) in fragments {
            let (fragment, actors, actor_status, _) =
                Self::compose_fragment(fragment, actors, job_definition.clone())?;
            let fragment_id = fragment.fragment_id;
            fragment_actors.insert(fragment_id, actors);
            all_actor_status.extend(actor_status);

            pb_fragments.insert(fragment_id, fragment);
        }

        let table_fragments = StreamJobFragments {
            stream_job_id: job_id,
            state: state as _,
            fragments: pb_fragments,
            ctx,
            max_parallelism,
        };

        Ok((table_fragments, fragment_actors, all_actor_status))
    }

    #[expect(clippy::type_complexity)]
    fn compose_fragment(
        fragment: fragment::Model,
        actors: Vec<ActorInfo>,
        job_definition: Option<String>,
    ) -> MetaResult<(
        Fragment,
        Vec<StreamActor>,
        HashMap<crate::model::ActorId, PbActorStatus>,
        HashMap<crate::model::ActorId, PbConnectorSplits>,
    )> {
        let fragment::Model {
            fragment_id,
            fragment_type_mask,
            distribution_type,
            stream_node,
            state_table_ids,
            vnode_count,
            ..
        } = fragment;

        let stream_node = stream_node.to_protobuf();
        let mut upstream_fragments = HashSet::new();
        visit_stream_node_body(&stream_node, |body| {
            if let NodeBody::Merge(m) = body {
                assert!(
                    upstream_fragments.insert(m.upstream_fragment_id),
                    "non-duplicate upstream fragment"
                );
            }
        });

        let mut pb_actors = vec![];

        let mut pb_actor_status = HashMap::new();
        let mut pb_actor_splits = HashMap::new();

        for actor in actors {
            if actor.fragment_id != fragment_id {
                bail!(
                    "fragment id {} from actor {} is different from fragment {}",
                    actor.fragment_id,
                    actor.actor_id,
                    fragment_id
                )
            }

            let ActorInfo {
                actor_id,
                fragment_id,
                worker_id,
                splits,
                vnode_bitmap,
                expr_context,
                config_override,
                ..
            } = actor;

            let vnode_bitmap =
                vnode_bitmap.map(|vnode_bitmap| Bitmap::from(vnode_bitmap.to_protobuf()));
            let pb_expr_context = Some(expr_context.to_protobuf());

            pb_actor_status.insert(
                actor_id as _,
                PbActorStatus {
                    location: PbActorLocation::from_worker(worker_id),
                },
            );

            pb_actor_splits.insert(actor_id as _, splits.to_protobuf());

            pb_actors.push(StreamActor {
                actor_id: actor_id as _,
                fragment_id: fragment_id as _,
                vnode_bitmap,
                mview_definition: job_definition.clone().unwrap_or("".to_owned()),
                expr_context: pb_expr_context,
                config_override,
            })
        }

        let pb_state_table_ids = state_table_ids.0;
        let pb_distribution_type = PbFragmentDistributionType::from(distribution_type) as _;
        let pb_fragment = Fragment {
            fragment_id: fragment_id as _,
            fragment_type_mask: fragment_type_mask.into(),
            distribution_type: pb_distribution_type,
            state_table_ids: pb_state_table_ids,
            maybe_vnode_count: VnodeCount::set(vnode_count).to_protobuf(),
            nodes: stream_node,
        };

        Ok((pb_fragment, pb_actors, pb_actor_status, pb_actor_splits))
    }

    pub async fn fragment_serving_infos(
        &self,
    ) -> MetaResult<HashMap<FragmentId, FragmentServingInfo>> {
        let inner = self.inner.read().await;
        inner.fragment_serving_infos().await
    }

    pub async fn fragment_job_mapping(&self) -> MetaResult<HashMap<FragmentId, JobId>> {
        let inner = self.inner.read().await;
        let fragment_jobs: Vec<(FragmentId, JobId)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::JobId])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(fragment_jobs.into_iter().collect())
    }

    pub async fn get_fragment_job_id(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;
        self.get_fragment_job_id_in_txn(&inner.db, fragment_ids)
            .await
    }

    pub async fn get_fragment_job_id_in_txn<C>(
        &self,
        txn: &C,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<Vec<ObjectId>>
    where
        C: ConnectionTrait + Send,
    {
        let object_ids: Vec<ObjectId> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::JobId)
            .filter(fragment::Column::FragmentId.is_in(fragment_ids))
            .into_tuple()
            .all(txn)
            .await?;

        Ok(object_ids)
    }

    pub async fn get_fragment_desc_by_id(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<Option<(FragmentDesc, Vec<FragmentId>)>> {
        let inner = self.inner.read().await;

        let fragment_model = match FragmentModel::find_by_id(fragment_id)
            .one(&inner.db)
            .await?
        {
            Some(fragment) => fragment,
            None => return Ok(None),
        };

        let job_parallelism: Option<(StreamingParallelism, Option<String>)> =
            StreamingJob::find_by_id(fragment_model.job_id)
                .select_only()
                .columns([
                    streaming_job::Column::Parallelism,
                    streaming_job::Column::AdaptiveParallelismStrategy,
                ])
                .into_tuple::<(StreamingParallelism, Option<String>)>()
                .one(&inner.db)
                .await?;

        let upstream_entries: Vec<(FragmentId, DispatcherType)> = FragmentRelation::find()
            .select_only()
            .columns([
                fragment_relation::Column::SourceFragmentId,
                fragment_relation::Column::DispatcherType,
            ])
            .filter(fragment_relation::Column::TargetFragmentId.eq(fragment_id))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let upstreams: Vec<_> = upstream_entries
            .into_iter()
            .map(|(source_id, _)| source_id)
            .collect();

        let root_fragment_map = find_fragment_no_shuffle_dags_detailed(&inner.db, &[fragment_id])
            .await
            .map(Self::collect_root_fragment_mapping)?;
        let root_fragments = root_fragment_map
            .get(&fragment_id)
            .cloned()
            .unwrap_or_default();

        let info = self.env.shared_actor_infos().read_guard();
        let SharedFragmentInfo { actors, .. } = info
            .get_fragment(fragment_model.fragment_id as _)
            .unwrap_or_else(|| {
                panic!(
                    "Failed to retrieve fragment description: fragment {} (job_id {}) not found in shared actor info",
                    fragment_model.fragment_id,
                    fragment_model.job_id
                )
            });

        let parallelism_policy = Self::format_fragment_parallelism_policy(
            fragment_model.distribution_type,
            fragment_model.parallelism.as_ref(),
            job_parallelism.as_ref().map(|(parallelism, _)| parallelism),
            job_parallelism
                .as_ref()
                .and_then(|(_, strategy)| strategy.as_deref()),
            &root_fragments,
        );

        let fragment = FragmentDesc {
            fragment_id: fragment_model.fragment_id,
            job_id: fragment_model.job_id,
            fragment_type_mask: fragment_model.fragment_type_mask,
            distribution_type: fragment_model.distribution_type,
            state_table_ids: fragment_model.state_table_ids.clone(),
            parallelism: actors.len() as _,
            vnode_count: fragment_model.vnode_count,
            stream_node: fragment_model.stream_node.clone(),
            parallelism_policy,
        };

        Ok(Some((fragment, upstreams)))
    }

    pub async fn list_fragment_database_ids(
        &self,
        select_fragment_ids: Option<Vec<FragmentId>>,
    ) -> MetaResult<Vec<(FragmentId, DatabaseId)>> {
        let inner = self.inner.read().await;
        let select = FragmentModel::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .column(object::Column::DatabaseId)
            .join(JoinType::InnerJoin, fragment::Relation::Object.def());
        let select = if let Some(select_fragment_ids) = select_fragment_ids {
            select.filter(fragment::Column::FragmentId.is_in(select_fragment_ids))
        } else {
            select
        };
        Ok(select.into_tuple().all(&inner.db).await?)
    }

    pub async fn get_job_fragments_by_id(
        &self,
        job_id: JobId,
    ) -> MetaResult<(
        StreamJobFragments,
        HashMap<FragmentId, Vec<StreamActor>>,
        HashMap<ActorId, PbActorStatus>,
    )> {
        let inner = self.inner.read().await;

        // Load fragments matching the job from the database
        let fragments: Vec<_> = FragmentModel::find()
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;

        let job_info = StreamingJob::find_by_id(job_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;

        let fragment_actors =
            self.collect_fragment_actor_pairs(fragments, job_info.stream_context())?;

        let job_definition = resolve_streaming_job_definition(&inner.db, &HashSet::from([job_id]))
            .await?
            .remove(&job_id);

        Self::compose_table_fragments(
            job_id,
            job_info.job_status.into(),
            job_info.stream_context(),
            fragment_actors,
            job_info.max_parallelism as _,
            job_definition,
        )
    }

    pub async fn get_fragment_actor_dispatchers(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentActorDispatchers> {
        let inner = self.inner.read().await;

        self.get_fragment_actor_dispatchers_txn(&inner.db, fragment_ids)
            .await
    }

    pub async fn get_fragment_actor_dispatchers_txn(
        &self,
        c: &impl ConnectionTrait,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentActorDispatchers> {
        let fragment_relations = FragmentRelation::find()
            .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids))
            .all(c)
            .await?;

        type FragmentActorInfo = (
            DistributionType,
            Arc<HashMap<crate::model::ActorId, Option<Bitmap>>>,
        );

        let shared_info = self.env.shared_actor_infos();
        let mut fragment_actor_cache: HashMap<FragmentId, FragmentActorInfo> = HashMap::new();
        let get_fragment_actors = |fragment_id: FragmentId| async move {
            let result: MetaResult<FragmentActorInfo> = try {
                let read_guard = shared_info.read_guard();

                let fragment = read_guard.get_fragment(fragment_id as _).unwrap();

                (
                    fragment.distribution_type,
                    Arc::new(
                        fragment
                            .actors
                            .iter()
                            .map(|(actor_id, actor_info)| {
                                (
                                    *actor_id,
                                    actor_info
                                        .vnode_bitmap
                                        .as_ref()
                                        .map(|bitmap| Bitmap::from(bitmap.to_protobuf())),
                                )
                            })
                            .collect(),
                    ),
                )
            };
            result
        };

        let mut actor_dispatchers_map: HashMap<_, HashMap<_, Vec<_>>> = HashMap::new();
        for fragment_relation::Model {
            source_fragment_id,
            target_fragment_id,
            dispatcher_type,
            dist_key_indices,
            output_indices,
            output_type_mapping,
        } in fragment_relations
        {
            let (source_fragment_distribution, source_fragment_actors) = {
                let (distribution, actors) = {
                    match fragment_actor_cache.entry(source_fragment_id) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(get_fragment_actors(source_fragment_id).await?)
                        }
                    }
                };
                (*distribution, actors.clone())
            };
            let (target_fragment_distribution, target_fragment_actors) = {
                let (distribution, actors) = {
                    match fragment_actor_cache.entry(target_fragment_id) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(get_fragment_actors(target_fragment_id).await?)
                        }
                    }
                };
                (*distribution, actors.clone())
            };
            let output_mapping = PbDispatchOutputMapping {
                indices: output_indices.into_u32_array(),
                types: output_type_mapping.unwrap_or_default().to_protobuf(),
            };
            let (dispatchers, _) = compose_dispatchers(
                source_fragment_distribution,
                &source_fragment_actors,
                target_fragment_id as _,
                target_fragment_distribution,
                &target_fragment_actors,
                dispatcher_type,
                dist_key_indices.into_u32_array(),
                output_mapping,
            );
            let actor_dispatchers_map = actor_dispatchers_map
                .entry(source_fragment_id as _)
                .or_default();
            for (actor_id, dispatchers) in dispatchers {
                actor_dispatchers_map
                    .entry(actor_id as _)
                    .or_default()
                    .push(dispatchers);
            }
        }
        Ok(actor_dispatchers_map)
    }

    pub async fn get_fragment_downstream_relations(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentDownstreamRelation> {
        let inner = self.inner.read().await;
        self.get_fragment_downstream_relations_in_txn(&inner.db, fragment_ids)
            .await
    }

    pub async fn get_fragment_downstream_relations_in_txn<C>(
        &self,
        txn: &C,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentDownstreamRelation>
    where
        C: ConnectionTrait + StreamTrait + Send,
    {
        let mut stream = FragmentRelation::find()
            .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids))
            .stream(txn)
            .await?;
        let mut relations = FragmentDownstreamRelation::new();
        while let Some(relation) = stream.try_next().await? {
            relations
                .entry(relation.source_fragment_id as _)
                .or_default()
                .push(DownstreamFragmentRelation {
                    downstream_fragment_id: relation.target_fragment_id as _,
                    dispatcher_type: relation.dispatcher_type,
                    dist_key_indices: relation.dist_key_indices.into_u32_array(),
                    output_mapping: PbDispatchOutputMapping {
                        indices: relation.output_indices.into_u32_array(),
                        types: relation
                            .output_type_mapping
                            .unwrap_or_default()
                            .to_protobuf(),
                    },
                });
        }
        Ok(relations)
    }

    pub async fn get_job_fragment_backfill_scan_type(
        &self,
        job_id: JobId,
    ) -> MetaResult<HashMap<crate::model::FragmentId, PbStreamScanType>> {
        let inner = self.inner.read().await;
        self.get_job_fragment_backfill_scan_type_in_txn(&inner.db, job_id)
            .await
    }

    pub async fn get_job_fragment_backfill_scan_type_in_txn<C>(
        &self,
        txn: &C,
        job_id: JobId,
    ) -> MetaResult<HashMap<crate::model::FragmentId, PbStreamScanType>>
    where
        C: ConnectionTrait + Send,
    {
        let fragments: Vec<_> = FragmentModel::find()
            .filter(fragment::Column::JobId.eq(job_id))
            .all(txn)
            .await?;

        let mut result = HashMap::new();

        for fragment::Model {
            fragment_id,
            stream_node,
            ..
        } in fragments
        {
            let stream_node = stream_node.to_protobuf();
            visit_stream_node_body(&stream_node, |body| {
                if let NodeBody::StreamScan(node) = body {
                    match node.stream_scan_type() {
                        StreamScanType::Unspecified => {}
                        scan_type => {
                            result.insert(fragment_id as crate::model::FragmentId, scan_type);
                        }
                    }
                }
            });
        }

        Ok(result)
    }

    pub async fn count_streaming_jobs(&self) -> MetaResult<usize> {
        let inner = self.inner.read().await;
        let count = StreamingJob::find().count(&inner.db).await?;
        Ok(usize::try_from(count).context("streaming job count overflow")?)
    }

    pub async fn list_streaming_job_infos(&self) -> MetaResult<Vec<StreamingJobInfo>> {
        let inner = self.inner.read().await;
        let job_states = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .column(object::Column::ObjType)
            .join(JoinType::LeftJoin, table::Relation::Object1.def().rev())
            .join(JoinType::LeftJoin, source::Relation::Object.def().rev())
            .join(JoinType::LeftJoin, sink::Relation::Object.def().rev())
            .column_as(
                Expr::if_null(
                    Expr::col((table::Entity, table::Column::Name)),
                    Expr::if_null(
                        Expr::col((source::Entity, source::Column::Name)),
                        Expr::if_null(
                            Expr::col((sink::Entity, sink::Column::Name)),
                            Expr::val("<unknown>"),
                        ),
                    ),
                ),
                "name",
            )
            .columns([
                streaming_job::Column::JobStatus,
                streaming_job::Column::Parallelism,
                streaming_job::Column::AdaptiveParallelismStrategy,
                streaming_job::Column::BackfillParallelism,
                streaming_job::Column::BackfillAdaptiveParallelismStrategy,
                streaming_job::Column::MaxParallelism,
            ])
            .column_as(
                Expr::if_null(
                    Expr::col((
                        streaming_job::Entity,
                        streaming_job::Column::SpecificResourceGroup,
                    )),
                    Expr::col((database::Entity, database::Column::ResourceGroup)),
                ),
                "resource_group",
            )
            .column_as(
                Expr::if_null(
                    Expr::col((streaming_job::Entity, streaming_job::Column::ConfigOverride)),
                    Expr::val(""),
                ),
                "config_override",
            )
            .column(object::Column::DatabaseId)
            .column(object::Column::SchemaId)
            .into_model()
            .all(&inner.db)
            .await?;
        Ok(job_states)
    }

    pub async fn get_max_parallelism_by_id(&self, job_id: JobId) -> MetaResult<usize> {
        let inner = self.inner.read().await;
        let max_parallelism: i32 = StreamingJob::find_by_id(job_id)
            .select_only()
            .column(streaming_job::Column::MaxParallelism)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;
        Ok(max_parallelism as usize)
    }

    /// Try to get internal table ids of each streaming job, used by metrics collection.
    pub async fn get_job_internal_table_ids(&self) -> Option<Vec<(JobId, Vec<TableId>)>> {
        if let Ok(inner) = self.inner.try_read()
            && let Ok(job_state_tables) = FragmentModel::find()
                .select_only()
                .columns([fragment::Column::JobId, fragment::Column::StateTableIds])
                .into_tuple::<(JobId, I32Array)>()
                .all(&inner.db)
                .await
        {
            let mut job_internal_table_ids = HashMap::new();
            for (job_id, state_table_ids) in job_state_tables {
                job_internal_table_ids
                    .entry(job_id)
                    .or_insert_with(Vec::new)
                    .extend(
                        state_table_ids
                            .into_inner()
                            .into_iter()
                            .map(|table_id| TableId::new(table_id as _)),
                    );
            }
            return Some(job_internal_table_ids.into_iter().collect());
        }
        None
    }

    pub async fn has_any_running_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = FragmentModel::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub fn worker_actor_count(&self) -> MetaResult<HashMap<WorkerId, usize>> {
        let read_guard = self.env.shared_actor_infos().read_guard();
        let actor_cnt: HashMap<WorkerId, _> = read_guard
            .iter_over_fragments()
            .flat_map(|(_, fragment)| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, actor)| (actor.worker_id, *actor_id))
            })
            .into_group_map()
            .into_iter()
            .map(|(k, v)| (k, v.len()))
            .collect();

        Ok(actor_cnt)
    }

    fn collect_fragment_actor_map(
        &self,
        fragment_ids: &[FragmentId],
        stream_context: StreamContext,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorInfo>>> {
        let guard = self.env.shared_actor_infos().read_guard();
        let pb_expr_context = stream_context.to_expr_context();
        let expr_context: ExprContext = (&pb_expr_context).into();

        let mut actor_map = HashMap::with_capacity(fragment_ids.len());
        for fragment_id in fragment_ids {
            let fragment_info = guard.get_fragment(*fragment_id as _).ok_or_else(|| {
                anyhow!("fragment {} not found in shared actor info", fragment_id)
            })?;

            let actors = fragment_info
                .actors
                .iter()
                .map(|(actor_id, actor_info)| ActorInfo {
                    actor_id: *actor_id as _,
                    fragment_id: *fragment_id,
                    splits: ConnectorSplits::from(&PbConnectorSplits {
                        splits: actor_info.splits.iter().map(ConnectorSplit::from).collect(),
                    }),
                    worker_id: actor_info.worker_id as _,
                    vnode_bitmap: actor_info
                        .vnode_bitmap
                        .as_ref()
                        .map(|bitmap| VnodeBitmap::from(&bitmap.to_protobuf())),
                    expr_context: expr_context.clone(),
                    config_override: stream_context.config_override.clone(),
                })
                .collect();

            actor_map.insert(*fragment_id, actors);
        }

        Ok(actor_map)
    }

    fn collect_fragment_actor_pairs(
        &self,
        fragments: Vec<fragment::Model>,
        stream_context: StreamContext,
    ) -> MetaResult<Vec<(fragment::Model, Vec<ActorInfo>)>> {
        let fragment_ids: Vec<_> = fragments.iter().map(|f| f.fragment_id).collect();
        let mut actor_map = self.collect_fragment_actor_map(&fragment_ids, stream_context)?;
        fragments
            .into_iter()
            .map(|fragment| {
                let actors = actor_map.remove(&fragment.fragment_id).ok_or_else(|| {
                    anyhow!(
                        "fragment {} missing in shared actor info map",
                        fragment.fragment_id
                    )
                })?;
                Ok((fragment, actors))
            })
            .collect()
    }

    // TODO: This function is too heavy, we should avoid using it and implement others on demand.
    pub async fn table_fragments(
        &self,
    ) -> MetaResult<
        BTreeMap<
            JobId,
            (
                StreamJobFragments,
                HashMap<FragmentId, Vec<StreamActor>>,
                HashMap<crate::model::ActorId, PbActorStatus>,
            ),
        >,
    > {
        let inner = self.inner.read().await;
        let jobs = StreamingJob::find().all(&inner.db).await?;

        let mut job_definition = resolve_streaming_job_definition(
            &inner.db,
            &HashSet::from_iter(jobs.iter().map(|job| job.job_id)),
        )
        .await?;

        let mut table_fragments = BTreeMap::new();
        for job in jobs {
            let fragments = FragmentModel::find()
                .filter(fragment::Column::JobId.eq(job.job_id))
                .all(&inner.db)
                .await?;

            let fragment_actors =
                self.collect_fragment_actor_pairs(fragments, job.stream_context())?;

            table_fragments.insert(
                job.job_id,
                Self::compose_table_fragments(
                    job.job_id,
                    job.job_status.into(),
                    job.stream_context(),
                    fragment_actors,
                    job.max_parallelism as _,
                    job_definition.remove(&job.job_id),
                )?,
            );
        }

        Ok(table_fragments)
    }

    pub async fn upstream_fragments(
        &self,
        fragment_ids: impl Iterator<Item = crate::model::FragmentId>,
    ) -> MetaResult<HashMap<crate::model::FragmentId, HashSet<crate::model::FragmentId>>> {
        let inner = self.inner.read().await;
        self.upstream_fragments_in_txn(&inner.db, fragment_ids)
            .await
    }

    pub async fn upstream_fragments_in_txn<C>(
        &self,
        txn: &C,
        fragment_ids: impl Iterator<Item = crate::model::FragmentId>,
    ) -> MetaResult<HashMap<crate::model::FragmentId, HashSet<crate::model::FragmentId>>>
    where
        C: ConnectionTrait + StreamTrait + Send,
    {
        let mut stream = FragmentRelation::find()
            .select_only()
            .columns([
                fragment_relation::Column::SourceFragmentId,
                fragment_relation::Column::TargetFragmentId,
            ])
            .filter(
                fragment_relation::Column::TargetFragmentId
                    .is_in(fragment_ids.map(|id| id as FragmentId)),
            )
            .into_tuple::<(FragmentId, FragmentId)>()
            .stream(txn)
            .await?;
        let mut upstream_fragments: HashMap<_, HashSet<_>> = HashMap::new();
        while let Some((upstream_fragment_id, downstream_fragment_id)) = stream.try_next().await? {
            upstream_fragments
                .entry(downstream_fragment_id as crate::model::FragmentId)
                .or_default()
                .insert(upstream_fragment_id as crate::model::FragmentId);
        }
        Ok(upstream_fragments)
    }

    pub fn list_actor_locations(&self) -> MetaResult<Vec<PartialActorLocation>> {
        let info = self.env.shared_actor_infos().read_guard();

        let actor_locations = info
            .iter_over_fragments()
            .flat_map(|(fragment_id, fragment)| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, actor)| PartialActorLocation {
                        actor_id: *actor_id as _,
                        fragment_id: *fragment_id as _,
                        worker_id: actor.worker_id,
                    })
            })
            .collect_vec();

        Ok(actor_locations)
    }

    pub async fn list_actor_info(
        &self,
    ) -> MetaResult<Vec<(ActorId, FragmentId, ObjectId, SchemaId, ObjectType)>> {
        let inner = self.inner.read().await;

        let fragment_objects: Vec<(FragmentId, ObjectId, SchemaId, ObjectType)> =
            FragmentModel::find()
                .select_only()
                .join(JoinType::LeftJoin, fragment::Relation::Object.def())
                .column(fragment::Column::FragmentId)
                .column_as(object::Column::Oid, "job_id")
                .column_as(object::Column::SchemaId, "schema_id")
                .column_as(object::Column::ObjType, "type")
                .into_tuple()
                .all(&inner.db)
                .await?;

        let actor_infos = {
            let info = self.env.shared_actor_infos().read_guard();

            let mut result = Vec::new();

            for (fragment_id, object_id, schema_id, object_type) in fragment_objects {
                let Some(fragment) = info.get_fragment(fragment_id as _) else {
                    return Err(MetaError::unavailable(format!(
                        "shared actor info missing for fragment {fragment_id} while listing actors"
                    )));
                };

                for actor_id in fragment.actors.keys() {
                    result.push((
                        *actor_id as _,
                        fragment.fragment_id as _,
                        object_id,
                        schema_id,
                        object_type,
                    ));
                }
            }

            result
        };

        Ok(actor_infos)
    }

    pub fn get_worker_slot_mappings(&self) -> Vec<PbFragmentWorkerSlotMapping> {
        let guard = self.env.shared_actor_info.read_guard();
        guard
            .iter_over_fragments()
            .map(|(_, fragment)| rebuild_fragment_mapping(fragment))
            .collect_vec()
    }

    pub async fn list_fragment_descs_with_node(
        &self,
        is_creating: bool,
    ) -> MetaResult<Vec<(FragmentDistribution, Vec<FragmentId>)>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments_query = Self::build_fragment_query(is_creating);
        let fragments: Vec<fragment::Model> = fragments_query.all(&txn).await?;

        let rows = fragments
            .into_iter()
            .map(|fragment| FragmentDescRow {
                fragment_id: fragment.fragment_id,
                job_id: fragment.job_id,
                fragment_type_mask: fragment.fragment_type_mask,
                distribution_type: fragment.distribution_type,
                state_table_ids: fragment.state_table_ids,
                vnode_count: fragment.vnode_count,
                parallelism_override: fragment.parallelism,
                stream_node: Some(fragment.stream_node),
            })
            .collect_vec();

        self.build_fragment_distributions(&txn, rows).await
    }

    pub async fn list_fragment_descs_without_node(
        &self,
        is_creating: bool,
    ) -> MetaResult<Vec<(FragmentDistribution, Vec<FragmentId>)>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments_query = Self::build_fragment_query(is_creating);
        #[expect(clippy::type_complexity)]
        let fragments: Vec<(
            FragmentId,
            JobId,
            i32,
            DistributionType,
            TableIdArray,
            i32,
            Option<StreamingParallelism>,
        )> = fragments_query
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::DistributionType,
                fragment::Column::StateTableIds,
                fragment::Column::VnodeCount,
                fragment::Column::Parallelism,
            ])
            .into_tuple()
            .all(&txn)
            .await?;

        let rows = fragments
            .into_iter()
            .map(
                |(
                    fragment_id,
                    job_id,
                    fragment_type_mask,
                    distribution_type,
                    state_table_ids,
                    vnode_count,
                    parallelism_override,
                )| FragmentDescRow {
                    fragment_id,
                    job_id,
                    fragment_type_mask,
                    distribution_type,
                    state_table_ids,
                    vnode_count,
                    parallelism_override,
                    stream_node: None,
                },
            )
            .collect_vec();

        self.build_fragment_distributions(&txn, rows).await
    }

    fn build_fragment_query(is_creating: bool) -> sea_orm::Select<fragment::Entity> {
        if is_creating {
            FragmentModel::find()
                .join(JoinType::LeftJoin, fragment::Relation::Object.def())
                .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
                .filter(
                    streaming_job::Column::JobStatus
                        .eq(JobStatus::Initial)
                        .or(streaming_job::Column::JobStatus.eq(JobStatus::Creating)),
                )
        } else {
            FragmentModel::find()
        }
    }

    async fn build_fragment_distributions(
        &self,
        txn: &DatabaseTransaction,
        rows: Vec<FragmentDescRow>,
    ) -> MetaResult<Vec<(FragmentDistribution, Vec<FragmentId>)>> {
        let fragment_ids = rows.iter().map(|row| row.fragment_id).collect_vec();
        let job_ids = rows.iter().map(|row| row.job_id).unique().collect_vec();

        let job_parallelisms: HashMap<JobId, (StreamingParallelism, Option<String>)> =
            if fragment_ids.is_empty() {
                HashMap::new()
            } else {
                StreamingJob::find()
                    .select_only()
                    .columns([
                        streaming_job::Column::JobId,
                        streaming_job::Column::Parallelism,
                        streaming_job::Column::AdaptiveParallelismStrategy,
                    ])
                    .filter(streaming_job::Column::JobId.is_in(job_ids))
                    .into_tuple::<(JobId, StreamingParallelism, Option<String>)>()
                    .all(txn)
                    .await?
                    .into_iter()
                    .map(|(job_id, parallelism, strategy)| (job_id, (parallelism, strategy)))
                    .collect()
            };

        let upstream_entries: Vec<(FragmentId, FragmentId, DispatcherType)> =
            if fragment_ids.is_empty() {
                Vec::new()
            } else {
                FragmentRelation::find()
                    .select_only()
                    .columns([
                        fragment_relation::Column::TargetFragmentId,
                        fragment_relation::Column::SourceFragmentId,
                        fragment_relation::Column::DispatcherType,
                    ])
                    .filter(fragment_relation::Column::TargetFragmentId.is_in(fragment_ids.clone()))
                    .into_tuple()
                    .all(txn)
                    .await?
            };

        let mut all_upstreams: HashMap<FragmentId, Vec<FragmentId>> = HashMap::new();
        for (target_id, source_id, _) in upstream_entries {
            all_upstreams.entry(target_id).or_default().push(source_id);
        }

        let root_fragment_map = if fragment_ids.is_empty() {
            HashMap::new()
        } else {
            let ensembles = find_fragment_no_shuffle_dags_detailed(txn, &fragment_ids).await?;
            Self::collect_root_fragment_mapping(ensembles)
        };

        let guard = self.env.shared_actor_info.read_guard();
        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let parallelism = guard
                .get_fragment(row.fragment_id as _)
                .map(|fragment| fragment.actors.len())
                .unwrap_or_default();

            let root_fragments = root_fragment_map
                .get(&row.fragment_id)
                .cloned()
                .unwrap_or_default();

            let upstreams = all_upstreams.remove(&row.fragment_id).unwrap_or_default();

            let parallelism_policy = Self::format_fragment_parallelism_policy(
                row.distribution_type,
                row.parallelism_override.as_ref(),
                job_parallelisms
                    .get(&row.job_id)
                    .map(|(parallelism, _)| parallelism),
                job_parallelisms
                    .get(&row.job_id)
                    .and_then(|(_, strategy)| strategy.as_deref()),
                &root_fragments,
            );

            let fragment = FragmentDistribution {
                fragment_id: row.fragment_id,
                table_id: row.job_id,
                distribution_type: PbFragmentDistributionType::from(row.distribution_type) as _,
                state_table_ids: row.state_table_ids.0,
                upstream_fragment_ids: upstreams.clone(),
                fragment_type_mask: row.fragment_type_mask as _,
                parallelism: parallelism as _,
                vnode_count: row.vnode_count as _,
                node: row.stream_node.map(|node| node.to_protobuf()),
                parallelism_policy,
            };

            result.push((fragment, upstreams));
        }

        Ok(result)
    }

    pub async fn list_sink_log_store_tables(&self) -> MetaResult<Vec<(SinkId, TableId)>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(JobId, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::JobId, fragment::Column::StreamNode])
            .filter(FragmentTypeMask::intersects(FragmentTypeFlag::Sink))
            .into_tuple()
            .all(&txn)
            .await?;

        let mut mapping: HashMap<SinkId, TableId> = HashMap::new();

        for (job_id, stream_node) in fragments {
            let sink_id = SinkId::new(job_id.as_raw_id());
            let stream_node = stream_node.to_protobuf();
            let mut internal_table_id: Option<TableId> = None;

            visit_stream_node_body(&stream_node, |body| {
                if let NodeBody::Sink(node) = body
                    && node.log_store_type == SinkLogStoreType::KvLogStore as i32
                    && let Some(table) = &node.table
                {
                    internal_table_id = Some(TableId::new(table.id.as_raw_id()));
                }
            });

            if let Some(table_id) = internal_table_id
                && let Some(existing) = mapping.insert(sink_id, table_id)
                && existing != table_id
            {
                tracing::warn!(
                    "sink {sink_id:?} has multiple log store tables: {existing:?} vs {table_id:?}"
                );
            }
        }

        Ok(mapping.into_iter().collect())
    }

    /// Build a fragment-to-root lookup for all reported root fragment ensembles.
    fn collect_root_fragment_mapping(
        ensembles: Vec<NoShuffleEnsemble>,
    ) -> HashMap<FragmentId, Vec<FragmentId>> {
        let mut mapping = HashMap::new();

        for ensemble in ensembles {
            let mut roots: Vec<_> = ensemble.entry_fragments().collect();
            roots.sort_unstable();
            roots.dedup();

            if roots.is_empty() {
                continue;
            }

            let root_set: HashSet<_> = roots.iter().copied().collect();

            for fragment_id in ensemble.component_fragments() {
                if root_set.contains(&fragment_id) {
                    mapping.insert(fragment_id, Vec::new());
                } else {
                    mapping.insert(fragment_id, roots.clone());
                }
            }
        }

        mapping
    }

    fn format_fragment_parallelism_policy(
        distribution_type: DistributionType,
        fragment_parallelism: Option<&StreamingParallelism>,
        job_parallelism: Option<&StreamingParallelism>,
        job_adaptive_parallelism_strategy: Option<&str>,
        root_fragments: &[FragmentId],
    ) -> String {
        if distribution_type == DistributionType::Single {
            return "single".to_owned();
        }

        if let Some(parallelism) = fragment_parallelism {
            return format!(
                "override({})",
                Self::format_streaming_parallelism(parallelism, job_adaptive_parallelism_strategy)
            );
        }

        if !root_fragments.is_empty() {
            let mut upstreams = root_fragments.to_vec();
            upstreams.sort_unstable();
            upstreams.dedup();

            return format!("upstream_fragment({upstreams:?})");
        }

        let inherited = job_parallelism
            .map(|parallelism| {
                Self::format_streaming_parallelism(parallelism, job_adaptive_parallelism_strategy)
            })
            .unwrap_or_else(|| "unknown".to_owned());
        format!("inherit({inherited})")
    }

    fn format_streaming_parallelism(
        parallelism: &StreamingParallelism,
        adaptive_parallelism_strategy: Option<&str>,
    ) -> String {
        match parallelism {
            StreamingParallelism::Adaptive => adaptive_parallelism_strategy
                .and_then(Self::format_adaptive_parallelism_strategy)
                .unwrap_or_else(|| "adaptive".to_owned()),
            StreamingParallelism::Fixed(n) => n.to_string(),
            StreamingParallelism::Custom => adaptive_parallelism_strategy
                .and_then(Self::format_adaptive_parallelism_strategy)
                .unwrap_or_else(|| "custom".to_owned()),
        }
    }

    fn format_adaptive_parallelism_strategy(strategy: &str) -> Option<String> {
        parse_strategy(strategy)
            .ok()
            .map(|strategy| match strategy {
                AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full => {
                    "adaptive".to_owned()
                }
                AdaptiveParallelismStrategy::Bounded(n) => format!("bounded({n})"),
                AdaptiveParallelismStrategy::Ratio(r) => format!("ratio({r})"),
            })
    }

    pub async fn list_sink_actor_mapping(
        &self,
    ) -> MetaResult<HashMap<SinkId, (String, Vec<ActorId>)>> {
        let inner = self.inner.read().await;
        let sink_id_names: Vec<(SinkId, String)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .into_tuple()
            .all(&inner.db)
            .await?;
        let (sink_ids, _): (Vec<_>, Vec<_>) = sink_id_names.iter().cloned().unzip();

        let sink_name_mapping: HashMap<SinkId, String> = sink_id_names.into_iter().collect();

        let actor_with_type: Vec<(ActorId, SinkId)> = {
            let info = self.env.shared_actor_infos().read_guard();

            info.iter_over_fragments()
                .filter(|(_, fragment)| {
                    sink_ids.contains(&fragment.job_id.as_sink_id())
                        && fragment.fragment_type_mask.contains(FragmentTypeFlag::Sink)
                })
                .flat_map(|(_, fragment)| {
                    fragment
                        .actors
                        .keys()
                        .map(move |actor_id| (*actor_id as _, fragment.job_id.as_sink_id()))
                })
                .collect()
        };

        let mut sink_actor_mapping = HashMap::new();
        for (actor_id, sink_id) in actor_with_type {
            sink_actor_mapping
                .entry(sink_id)
                .or_insert_with(|| (sink_name_mapping.get(&sink_id).unwrap().clone(), vec![]))
                .1
                .push(actor_id);
        }

        Ok(sink_actor_mapping)
    }

    pub async fn list_fragment_state_tables(&self) -> MetaResult<Vec<PartialFragmentStateTables>> {
        let inner = self.inner.read().await;
        let fragment_state_tables: Vec<PartialFragmentStateTables> = FragmentModel::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::StateTableIds,
            ])
            .into_partial_model()
            .all(&inner.db)
            .await?;
        Ok(fragment_state_tables)
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`], load all running actor that need to be sent or
    /// collected
    pub async fn load_all_actors_dynamic(
        &self,
        database_id: Option<DatabaseId>,
        worker_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<FragmentRenderMap> {
        let loaded = self.load_fragment_context(database_id).await?;

        if loaded.is_empty() {
            return Ok(HashMap::new());
        }

        let RenderedGraph { fragments, .. } = render_actor_assignments(
            self.env.actor_id_generator(),
            worker_nodes.current(),
            &loaded,
        )?;

        tracing::trace!(?fragments, "reload all actors");

        Ok(fragments)
    }

    /// Async load stage: collects all metadata required for rendering actor assignments.
    pub async fn load_fragment_context(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<LoadedFragmentContext> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        self.load_fragment_context_in_txn(&txn, database_id).await
    }

    pub async fn load_fragment_context_in_txn<C>(
        &self,
        txn: &C,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<LoadedFragmentContext>
    where
        C: ConnectionTrait,
    {
        let mut query = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId);

        if let Some(database_id) = database_id {
            query = query
                .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
                .filter(object::Column::DatabaseId.eq(database_id));
        }

        let jobs: Vec<JobId> = query.into_tuple().all(txn).await?;

        let jobs: HashSet<JobId> = jobs.into_iter().collect();

        if jobs.is_empty() {
            return Ok(LoadedFragmentContext::default());
        }

        load_fragment_context_for_jobs(txn, jobs).await
    }

    #[await_tree::instrument]
    pub async fn fill_snapshot_backfill_epoch(
        &self,
        fragment_ids: impl Iterator<Item = FragmentId>,
        snapshot_backfill_info: Option<&SnapshotBackfillInfo>,
        cross_db_snapshot_backfill_info: &SnapshotBackfillInfo,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for fragment_id in fragment_ids {
            let fragment = FragmentModel::find_by_id(fragment_id)
                .one(&txn)
                .await?
                .context(format!("fragment {} not found", fragment_id))?;
            let mut node = fragment.stream_node.to_protobuf();
            if crate::stream::fill_snapshot_backfill_epoch(
                &mut node,
                snapshot_backfill_info,
                cross_db_snapshot_backfill_info,
            )? {
                let node = StreamNode::from(&node);
                FragmentModel::update(fragment::ActiveModel {
                    fragment_id: Set(fragment_id),
                    stream_node: Set(node),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
        }
        txn.commit().await?;
        Ok(())
    }

    /// Get the actor ids of the fragment with `fragment_id` with `Running` status.
    pub fn get_running_actors_of_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<HashSet<model::ActorId>> {
        let info = self.env.shared_actor_infos().read_guard();

        let actors = info
            .get_fragment(fragment_id as _)
            .map(|SharedFragmentInfo { actors, .. }| actors.keys().copied().collect())
            .unwrap_or_default();

        Ok(actors)
    }

    /// Get the actor ids, and each actor's upstream source actor ids of the fragment with `fragment_id` with `Running` status.
    /// (`backfill_actor_id`, `upstream_source_actor_id`)
    pub async fn get_running_actors_for_source_backfill(
        &self,
        source_backfill_fragment_id: FragmentId,
        source_fragment_id: FragmentId,
    ) -> MetaResult<Vec<(ActorId, ActorId)>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragment_relation: DispatcherType = FragmentRelation::find()
            .select_only()
            .column(fragment_relation::Column::DispatcherType)
            .filter(fragment_relation::Column::SourceFragmentId.eq(source_fragment_id))
            .filter(fragment_relation::Column::TargetFragmentId.eq(source_backfill_fragment_id))
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "no fragment connection from source fragment {} to source backfill fragment {}",
                    source_fragment_id,
                    source_backfill_fragment_id
                )
            })?;

        if fragment_relation != DispatcherType::NoShuffle {
            return Err(anyhow!("expected NoShuffle but got {:?}", fragment_relation).into());
        }

        let load_fragment_distribution_type = |txn, fragment_id: FragmentId| async move {
            let result: MetaResult<DistributionType> = try {
                FragmentModel::find_by_id(fragment_id)
                    .select_only()
                    .column(fragment::Column::DistributionType)
                    .into_tuple()
                    .one(txn)
                    .await
                    .map_err(MetaError::from)?
                    .ok_or_else(|| {
                        MetaError::from(anyhow!("failed to find fragment: {}", fragment_id))
                    })?
            };
            result
        };

        let source_backfill_distribution_type =
            load_fragment_distribution_type(&txn, source_backfill_fragment_id).await?;
        let source_distribution_type =
            load_fragment_distribution_type(&txn, source_fragment_id).await?;

        let load_fragment_actor_distribution =
            |actor_info: &SharedActorInfos,
             fragment_id: FragmentId|
             -> HashMap<crate::model::ActorId, Option<Bitmap>> {
                let guard = actor_info.read_guard();

                guard
                    .get_fragment(fragment_id as _)
                    .map(|fragment| {
                        fragment
                            .actors
                            .iter()
                            .map(|(actor_id, actor)| {
                                (
                                    *actor_id as _,
                                    actor
                                        .vnode_bitmap
                                        .as_ref()
                                        .map(|bitmap| Bitmap::from(bitmap.to_protobuf())),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };

        let source_backfill_actors: HashMap<crate::model::ActorId, Option<Bitmap>> =
            load_fragment_actor_distribution(
                self.env.shared_actor_infos(),
                source_backfill_fragment_id,
            );

        let source_actors =
            load_fragment_actor_distribution(self.env.shared_actor_infos(), source_fragment_id);

        Ok(resolve_no_shuffle_actor_mapping(
            source_distribution_type,
            source_actors.iter().map(|(&id, bitmap)| (id, bitmap)),
            source_backfill_distribution_type,
            source_backfill_actors
                .iter()
                .map(|(&id, bitmap)| (id, bitmap)),
        )
        .into_iter()
        .map(|(source_actor, source_backfill_actor)| {
            (source_backfill_actor as _, source_actor as _)
        })
        .collect())
    }

    /// Get and filter the "**root**" fragments of the specified jobs.
    /// The root fragment is the bottom-most fragment of its fragment graph, and can be a `MView` or a `Source`.
    ///
    /// Root fragment connects to downstream jobs.
    ///
    /// ## What can be the root fragment
    /// - For sink, it should have one `Sink` fragment.
    /// - For MV, it should have one `MView` fragment.
    /// - For table, it should have one `MView` fragment and one or two `Source` fragments. `MView` should be the root.
    /// - For source, it should have one `Source` fragment.
    ///
    /// In other words, it's the `MView` or `Sink` fragment if it exists, otherwise it's the `Source` fragment.
    pub async fn get_root_fragments(
        &self,
        job_ids: Vec<JobId>,
    ) -> MetaResult<HashMap<JobId, Fragment>> {
        let inner = self.inner.read().await;

        let all_fragments = FragmentModel::find()
            .filter(fragment::Column::JobId.is_in(job_ids))
            .all(&inner.db)
            .await?;
        // job_id -> fragment
        let mut root_fragments = HashMap::<JobId, Fragment>::new();
        for fragment in all_fragments {
            let mask = FragmentTypeMask::from(fragment.fragment_type_mask);
            if mask.contains_any([FragmentTypeFlag::Mview, FragmentTypeFlag::Sink]) {
                _ = root_fragments.insert(fragment.job_id, fragment.into());
            } else if mask.contains(FragmentTypeFlag::Source) {
                // look for Source fragment only if there's no MView fragment
                // (notice try_insert here vs insert above)
                _ = root_fragments.try_insert(fragment.job_id, fragment.into());
            }
        }

        Ok(root_fragments)
    }

    pub async fn get_root_fragment(&self, job_id: JobId) -> MetaResult<Fragment> {
        let mut root_fragments = self.get_root_fragments(vec![job_id]).await?;
        let root_fragment = root_fragments
            .remove(&job_id)
            .context(format!("root fragment for job {} not found", job_id))?;

        Ok(root_fragment)
    }

    /// Get the downstream fragments connected to the specified job.
    pub async fn get_downstream_fragments(
        &self,
        job_id: JobId,
    ) -> MetaResult<Vec<(stream_plan::DispatcherType, Fragment)>> {
        let root_fragment = self.get_root_fragment(job_id).await?;

        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        let downstream_fragment_relations: Vec<fragment_relation::Model> = FragmentRelation::find()
            .filter(
                fragment_relation::Column::SourceFragmentId
                    .eq(root_fragment.fragment_id as FragmentId),
            )
            .all(&txn)
            .await?;

        let downstream_fragment_ids = downstream_fragment_relations
            .iter()
            .map(|model| model.target_fragment_id as FragmentId)
            .collect::<HashSet<_>>();

        let downstream_fragments: Vec<fragment::Model> = FragmentModel::find()
            .filter(fragment::Column::FragmentId.is_in(downstream_fragment_ids))
            .all(&txn)
            .await?;

        let mut downstream_fragments_map: HashMap<_, _> = downstream_fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect();

        let mut downstream_fragments = vec![];

        let fragment_map: HashMap<_, _> = downstream_fragment_relations
            .iter()
            .map(|model| (model.target_fragment_id, model.dispatcher_type))
            .collect();

        for (fragment_id, dispatcher_type) in fragment_map {
            let dispatch_type = PbDispatcherType::from(dispatcher_type);

            let fragment = downstream_fragments_map
                .remove(&fragment_id)
                .context(format!(
                    "downstream fragment node for id {} not found",
                    fragment_id
                ))?
                .into();

            downstream_fragments.push((dispatch_type, fragment));
        }
        Ok(downstream_fragments)
    }

    pub async fn load_source_fragment_ids(
        &self,
    ) -> MetaResult<HashMap<SourceId, BTreeSet<FragmentId>>> {
        let inner = self.inner.read().await;
        let fragments: Vec<(FragmentId, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
            .filter(FragmentTypeMask::intersects(FragmentTypeFlag::Source))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut source_fragment_ids = HashMap::new();
        for (fragment_id, stream_node) in fragments {
            if let Some(source_id) = stream_node.to_protobuf().find_stream_source() {
                source_fragment_ids
                    .entry(source_id)
                    .or_insert_with(BTreeSet::new)
                    .insert(fragment_id);
            }
        }
        Ok(source_fragment_ids)
    }

    pub async fn load_backfill_fragment_ids(
        &self,
    ) -> MetaResult<HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>> {
        let inner = self.inner.read().await;
        let fragments: Vec<(FragmentId, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
            .filter(FragmentTypeMask::intersects(FragmentTypeFlag::SourceScan))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut source_fragment_ids = HashMap::new();
        for (fragment_id, stream_node) in fragments {
            if let Some((source_id, upstream_source_fragment_id)) =
                stream_node.to_protobuf().find_source_backfill()
            {
                source_fragment_ids
                    .entry(source_id)
                    .or_insert_with(BTreeSet::new)
                    .insert((fragment_id, upstream_source_fragment_id));
            }
        }
        Ok(source_fragment_ids)
    }

    pub async fn get_all_upstream_sink_infos(
        &self,
        target_table: &PbTable,
        target_fragment_id: FragmentId,
    ) -> MetaResult<Vec<UpstreamSinkInfo>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        self.get_all_upstream_sink_infos_in_txn(&txn, target_table, target_fragment_id)
            .await
    }

    pub async fn get_all_upstream_sink_infos_in_txn<C>(
        &self,
        txn: &C,
        target_table: &PbTable,
        target_fragment_id: FragmentId,
    ) -> MetaResult<Vec<UpstreamSinkInfo>>
    where
        C: ConnectionTrait,
    {
        let incoming_sinks = Sink::find()
            .filter(sink::Column::TargetTable.eq(target_table.id))
            .all(txn)
            .await?;

        let sink_ids = incoming_sinks.iter().map(|s| s.sink_id).collect_vec();
        let sink_fragment_ids = get_sink_fragment_by_ids(txn, sink_ids).await?;

        let mut upstream_sink_infos = Vec::with_capacity(incoming_sinks.len());
        for sink in &incoming_sinks {
            let sink_fragment_id =
                sink_fragment_ids
                    .get(&sink.sink_id)
                    .cloned()
                    .ok_or(anyhow::anyhow!(
                        "sink fragment not found for sink id {}",
                        sink.sink_id
                    ))?;
            let upstream_info = build_upstream_sink_info(
                sink.sink_id,
                sink.original_target_columns
                    .as_ref()
                    .map(|cols| cols.to_protobuf())
                    .unwrap_or_default(),
                sink_fragment_id,
                target_table,
                target_fragment_id,
            )?;
            upstream_sink_infos.push(upstream_info);
        }

        Ok(upstream_sink_infos)
    }

    pub async fn get_mview_fragment_by_id(&self, job_id: JobId) -> MetaResult<FragmentId> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let mview_fragment: Vec<FragmentId> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(
                fragment::Column::JobId
                    .eq(job_id)
                    .and(FragmentTypeMask::intersects(FragmentTypeFlag::Mview)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        if mview_fragment.len() != 1 {
            return Err(anyhow::anyhow!(
                "expected exactly one mview fragment for job {}, found {}",
                job_id,
                mview_fragment.len()
            )
            .into());
        }

        Ok(mview_fragment.into_iter().next().unwrap())
    }

    pub async fn has_table_been_migrated(&self, table_id: TableId) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        has_table_been_migrated(&txn, table_id).await
    }

    pub async fn update_fragment_splits<C>(
        &self,
        txn: &C,
        fragment_splits: &HashMap<FragmentId, Vec<SplitImpl>>,
    ) -> MetaResult<()>
    where
        C: ConnectionTrait,
    {
        if fragment_splits.is_empty() {
            return Ok(());
        }

        let existing_fragment_ids: HashSet<FragmentId> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::FragmentId.is_in(fragment_splits.keys().copied()))
            .into_tuple()
            .all(txn)
            .await?
            .into_iter()
            .collect();

        // Filter out stale fragment ids to avoid FK violations when split updates race with drop.
        let (models, skipped_fragment_ids): (Vec<_>, Vec<_>) = fragment_splits
            .iter()
            .partition_map(|(fragment_id, splits)| {
                if existing_fragment_ids.contains(fragment_id) {
                    Either::Left(fragment_splits::ActiveModel {
                        fragment_id: Set(*fragment_id as _),
                        splits: Set(Some(ConnectorSplits::from(&PbConnectorSplits {
                            splits: splits.iter().map(Into::into).collect_vec(),
                        }))),
                    })
                } else {
                    Either::Right(*fragment_id)
                }
            });

        if !skipped_fragment_ids.is_empty() {
            tracing::warn!(
                skipped_fragment_ids = ?skipped_fragment_ids,
                total_fragment_ids = fragment_splits.len(),
                "skipping stale fragment split updates for missing fragments"
            );
        }

        if models.is_empty() {
            return Ok(());
        }

        FragmentSplits::insert_many(models)
            .on_conflict(
                OnConflict::column(fragment_splits::Column::FragmentId)
                    .update_column(fragment_splits::Column::Splits)
                    .to_owned(),
            )
            .exec(txn)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use itertools::Itertools;
    use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
    use risingwave_common::hash::{ActorMapping, VirtualNode, VnodeCount};
    use risingwave_common::id::JobId;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_meta_model::*;
    use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
    use risingwave_pb::plan_common::PbExprContext;
    use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
    use risingwave_pb::stream_plan::stream_node::PbNodeBody;
    use risingwave_pb::stream_plan::{MergeNode, PbStreamNode, PbUnionNode};

    use super::ActorInfo;
    use crate::MetaResult;
    use crate::controller::catalog::CatalogController;
    use crate::model::{Fragment, StreamActor};

    type ActorUpstreams = BTreeMap<crate::model::FragmentId, HashSet<crate::model::ActorId>>;

    type FragmentActorUpstreams = HashMap<crate::model::ActorId, ActorUpstreams>;

    const TEST_FRAGMENT_ID: FragmentId = FragmentId::new(1);

    const TEST_UPSTREAM_FRAGMENT_ID: FragmentId = FragmentId::new(2);

    const TEST_JOB_ID: JobId = JobId::new(1);

    const TEST_STATE_TABLE_ID: TableId = TableId::new(1000);

    fn generate_upstream_actor_ids_for_actor(actor_id: ActorId) -> ActorUpstreams {
        let mut upstream_actor_ids = BTreeMap::new();
        upstream_actor_ids.insert(
            TEST_UPSTREAM_FRAGMENT_ID,
            HashSet::from_iter([(actor_id + 100)]),
        );
        upstream_actor_ids.insert(
            (TEST_UPSTREAM_FRAGMENT_ID + 1) as _,
            HashSet::from_iter([(actor_id + 200)]),
        );
        upstream_actor_ids
    }

    fn generate_merger_stream_node(actor_upstream_actor_ids: &ActorUpstreams) -> PbStreamNode {
        let mut input = vec![];
        for &upstream_fragment_id in actor_upstream_actor_ids.keys() {
            input.push(PbStreamNode {
                node_body: Some(PbNodeBody::Merge(Box::new(MergeNode {
                    upstream_fragment_id,
                    ..Default::default()
                }))),
                ..Default::default()
            });
        }

        PbStreamNode {
            input,
            node_body: Some(PbNodeBody::Union(PbUnionNode {})),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_extract_fragment() -> MetaResult<()> {
        let actor_count = 3u32;
        let upstream_actor_ids: FragmentActorUpstreams = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id.into(),
                    generate_upstream_actor_ids_for_actor(actor_id.into()),
                )
            })
            .collect();

        let stream_node = generate_merger_stream_node(upstream_actor_ids.values().next().unwrap());

        let pb_fragment = Fragment {
            fragment_id: TEST_FRAGMENT_ID as _,
            fragment_type_mask: FragmentTypeMask::from(FragmentTypeFlag::Source as u32),
            distribution_type: PbFragmentDistributionType::Hash as _,
            state_table_ids: vec![TEST_STATE_TABLE_ID as _],
            maybe_vnode_count: VnodeCount::for_test().to_protobuf(),
            nodes: stream_node,
        };

        let fragment =
            CatalogController::prepare_fragment_model_for_new_job(TEST_JOB_ID, &pb_fragment)?;

        check_fragment(fragment, pb_fragment);

        Ok(())
    }

    #[tokio::test]
    async fn test_compose_fragment() -> MetaResult<()> {
        let actor_count = 3u32;

        let upstream_actor_ids: FragmentActorUpstreams = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id.into(),
                    generate_upstream_actor_ids_for_actor(actor_id.into()),
                )
            })
            .collect();

        let mut actor_bitmaps = ActorMapping::new_uniform(
            (0..actor_count).map(|i| i.into()),
            VirtualNode::COUNT_FOR_TEST,
        )
        .to_bitmaps();

        let actors = (0..actor_count)
            .map(|actor_id| {
                let actor_splits = ConnectorSplits::from(&PbConnectorSplits {
                    splits: vec![PbConnectorSplit {
                        split_type: "dummy".to_owned(),
                        ..Default::default()
                    }],
                });

                ActorInfo {
                    actor_id: actor_id.into(),
                    fragment_id: TEST_FRAGMENT_ID,
                    splits: actor_splits,
                    worker_id: 0.into(),
                    vnode_bitmap: actor_bitmaps
                        .remove(&actor_id)
                        .map(|bitmap| bitmap.to_protobuf())
                        .as_ref()
                        .map(VnodeBitmap::from),
                    expr_context: ExprContext::from(&PbExprContext {
                        time_zone: String::from("America/New_York"),
                        strict_mode: false,
                    }),
                    config_override: "a.b.c = true".into(),
                }
            })
            .collect_vec();

        let stream_node = {
            let template_actor = actors.first().cloned().unwrap();

            let template_upstream_actor_ids =
                upstream_actor_ids.get(&template_actor.actor_id).unwrap();

            generate_merger_stream_node(template_upstream_actor_ids)
        };

        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: TEST_FRAGMENT_ID,
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&stream_node),
            state_table_ids: TableIdArray(vec![TEST_STATE_TABLE_ID]),
            upstream_fragment_id: Default::default(),
            vnode_count: VirtualNode::COUNT_FOR_TEST as _,
            parallelism: None,
        };

        let (pb_fragment, pb_actors, pb_actor_status, pb_actor_splits) =
            CatalogController::compose_fragment(fragment.clone(), actors.clone(), None).unwrap();

        assert_eq!(pb_actor_status.len(), actor_count as usize);
        assert!(
            pb_actor_status
                .values()
                .all(|actor_status| actor_status.location.is_some())
        );
        assert_eq!(pb_actor_splits.len(), actor_count as usize);

        check_fragment(fragment, pb_fragment);
        check_actors(
            actors,
            &upstream_actor_ids,
            pb_actors,
            pb_actor_splits,
            &stream_node,
        );

        Ok(())
    }

    fn check_actors(
        actors: Vec<ActorInfo>,
        actor_upstreams: &FragmentActorUpstreams,
        pb_actors: Vec<StreamActor>,
        pb_actor_splits: HashMap<ActorId, PbConnectorSplits>,
        stream_node: &PbStreamNode,
    ) {
        for (
            ActorInfo {
                actor_id,
                fragment_id,
                splits,
                worker_id: _,
                vnode_bitmap,
                expr_context,
                ..
            },
            StreamActor {
                actor_id: pb_actor_id,
                fragment_id: pb_fragment_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition,
                expr_context: pb_expr_context,
                ..
            },
        ) in actors.into_iter().zip_eq_debug(pb_actors.into_iter())
        {
            assert_eq!(actor_id, pb_actor_id as ActorId);
            assert_eq!(fragment_id, pb_fragment_id as FragmentId);

            assert_eq!(
                vnode_bitmap.map(|bitmap| bitmap.to_protobuf().into()),
                pb_vnode_bitmap,
            );

            assert_eq!(mview_definition, "");

            visit_stream_node_body(stream_node, |body| {
                if let PbNodeBody::Merge(m) = body {
                    assert!(
                        actor_upstreams
                            .get(&actor_id)
                            .unwrap()
                            .contains_key(&m.upstream_fragment_id)
                    );
                }
            });

            assert_eq!(
                splits,
                pb_actor_splits
                    .get(&pb_actor_id)
                    .map(ConnectorSplits::from)
                    .unwrap_or_default()
            );

            assert_eq!(Some(expr_context.to_protobuf()), pb_expr_context);
        }
    }

    fn check_fragment(fragment: fragment::Model, pb_fragment: Fragment) {
        let Fragment {
            fragment_id,
            fragment_type_mask,
            distribution_type: pb_distribution_type,
            state_table_ids: pb_state_table_ids,
            maybe_vnode_count: _,
            nodes,
        } = pb_fragment;

        assert_eq!(fragment_id, TEST_FRAGMENT_ID);
        assert_eq!(fragment_type_mask, fragment.fragment_type_mask.into());
        assert_eq!(
            pb_distribution_type,
            PbFragmentDistributionType::from(fragment.distribution_type)
        );

        assert_eq!(pb_state_table_ids, fragment.state_table_ids.0);
        assert_eq!(fragment.stream_node.to_protobuf(), nodes);
    }

    #[test]
    fn test_parallelism_policy_with_root_fragments() {
        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: 3.into(),
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray::default(),
            upstream_fragment_id: Default::default(),
            vnode_count: 0,
            parallelism: None,
        };

        let job_parallelism = StreamingParallelism::Fixed(4);

        let policy = super::CatalogController::format_fragment_parallelism_policy(
            fragment.distribution_type,
            fragment.parallelism.as_ref(),
            Some(&job_parallelism),
            None,
            &[],
        );

        assert_eq!(policy, "inherit(4)");
    }

    #[test]
    fn test_parallelism_policy_with_adaptive_strategy() {
        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: 4.into(),
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray::default(),
            upstream_fragment_id: Default::default(),
            vnode_count: 0,
            parallelism: None,
        };

        let job_parallelism = StreamingParallelism::Adaptive;

        let policy = super::CatalogController::format_fragment_parallelism_policy(
            fragment.distribution_type,
            fragment.parallelism.as_ref(),
            Some(&job_parallelism),
            Some("RATIO(0.5)"),
            &[],
        );

        assert_eq!(policy, "inherit(ratio(0.5))");
    }

    #[test]
    fn test_parallelism_policy_with_custom_strategy() {
        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: 6.into(),
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray::default(),
            upstream_fragment_id: Default::default(),
            vnode_count: 0,
            parallelism: None,
        };

        let job_parallelism = StreamingParallelism::Custom;

        let policy = super::CatalogController::format_fragment_parallelism_policy(
            fragment.distribution_type,
            fragment.parallelism.as_ref(),
            Some(&job_parallelism),
            Some("BOUNDED(8)"),
            &[],
        );

        assert_eq!(policy, "inherit(bounded(8))");
    }

    #[test]
    fn test_parallelism_policy_with_invalid_adaptive_strategy_falls_back() {
        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: 7.into(),
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray::default(),
            upstream_fragment_id: Default::default(),
            vnode_count: 0,
            parallelism: None,
        };

        let job_parallelism = StreamingParallelism::Adaptive;

        let policy = super::CatalogController::format_fragment_parallelism_policy(
            fragment.distribution_type,
            fragment.parallelism.as_ref(),
            Some(&job_parallelism),
            Some("NOT_A_STRATEGY"),
            &[],
        );

        assert_eq!(policy, "inherit(adaptive)");
    }

    #[test]
    fn test_parallelism_policy_with_upstream_roots() {
        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: 5.into(),
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray::default(),
            upstream_fragment_id: Default::default(),
            vnode_count: 0,
            parallelism: None,
        };

        let policy = super::CatalogController::format_fragment_parallelism_policy(
            fragment.distribution_type,
            fragment.parallelism.as_ref(),
            None,
            None,
            &[3.into(), 1.into(), 2.into(), 1.into()],
        );

        assert_eq!(policy, "upstream_fragment([1, 2, 3])");
    }
}
