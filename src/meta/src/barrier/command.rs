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

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::hash::{ActorMapping, VnodeCountCompat};
use risingwave_common::id::{JobId, SinkId, SourceId};
use risingwave_common::must_match;
use risingwave_connector::source::{CdcTableSnapshotSplitRaw, SplitImpl};
use risingwave_hummock_sdk::change_log::build_table_change_log_delta;
use risingwave_hummock_sdk::vector_index::VectorIndexDelta;
use risingwave_meta_model::{DispatcherType, WorkerId, fragment_relation, streaming_job};
use risingwave_pb::catalog::CreateType;
use risingwave_pb::common::PbActorInfo;
use risingwave_pb::hummock::vector_index_delta::PbVectorIndexInit;
use risingwave_pb::plan_common::{ColumnCatalog as PbColumnCatalog, PbField};
use risingwave_pb::source::{
    ConnectorSplit, ConnectorSplits, PbCdcTableSnapshotSplits,
    PbCdcTableSnapshotSplitsWithGeneration,
};
use risingwave_pb::stream_plan::add_mutation::PbNewUpstreamSink;
use risingwave_pb::stream_plan::barrier::BarrierKind as PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::connector_props_change_mutation::ConnectorPropsInfo;
use risingwave_pb::stream_plan::sink_schema_change::Op as PbSinkSchemaChangeOp;
use risingwave_pb::stream_plan::throttle_mutation::ThrottleConfig;
use risingwave_pb::stream_plan::update_mutation::{DispatcherUpdate, MergeUpdate};
use risingwave_pb::stream_plan::{
    AddMutation, ConnectorPropsChangeMutation, Dispatcher, Dispatchers, DropSubscriptionsMutation,
    ListFinishMutation, LoadFinishMutation, PauseMutation, PbSinkAddColumnsOp, PbSinkDropColumnsOp,
    PbSinkSchemaChange, PbStreamNode, PbUpstreamSinkInfo, ResumeMutation,
    SourceChangeSplitMutation, StartFragmentBackfillMutation, StopMutation,
    SubscriptionUpstreamInfo, ThrottleMutation, UpdateMutation,
};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::warn;

use super::info::InflightDatabaseInfo;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::complete_task::CompleteBarrierTask;
use crate::barrier::edge_builder::FragmentEdgeBuildResult;
use crate::barrier::partial_graph::PartialGraphBarrierInfo;
use crate::barrier::rpc::{ControlStreamManager, to_partial_graph_id};
use crate::barrier::utils::{collect_new_vector_index_info, collect_resp_info};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::scale::LoadedFragmentContext;
use crate::controller::utils::StreamingJobExtraInfo;
use crate::hummock::NewTableFragmentInfo;
use crate::manager::{StreamingJob, StreamingJobType};
use crate::model::{
    ActorId, ActorUpstreams, DispatcherId, FragmentActorDispatchers, FragmentDownstreamRelation,
    FragmentId, FragmentReplaceUpstream, StreamActor, StreamActorWithDispatchers,
    StreamJobActorsToCreate, StreamJobFragments, StreamJobFragmentsToCreate, SubscriptionId,
};
use crate::stream::{
    AutoRefreshSchemaSinkContext, ConnectorPropsChange, ExtendedFragmentBackfillOrder,
    ReplaceJobSplitPlan, SourceSplitAssignment, SplitAssignment, SplitState, UpstreamSinkInfo,
    build_actor_connector_splits,
};
use crate::{MetaError, MetaResult};

pub(crate) type ThrottleConfigMap = HashMap<FragmentId, (ThrottleConfig, PbStreamNode)>;

pub(crate) fn extract_throttle_config(
    config: &mut ThrottleConfigMap,
    mut apply: impl FnMut(FragmentId, &PbStreamNode) -> bool,
) -> Option<Mutation> {
    let fragment_throttle = config
        .extract_if(|fragment_id, (_, stream_node)| apply(*fragment_id, stream_node))
        .map(|(fragment_id, (throttle_config, _))| (fragment_id, throttle_config))
        .collect::<HashMap<_, _>>();
    (!fragment_throttle.is_empty())
        .then_some(Mutation::Throttle(ThrottleMutation { fragment_throttle }))
}

/// [`Reschedule`] describes per-fragment changes in a resolved reschedule plan,
/// used for actor scaling or migration.
#[derive(Debug, Clone)]
pub struct Reschedule {
    /// Added actors in this fragment.
    pub added_actors: HashMap<WorkerId, Vec<ActorId>>,

    /// Removed actors in this fragment.
    pub removed_actors: HashSet<ActorId>,

    /// Vnode bitmap updates for some actors in this fragment.
    pub vnode_bitmap_updates: HashMap<ActorId, Bitmap>,

    /// The upstream fragments of this fragment, and the dispatchers that should be updated.
    pub upstream_fragment_dispatcher_ids: Vec<(FragmentId, DispatcherId)>,
    /// New hash mapping of the upstream dispatcher to be updated.
    ///
    /// This field exists only when there's upstream fragment and the current fragment is
    /// hash-sharded.
    pub upstream_dispatcher_mapping: Option<ActorMapping>,

    /// The downstream fragments of this fragment.
    pub downstream_fragment_ids: Vec<FragmentId>,

    /// Reassigned splits for source actors.
    /// It becomes the `actor_splits` in [`UpdateMutation`].
    /// `Source` and `SourceBackfill` are handled together here.
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,

    pub newly_created_actors: HashMap<ActorId, (StreamActorWithDispatchers, WorkerId)>,
}

#[derive(Debug, Clone)]
pub struct ReschedulePlan {
    pub reschedules: HashMap<FragmentId, Reschedule>,
    /// Should contain the actor ids in upstream and downstream fragments referenced by
    /// `reschedules`.
    pub fragment_actors: HashMap<FragmentId, HashSet<ActorId>>,
}

/// Preloaded context for rescheduling, built outside the barrier worker.
#[derive(Debug, Clone)]
pub struct RescheduleContext {
    pub loaded: LoadedFragmentContext,
    pub job_extra_info: HashMap<JobId, StreamingJobExtraInfo>,
    pub upstream_fragments: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,
    pub downstream_fragments: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,
    pub downstream_relations: HashMap<(FragmentId, FragmentId), fragment_relation::Model>,
}

impl RescheduleContext {
    pub fn empty() -> Self {
        Self {
            loaded: LoadedFragmentContext::default(),
            job_extra_info: HashMap::new(),
            upstream_fragments: HashMap::new(),
            downstream_fragments: HashMap::new(),
            downstream_relations: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.loaded.is_empty()
    }

    pub fn for_database(&self, database_id: DatabaseId) -> Option<Self> {
        let loaded = self.loaded.for_database(database_id)?;
        let job_ids: HashSet<JobId> = loaded.job_map.keys().copied().collect();
        // Use the filtered loaded context as the source of truth so every side map is pruned by
        // the same fragment set.
        let fragment_ids: HashSet<FragmentId> = loaded
            .job_fragments
            .values()
            .flat_map(|fragments| fragments.keys().copied())
            .collect();

        let job_extra_info = self
            .job_extra_info
            .iter()
            .filter(|(job_id, _)| job_ids.contains(*job_id))
            .map(|(job_id, info)| (*job_id, info.clone()))
            .collect();

        let upstream_fragments = self
            .upstream_fragments
            .iter()
            .filter(|(fragment_id, _)| fragment_ids.contains(*fragment_id))
            .map(|(fragment_id, upstreams)| (*fragment_id, upstreams.clone()))
            .collect();

        let downstream_fragments = self
            .downstream_fragments
            .iter()
            .filter(|(fragment_id, _)| fragment_ids.contains(*fragment_id))
            .map(|(fragment_id, downstreams)| (*fragment_id, downstreams.clone()))
            .collect();

        let downstream_relations = self
            .downstream_relations
            .iter()
            // Ownership of this map is source-fragment based. We keep all downstream edges for
            // selected sources because the target side can still be referenced during dispatcher
            // reconstruction even if that target fragment is not being rescheduled.
            .filter(|((source_fragment_id, _), _)| fragment_ids.contains(source_fragment_id))
            .map(|(key, relation)| (*key, relation.clone()))
            .collect();

        Some(Self {
            loaded,
            job_extra_info,
            upstream_fragments,
            downstream_fragments,
            downstream_relations,
        })
    }

    /// Split this context into per-database contexts without cloning the large loaded graph
    /// payloads.
    pub fn into_database_contexts(self) -> HashMap<DatabaseId, Self> {
        let Self {
            loaded,
            job_extra_info,
            upstream_fragments,
            downstream_fragments,
            downstream_relations,
        } = self;

        let mut contexts: HashMap<_, _> = loaded
            .into_database_contexts()
            .into_iter()
            .map(|(database_id, loaded)| {
                (
                    database_id,
                    Self {
                        loaded,
                        job_extra_info: HashMap::new(),
                        upstream_fragments: HashMap::new(),
                        downstream_fragments: HashMap::new(),
                        downstream_relations: HashMap::new(),
                    },
                )
            })
            .collect();

        if contexts.is_empty() {
            return contexts;
        }

        let mut job_databases = HashMap::new();
        let mut fragment_databases = HashMap::new();
        for (&database_id, context) in &contexts {
            for job_id in context.loaded.job_map.keys().copied() {
                job_databases.insert(job_id, database_id);
            }
            for fragment_id in context
                .loaded
                .job_fragments
                .values()
                .flat_map(|fragments| fragments.keys().copied())
            {
                fragment_databases.insert(fragment_id, database_id);
            }
        }

        for (job_id, info) in job_extra_info {
            if let Some(database_id) = job_databases.get(&job_id).copied() {
                contexts
                    .get_mut(&database_id)
                    .expect("database context should exist for job")
                    .job_extra_info
                    .insert(job_id, info);
            }
        }

        for (fragment_id, upstreams) in upstream_fragments {
            if let Some(database_id) = fragment_databases.get(&fragment_id).copied() {
                contexts
                    .get_mut(&database_id)
                    .expect("database context should exist for fragment")
                    .upstream_fragments
                    .insert(fragment_id, upstreams);
            }
        }

        for (fragment_id, downstreams) in downstream_fragments {
            if let Some(database_id) = fragment_databases.get(&fragment_id).copied() {
                contexts
                    .get_mut(&database_id)
                    .expect("database context should exist for fragment")
                    .downstream_fragments
                    .insert(fragment_id, downstreams);
            }
        }

        for ((source_fragment_id, target_fragment_id), relation) in downstream_relations {
            // Route by source fragment ownership. A target may be outside of current reschedule
            // set, but this edge still belongs to the source-side command.
            if let Some(database_id) = fragment_databases.get(&source_fragment_id).copied() {
                contexts
                    .get_mut(&database_id)
                    .expect("database context should exist for relation source")
                    .downstream_relations
                    .insert((source_fragment_id, target_fragment_id), relation);
            }
        }

        contexts
    }
}

/// Replacing an old job with a new one. All actors in the job will be rebuilt.
///
/// Current use cases:
/// - `ALTER SOURCE` (via [`Command::ReplaceStreamJob`]) will replace a source job's plan.
/// - `ALTER TABLE` (via [`Command::ReplaceStreamJob`]) and `CREATE SINK INTO table` ([`Command::CreateStreamingJob`])
///   will replace a table job's plan.
#[derive(Debug, Clone)]
pub struct ReplaceStreamJobPlan {
    pub old_fragments: StreamJobFragments,
    pub new_fragments: StreamJobFragmentsToCreate,
    /// The resource group of the database this job belongs to.
    pub database_resource_group: String,
    /// Downstream jobs of the replaced job need to update their `Merge` node to
    /// connect to the new fragment.
    pub replace_upstream: FragmentReplaceUpstream,
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,
    /// Split plan for the replace job. Determines how splits are resolved in Phase 2
    /// inside the barrier worker.
    pub split_plan: ReplaceJobSplitPlan,
    /// The `StreamingJob` info of the table to be replaced. Must be `StreamingJob::Table`
    pub streaming_job: StreamingJob,
    /// The `streaming_job::Model` for this job, loaded from meta store.
    pub streaming_job_model: streaming_job::Model,
    /// The temporary dummy job fragments id of new table fragment
    pub tmp_id: JobId,
    /// The state table ids to be dropped.
    pub to_drop_state_table_ids: Vec<TableId>,
    pub auto_refresh_schema_sinks: Option<Vec<AutoRefreshSchemaSinkContext>>,
}

impl ReplaceStreamJobPlan {
    /// `old_fragment_id` -> `new_fragment_id`
    pub fn fragment_replacements(&self) -> HashMap<FragmentId, FragmentId> {
        let mut fragment_replacements = HashMap::new();
        for (upstream_fragment_id, new_upstream_fragment_id) in
            self.replace_upstream.values().flatten()
        {
            {
                let r =
                    fragment_replacements.insert(*upstream_fragment_id, *new_upstream_fragment_id);
                if let Some(r) = r {
                    assert_eq!(
                        *new_upstream_fragment_id, r,
                        "one fragment is replaced by multiple fragments"
                    );
                }
            }
        }
        fragment_replacements
    }
}

#[derive(educe::Educe, Clone)]
#[educe(Debug)]
pub struct CreateStreamingJobCommandInfo {
    #[educe(Debug(ignore))]
    pub stream_job_fragments: StreamJobFragmentsToCreate,
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,
    /// The resource group of the database this job belongs to.
    pub database_resource_group: String,
    /// Source-level split assignment (Phase 1). Resolved to actor-level in the barrier worker.
    pub init_split_assignment: SourceSplitAssignment,
    pub definition: String,
    pub job_type: StreamingJobType,
    pub create_type: CreateType,
    pub streaming_job: StreamingJob,
    pub fragment_backfill_ordering: ExtendedFragmentBackfillOrder,
    pub cdc_table_snapshot_splits: Option<Vec<CdcTableSnapshotSplitRaw>>,
    pub locality_fragment_state_table_mapping: HashMap<FragmentId, Vec<TableId>>,
    pub is_serverless: bool,
    /// The `streaming_job::Model` for this job, loaded from meta store.
    pub streaming_job_model: streaming_job::Model,
    /// If set, this create command replaces an existing sink while creating the new sink job.
    pub replace_sink: Option<SinkId>,
    /// Batch refresh interval in seconds. If set, the MV uses batch refresh semantics.
    pub refresh_interval_sec: Option<u64>,
}

impl StreamJobFragments {
    /// Build fragment-level info for new fragments, populating actor infos from the
    /// rendered actors and their locations, and applying split assignment to actor splits.
    pub(super) fn new_fragment_info<'a>(
        &'a self,
        stream_actors: &'a HashMap<FragmentId, Vec<StreamActor>>,
        actor_location: &'a HashMap<ActorId, WorkerId>,
        assignment: &'a SplitAssignment,
    ) -> impl Iterator<Item = (FragmentId, InflightFragmentInfo)> + 'a {
        self.fragments.values().map(|fragment| {
            (
                fragment.fragment_id,
                InflightFragmentInfo {
                    fragment_id: fragment.fragment_id,
                    distribution_type: fragment.distribution_type.into(),
                    fragment_type_mask: fragment.fragment_type_mask,
                    vnode_count: fragment.vnode_count(),
                    nodes: fragment.nodes.clone(),
                    actors: stream_actors
                        .get(&fragment.fragment_id)
                        .into_iter()
                        .flatten()
                        .map(|actor| {
                            (
                                actor.actor_id,
                                InflightActorInfo {
                                    worker_id: actor_location[&actor.actor_id],
                                    vnode_bitmap: actor.vnode_bitmap.clone(),
                                    splits: assignment
                                        .get(&fragment.fragment_id)
                                        .and_then(|s| s.get(&actor.actor_id))
                                        .cloned()
                                        .unwrap_or_default(),
                                },
                            )
                        })
                        .collect(),
                    state_table_ids: fragment.state_table_ids.iter().copied().collect(),
                },
            )
        })
    }
}

pub type TableLogEpochs = Vec<(Vec<u64>, u64)>;
pub type UpstreamTableLogEpochs = HashMap<TableId, TableLogEpochs>;
pub type SinceTimestampResolvedEpoch = (u64, TableLogEpochs);

#[derive(Debug, Clone)]
pub struct SnapshotBackfillInfo {
    /// `table_id` -> `Some(snapshot_backfill_epoch)`
    /// The `snapshot_backfill_epoch` should be None at the beginning, and be filled
    /// by global barrier worker when handling the command.
    pub upstream_mv_table_id_to_backfill_epoch: HashMap<TableId, Option<u64>>,
}

#[derive(Debug, Clone)]
pub struct SinceEpochInfo {
    pub provided_since_epoch: u64,
    pub resolved: Option<SinceTimestampResolvedEpoch>,
}

#[derive(Debug, Clone)]
pub struct BatchRefreshInfo {
    pub snapshot_backfill_info: SnapshotBackfillInfo,
    pub refresh_interval_sec: u64,
}

#[derive(Debug, Clone)]
pub enum CreateStreamingJobType {
    Normal,
    SinkIntoTable(UpstreamSinkInfo),
    SnapshotBackfill {
        snapshot_backfill_info: SnapshotBackfillInfo,
        since_epoch: Option<SinceEpochInfo>,
    },
    BatchRefresh(BatchRefreshInfo),
}

/// [`Command`] is the input of [`crate::barrier::worker::GlobalBarrierWorker`]. For different commands,
/// it will build different barriers to send via corresponding `*_to_mutation` helpers,
/// and may [do different stuffs after the barrier is collected](PostCollectCommand::post_collect).
// FIXME: this enum is significantly large on stack, box it
#[derive(Debug)]
pub enum Command {
    /// `Flush` command will generate a checkpoint barrier. After the barrier is collected and committed
    /// all messages before the checkpoint barrier should have been committed.
    Flush,

    /// `Pause` command generates a `Pause` barrier **only if**
    /// the cluster is not already paused. Otherwise, a barrier with no mutation will be generated.
    Pause,

    /// `Resume` command generates a `Resume` barrier **only
    /// if** the cluster is paused with the same reason. Otherwise, a barrier with no mutation
    /// will be generated.
    Resume,

    /// `DropStreamingJobs` command generates a `Stop` barrier to stop the given
    /// [`Vec<ActorId>`]. The catalog has ensured that these streaming jobs are safe to be
    /// dropped by reference counts before.
    ///
    /// Barriers from the actors to be dropped will STILL be collected.
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the job fragments info from meta store.
    DropStreamingJobs {
        streaming_job_ids: HashSet<JobId>,
        /// Used by recovery quick path when draining buffered drop/cancel commands.
        unregistered_state_table_ids: HashSet<TableId>,
        // target_fragment -> [sink_fragments]
        dropped_sink_fragment_by_targets: HashMap<FragmentId, Vec<FragmentId>>,
    },

    /// `CreateStreamingJob` command generates a `Add` barrier by given info.
    ///
    /// Barriers from the actors to be created, which is marked as `Inactive` at first, will STILL
    /// be collected since the barrier should be passthrough.
    ///
    /// After the barrier is collected, these newly created actors will be marked as `Running`. And
    /// it adds the job fragments info to meta store. However, the creating progress will **last
    /// for a while** until the `finish` channel is signaled, then the state of `TableFragments`
    /// will be set to `Created`.
    CreateStreamingJob {
        info: CreateStreamingJobCommandInfo,
        job_type: CreateStreamingJobType,
        cross_db_snapshot_backfill_info: SnapshotBackfillInfo,
    },

    /// Reschedule context. It must be resolved inside the barrier worker before injection.
    RescheduleIntent {
        context: RescheduleContext,
        /// Filled by the barrier worker after resolving `context` against current worker topology.
        ///
        /// We keep unresolved `context` outside of checkpoint state and only materialize this
        /// execution plan right before injection, then drop `context` to release memory earlier.
        reschedule_plan: Option<ReschedulePlan>,
    },

    /// `ReplaceStreamJob` command generates a `Update` barrier with the given `replace_upstream`. This is
    /// essentially switching the downstream of the old job fragments to the new ones, and
    /// dropping the old job fragments. Used for schema change.
    ///
    /// This can be treated as a special case of reschedule, while the upstream fragment
    /// of the Merge executors are changed additionally.
    ReplaceStreamJob(ReplaceStreamJobPlan),

    /// `SourceChangeSplit` generates a `Splits` barrier for pushing initialized splits or
    /// changed splits.
    SourceChangeSplit(SplitState),

    /// `Throttle` command generates a `Throttle` barrier with the given throttle config to change
    /// the `rate_limit` of executors. `throttle_type` specifies which executor kinds should apply it.
    Throttle {
        config: ThrottleConfigMap,
    },

    /// `CreateSubscription` command generates a `CreateSubscriptionMutation` to notify
    /// materialize executor to start storing old value for subscription.
    CreateSubscription {
        subscription_id: SubscriptionId,
        upstream_mv_table_id: TableId,
        retention_second: u64,
    },

    /// `DropSubscription` command generates a `DropSubscriptionsMutation` to notify
    /// materialize executor to stop storing old value when there is no
    /// subscription depending on it.
    DropSubscription {
        subscription_id: SubscriptionId,
        upstream_mv_table_id: TableId,
    },

    /// `AlterSubscriptionRetention` command updates the subscription retention time.
    AlterSubscriptionRetention {
        subscription_id: SubscriptionId,
        upstream_mv_table_id: TableId,
        retention_second: u64,
    },

    ConnectorPropsChange(ConnectorPropsChange),

    /// `Refresh` command generates a barrier to refresh a table by truncating state
    /// and reloading data from source.
    Refresh {
        table_id: TableId,
        associated_source_id: SourceId,
    },
    ListFinish {
        table_id: TableId,
        associated_source_id: SourceId,
    },
    LoadFinish {
        table_id: TableId,
        associated_source_id: SourceId,
    },

    /// `ResetSource` command generates a barrier to reset CDC source offset to latest.
    /// Used when upstream binlog/oplog has expired.
    ResetSource {
        source_id: SourceId,
    },

    /// `ResumeBackfill` command generates a `StartFragmentBackfill` barrier to force backfill
    /// to resume for troubleshooting.
    ResumeBackfill {
        target: ResumeBackfillTarget,
    },

    /// `InjectSourceOffsets` command generates a barrier to inject specific offsets
    /// into source splits (UNSAFE - admin only).
    /// This can cause data duplication or loss depending on the correctness of the provided offsets.
    InjectSourceOffsets {
        source_id: SourceId,
        /// Split ID -> offset (JSON-encoded based on connector type)
        split_offsets: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum ResumeBackfillTarget {
    Job(JobId),
    Fragment(FragmentId),
}

// For debugging and observability purposes. Can add more details later if needed.
impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Flush => write!(f, "Flush"),
            Command::Pause => write!(f, "Pause"),
            Command::Resume => write!(f, "Resume"),
            Command::DropStreamingJobs {
                streaming_job_ids, ..
            } => {
                write!(
                    f,
                    "DropStreamingJobs: {}",
                    streaming_job_ids.iter().sorted().join(", ")
                )
            }
            Command::CreateStreamingJob { info, .. } => {
                write!(f, "CreateStreamingJob: {}", info.streaming_job)
            }
            Command::RescheduleIntent {
                reschedule_plan, ..
            } => {
                if reschedule_plan.is_some() {
                    write!(f, "RescheduleIntent(planned)")
                } else {
                    write!(f, "RescheduleIntent")
                }
            }
            Command::ReplaceStreamJob(plan) => {
                write!(f, "ReplaceStreamJob: {}", plan.streaming_job)
            }
            Command::SourceChangeSplit { .. } => write!(f, "SourceChangeSplit"),
            Command::Throttle { .. } => write!(f, "Throttle"),
            Command::CreateSubscription {
                subscription_id, ..
            } => write!(f, "CreateSubscription: {subscription_id}"),
            Command::DropSubscription {
                subscription_id, ..
            } => write!(f, "DropSubscription: {subscription_id}"),
            Command::AlterSubscriptionRetention {
                subscription_id,
                retention_second,
                ..
            } => write!(
                f,
                "AlterSubscriptionRetention: {subscription_id} -> {retention_second}"
            ),
            Command::ConnectorPropsChange(_) => write!(f, "ConnectorPropsChange"),
            Command::Refresh {
                table_id,
                associated_source_id,
            } => write!(
                f,
                "Refresh: {} (source: {})",
                table_id, associated_source_id
            ),
            Command::ListFinish {
                table_id,
                associated_source_id,
            } => write!(
                f,
                "ListFinish: {} (source: {})",
                table_id, associated_source_id
            ),
            Command::LoadFinish {
                table_id,
                associated_source_id,
            } => write!(
                f,
                "LoadFinish: {} (source: {})",
                table_id, associated_source_id
            ),
            Command::ResetSource { source_id } => write!(f, "ResetSource: {source_id}"),
            Command::ResumeBackfill { target } => match target {
                ResumeBackfillTarget::Job(job_id) => {
                    write!(f, "ResumeBackfill: job={job_id}")
                }
                ResumeBackfillTarget::Fragment(fragment_id) => {
                    write!(f, "ResumeBackfill: fragment={fragment_id}")
                }
            },
            Command::InjectSourceOffsets {
                source_id,
                split_offsets,
            } => write!(
                f,
                "InjectSourceOffsets: {} ({} splits)",
                source_id,
                split_offsets.len()
            ),
        }
    }
}

impl Command {
    pub fn pause() -> Self {
        Self::Pause
    }

    pub fn resume() -> Self {
        Self::Resume
    }

    pub fn need_checkpoint(&self) -> bool {
        // todo! Reviewing the flow of different command to reduce the amount of checkpoint
        !matches!(self, Command::Resume)
    }
}

#[derive(Debug)]
pub enum PostCollectCommand {
    Command(String),
    DropStreamingJobs,
    CreateStreamingJob {
        info: CreateStreamingJobCommandInfo,
        job_type: CreateStreamingJobType,
        cross_db_snapshot_backfill_info: SnapshotBackfillInfo,
        resolved_split_assignment: SplitAssignment,
    },
    Reschedule {
        reschedules: HashMap<FragmentId, Reschedule>,
    },
    ReplaceStreamJob {
        plan: ReplaceStreamJobPlan,
        resolved_split_assignment: SplitAssignment,
    },
    SourceChangeSplit {
        split_assignment: SplitAssignment,
    },
    CreateSubscription {
        subscription_id: SubscriptionId,
    },
    ConnectorPropsChange(ConnectorPropsChange),
    ResumeBackfill {
        target: ResumeBackfillTarget,
    },
}

impl PostCollectCommand {
    pub fn barrier() -> Self {
        PostCollectCommand::Command("barrier".to_owned())
    }

    pub fn should_checkpoint(&self) -> bool {
        match self {
            PostCollectCommand::DropStreamingJobs
            | PostCollectCommand::CreateStreamingJob { .. }
            | PostCollectCommand::Reschedule { .. }
            | PostCollectCommand::ReplaceStreamJob { .. }
            | PostCollectCommand::SourceChangeSplit { .. }
            | PostCollectCommand::CreateSubscription { .. }
            | PostCollectCommand::ConnectorPropsChange(_)
            | PostCollectCommand::ResumeBackfill { .. } => true,
            PostCollectCommand::Command(_) => false,
        }
    }

    pub fn command_name(&self) -> &str {
        match self {
            PostCollectCommand::Command(name) => name.as_str(),
            PostCollectCommand::DropStreamingJobs => "DropStreamingJobs",
            PostCollectCommand::CreateStreamingJob { .. } => "CreateStreamingJob",
            PostCollectCommand::Reschedule { .. } => "Reschedule",
            PostCollectCommand::ReplaceStreamJob { .. } => "ReplaceStreamJob",
            PostCollectCommand::SourceChangeSplit { .. } => "SourceChangeSplit",
            PostCollectCommand::CreateSubscription { .. } => "CreateSubscription",
            PostCollectCommand::ConnectorPropsChange(_) => "ConnectorPropsChange",
            PostCollectCommand::ResumeBackfill { .. } => "ResumeBackfill",
        }
    }
}

impl Display for PostCollectCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.command_name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BarrierKind {
    Initial,
    Barrier,
    /// Hold a list of previous non-checkpoint prev-epoch + current prev-epoch
    Checkpoint(Vec<u64>),
}

impl BarrierKind {
    pub fn to_protobuf(&self) -> PbBarrierKind {
        match self {
            BarrierKind::Initial => PbBarrierKind::Initial,
            BarrierKind::Barrier => PbBarrierKind::Barrier,
            BarrierKind::Checkpoint(_) => PbBarrierKind::Checkpoint,
        }
    }

    pub fn is_checkpoint(&self) -> bool {
        matches!(self, BarrierKind::Checkpoint(_))
    }

    pub fn is_initial(&self) -> bool {
        matches!(self, BarrierKind::Initial)
    }

    pub fn as_str_name(&self) -> &'static str {
        match self {
            BarrierKind::Initial => "Initial",
            BarrierKind::Barrier => "Barrier",
            BarrierKind::Checkpoint(_) => "Checkpoint",
        }
    }
}

fn sink_original_schema_fields(columns: &[PbColumnCatalog]) -> Vec<PbField> {
    columns
        .iter()
        .filter(|col| !col.is_hidden)
        .map(|col| {
            let column_desc = col
                .column_desc
                .as_ref()
                .expect("sink column catalog should have a column descriptor");
            PbField {
                data_type: Some(
                    column_desc
                        .column_type
                        .as_ref()
                        .expect("sink column descriptor should have a column type")
                        .clone(),
                ),
                name: column_desc.name.clone(),
            }
        })
        .collect()
}

impl Command {
    pub(super) fn collect_commit_epoch_info(
        database_info: &InflightDatabaseInfo,
        barrier_info: &PartialGraphBarrierInfo,
        task: &mut CompleteBarrierTask,
        resps: Vec<BarrierCompleteResponse>,
        backfill_pinned_upstream_tables: HashSet<TableId>,
    ) {
        let (
            sst_to_context,
            synced_ssts,
            new_table_watermarks,
            old_value_ssts,
            vector_index_adds,
            truncate_tables,
            iceberg_pk_index_sink_metadata,
        ) = collect_resp_info(resps);

        let new_table_fragment_infos = match &barrier_info.post_collect_command {
            PostCollectCommand::CreateStreamingJob { info, job_type, .. } => {
                assert!(!matches!(
                    job_type,
                    CreateStreamingJobType::SnapshotBackfill { .. }
                        | CreateStreamingJobType::BatchRefresh(_)
                ));
                let table_fragments = &info.stream_job_fragments;
                let mut table_ids: HashSet<_> =
                    table_fragments.internal_table_ids().into_iter().collect();
                if let Some(mv_table_id) = table_fragments.mv_table_id() {
                    table_ids.insert(mv_table_id);
                }

                vec![NewTableFragmentInfo { table_ids }]
            }
            _ => vec![],
        };

        let mut log_store_table_ids = HashSet::new();
        // TODO: may collect cross db snapshot backfill
        log_store_table_ids.extend(database_info.subscribed_tables());
        log_store_table_ids.extend(backfill_pinned_upstream_tables);

        let table_new_change_log = build_table_change_log_delta(
            old_value_ssts.into_iter(),
            synced_ssts.iter().map(|sst| &sst.sst_info),
            must_match!(&barrier_info.barrier_info.kind, BarrierKind::Checkpoint(epochs) => epochs),
            log_store_table_ids.into_iter(),
        );

        let epoch = barrier_info.barrier_info.prev_epoch();
        let info = &mut task.commit_info;
        for table_id in &barrier_info.table_ids_to_commit {
            info.tables_to_commit
                .try_insert(*table_id, epoch)
                .expect("non duplicate");
        }

        info.sstables.extend(synced_ssts);
        info.new_table_watermarks.extend(new_table_watermarks);
        info.sst_to_context.extend(sst_to_context);
        info.new_table_fragment_infos
            .extend(new_table_fragment_infos);
        info.change_log_delta.extend(table_new_change_log);
        for (table_id, vector_index_adds) in vector_index_adds {
            info.vector_index_delta
                .try_insert(table_id, VectorIndexDelta::Adds(vector_index_adds))
                .expect("non-duplicate");
        }
        if let PostCollectCommand::CreateStreamingJob { info: job_info, .. } =
            &barrier_info.post_collect_command
            && let Some(index_table) = collect_new_vector_index_info(job_info)
        {
            info.vector_index_delta
                .try_insert(
                    index_table.id,
                    VectorIndexDelta::Init(PbVectorIndexInit {
                        info: Some(index_table.vector_index_info.unwrap()),
                    }),
                )
                .expect("non-duplicate");
        }
        info.truncate_tables.extend(truncate_tables);
        task.iceberg_pk_index_sink_metadata
            .extend(iceberg_pk_index_sink_metadata);
    }
}

impl Command {
    /// Build the `Pause` mutation.
    pub(super) fn pause_to_mutation(is_currently_paused: bool) -> Option<Mutation> {
        {
            {
                // Only pause when the cluster is not already paused.
                // XXX: what if pause(r1) - pause(r2) - resume(r1) - resume(r2)??
                if !is_currently_paused {
                    Some(Mutation::Pause(PauseMutation {}))
                } else {
                    None
                }
            }
        }
    }

    /// Build the `Resume` mutation.
    pub(super) fn resume_to_mutation(is_currently_paused: bool) -> Option<Mutation> {
        {
            {
                // Only resume when the cluster is paused with the same reason.
                if is_currently_paused {
                    Some(Mutation::Resume(ResumeMutation {}))
                } else {
                    None
                }
            }
        }
    }

    /// Build the `Splits` mutation for `SourceChangeSplit`.
    pub(super) fn source_change_split_to_mutation(split_assignment: &SplitAssignment) -> Mutation {
        {
            {
                let mut diff = HashMap::new();

                for actor_splits in split_assignment.values() {
                    diff.extend(actor_splits.clone());
                }

                Mutation::Splits(SourceChangeSplitMutation {
                    actor_splits: build_actor_connector_splits(&diff),
                })
            }
        }
    }

    /// Build the `Stop` mutation for `DropStreamingJobs`.
    pub(super) fn drop_streaming_jobs_to_mutation(
        actors: &Vec<ActorId>,
        dropped_sink_fragment_by_targets: &HashMap<FragmentId, Vec<FragmentId>>,
    ) -> Mutation {
        {
            Mutation::Stop(StopMutation {
                actors: actors.clone(),
                dropped_sink_fragments: dropped_sink_fragment_by_targets
                    .values()
                    .flatten()
                    .cloned()
                    .collect(),
            })
        }
    }

    /// Build the `Add` mutation for `CreateStreamingJob`.
    pub(super) fn create_streaming_job_to_mutation(
        info: &CreateStreamingJobCommandInfo,
        job_type: &CreateStreamingJobType,
        dropped_actors: impl IntoIterator<Item = ActorId>,
        is_currently_paused: bool,
        edges: &mut FragmentEdgeBuildResult,
        control_stream_manager: &ControlStreamManager,
        actor_cdc_table_snapshot_splits: Option<HashMap<ActorId, PbCdcTableSnapshotSplits>>,
        split_assignment: &SplitAssignment,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> MetaResult<Mutation> {
        {
            {
                let CreateStreamingJobCommandInfo {
                    stream_job_fragments,
                    upstream_fragment_downstreams,
                    fragment_backfill_ordering,
                    streaming_job,
                    ..
                } = info;
                let database_id = streaming_job.database_id();
                let added_actors: Vec<ActorId> = stream_actors
                    .values()
                    .flatten()
                    .map(|actor| actor.actor_id)
                    .collect();
                let dropped_actors = dropped_actors.into_iter().collect();
                let actor_splits = split_assignment
                    .values()
                    .flat_map(build_actor_connector_splits)
                    .collect();
                let subscriptions_to_add = {
                    if let CreateStreamingJobType::SnapshotBackfill {
                        snapshot_backfill_info,
                        ..
                    }
                    | CreateStreamingJobType::BatchRefresh(BatchRefreshInfo {
                        snapshot_backfill_info,
                        ..
                    }) = job_type
                    {
                        snapshot_backfill_info
                            .upstream_mv_table_id_to_backfill_epoch
                            .keys()
                            .map(|table_id| SubscriptionUpstreamInfo {
                                subscriber_id: stream_job_fragments
                                    .stream_job_id()
                                    .as_subscriber_id(),
                                upstream_mv_table_id: *table_id,
                            })
                            .collect()
                    } else {
                        Default::default()
                    }
                };
                let backfill_nodes_to_pause: Vec<_> =
                    get_nodes_with_backfill_dependencies(fragment_backfill_ordering)
                        .into_iter()
                        .collect();

                let new_upstream_sinks =
                    if let CreateStreamingJobType::SinkIntoTable(UpstreamSinkInfo {
                        sink_fragment_id,
                        sink_output_fields,
                        project_exprs,
                        new_sink_downstream,
                        ..
                    }) = job_type
                    {
                        let new_sink_actors = stream_actors
                            .get(sink_fragment_id)
                            .unwrap_or_else(|| {
                                panic!("upstream sink fragment {sink_fragment_id} does not exist")
                            })
                            .iter()
                            .map(|actor| {
                                let worker_id = actor_location[&actor.actor_id];
                                PbActorInfo {
                                    actor_id: actor.actor_id,
                                    host: Some(control_stream_manager.host_addr(worker_id)),
                                    partial_graph_id: to_partial_graph_id(database_id, None),
                                }
                            });
                        let new_upstream_sink = PbNewUpstreamSink {
                            info: Some(PbUpstreamSinkInfo {
                                upstream_fragment_id: *sink_fragment_id,
                                sink_output_schema: sink_output_fields.clone(),
                                project_exprs: project_exprs.clone(),
                            }),
                            upstream_actors: new_sink_actors.collect(),
                        };
                        HashMap::from([(
                            new_sink_downstream.downstream_fragment_id,
                            new_upstream_sink,
                        )])
                    } else {
                        HashMap::new()
                    };

                let actor_cdc_table_snapshot_splits = actor_cdc_table_snapshot_splits
                    .map(|splits| PbCdcTableSnapshotSplitsWithGeneration { splits });

                let add_mutation = AddMutation {
                    actor_dispatchers: edges
                        .dispatchers
                        .extract_if(|fragment_id, _| {
                            upstream_fragment_downstreams.contains_key(fragment_id)
                        })
                        .flat_map(|(_, fragment_dispatchers)| fragment_dispatchers.into_iter())
                        .map(|(actor_id, dispatchers)| (actor_id, Dispatchers { dispatchers }))
                        .collect(),
                    added_actors,
                    actor_splits,
                    // If the cluster is already paused, the new actors should be paused too.
                    pause: is_currently_paused,
                    subscriptions_to_add,
                    backfill_nodes_to_pause,
                    actor_cdc_table_snapshot_splits,
                    new_upstream_sinks,
                    dropped_actors,
                    sink_log_store_flush: info
                        .replace_sink
                        .map(|old_sink_id| vec![old_sink_id])
                        .unwrap_or_default(),
                };

                Ok(Mutation::Add(add_mutation))
            }
        }
    }

    /// Build the `Update` mutation for `ReplaceStreamJob`.
    pub(super) fn replace_stream_job_to_mutation(
        ReplaceStreamJobPlan {
            old_fragments,
            replace_upstream,
            upstream_fragment_downstreams,
            auto_refresh_schema_sinks,
            ..
        }: &ReplaceStreamJobPlan,
        edges: &mut FragmentEdgeBuildResult,
        database_info: &mut InflightDatabaseInfo,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<Option<Mutation>> {
        {
            {
                let merge_updates = edges
                    .merge_updates
                    .extract_if(|fragment_id, _| replace_upstream.contains_key(fragment_id))
                    .collect();
                let dispatchers = edges
                    .dispatchers
                    .extract_if(|fragment_id, _| {
                        upstream_fragment_downstreams.contains_key(fragment_id)
                    })
                    .collect();
                let actor_cdc_table_snapshot_splits = database_info
                    .assign_cdc_backfill_splits(old_fragments.stream_job_id)?
                    .map(|splits| PbCdcTableSnapshotSplitsWithGeneration { splits });
                let old_fragments = old_fragments.fragments.keys().copied();
                let auto_refresh_sink_fragment_ids = auto_refresh_schema_sinks
                    .as_ref()
                    .into_iter()
                    .flat_map(|sinks| sinks.iter())
                    .map(|sink| sink.original_fragment.fragment_id);
                Ok(Self::generate_update_mutation_for_replace_table(
                    old_fragments
                        .chain(auto_refresh_sink_fragment_ids)
                        .flat_map(|fragment_id| {
                            database_info.fragment(fragment_id).actors.keys().copied()
                        }),
                    merge_updates,
                    dispatchers,
                    split_assignment,
                    actor_cdc_table_snapshot_splits,
                    auto_refresh_schema_sinks.as_ref(),
                ))
            }
        }
    }

    /// Build the `Update` mutation for `RescheduleIntent`.
    pub(super) fn reschedule_to_mutation(
        reschedules: &HashMap<FragmentId, Reschedule>,
        fragment_actors: &HashMap<FragmentId, HashSet<ActorId>>,
        control_stream_manager: &ControlStreamManager,
        database_info: &mut InflightDatabaseInfo,
    ) -> MetaResult<Option<Mutation>> {
        {
            {
                let database_id = database_info.database_id;
                let mut dispatcher_update = HashMap::new();
                for reschedule in reschedules.values() {
                    for &(upstream_fragment_id, dispatcher_id) in
                        &reschedule.upstream_fragment_dispatcher_ids
                    {
                        // Find the actors of the upstream fragment.
                        let upstream_actor_ids = fragment_actors
                            .get(&upstream_fragment_id)
                            .expect("should contain");

                        let upstream_reschedule = reschedules.get(&upstream_fragment_id);

                        // Record updates for all actors.
                        for &actor_id in upstream_actor_ids {
                            let added_downstream_actor_id = if upstream_reschedule
                                .map(|reschedule| !reschedule.removed_actors.contains(&actor_id))
                                .unwrap_or(true)
                            {
                                reschedule
                                    .added_actors
                                    .values()
                                    .flatten()
                                    .cloned()
                                    .collect()
                            } else {
                                Default::default()
                            };
                            // Index with the dispatcher id to check duplicates.
                            dispatcher_update
                                .try_insert(
                                    (actor_id, dispatcher_id),
                                    DispatcherUpdate {
                                        actor_id,
                                        dispatcher_id,
                                        hash_mapping: reschedule
                                            .upstream_dispatcher_mapping
                                            .as_ref()
                                            .map(|m| m.to_protobuf()),
                                        added_downstream_actor_id,
                                        removed_downstream_actor_id: reschedule
                                            .removed_actors
                                            .iter()
                                            .cloned()
                                            .collect(),
                                    },
                                )
                                .unwrap();
                        }
                    }
                }
                let dispatcher_update = dispatcher_update.into_values().collect();

                let mut merge_update = HashMap::new();
                for (&fragment_id, reschedule) in reschedules {
                    for &downstream_fragment_id in &reschedule.downstream_fragment_ids {
                        // Find the actors of the downstream fragment.
                        let downstream_actor_ids = fragment_actors
                            .get(&downstream_fragment_id)
                            .expect("should contain");

                        // Downstream removed actors should be skipped
                        // Newly created actors of the current fragment will not dispatch Update
                        // barriers to them
                        let downstream_removed_actors: HashSet<_> = reschedules
                            .get(&downstream_fragment_id)
                            .map(|downstream_reschedule| {
                                downstream_reschedule
                                    .removed_actors
                                    .iter()
                                    .copied()
                                    .collect()
                            })
                            .unwrap_or_default();

                        // Record updates for all actors.
                        for &actor_id in downstream_actor_ids {
                            if downstream_removed_actors.contains(&actor_id) {
                                continue;
                            }

                            // Index with the fragment id to check duplicates.
                            merge_update
                                .try_insert(
                                    (actor_id, fragment_id),
                                    MergeUpdate {
                                        actor_id,
                                        upstream_fragment_id: fragment_id,
                                        new_upstream_fragment_id: None,
                                        added_upstream_actors: reschedule
                                            .added_actors
                                            .iter()
                                            .flat_map(|(worker_id, actors)| {
                                                let host =
                                                    control_stream_manager.host_addr(*worker_id);
                                                actors.iter().map(move |&actor_id| PbActorInfo {
                                                    actor_id,
                                                    host: Some(host.clone()),
                                                    // we assume that we only scale the partial graph of database
                                                    partial_graph_id: to_partial_graph_id(
                                                        database_id,
                                                        None,
                                                    ),
                                                })
                                            })
                                            .collect(),
                                        removed_upstream_actor_id: reschedule
                                            .removed_actors
                                            .iter()
                                            .cloned()
                                            .collect(),
                                    },
                                )
                                .unwrap();
                        }
                    }
                }
                let merge_update = merge_update.into_values().collect();

                let mut actor_vnode_bitmap_update = HashMap::new();
                for reschedule in reschedules.values() {
                    // Record updates for all actors in this fragment.
                    for (&actor_id, bitmap) in &reschedule.vnode_bitmap_updates {
                        let bitmap = bitmap.to_protobuf();
                        actor_vnode_bitmap_update
                            .try_insert(actor_id, bitmap)
                            .unwrap();
                    }
                }
                let dropped_actors = reschedules
                    .values()
                    .flat_map(|r| r.removed_actors.iter().copied())
                    .collect();
                let mut actor_splits = HashMap::new();
                let mut actor_cdc_table_snapshot_splits = HashMap::new();
                for (fragment_id, reschedule) in reschedules {
                    for (actor_id, splits) in &reschedule.actor_splits {
                        actor_splits.insert(
                            *actor_id,
                            ConnectorSplits {
                                splits: splits.iter().map(ConnectorSplit::from).collect(),
                            },
                        );
                    }

                    if let Some(assignment) =
                        database_info.may_assign_fragment_cdc_backfill_splits(*fragment_id)?
                    {
                        actor_cdc_table_snapshot_splits.extend(assignment)
                    }
                }

                // we don't create dispatchers in reschedule scenario
                let actor_new_dispatchers = HashMap::new();
                let mutation = Mutation::Update(UpdateMutation {
                    dispatcher_update,
                    merge_update,
                    actor_vnode_bitmap_update,
                    dropped_actors,
                    actor_splits,
                    actor_new_dispatchers,
                    actor_cdc_table_snapshot_splits: Some(PbCdcTableSnapshotSplitsWithGeneration {
                        splits: actor_cdc_table_snapshot_splits,
                    }),
                    sink_schema_change: Default::default(),
                    subscriptions_to_drop: vec![],
                });
                tracing::debug!("update mutation: {mutation:?}");
                Ok(Some(mutation))
            }
        }
    }

    /// Build the `Add` mutation for `CreateSubscription`.
    pub(super) fn create_subscription_to_mutation(
        upstream_mv_table_id: TableId,
        subscription_id: SubscriptionId,
    ) -> Mutation {
        {
            Mutation::Add(AddMutation {
                actor_dispatchers: Default::default(),
                added_actors: vec![],
                actor_splits: Default::default(),
                pause: false,
                subscriptions_to_add: vec![SubscriptionUpstreamInfo {
                    upstream_mv_table_id,
                    subscriber_id: subscription_id.as_subscriber_id(),
                }],
                backfill_nodes_to_pause: vec![],
                actor_cdc_table_snapshot_splits: None,
                new_upstream_sinks: Default::default(),
                dropped_actors: Default::default(),
                sink_log_store_flush: Default::default(),
            })
        }
    }

    /// Build the `DropSubscriptions` mutation for `DropSubscription`.
    pub(super) fn drop_subscription_to_mutation(
        upstream_mv_table_id: TableId,
        subscription_id: SubscriptionId,
    ) -> Mutation {
        {
            Mutation::DropSubscriptions(DropSubscriptionsMutation {
                info: vec![SubscriptionUpstreamInfo {
                    subscriber_id: subscription_id.as_subscriber_id(),
                    upstream_mv_table_id,
                }],
            })
        }
    }

    /// Build the `ConnectorPropsChange` mutation.
    pub(super) fn connector_props_change_to_mutation(config: &ConnectorPropsChange) -> Mutation {
        {
            {
                let mut connector_props_infos = HashMap::default();
                for (k, v) in config {
                    connector_props_infos.insert(
                        k.as_raw_id(),
                        ConnectorPropsInfo {
                            connector_props_info: v.clone(),
                        },
                    );
                }
                Mutation::ConnectorPropsChange(ConnectorPropsChangeMutation {
                    connector_props_infos,
                })
            }
        }
    }

    /// Build the `RefreshStart` mutation.
    pub(super) fn refresh_to_mutation(
        table_id: TableId,
        associated_source_id: SourceId,
    ) -> Mutation {
        Mutation::RefreshStart(risingwave_pb::stream_plan::RefreshStartMutation {
            table_id,
            associated_source_id,
        })
    }

    /// Build the `ListFinish` mutation.
    pub(super) fn list_finish_to_mutation(associated_source_id: SourceId) -> Mutation {
        Mutation::ListFinish(ListFinishMutation {
            associated_source_id,
        })
    }

    /// Build the `LoadFinish` mutation.
    pub(super) fn load_finish_to_mutation(associated_source_id: SourceId) -> Mutation {
        Mutation::LoadFinish(LoadFinishMutation {
            associated_source_id,
        })
    }

    /// Build the `ResetSource` mutation.
    pub(super) fn reset_source_to_mutation(source_id: SourceId) -> Mutation {
        Mutation::ResetSource(risingwave_pb::stream_plan::ResetSourceMutation {
            source_id: source_id.as_raw_id(),
        })
    }

    /// Build the `StartFragmentBackfill` mutation for `ResumeBackfill`.
    pub(super) fn resume_backfill_to_mutation(
        target: &ResumeBackfillTarget,
        database_info: &InflightDatabaseInfo,
    ) -> MetaResult<Option<Mutation>> {
        {
            {
                let fragment_ids: HashSet<_> = match target {
                    ResumeBackfillTarget::Job(job_id) => {
                        database_info.backfill_fragment_ids_for_job(*job_id)?
                    }
                    ResumeBackfillTarget::Fragment(fragment_id) => {
                        if !database_info.is_backfill_fragment(*fragment_id)? {
                            return Err(MetaError::invalid_parameter(format!(
                                "fragment {} is not a backfill node",
                                fragment_id
                            )));
                        }
                        HashSet::from([*fragment_id])
                    }
                };
                if fragment_ids.is_empty() {
                    warn!(
                        ?target,
                        "resume backfill command ignored because no backfill fragments found"
                    );
                    Ok(None)
                } else {
                    Ok(Some(Mutation::StartFragmentBackfill(
                        StartFragmentBackfillMutation {
                            fragment_ids: fragment_ids.into_iter().collect(),
                        },
                    )))
                }
            }
        }
    }

    /// Build the `InjectSourceOffsets` mutation.
    pub(super) fn inject_source_offsets_to_mutation(
        source_id: SourceId,
        split_offsets: &HashMap<String, String>,
    ) -> Mutation {
        Mutation::InjectSourceOffsets(risingwave_pb::stream_plan::InjectSourceOffsetsMutation {
            source_id: source_id.as_raw_id(),
            split_offsets: split_offsets.clone(),
        })
    }

    /// Collect actors to create for `CreateStreamingJob` (non-snapshot-backfill).
    pub(super) fn create_streaming_job_actors_to_create(
        info: &CreateStreamingJobCommandInfo,
        edges: &mut FragmentEdgeBuildResult,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> StreamJobActorsToCreate {
        {
            {
                edges.collect_actors_to_create(info.stream_job_fragments.fragments.values().map(
                    |fragment| {
                        let actors = stream_actors
                            .get(&fragment.fragment_id)
                            .into_iter()
                            .flatten()
                            .map(|actor| (actor, actor_location[&actor.actor_id]));
                        (
                            fragment.fragment_id,
                            &fragment.nodes,
                            actors,
                            [], // no subscriber for new job to create
                        )
                    },
                ))
            }
        }
    }

    /// Collect actors to create for `RescheduleIntent`.
    pub(super) fn reschedule_actors_to_create(
        reschedules: &HashMap<FragmentId, Reschedule>,
        fragment_actors: &HashMap<FragmentId, HashSet<ActorId>>,
        database_info: &InflightDatabaseInfo,
        control_stream_manager: &ControlStreamManager,
    ) -> StreamJobActorsToCreate {
        {
            {
                let mut actor_upstreams = Self::collect_database_partial_graph_actor_upstreams(
                    reschedules.iter().map(|(fragment_id, reschedule)| {
                        (
                            *fragment_id,
                            reschedule.newly_created_actors.values().map(
                                |((actor, dispatchers), _)| {
                                    (actor.actor_id, dispatchers.as_slice())
                                },
                            ),
                        )
                    }),
                    Some((reschedules, fragment_actors)),
                    database_info,
                    control_stream_manager,
                );
                let mut map: HashMap<WorkerId, HashMap<_, (_, Vec<_>, _)>> = HashMap::new();
                for (fragment_id, (actor, dispatchers), worker_id) in
                    reschedules.iter().flat_map(|(fragment_id, reschedule)| {
                        reschedule
                            .newly_created_actors
                            .values()
                            .map(|(actors, status)| (*fragment_id, actors, status))
                    })
                {
                    let upstreams = actor_upstreams.remove(&actor.actor_id).unwrap_or_default();
                    map.entry(*worker_id)
                        .or_default()
                        .entry(fragment_id)
                        .or_insert_with(|| {
                            let node = database_info.fragment(fragment_id).nodes.clone();
                            let subscribers =
                                database_info.fragment_subscribers(fragment_id).collect();
                            (node, vec![], subscribers)
                        })
                        .1
                        .push((actor.clone(), upstreams, dispatchers.clone()));
                }
                map
            }
        }
    }

    /// Collect actors to create for `ReplaceStreamJob`.
    pub(super) fn replace_stream_job_actors_to_create(
        replace_table: &ReplaceStreamJobPlan,
        edges: &mut FragmentEdgeBuildResult,
        database_info: &InflightDatabaseInfo,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> StreamJobActorsToCreate {
        {
            {
                let mut actors = edges.collect_actors_to_create(
                    replace_table
                        .new_fragments
                        .fragments
                        .values()
                        .map(|fragment| {
                            let actors = stream_actors
                                .get(&fragment.fragment_id)
                                .into_iter()
                                .flatten()
                                .map(|actor| (actor, actor_location[&actor.actor_id]));
                            (
                                fragment.fragment_id,
                                &fragment.nodes,
                                actors,
                                database_info
                                    .job_subscribers(replace_table.old_fragments.stream_job_id),
                            )
                        }),
                );

                // Handle auto-refresh schema sinks
                if let Some(sinks) = &replace_table.auto_refresh_schema_sinks {
                    let sink_actors = edges.collect_actors_to_create(sinks.iter().map(|sink| {
                        (
                            sink.new_fragment.fragment_id,
                            &sink.new_fragment.nodes,
                            stream_actors
                                .get(&sink.new_fragment.fragment_id)
                                .into_iter()
                                .flatten()
                                .map(|actor| (actor, actor_location[&actor.actor_id])),
                            database_info.job_subscribers(sink.original_sink.id.as_job_id()),
                        )
                    }));
                    for (worker_id, fragment_actors) in sink_actors {
                        actors.entry(worker_id).or_default().extend(fragment_actors);
                    }
                }
                actors
            }
        }
    }

    fn generate_update_mutation_for_replace_table(
        dropped_actors: impl IntoIterator<Item = ActorId>,
        merge_updates: HashMap<FragmentId, Vec<MergeUpdate>>,
        dispatchers: FragmentActorDispatchers,
        split_assignment: &SplitAssignment,
        cdc_table_snapshot_split_assignment: Option<PbCdcTableSnapshotSplitsWithGeneration>,
        auto_refresh_schema_sinks: Option<&Vec<AutoRefreshSchemaSinkContext>>,
    ) -> Option<Mutation> {
        let dropped_actors = dropped_actors.into_iter().collect();

        let actor_new_dispatchers = dispatchers
            .into_values()
            .flatten()
            .map(|(actor_id, dispatchers)| (actor_id, Dispatchers { dispatchers }))
            .collect();

        let actor_splits = split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();
        Some(Mutation::Update(UpdateMutation {
            actor_new_dispatchers,
            merge_update: merge_updates.into_values().flatten().collect(),
            dropped_actors,
            actor_splits,
            actor_cdc_table_snapshot_splits: cdc_table_snapshot_split_assignment,
            sink_schema_change: auto_refresh_schema_sinks
                .as_ref()
                .into_iter()
                .flat_map(|sinks| {
                    sinks.iter().map(|sink| {
                        let op = if !sink.removed_column_names.is_empty() {
                            PbSinkSchemaChangeOp::DropColumns(PbSinkDropColumnsOp {
                                column_names: sink.removed_column_names.clone(),
                            })
                        } else {
                            PbSinkSchemaChangeOp::AddColumns(PbSinkAddColumnsOp {
                                fields: sink
                                    .newly_add_fields
                                    .iter()
                                    .map(|field| field.to_prost())
                                    .collect(),
                            })
                        };
                        (
                            sink.original_sink.id.as_raw_id(),
                            PbSinkSchemaChange {
                                original_schema: sink_original_schema_fields(
                                    &sink.original_sink.columns,
                                ),
                                op: Some(op),
                            },
                        )
                    })
                })
                .collect(),
            ..Default::default()
        }))
    }
}

impl Command {
    #[expect(clippy::type_complexity)]
    pub(super) fn collect_database_partial_graph_actor_upstreams(
        actor_dispatchers: impl Iterator<
            Item = (FragmentId, impl Iterator<Item = (ActorId, &[Dispatcher])>),
        >,
        reschedule_dispatcher_update: Option<(
            &HashMap<FragmentId, Reschedule>,
            &HashMap<FragmentId, HashSet<ActorId>>,
        )>,
        database_info: &InflightDatabaseInfo,
        control_stream_manager: &ControlStreamManager,
    ) -> HashMap<ActorId, ActorUpstreams> {
        let mut actor_upstreams: HashMap<ActorId, ActorUpstreams> = HashMap::new();
        for (upstream_fragment_id, upstream_actors) in actor_dispatchers {
            let upstream_fragment = database_info.fragment(upstream_fragment_id);
            for (upstream_actor_id, dispatchers) in upstream_actors {
                let upstream_actor_location =
                    upstream_fragment.actors[&upstream_actor_id].worker_id;
                let upstream_actor_host = control_stream_manager.host_addr(upstream_actor_location);
                for downstream_actor_id in dispatchers
                    .iter()
                    .flat_map(|dispatcher| dispatcher.downstream_actor_id.iter())
                {
                    actor_upstreams
                        .entry(*downstream_actor_id)
                        .or_default()
                        .entry(upstream_fragment_id)
                        .or_default()
                        .insert(
                            upstream_actor_id,
                            PbActorInfo {
                                actor_id: upstream_actor_id,
                                host: Some(upstream_actor_host.clone()),
                                partial_graph_id: to_partial_graph_id(
                                    database_info.database_id,
                                    None,
                                ),
                            },
                        );
                }
            }
        }
        if let Some((reschedules, fragment_actors)) = reschedule_dispatcher_update {
            for reschedule in reschedules.values() {
                for (upstream_fragment_id, _) in &reschedule.upstream_fragment_dispatcher_ids {
                    let upstream_fragment = database_info.fragment(*upstream_fragment_id);
                    let upstream_reschedule = reschedules.get(upstream_fragment_id);
                    for upstream_actor_id in fragment_actors
                        .get(upstream_fragment_id)
                        .expect("should exist")
                    {
                        let upstream_actor_location =
                            upstream_fragment.actors[upstream_actor_id].worker_id;
                        let upstream_actor_host =
                            control_stream_manager.host_addr(upstream_actor_location);
                        if let Some(upstream_reschedule) = upstream_reschedule
                            && upstream_reschedule
                                .removed_actors
                                .contains(upstream_actor_id)
                        {
                            continue;
                        }
                        for (_, downstream_actor_id) in
                            reschedule
                                .added_actors
                                .iter()
                                .flat_map(|(worker_id, actors)| {
                                    actors.iter().map(|actor| (*worker_id, *actor))
                                })
                        {
                            actor_upstreams
                                .entry(downstream_actor_id)
                                .or_default()
                                .entry(*upstream_fragment_id)
                                .or_default()
                                .insert(
                                    *upstream_actor_id,
                                    PbActorInfo {
                                        actor_id: *upstream_actor_id,
                                        host: Some(upstream_actor_host.clone()),
                                        partial_graph_id: to_partial_graph_id(
                                            database_info.database_id,
                                            None,
                                        ),
                                    },
                                );
                        }
                    }
                }
            }
        }
        actor_upstreams
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::data::data_type::PbTypeName;
    use risingwave_pb::plan_common::{ColumnCatalog as PbColumnCatalog, ColumnDesc};

    use super::sink_original_schema_fields;

    fn column(name: &str, type_name: PbTypeName, is_hidden: bool) -> PbColumnCatalog {
        PbColumnCatalog {
            column_desc: Some(ColumnDesc {
                column_type: Some(PbDataType {
                    type_name: type_name as i32,
                    ..Default::default()
                }),
                name: name.to_owned(),
                ..Default::default()
            }),
            is_hidden,
        }
    }

    #[test]
    fn test_sink_original_schema_fields_skips_hidden_columns() {
        let columns = vec![
            column("k", PbTypeName::Int32, false),
            column("v", PbTypeName::Varchar, false),
            column("_row_id", PbTypeName::Serial, true),
        ];

        let fields = sink_original_schema_fields(&columns);
        let field_names = fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(field_names, ["k", "v"]);
    }
}
