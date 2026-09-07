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

mod prelude;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::future::pending;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::vec;

use await_tree::InstrumentAwait;
use enum_as_inner::EnumAsInner;
use futures::future::try_join_all;
use futures::stream::{BoxStream, FusedStream, FuturesUnordered, StreamFuture};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::config::StreamingConfig;
use risingwave_common::metrics::LabelGuardedMetric;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl};
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_common::util::tracing::TracingContext;
use risingwave_common::util::value_encoding::{DatumFromProtoExt, DatumToProtoExt};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::source::SplitImpl;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_pb::data::PbEpoch;
use risingwave_pb::expr::PbInputRef;
use risingwave_pb::stream_plan::add_mutation::PbNewUpstreamSink;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation as PbMutation;
use risingwave_pb::stream_plan::stream_node::PbStreamKind;
use risingwave_pb::stream_plan::throttle_mutation::ThrottleConfig;
use risingwave_pb::stream_plan::update_mutation::{DispatcherUpdate, MergeUpdate};
use risingwave_pb::stream_plan::{
    IcebergPkIndexCompactionContext, PbBarrier, PbBarrierMutation, PbDispatcher,
    PbSinkSchemaChange, PbStreamMessageBatch, PbWatermark, SubscriptionUpstreamInfo,
};
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

use crate::error::StreamResult;
use crate::executor::exchange::input::{
    BoxedActorInput, BoxedInput, assert_equal_dispatcher_barrier, new_input,
};
use crate::executor::monitor::ActorInputMetrics;
use crate::executor::prelude::StreamingMetrics;
use crate::executor::watermark::BufferedWatermarks;
use crate::task::{ActorId, FragmentId, LocalBarrierManager};

mod actor;
mod barrier_align;
pub mod exchange;
pub mod monitor;

pub mod aggregate;
pub mod asof_join;
mod backfill;
mod barrier_recv;
mod batch_query;
mod chain;
mod changelog;
mod dedup;
mod dispatch;
pub mod dml;
mod dynamic_filter;
pub mod eowc;
pub mod error;
mod expand;
mod filter;
mod gap_fill;
pub mod hash_join;
mod hop_window;
pub(crate) mod iceberg_with_pk_index;
mod join;
pub mod locality_provider;
mod lookup;
mod lookup_union;
pub mod match_recognize;
mod merge;
mod mview;
mod nested_loop_temporal_join;
mod no_op;
mod now;
mod over_window;
pub mod project;
mod receiver;
pub mod row_id_gen;
mod sink;
pub mod source;
mod stream_reader;
pub mod subtask;
mod temporal_join;
mod top_n;
mod troublemaker;
mod union;
mod upstream_sink_union;
mod values;
mod watermark;
mod watermark_filter;
mod wrapper;

mod approx_percentile;

mod row_merge;

#[cfg(test)]
mod integration_tests;
mod sync_kv_log_store;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
mod utils;
mod vector;

pub use actor::{Actor, ActorContext, ActorContextRef};
use anyhow::{Context, anyhow};
pub use approx_percentile::global::GlobalApproxPercentileExecutor;
pub use approx_percentile::local::LocalApproxPercentileExecutor;
pub use backfill::arrangement_backfill::*;
pub use backfill::cdc::{
    CdcBackfillExecutor, ExternalStorageTable, ParallelizedCdcBackfillExecutor,
};
pub use backfill::no_shuffle_backfill::*;
pub use backfill::snapshot_backfill::*;
pub use barrier_recv::BarrierRecvExecutor;
pub use batch_query::BatchQueryExecutor;
pub use chain::ChainExecutor;
pub use changelog::ChangeLogExecutor;
pub use dedup::AppendOnlyDedupExecutor;
pub use dispatch::{DispatchExecutor, SyncLogStoreDispatchExecutor};
pub use dynamic_filter::DynamicFilterExecutor;
pub use error::{StreamExecutorError, StreamExecutorResult};
pub use expand::ExpandExecutor;
pub use filter::{FilterExecutor, UpsertFilterExecutor};
pub use gap_fill::{GapFillExecutor, GapFillExecutorArgs};
pub use hash_join::*;
pub use hop_window::HopWindowExecutor;
pub use iceberg_with_pk_index::{
    CompactionResolverExecutor, IcebergWriterImpl, PositionDeleteHandlerImpl,
    PositionDeleteMergerExecutor, WriterExecutor,
};
pub use join::asof_join::{AsOfCpuEncoding, AsOfMemoryEncoding};
pub use join::row::{CachedJoinRow, CpuEncoding, JoinEncoding, MemoryEncoding};
pub use join::{AsOfDesc, AsOfJoinType, JoinType};
pub use lookup::*;
pub use lookup_union::LookupUnionExecutor;
pub use merge::MergeExecutor;
pub(crate) use merge::{MergeExecutorInput, MergeExecutorUpstream};
pub use mview::{MaterializeExecutor, RefreshableMaterializeArgs};
pub use nested_loop_temporal_join::NestedLoopTemporalJoinExecutor;
pub use no_op::NoOpExecutor;
pub use now::*;
pub use over_window::*;
pub use receiver::ReceiverExecutor;
use risingwave_common::id::SourceId;
pub use row_merge::RowMergeExecutor;
pub use sink::SinkExecutor;
pub use sync_kv_log_store::SyncedKvLogStoreExecutor;
pub use sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
pub use temporal_join::TemporalJoinExecutor;
pub use top_n::{
    AppendOnlyGroupTopNExecutor, AppendOnlyTopNExecutor, GroupTopNExecutor, TopNExecutor,
};
pub use troublemaker::TroublemakerExecutor;
pub use union::UnionExecutor;
pub use upstream_sink_union::{UpstreamFragmentInfo, UpstreamSinkUnionExecutor};
pub use utils::DummyExecutor;
pub use values::ValuesExecutor;
pub use vector::*;
pub use watermark_filter::{UpsertWatermarkFilterExecutor, WatermarkFilterExecutor};
pub use wrapper::WrapperExecutor;

use self::barrier_align::AlignedMessageStream;

pub type MessageStreamItemInner<M> = StreamExecutorResult<MessageInner<M>>;
pub type MessageStreamItem = MessageStreamItemInner<BarrierMutationType>;
pub type DispatcherMessageStreamItem = StreamExecutorResult<DispatcherMessage>;
pub type BoxedMessageStream = BoxStream<'static, MessageStreamItem>;

pub use risingwave_common::util::epoch::task_local::{curr_epoch, epoch, prev_epoch};
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::source::cdc::{
    CdcTableSnapshotSplitAssignmentWithGeneration,
    build_actor_cdc_table_snapshot_splits_with_generation,
};
use risingwave_pb::id::{ExecutorId, SubscriberId};
use risingwave_pb::stream_plan::stream_message_batch::{BarrierBatch, StreamMessageBatch};

pub trait MessageStreamInner<M> = Stream<Item = MessageStreamItemInner<M>> + Send;
pub trait MessageStream = Stream<Item = MessageStreamItem> + Send;
pub trait DispatcherMessageStream = Stream<Item = DispatcherMessageStreamItem> + Send;

/// Static information of an executor.
#[derive(Debug, Default, Clone)]
pub struct ExecutorInfo {
    /// The schema of the OUTPUT of the executor.
    pub schema: Schema,

    /// The stream key indices of the OUTPUT of the executor.
    pub stream_key: StreamKey,

    /// The stream kind of the OUTPUT of the executor.
    pub stream_kind: PbStreamKind,

    /// Identity of the executor.
    pub identity: String,

    /// The executor id of the executor.
    pub id: ExecutorId,
}

impl ExecutorInfo {
    pub fn for_test(schema: Schema, stream_key: StreamKey, identity: String, id: u64) -> Self {
        Self {
            schema,
            stream_key,
            stream_kind: PbStreamKind::Retract, // dummy value for test
            identity,
            id: id.into(),
        }
    }
}

/// [`Execute`] describes the methods an executor should implement to handle control messages.
pub trait Execute: Send + 'static {
    fn execute(self: Box<Self>) -> BoxedMessageStream;

    fn boxed(self) -> Box<dyn Execute>
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

/// [`Executor`] combines the static information ([`ExecutorInfo`]) and the executable object to
/// handle messages ([`Execute`]).
pub struct Executor {
    info: ExecutorInfo,
    execute: Box<dyn Execute>,
}

impl Executor {
    pub fn new(info: ExecutorInfo, execute: Box<dyn Execute>) -> Self {
        Self { info, execute }
    }

    pub fn info(&self) -> &ExecutorInfo {
        &self.info
    }

    pub fn schema(&self) -> &Schema {
        &self.info.schema
    }

    pub fn stream_key(&self) -> StreamKeyRef<'_> {
        &self.info.stream_key
    }

    pub fn stream_kind(&self) -> PbStreamKind {
        self.info.stream_kind
    }

    pub fn identity(&self) -> &str {
        &self.info.identity
    }

    pub fn execute(self) -> BoxedMessageStream {
        self.execute.execute()
    }
}

impl std::fmt::Debug for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.identity())
    }
}

impl From<(ExecutorInfo, Box<dyn Execute>)> for Executor {
    fn from((info, execute): (ExecutorInfo, Box<dyn Execute>)) -> Self {
        Self::new(info, execute)
    }
}

impl<E> From<(ExecutorInfo, E)> for Executor
where
    E: Execute,
{
    fn from((info, execute): (ExecutorInfo, E)) -> Self {
        Self::new(info, execute.boxed())
    }
}

pub const INVALID_EPOCH: u64 = 0;

type UpstreamFragmentId = FragmentId;
type SplitAssignments = HashMap<ActorId, Vec<SplitImpl>>;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct UpdateMutation {
    pub dispatchers: HashMap<ActorId, Vec<DispatcherUpdate>>,
    pub merges: HashMap<(ActorId, UpstreamFragmentId), MergeUpdate>,
    pub vnode_bitmaps: HashMap<ActorId, Arc<Bitmap>>,
    pub dropped_actors: HashSet<ActorId>,
    pub actor_splits: SplitAssignments,
    pub actor_new_dispatchers: HashMap<ActorId, Vec<PbDispatcher>>,
    pub actor_cdc_table_snapshot_splits: CdcTableSnapshotSplitAssignmentWithGeneration,
    pub sink_schema_change: HashMap<SinkId, PbSinkSchemaChange>,
    pub subscriptions_to_drop: Vec<SubscriptionUpstreamInfo>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct AddMutation {
    pub adds: HashMap<ActorId, Vec<PbDispatcher>>,
    pub added_actors: HashSet<ActorId>,
    pub dropped_actors: HashSet<ActorId>,
    // TODO: remove this and use `SourceChangesSplit` after we support multiple mutations.
    pub splits: SplitAssignments,
    pub pause: bool,
    /// (`upstream_mv_table_id`,  `subscriber_id`)
    pub subscriptions_to_add: Vec<(TableId, SubscriberId)>,
    /// nodes which should start backfill
    pub backfill_nodes_to_pause: HashSet<FragmentId>,
    pub actor_cdc_table_snapshot_splits: CdcTableSnapshotSplitAssignmentWithGeneration,
    pub new_upstream_sinks: HashMap<FragmentId, PbNewUpstreamSink>,
    pub sink_log_store_flush: HashSet<SinkId>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct StopMutation {
    pub dropped_actors: HashSet<ActorId>,
    pub dropped_sink_fragments: HashSet<FragmentId>,
}

/// See [`PbMutation`] for the semantics of each mutation.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Stop(StopMutation),
    Update(UpdateMutation),
    Add(AddMutation),
    SourceChangeSplit(SplitAssignments),
    Pause,
    Resume,
    Throttle(HashMap<FragmentId, ThrottleConfig>),
    ConnectorPropsChange(HashMap<u32, HashMap<String, String>>),
    DropSubscriptions {
        /// `subscriber` -> `upstream_mv_table_id`
        subscriptions_to_drop: Vec<SubscriptionUpstreamInfo>,
    },
    StartFragmentBackfill {
        fragment_ids: HashSet<FragmentId>,
    },
    RefreshStart {
        table_id: TableId,
        associated_source_id: SourceId,
    },
    ListFinish {
        associated_source_id: SourceId,
    },
    LoadFinish {
        associated_source_id: SourceId,
    },
    ResetSource {
        source_id: SourceId,
    },
    InjectSourceOffsets {
        source_id: SourceId,
        /// Split ID -> offset (JSON-encoded based on connector type)
        split_offsets: HashMap<String, String>,
    },
}

/// The generic type `M` is the mutation type of the barrier.
///
/// For barrier of in the dispatcher, `M` is `()`, which means the mutation is erased.
/// For barrier flowing within the streaming actor, `M` is the normal `BarrierMutationType`.
#[derive(Debug, Clone)]
pub struct BarrierInner<M> {
    pub epoch: EpochPair,
    pub mutation: M,
    pub kind: BarrierKind,

    /// Tracing context for the **current** epoch of this barrier.
    pub tracing_context: TracingContext,
    pub iceberg_pk_index_compaction: Option<IcebergPkIndexCompactionContext>,
}

pub type BarrierMutationType = Option<Arc<Mutation>>;
pub type Barrier = BarrierInner<BarrierMutationType>;
pub type DispatcherBarrier = BarrierInner<()>;

impl<M: Default> BarrierInner<M> {
    /// Create a plain barrier.
    pub fn new_test_barrier(epoch: u64) -> Self {
        Self {
            epoch: EpochPair::new_test_epoch(epoch),
            kind: BarrierKind::Checkpoint,
            tracing_context: TracingContext::none(),
            mutation: Default::default(),
            iceberg_pk_index_compaction: None,
        }
    }

    pub fn with_prev_epoch_for_test(epoch: u64, prev_epoch: u64) -> Self {
        Self {
            epoch: EpochPair::new(epoch, prev_epoch),
            kind: BarrierKind::Checkpoint,
            tracing_context: TracingContext::none(),
            mutation: Default::default(),
            iceberg_pk_index_compaction: None,
        }
    }
}

impl Barrier {
    pub fn into_dispatcher(self) -> DispatcherBarrier {
        DispatcherBarrier {
            epoch: self.epoch,
            mutation: (),
            kind: self.kind,
            tracing_context: self.tracing_context,
            iceberg_pk_index_compaction: self.iceberg_pk_index_compaction,
        }
    }

    #[must_use]
    pub fn with_mutation(self, mutation: Mutation) -> Self {
        Self {
            mutation: Some(Arc::new(mutation)),
            ..self
        }
    }

    #[must_use]
    pub fn with_stop(self) -> Self {
        self.with_mutation(Mutation::Stop(StopMutation {
            dropped_actors: Default::default(),
            dropped_sink_fragments: Default::default(),
        }))
    }

    pub fn with_iceberg_pk_index_compaction(
        self,
        context: IcebergPkIndexCompactionContext,
    ) -> Self {
        Self {
            iceberg_pk_index_compaction: Some(context),
            ..self
        }
    }

    /// Whether this barrier carries stop mutation.
    pub fn is_with_stop_mutation(&self) -> bool {
        matches!(self.mutation.as_deref(), Some(Mutation::Stop(_)))
    }

    /// Whether this barrier is to stop the actor with `actor_id`.
    pub fn is_stop(&self, actor_id: ActorId) -> bool {
        self.all_stop_actors()
            .is_some_and(|actors| actors.contains(&actor_id))
    }

    pub fn is_checkpoint(&self) -> bool {
        self.kind == BarrierKind::Checkpoint
    }

    /// Get the initial split assignments for the actor with `actor_id`.
    ///
    /// This should only be called on the initial barrier received by the executor. It must be
    ///
    /// - `Add` mutation when it's a new streaming job, or recovery.
    /// - `Update` mutation when it's created for scaling.
    ///
    /// Note that `SourceChangeSplit` is **not** included, because it's only used for changing splits
    /// of existing executors.
    pub fn initial_split_assignment(&self, actor_id: ActorId) -> Option<&[SplitImpl]> {
        match self.mutation.as_deref()? {
            Mutation::Update(UpdateMutation { actor_splits, .. })
            | Mutation::Add(AddMutation {
                splits: actor_splits,
                ..
            }) => actor_splits.get(&actor_id),

            _ => {
                if cfg!(debug_assertions) {
                    panic!(
                        "the initial mutation of the barrier should not be {:?}",
                        self.mutation
                    );
                }
                None
            }
        }
        .map(|s| s.as_slice())
    }

    /// Get all actors that to be stopped (dropped) by this barrier.
    pub fn all_stop_actors(&self) -> Option<&HashSet<ActorId>> {
        self.mutation.as_deref()?.all_stop_actors()
    }

    /// Whether this barrier is to newly add the actor with `actor_id`. This is used for `Chain` and
    /// `Values` to decide whether to output the existing (historical) data.
    ///
    /// By "newly", we mean the actor belongs to a subgraph of a new streaming job. That is, actors
    /// added for scaling are not included.
    pub fn is_newly_added(&self, actor_id: ActorId) -> bool {
        match self.mutation.as_deref() {
            Some(Mutation::Add(AddMutation { added_actors, .. })) => {
                added_actors.contains(&actor_id)
            }
            _ => false,
        }
    }

    pub fn should_start_fragment_backfill(&self, fragment_id: FragmentId) -> bool {
        if let Some(Mutation::StartFragmentBackfill { fragment_ids }) = self.mutation.as_deref() {
            fragment_ids.contains(&fragment_id)
        } else {
            false
        }
    }

    /// Whether this barrier adds new downstream fragment for the actor with `upstream_actor_id`.
    ///
    /// # Use case
    /// Some optimizations are applied when an actor doesn't have any downstreams ("standalone" actors).
    /// * Pause a standalone shared `SourceExecutor`.
    /// * Disable a standalone `MaterializeExecutor`'s conflict check.
    ///
    /// This is implemented by checking `actor_context.initial_dispatch_num` on startup, and
    /// check `has_more_downstream_fragments` on barrier to see whether the optimization
    /// needs to be turned off.
    ///
    /// ## Some special cases not included
    ///
    /// Note that this is not `has_new_downstream_actor/fragment`. For our use case, we only
    /// care about **number of downstream fragments** (more precisely, existence).
    /// - When scaling, the number of downstream actors is changed, and they are "new", but downstream fragments is not changed.
    /// - When `ALTER TABLE sink_into_table`, the fragment is replaced with a "new" one, but the number is not changed.
    pub fn has_more_downstream_fragments(&self, upstream_actor_id: ActorId) -> bool {
        let Some(mutation) = self.mutation.as_deref() else {
            return false;
        };
        match mutation {
            // Add is for mv, index and sink creation.
            Mutation::Add(AddMutation { adds, .. }) => adds.get(&upstream_actor_id).is_some(),
            Mutation::Update(_)
            | Mutation::Stop(_)
            | Mutation::Pause
            | Mutation::Resume
            | Mutation::SourceChangeSplit(_)
            | Mutation::Throttle { .. }
            | Mutation::DropSubscriptions { .. }
            | Mutation::ConnectorPropsChange(_)
            | Mutation::StartFragmentBackfill { .. }
            | Mutation::RefreshStart { .. }
            | Mutation::ListFinish { .. }
            | Mutation::LoadFinish { .. }
            | Mutation::ResetSource { .. }
            | Mutation::InjectSourceOffsets { .. } => false,
        }
    }

    /// Whether this barrier requires the executor to pause its data stream on startup.
    pub fn is_pause_on_startup(&self) -> bool {
        match self.mutation.as_deref() {
            Some(Mutation::Add(AddMutation { pause, .. })) => *pause,
            _ => false,
        }
    }

    pub fn is_backfill_pause_on_startup(&self, backfill_fragment_id: FragmentId) -> bool {
        match self.mutation.as_deref() {
            Some(Mutation::Add(AddMutation {
                backfill_nodes_to_pause,
                ..
            })) => backfill_nodes_to_pause.contains(&backfill_fragment_id),
            Some(Mutation::Update(_)) => false,
            _ => {
                tracing::warn!(
                    "expected an AddMutation or UpdateMutation on Startup, instead got {:?}",
                    self
                );
                false
            }
        }
    }

    /// Whether this barrier is for resume.
    pub fn is_resume(&self) -> bool {
        matches!(self.mutation.as_deref(), Some(Mutation::Resume))
    }

    /// Returns the [`MergeUpdate`] if this barrier is to update the merge executors for the actor
    /// with `actor_id`.
    pub fn as_update_merge(
        &self,
        actor_id: ActorId,
        upstream_fragment_id: UpstreamFragmentId,
    ) -> Option<&MergeUpdate> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Update(UpdateMutation { merges, .. }) => {
                    merges.get(&(actor_id, upstream_fragment_id))
                }
                _ => None,
            })
    }

    /// Returns the new upstream sink information if this barrier is to add a new upstream sink for
    /// the specified downstream fragment.
    pub fn as_new_upstream_sink(&self, fragment_id: FragmentId) -> Option<&PbNewUpstreamSink> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Add(AddMutation {
                    new_upstream_sinks, ..
                }) => new_upstream_sinks.get(&fragment_id),
                _ => None,
            })
    }

    /// Returns the dropped upstream sink-fragment if this barrier is to drop any sink.
    pub fn as_dropped_upstream_sinks(&self) -> Option<&HashSet<FragmentId>> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Stop(StopMutation {
                    dropped_sink_fragments,
                    ..
                }) => Some(dropped_sink_fragments),
                _ => None,
            })
    }

    /// Returns the new vnode bitmap if this barrier is to update the vnode bitmap for the actor
    /// with `actor_id`.
    ///
    /// Actually, this vnode bitmap update is only useful for the record accessing validation for
    /// distributed executors, since the read/write pattern will never be across multiple vnodes.
    pub fn as_update_vnode_bitmap(&self, actor_id: ActorId) -> Option<Arc<Bitmap>> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Update(UpdateMutation { vnode_bitmaps, .. }) => {
                    vnode_bitmaps.get(&actor_id).cloned()
                }
                _ => None,
            })
    }

    pub fn assume_no_update_vnode_bitmap(&self, actor_id: ActorId) -> StreamExecutorResult<()> {
        if self.as_update_vnode_bitmap(actor_id).is_some() {
            return Err(anyhow!("updating vnode bitmap in place is not supported").into());
        }
        Ok(())
    }

    pub fn as_sink_schema_change(&self, sink_id: SinkId) -> Option<PbSinkSchemaChange> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Update(UpdateMutation {
                    sink_schema_change, ..
                }) => sink_schema_change.get(&sink_id).cloned(),
                _ => None,
            })
    }

    pub fn should_flush_sink_log_store(&self, sink_id: SinkId) -> bool {
        self.mutation
            .as_deref()
            .is_some_and(|mutation| match mutation {
                Mutation::Add(AddMutation {
                    sink_log_store_flush,
                    ..
                }) => sink_log_store_flush.contains(&sink_id),
                _ => false,
            })
    }

    pub fn as_subscriptions_to_drop(&self) -> Option<&[SubscriptionUpstreamInfo]> {
        match self.mutation.as_deref() {
            Some(Mutation::DropSubscriptions {
                subscriptions_to_drop,
            })
            | Some(Mutation::Update(UpdateMutation {
                subscriptions_to_drop,
                ..
            })) => Some(subscriptions_to_drop.as_slice()),
            _ => None,
        }
    }

    pub fn get_curr_epoch(&self) -> Epoch {
        Epoch(self.epoch.curr)
    }

    /// Retrieve the tracing context for the **current** epoch of this barrier.
    pub fn tracing_context(&self) -> &TracingContext {
        &self.tracing_context
    }

    pub fn added_subscriber_on_mv_table(
        &self,
        mv_table_id: TableId,
    ) -> impl Iterator<Item = SubscriberId> + '_ {
        if let Some(Mutation::Add(add)) = self.mutation.as_deref() {
            Some(add)
        } else {
            None
        }
        .into_iter()
        .flat_map(move |add| {
            add.subscriptions_to_add.iter().filter_map(
                move |(upstream_mv_table_id, subscriber_id)| {
                    if *upstream_mv_table_id == mv_table_id {
                        Some(*subscriber_id)
                    } else {
                        None
                    }
                },
            )
        })
    }
}

impl<M: PartialEq> PartialEq for BarrierInner<M> {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.mutation == other.mutation
    }
}

impl Mutation {
    /// Get all actors to be stopped (dropped) by this mutation.
    pub fn all_stop_actors(&self) -> Option<&HashSet<ActorId>> {
        match self {
            Mutation::Stop(StopMutation { dropped_actors, .. })
            | Mutation::Update(UpdateMutation { dropped_actors, .. })
            | Mutation::Add(AddMutation { dropped_actors, .. }) => Some(dropped_actors),
            _ => None,
        }
    }

    /// Return true if the mutation stops the given actor.
    pub fn is_stop(&self, actor_id: ActorId) -> bool {
        self.all_stop_actors()
            .is_some_and(|actors| actors.contains(&actor_id))
    }

    /// Return true if the mutation is stop.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop_mutation(&self) -> bool {
        matches!(self, Mutation::Stop(_))
    }

    #[cfg(test)]
    fn to_protobuf(&self) -> PbMutation {
        use risingwave_pb::source::{
            ConnectorSplit, ConnectorSplits, PbCdcTableSnapshotSplitsWithGeneration,
        };
        use risingwave_pb::stream_plan::connector_props_change_mutation::ConnectorPropsInfo;
        use risingwave_pb::stream_plan::{
            PbAddMutation, PbConnectorPropsChangeMutation, PbDispatchers,
            PbDropSubscriptionsMutation, PbPauseMutation, PbResumeMutation,
            PbSourceChangeSplitMutation, PbStartFragmentBackfillMutation, PbStopMutation,
            PbThrottleMutation, PbUpdateMutation,
        };
        let actor_splits_to_protobuf = |actor_splits: &SplitAssignments| {
            actor_splits
                .iter()
                .map(|(&actor_id, splits)| {
                    (
                        actor_id,
                        ConnectorSplits {
                            splits: splits.clone().iter().map(ConnectorSplit::from).collect(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        match self {
            Mutation::Stop(StopMutation {
                dropped_actors,
                dropped_sink_fragments,
            }) => PbMutation::Stop(PbStopMutation {
                actors: dropped_actors.iter().copied().collect(),
                dropped_sink_fragments: dropped_sink_fragments.iter().copied().collect(),
            }),
            Mutation::Update(UpdateMutation {
                dispatchers,
                merges,
                vnode_bitmaps,
                dropped_actors,
                actor_splits,
                actor_new_dispatchers,
                actor_cdc_table_snapshot_splits,
                sink_schema_change,
                subscriptions_to_drop,
            }) => PbMutation::Update(PbUpdateMutation {
                dispatcher_update: dispatchers.values().flatten().cloned().collect(),
                merge_update: merges.values().cloned().collect(),
                actor_vnode_bitmap_update: vnode_bitmaps
                    .iter()
                    .map(|(&actor_id, bitmap)| (actor_id, bitmap.to_protobuf()))
                    .collect(),
                dropped_actors: dropped_actors.iter().copied().collect(),
                actor_splits: actor_splits_to_protobuf(actor_splits),
                actor_new_dispatchers: actor_new_dispatchers
                    .iter()
                    .map(|(&actor_id, dispatchers)| {
                        (
                            actor_id,
                            PbDispatchers {
                                dispatchers: dispatchers.clone(),
                            },
                        )
                    })
                    .collect(),
                actor_cdc_table_snapshot_splits: Some(PbCdcTableSnapshotSplitsWithGeneration {
                    splits:actor_cdc_table_snapshot_splits.splits.iter().map(|(actor_id,(splits, generation))| {
                        (*actor_id, risingwave_pb::source::PbCdcTableSnapshotSplits {
                            splits: splits.iter().map(risingwave_connector::source::cdc::build_cdc_table_snapshot_split).collect(),
                            generation: *generation,
                        })
                    }).collect()
                }),
                sink_schema_change: sink_schema_change
                    .iter()
                    .map(|(sink_id, change)| ((*sink_id).as_raw_id(), change.clone()))
                    .collect(),
                subscriptions_to_drop: subscriptions_to_drop.clone(),
            }),
            Mutation::Add(AddMutation {
                adds,
                added_actors,
                dropped_actors,
                splits,
                pause,
                subscriptions_to_add,
                backfill_nodes_to_pause,
                actor_cdc_table_snapshot_splits,
                new_upstream_sinks,
                sink_log_store_flush,
            }) => PbMutation::Add(PbAddMutation {
                actor_dispatchers: adds
                    .iter()
                    .map(|(&actor_id, dispatchers)| {
                        (
                            actor_id,
                            PbDispatchers {
                                dispatchers: dispatchers.clone(),
                            },
                        )
                    })
                    .collect(),
                added_actors: added_actors.iter().copied().collect(),
                actor_splits: actor_splits_to_protobuf(splits),
                pause: *pause,
                subscriptions_to_add: subscriptions_to_add
                    .iter()
                    .map(|(table_id, subscriber_id)| SubscriptionUpstreamInfo {
                        subscriber_id: *subscriber_id,
                        upstream_mv_table_id: *table_id,
                    })
                    .collect(),
                backfill_nodes_to_pause: backfill_nodes_to_pause.iter().copied().collect(),
                actor_cdc_table_snapshot_splits:
                Some(PbCdcTableSnapshotSplitsWithGeneration {
                    splits:actor_cdc_table_snapshot_splits.splits.iter().map(|(actor_id,(splits, generation))| {
                        (*actor_id, risingwave_pb::source::PbCdcTableSnapshotSplits {
                            splits: splits.iter().map(risingwave_connector::source::cdc::build_cdc_table_snapshot_split).collect(),
                            generation: *generation,
                        })
                    }).collect()
                }),
                new_upstream_sinks: new_upstream_sinks
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect(),
                dropped_actors: dropped_actors.iter().copied().collect(),
                sink_log_store_flush: sink_log_store_flush.iter().copied().collect(),
            }),
            Mutation::SourceChangeSplit(changes) => {
                PbMutation::Splits(PbSourceChangeSplitMutation {
                    actor_splits: changes
                        .iter()
                        .map(|(&actor_id, splits)| {
                            (
                                actor_id,
                                ConnectorSplits {
                                    splits: splits
                                        .clone()
                                        .iter()
                                        .map(ConnectorSplit::from)
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
            }
            Mutation::Pause => PbMutation::Pause(PbPauseMutation {}),
            Mutation::Resume => PbMutation::Resume(PbResumeMutation {}),
            Mutation::Throttle (changes) => PbMutation::Throttle(PbThrottleMutation {
                fragment_throttle: changes.clone(),
            }),
            Mutation::DropSubscriptions {
                subscriptions_to_drop,
            } => PbMutation::DropSubscriptions(PbDropSubscriptionsMutation {
                info: subscriptions_to_drop.clone(),
            }),
            Mutation::ConnectorPropsChange(map) => {
                PbMutation::ConnectorPropsChange(PbConnectorPropsChangeMutation {
                    connector_props_infos: map
                        .iter()
                        .map(|(actor_id, options)| {
                            (
                                *actor_id,
                                ConnectorPropsInfo {
                                    connector_props_info: options
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
            }
            Mutation::StartFragmentBackfill { fragment_ids } => {
                PbMutation::StartFragmentBackfill(PbStartFragmentBackfillMutation {
                    fragment_ids: fragment_ids.iter().copied().collect(),
                })
            }
            Mutation::RefreshStart {
                table_id,
                associated_source_id,
            } => PbMutation::RefreshStart(risingwave_pb::stream_plan::RefreshStartMutation {
                table_id: *table_id,
                associated_source_id: *associated_source_id,
            }),
            Mutation::ListFinish {
                associated_source_id,
            } => PbMutation::ListFinish(risingwave_pb::stream_plan::ListFinishMutation {
                associated_source_id: *associated_source_id,
            }),
            Mutation::LoadFinish {
                associated_source_id,
            } => PbMutation::LoadFinish(risingwave_pb::stream_plan::LoadFinishMutation {
                associated_source_id: *associated_source_id,
            }),
            Mutation::ResetSource { source_id } => {
                PbMutation::ResetSource(risingwave_pb::stream_plan::ResetSourceMutation {
                    source_id: source_id.as_raw_id(),
                })
            }
            Mutation::InjectSourceOffsets {
                source_id,
                split_offsets,
            } => PbMutation::InjectSourceOffsets(
                risingwave_pb::stream_plan::InjectSourceOffsetsMutation {
                    source_id: source_id.as_raw_id(),
                    split_offsets: split_offsets.clone(),
                },
            ),
        }
    }

    fn from_protobuf(prost: &PbMutation) -> StreamExecutorResult<Self> {
        let mutation = match prost {
            PbMutation::Stop(stop) => Mutation::Stop(StopMutation {
                dropped_actors: stop.actors.iter().copied().collect(),
                dropped_sink_fragments: stop.dropped_sink_fragments.iter().copied().collect(),
            }),

            PbMutation::Update(update) => Mutation::Update(UpdateMutation {
                dispatchers: update
                    .dispatcher_update
                    .iter()
                    .map(|u| (u.actor_id, u.clone()))
                    .into_group_map(),
                merges: update
                    .merge_update
                    .iter()
                    .map(|u| ((u.actor_id, u.upstream_fragment_id), u.clone()))
                    .collect(),
                vnode_bitmaps: update
                    .actor_vnode_bitmap_update
                    .iter()
                    .map(|(&actor_id, bitmap)| (actor_id, Arc::new(bitmap.into())))
                    .collect(),
                dropped_actors: update.dropped_actors.iter().copied().collect(),
                actor_splits: update
                    .actor_splits
                    .iter()
                    .map(|(&actor_id, splits)| {
                        (
                            actor_id,
                            splits
                                .splits
                                .iter()
                                .map(|split| split.try_into().unwrap())
                                .collect(),
                        )
                    })
                    .collect(),
                actor_new_dispatchers: update
                    .actor_new_dispatchers
                    .iter()
                    .map(|(&actor_id, dispatchers)| (actor_id, dispatchers.dispatchers.clone()))
                    .collect(),
                actor_cdc_table_snapshot_splits:
                    build_actor_cdc_table_snapshot_splits_with_generation(
                        update
                            .actor_cdc_table_snapshot_splits
                            .clone()
                            .unwrap_or_default(),
                    ),
                sink_schema_change: update
                    .sink_schema_change
                    .iter()
                    .map(|(sink_id, change)| (SinkId::from(*sink_id), change.clone()))
                    .collect(),
                subscriptions_to_drop: update.subscriptions_to_drop.clone(),
            }),

            PbMutation::Add(add) => Mutation::Add(AddMutation {
                adds: add
                    .actor_dispatchers
                    .iter()
                    .map(|(&actor_id, dispatchers)| (actor_id, dispatchers.dispatchers.clone()))
                    .collect(),
                added_actors: add.added_actors.iter().copied().collect(),
                dropped_actors: add.dropped_actors.iter().copied().collect(),
                // TODO: remove this and use `SourceChangesSplit` after we support multiple
                // mutations.
                splits: add
                    .actor_splits
                    .iter()
                    .map(|(&actor_id, splits)| {
                        (
                            actor_id,
                            splits
                                .splits
                                .iter()
                                .map(|split| split.try_into().unwrap())
                                .collect(),
                        )
                    })
                    .collect(),
                pause: add.pause,
                subscriptions_to_add: add
                    .subscriptions_to_add
                    .iter()
                    .map(
                        |SubscriptionUpstreamInfo {
                             subscriber_id,
                             upstream_mv_table_id,
                         }| { (*upstream_mv_table_id, *subscriber_id) },
                    )
                    .collect(),
                backfill_nodes_to_pause: add.backfill_nodes_to_pause.iter().copied().collect(),
                actor_cdc_table_snapshot_splits:
                    build_actor_cdc_table_snapshot_splits_with_generation(
                        add.actor_cdc_table_snapshot_splits
                            .clone()
                            .unwrap_or_default(),
                    ),
                new_upstream_sinks: add
                    .new_upstream_sinks
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect(),
                sink_log_store_flush: add.sink_log_store_flush.iter().copied().collect(),
            }),

            PbMutation::Splits(s) => {
                let mut change_splits: Vec<(ActorId, Vec<SplitImpl>)> =
                    Vec::with_capacity(s.actor_splits.len());
                for (&actor_id, splits) in &s.actor_splits {
                    if !splits.splits.is_empty() {
                        change_splits.push((
                            actor_id,
                            splits
                                .splits
                                .iter()
                                .map(SplitImpl::try_from)
                                .try_collect()?,
                        ));
                    }
                }
                Mutation::SourceChangeSplit(change_splits.into_iter().collect())
            }
            PbMutation::Pause(_) => Mutation::Pause,
            PbMutation::Resume(_) => Mutation::Resume,
            PbMutation::Throttle(changes) => Mutation::Throttle(changes.fragment_throttle.clone()),
            PbMutation::DropSubscriptions(drop) => Mutation::DropSubscriptions {
                subscriptions_to_drop: drop.info.clone(),
            },
            PbMutation::ConnectorPropsChange(alter_connector_props) => {
                Mutation::ConnectorPropsChange(
                    alter_connector_props
                        .connector_props_infos
                        .iter()
                        .map(|(connector_id, options)| {
                            (
                                *connector_id,
                                options
                                    .connector_props_info
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                            )
                        })
                        .collect(),
                )
            }
            PbMutation::StartFragmentBackfill(start_fragment_backfill) => {
                Mutation::StartFragmentBackfill {
                    fragment_ids: start_fragment_backfill
                        .fragment_ids
                        .iter()
                        .copied()
                        .collect(),
                }
            }
            PbMutation::RefreshStart(refresh_start) => Mutation::RefreshStart {
                table_id: refresh_start.table_id,
                associated_source_id: refresh_start.associated_source_id,
            },
            PbMutation::ListFinish(list_finish) => Mutation::ListFinish {
                associated_source_id: list_finish.associated_source_id,
            },
            PbMutation::LoadFinish(load_finish) => Mutation::LoadFinish {
                associated_source_id: load_finish.associated_source_id,
            },
            PbMutation::ResetSource(reset_source) => Mutation::ResetSource {
                source_id: SourceId::from(reset_source.source_id),
            },
            PbMutation::InjectSourceOffsets(inject) => Mutation::InjectSourceOffsets {
                source_id: SourceId::from(inject.source_id),
                split_offsets: inject.split_offsets.clone(),
            },
        };
        Ok(mutation)
    }
}

impl<M> BarrierInner<M> {
    fn to_protobuf_inner(&self, barrier_fn: impl FnOnce(&M) -> Option<PbMutation>) -> PbBarrier {
        let Self {
            epoch,
            mutation,
            kind,
            tracing_context,
            iceberg_pk_index_compaction,
        } = self;

        PbBarrier {
            epoch: Some(PbEpoch {
                curr: epoch.curr,
                prev: epoch.prev,
            }),
            mutation: barrier_fn(mutation).map(|mutation| PbBarrierMutation {
                mutation: Some(mutation),
            }),
            tracing_context: tracing_context.to_protobuf(),
            kind: *kind as _,
            iceberg_pk_index_compaction: *iceberg_pk_index_compaction,
        }
    }

    fn from_protobuf_inner(
        prost: &PbBarrier,
        mutation_from_pb: impl FnOnce(Option<&PbMutation>) -> StreamExecutorResult<M>,
    ) -> StreamExecutorResult<Self> {
        let epoch = prost.get_epoch()?;

        Ok(Self {
            kind: prost.kind(),
            epoch: EpochPair::new(epoch.curr, epoch.prev),
            mutation: mutation_from_pb(
                (prost.mutation.as_ref()).and_then(|mutation| mutation.mutation.as_ref()),
            )?,
            tracing_context: TracingContext::from_protobuf(&prost.tracing_context),
            iceberg_pk_index_compaction: prost.iceberg_pk_index_compaction,
        })
    }

    pub fn map_mutation<M2>(self, f: impl FnOnce(M) -> M2) -> BarrierInner<M2> {
        BarrierInner {
            epoch: self.epoch,
            mutation: f(self.mutation),
            kind: self.kind,
            tracing_context: self.tracing_context,
            iceberg_pk_index_compaction: self.iceberg_pk_index_compaction,
        }
    }

    pub fn iceberg_pk_index_compaction(&self) -> Option<&IcebergPkIndexCompactionContext> {
        self.iceberg_pk_index_compaction.as_ref()
    }
}

impl DispatcherBarrier {
    pub fn to_protobuf(&self) -> PbBarrier {
        self.to_protobuf_inner(|_| None)
    }
}

impl Barrier {
    #[cfg(test)]
    pub fn to_protobuf(&self) -> PbBarrier {
        self.to_protobuf_inner(|mutation| mutation.as_ref().map(|mutation| mutation.to_protobuf()))
    }

    pub fn from_protobuf(prost: &PbBarrier) -> StreamExecutorResult<Self> {
        Self::from_protobuf_inner(prost, |mutation| {
            mutation
                .map(|m| Mutation::from_protobuf(m).map(Arc::new))
                .transpose()
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, EstimateSize)]
pub struct Watermark {
    pub col_idx: usize,
    #[estimate_size(ignore)]
    pub data_type: DataType,
    pub val: ScalarImpl,
}

impl PartialOrd for Watermark {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Watermark {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.val.default_cmp(&other.val)
    }
}

impl Watermark {
    pub fn new(col_idx: usize, data_type: DataType, val: ScalarImpl) -> Self {
        Self {
            col_idx,
            data_type,
            val,
        }
    }

    pub async fn transform_with_expr(
        self,
        expr: &NonStrictExpression,
        new_col_idx: usize,
    ) -> Option<Self> {
        let Self { col_idx, val, .. } = self;
        let row = {
            let mut row = vec![None; col_idx + 1];
            row[col_idx] = Some(val);
            OwnedRow::new(row)
        };
        let val = expr.eval_row_infallible(&row).await?;
        Some(Self::new(new_col_idx, expr.inner().return_type(), val))
    }

    /// Transform the watermark with the given output indices. If this watermark is not in the
    /// output, return `None`.
    pub fn transform_with_indices(self, output_indices: &[usize]) -> Option<Self> {
        output_indices
            .iter()
            .position(|p| *p == self.col_idx)
            .map(|new_col_idx| self.with_idx(new_col_idx))
    }

    pub fn to_protobuf(&self) -> PbWatermark {
        PbWatermark {
            column: Some(PbInputRef {
                index: self.col_idx as _,
                r#type: Some(self.data_type.to_protobuf()),
            }),
            val: Some(&self.val).to_protobuf().into(),
        }
    }

    pub fn from_protobuf(prost: &PbWatermark) -> StreamExecutorResult<Self> {
        let col_ref = prost.get_column()?;
        let data_type = DataType::from(col_ref.get_type()?);
        let val = Datum::from_protobuf(prost.get_val()?, &data_type)?
            .expect("watermark value cannot be null");
        Ok(Self::new(col_ref.get_index() as _, data_type, val))
    }

    pub fn with_idx(self, idx: usize) -> Self {
        Self::new(idx, self.data_type, self.val)
    }
}

#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
#[derive(Debug, EnumAsInner, Clone)]
pub enum MessageInner<M> {
    Chunk(StreamChunk),
    Barrier(BarrierInner<M>),
    Watermark(Watermark),
}

impl<M> MessageInner<M> {
    pub fn map_mutation<M2>(self, f: impl FnOnce(M) -> M2) -> MessageInner<M2> {
        match self {
            MessageInner::Chunk(chunk) => MessageInner::Chunk(chunk),
            MessageInner::Barrier(barrier) => MessageInner::Barrier(barrier.map_mutation(f)),
            MessageInner::Watermark(watermark) => MessageInner::Watermark(watermark),
        }
    }
}

pub type Message = MessageInner<BarrierMutationType>;
pub type DispatcherMessage = MessageInner<()>;

/// `MessageBatchInner` is used exclusively by `Dispatcher` and the `Merger`/`Receiver` for exchanging messages between them.
/// It shares the same message type as the fundamental `MessageInner`, but batches multiple barriers into a single message.
#[derive(Debug, EnumAsInner, Clone)]
pub enum MessageBatchInner<M> {
    Chunk(StreamChunk),
    BarrierBatch(Vec<BarrierInner<M>>),
    Watermark(Watermark),
}
pub type MessageBatch = MessageBatchInner<BarrierMutationType>;
pub type DispatcherBarriers = Vec<DispatcherBarrier>;
pub type DispatcherMessageBatch = MessageBatchInner<()>;

impl<M> From<MessageInner<M>> for MessageBatchInner<M> {
    fn from(m: MessageInner<M>) -> Self {
        match m {
            MessageInner::Chunk(c) => Self::Chunk(c),
            MessageInner::Barrier(b) => Self::BarrierBatch(vec![b]),
            MessageInner::Watermark(w) => Self::Watermark(w),
        }
    }
}

impl From<StreamChunk> for Message {
    fn from(chunk: StreamChunk) -> Self {
        Message::Chunk(chunk)
    }
}

impl<'a> TryFrom<&'a Message> for &'a Barrier {
    type Error = ();

    fn try_from(m: &'a Message) -> std::result::Result<Self, Self::Error> {
        match m {
            Message::Chunk(_) => Err(()),
            Message::Barrier(b) => Ok(b),
            Message::Watermark(_) => Err(()),
        }
    }
}

impl Message {
    /// Return true if the message is a stop barrier, meaning the stream
    /// will not continue, false otherwise.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(
            self,
            Message::Barrier(Barrier {
                mutation,
                ..
            }) if mutation.as_ref().unwrap().is_stop_mutation()
        )
    }
}

impl DispatcherMessageBatch {
    pub fn to_protobuf(&self) -> PbStreamMessageBatch {
        let prost = match self {
            Self::Chunk(stream_chunk) => {
                let prost_stream_chunk = stream_chunk.to_protobuf();
                StreamMessageBatch::StreamChunk(prost_stream_chunk)
            }
            Self::BarrierBatch(barrier_batch) => StreamMessageBatch::BarrierBatch(BarrierBatch {
                barriers: barrier_batch.iter().map(|b| b.to_protobuf()).collect(),
            }),
            Self::Watermark(watermark) => StreamMessageBatch::Watermark(watermark.to_protobuf()),
        };
        PbStreamMessageBatch {
            stream_message_batch: Some(prost),
        }
    }

    pub fn from_protobuf(prost: &PbStreamMessageBatch) -> StreamExecutorResult<Self> {
        let res = match prost.get_stream_message_batch()? {
            StreamMessageBatch::StreamChunk(chunk) => {
                Self::Chunk(StreamChunk::from_protobuf(chunk)?)
            }
            StreamMessageBatch::BarrierBatch(barrier_batch) => {
                let barriers = barrier_batch
                    .barriers
                    .iter()
                    .map(|barrier| {
                        DispatcherBarrier::from_protobuf_inner(barrier, |mutation| {
                            if mutation.is_some() {
                                if cfg!(debug_assertions) {
                                    panic!("should not receive message of barrier with mutation");
                                } else {
                                    warn!(?barrier, "receive message of barrier with mutation");
                                }
                            }
                            Ok(())
                        })
                    })
                    .try_collect()?;
                Self::BarrierBatch(barriers)
            }
            StreamMessageBatch::Watermark(watermark) => {
                Self::Watermark(Watermark::from_protobuf(watermark)?)
            }
        };
        Ok(res)
    }

    pub fn get_encoded_len(msg: &impl ::prost::Message) -> usize {
        ::prost::Message::encoded_len(msg)
    }
}

pub type StreamKey = Vec<usize>;
pub type StreamKeyRef<'a> = &'a [usize];
pub type StreamKeyDataTypes = SmallVec<[DataType; 1]>;

/// Expect the first message of the given `stream` as a barrier.
pub async fn expect_first_barrier<M: Debug>(
    stream: &mut (impl MessageStreamInner<M> + Unpin),
) -> StreamExecutorResult<BarrierInner<M>> {
    let message = stream
        .next()
        .instrument_await("expect_first_barrier")
        .await
        .context("failed to extract the first message: stream closed unexpectedly")??;
    let barrier = message
        .into_barrier()
        .expect("the first message must be a barrier");
    // TODO: Is this check correct?
    assert!(matches!(
        barrier.kind,
        BarrierKind::Checkpoint | BarrierKind::Initial
    ));
    Ok(barrier)
}

/// Expect the first message of the given `stream` as a barrier.
pub async fn expect_first_barrier_from_aligned_stream(
    stream: &mut (impl AlignedMessageStream + Unpin),
) -> StreamExecutorResult<Barrier> {
    let message = stream
        .next()
        .instrument_await("expect_first_barrier")
        .await
        .context("failed to extract the first message: stream closed unexpectedly")??;
    let barrier = message
        .into_barrier()
        .expect("the first message must be a barrier");
    Ok(barrier)
}

/// `StreamConsumer` is the last step in an actor.
pub trait StreamConsumer: Send + 'static {
    type BarrierStream: Stream<Item = StreamResult<Barrier>> + Send;

    fn execute(self: Box<Self>) -> Self::BarrierStream;
}

type BoxedMessageInput<InputId, M> = BoxedInput<InputId, MessageStreamItemInner<M>>;

/// A stream for merging messages from multiple upstreams.
/// Can dynamically add and delete upstream streams.
/// For the meaning of the generic parameter `M` used, refer to `BarrierInner<M>`.
pub struct DynamicReceivers<InputId, M> {
    /// The barrier we're aligning to. If this is `None`, then `blocked_upstreams` is empty.
    barrier: Option<BarrierInner<M>>,
    /// The start timestamp of the current barrier. Used for measuring the alignment duration.
    start_ts: Option<Instant>,
    /// The upstreams that're blocked by the `barrier`.
    blocked: Vec<BoxedMessageInput<InputId, M>>,
    /// The upstreams that're not blocked and can be polled.
    active: FuturesUnordered<StreamFuture<BoxedMessageInput<InputId, M>>>,
    /// watermark column index -> `BufferedWatermarks`
    buffered_watermarks: BTreeMap<usize, BufferedWatermarks<InputId>>,
    /// Currently only used for union.
    barrier_align_duration: Option<LabelGuardedMetric<GenericCounter<AtomicU64>>>,
    /// Only for merge. If None, then we don't take `Instant::now()` and `observe` during `poll_next`
    merge_barrier_align_duration: Option<LabelGuardedMetric<GenericCounter<AtomicU64>>>,
}

impl<InputId: Clone + Ord + Hash + std::fmt::Debug + Unpin, M: Clone + Unpin> Stream
    for DynamicReceivers<InputId, M>
{
    type Item = MessageStreamItemInner<M>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.is_empty() {
            return Poll::Ready(None);
        }

        loop {
            match futures::ready!(self.active.poll_next_unpin(cx)) {
                // Directly forward the error.
                Some((Some(Err(e)), _)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                // Handle the message from some upstream.
                Some((Some(Ok(message)), remaining)) => {
                    let input_id = remaining.id();
                    match message {
                        MessageInner::Chunk(chunk) => {
                            // Continue polling this upstream by pushing it back to `active`.
                            self.active.push(remaining.into_future());
                            return Poll::Ready(Some(Ok(MessageInner::Chunk(chunk))));
                        }
                        MessageInner::Watermark(watermark) => {
                            // Continue polling this upstream by pushing it back to `active`.
                            self.active.push(remaining.into_future());
                            if let Some(watermark) = self.handle_watermark(input_id, watermark) {
                                return Poll::Ready(Some(Ok(MessageInner::Watermark(watermark))));
                            }
                        }
                        MessageInner::Barrier(barrier) => {
                            // Block this upstream by pushing it to `blocked`.
                            if self.blocked.is_empty() {
                                self.start_ts = Some(Instant::now());
                            }
                            self.blocked.push(remaining);
                            if let Some(current_barrier) = self.barrier.as_ref() {
                                if current_barrier.epoch != barrier.epoch {
                                    return Poll::Ready(Some(Err(
                                        StreamExecutorError::align_barrier(
                                            current_barrier.clone().map_mutation(|_| None),
                                            barrier.map_mutation(|_| None),
                                        ),
                                    )));
                                }
                            } else {
                                self.barrier = Some(barrier);
                            }
                        }
                    }
                }
                // We use barrier as the control message of the stream. That is, we always stop the
                // actors actively when we receive a `Stop` mutation, instead of relying on the stream
                // termination.
                //
                // Besides, in abnormal cases when the other side of the `Input` closes unexpectedly,
                // we also yield an `Err(ExchangeChannelClosed)`, which will hit the `Err` arm above.
                // So this branch will never be reached in all cases.
                Some((None, remaining)) => {
                    return Poll::Ready(Some(Err(StreamExecutorError::channel_closed(format!(
                        "upstream input {:?} unexpectedly closed",
                        remaining.id()
                    )))));
                }
                // There's no active upstreams. Process the barrier and resume the blocked ones.
                None => {
                    assert!(!self.blocked.is_empty());

                    let start_ts = self
                        .start_ts
                        .take()
                        .expect("should have received at least one barrier");
                    if let Some(barrier_align_duration) = &self.barrier_align_duration {
                        barrier_align_duration.inc_by(start_ts.elapsed().as_nanos() as u64);
                    }
                    if let Some(merge_barrier_align_duration) = &self.merge_barrier_align_duration {
                        merge_barrier_align_duration.inc_by(start_ts.elapsed().as_nanos() as u64);
                    }

                    break;
                }
            }
        }

        assert!(self.active.is_terminated());

        let barrier = self.barrier.take().unwrap();

        let upstreams = std::mem::take(&mut self.blocked);
        self.extend_active(upstreams);
        assert!(!self.active.is_terminated());

        Poll::Ready(Some(Ok(MessageInner::Barrier(barrier))))
    }
}

impl<InputId: Clone + Ord + Hash + std::fmt::Debug, M> DynamicReceivers<InputId, M> {
    pub fn new(
        upstreams: Vec<BoxedMessageInput<InputId, M>>,
        barrier_align_duration: Option<LabelGuardedMetric<GenericCounter<AtomicU64>>>,
        merge_barrier_align_duration: Option<LabelGuardedMetric<GenericCounter<AtomicU64>>>,
    ) -> Self {
        let mut this = Self {
            barrier: None,
            start_ts: None,
            blocked: Vec::with_capacity(upstreams.len()),
            active: Default::default(),
            buffered_watermarks: Default::default(),
            merge_barrier_align_duration,
            barrier_align_duration,
        };
        this.extend_active(upstreams);
        this
    }

    /// Extend the active upstreams with the given upstreams. The current stream must be at the
    /// clean state right after a barrier.
    pub fn extend_active(
        &mut self,
        upstreams: impl IntoIterator<Item = BoxedMessageInput<InputId, M>>,
    ) {
        assert!(self.blocked.is_empty() && self.barrier.is_none());

        self.active
            .extend(upstreams.into_iter().map(|s| s.into_future()));
    }

    /// Handle a new watermark message. Optionally returns the watermark message to emit.
    pub fn handle_watermark(
        &mut self,
        input_id: InputId,
        watermark: Watermark,
    ) -> Option<Watermark> {
        let col_idx = watermark.col_idx;
        // Insert a buffer watermarks when first received from a column.
        let upstream_ids: Vec<_> = self.upstream_input_ids().collect();
        let watermarks = self
            .buffered_watermarks
            .entry(col_idx)
            .or_insert_with(|| BufferedWatermarks::with_ids(upstream_ids));
        watermarks.handle_watermark(input_id, watermark)
    }

    /// Consume `other` and add its upstreams to `self`. The two streams must be at the clean state
    /// right after a barrier.
    pub fn add_upstreams_from(
        &mut self,
        new_inputs: impl IntoIterator<Item = BoxedMessageInput<InputId, M>>,
    ) {
        assert!(self.blocked.is_empty() && self.barrier.is_none());

        let new_inputs: Vec<_> = new_inputs.into_iter().collect();
        let input_ids = new_inputs.iter().map(|input| input.id());
        self.buffered_watermarks.values_mut().for_each(|buffers| {
            // Add buffers to the buffered watermarks for all cols
            buffers.add_buffers(input_ids.clone());
        });
        self.active
            .extend(new_inputs.into_iter().map(|s| s.into_future()));
    }

    /// Remove upstreams from `self` in `upstream_input_ids`. The current stream must be at the
    /// clean state right after a barrier.
    /// The current container does not necessarily contain all the input ids passed in.
    pub fn remove_upstreams(&mut self, upstream_input_ids: &HashSet<InputId>) {
        assert!(self.blocked.is_empty() && self.barrier.is_none());

        let new_upstreams = std::mem::take(&mut self.active)
            .into_iter()
            .map(|s| s.into_inner().unwrap())
            .filter(|u| !upstream_input_ids.contains(&u.id()));
        self.extend_active(new_upstreams);
        self.buffered_watermarks.values_mut().for_each(|buffers| {
            // Call `check_heap` in case the only upstream(s) that does not have
            // watermark in heap is removed
            buffers.remove_buffer(upstream_input_ids.clone());
        });
    }

    pub fn merge_barrier_align_duration(
        &self,
    ) -> Option<LabelGuardedMetric<GenericCounter<AtomicU64>>> {
        self.merge_barrier_align_duration.clone()
    }

    pub fn flush_buffered_watermarks(&mut self) {
        self.buffered_watermarks
            .values_mut()
            .for_each(|buffers| buffers.clear());
    }

    pub fn upstream_input_ids(&self) -> impl Iterator<Item = InputId> + '_ {
        self.blocked
            .iter()
            .map(|s| s.id())
            .chain(self.active.iter().map(|s| s.get_ref().unwrap().id()))
    }

    pub fn is_empty(&self) -> bool {
        self.blocked.is_empty() && self.active.is_empty()
    }
}

// Explanation of why we need `DispatchBarrierBuffer`:
//
// When we need to create or replace an upstream fragment for the current fragment, the `Merge` operator must
// add some new upstream actor inputs. However, the `Merge` operator may still have old upstreams. We must wait
// for these old upstreams to completely process their barriers and align before we can safely update the
// `upstream-input-set`.
//
// Meanwhile, the creation of a new upstream actor can only succeed after the channel to the downstream `Merge`
// operator has been established. This creates a potential dependency chain: [new_actor_creation ->
// downstream_merge_update -> old_actor_processing]
//
// To address this, we split the application of a barrier's `Mutation` into two steps:
// 1. Parse the `Mutation`. If there is an addition on the upstream-set, establish a channel with the upstream
//    and cache it.
// 2. When the upstream barrier actually arrives, apply the cached upstream changes to the upstream-set
//
// Additionally, since receiving a barrier from current upstream input and from the `barrier_rx` are
// asynchronous, we cannot determine which will arrive first. Therefore, when a barrier is received from an
// upstream: if a cached mutation is present, we apply it. Otherwise, we must fetch a new barrier from
// `barrier_rx`.
pub(crate) struct DispatchBarrierBuffer {
    buffer: VecDeque<(Barrier, Option<Vec<BoxedActorInput>>)>,
    barrier_rx: mpsc::UnboundedReceiver<Barrier>,
    recv_state: BarrierReceiverState,
    curr_upstream_fragment_id: FragmentId,
    actor_id: ActorId,
    // read-only context for building new inputs
    build_input_ctx: Arc<BuildInputContext>,
}

struct BuildInputContext {
    pub actor_id: ActorId,
    pub local_barrier_manager: LocalBarrierManager,
    pub metrics: Arc<StreamingMetrics>,
    pub fragment_id: FragmentId,
    pub actor_config: Arc<StreamingConfig>,
}

type BoxedNewInputsFuture =
    Pin<Box<dyn Future<Output = StreamExecutorResult<Vec<BoxedActorInput>>> + Send>>;

enum BarrierReceiverState {
    ReceivingBarrier,
    CreatingNewInput(Barrier, BoxedNewInputsFuture),
}

impl DispatchBarrierBuffer {
    pub fn new(
        barrier_rx: mpsc::UnboundedReceiver<Barrier>,
        actor_id: ActorId,
        curr_upstream_fragment_id: FragmentId,
        local_barrier_manager: LocalBarrierManager,
        metrics: Arc<StreamingMetrics>,
        fragment_id: FragmentId,
        actor_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            buffer: VecDeque::new(),
            barrier_rx,
            recv_state: BarrierReceiverState::ReceivingBarrier,
            curr_upstream_fragment_id,
            actor_id,
            build_input_ctx: Arc::new(BuildInputContext {
                actor_id,
                local_barrier_manager,
                metrics,
                fragment_id,
                actor_config,
            }),
        }
    }

    pub async fn await_next_message(
        &mut self,
        stream: &mut (impl Stream<Item = StreamExecutorResult<DispatcherMessage>> + Unpin),
        metrics: &ActorInputMetrics,
        upstream_is_empty: bool,
    ) -> StreamExecutorResult<DispatcherMessage> {
        if upstream_is_empty {
            while self.buffer.is_empty() {
                self.try_fetch_barrier_rx(false).await?;
            }
            let (barrier, _) = self.buffer.front().unwrap();
            return Ok(DispatcherMessage::Barrier(
                barrier.clone().into_dispatcher(),
            ));
        }

        let mut start_time = Instant::now();
        let interval_duration = Duration::from_secs(15);
        let mut interval =
            tokio::time::interval_at(start_time + interval_duration, interval_duration);

        loop {
            tokio::select! {
                biased;
                msg = stream.try_next() => {
                    metrics
                        .actor_input_buffer_blocking_duration_ns
                        .inc_by(start_time.elapsed().as_nanos() as u64);
                    return msg?.ok_or_else(
                        || StreamExecutorError::channel_closed("upstream executor closed unexpectedly")
                    );
                }

                e = self.continuously_fetch_barrier_rx() => {
                    return Err(e);
                }

                _ = interval.tick() => {
                    start_time = Instant::now();
                    metrics.actor_input_buffer_blocking_duration_ns.inc_by(interval_duration.as_nanos() as u64);
                    continue;
                }
            }
        }
    }

    pub async fn pop_barrier_with_inputs(
        &mut self,
        barrier: DispatcherBarrier,
    ) -> StreamExecutorResult<(Barrier, Option<Vec<BoxedActorInput>>)> {
        while self.buffer.is_empty() {
            self.try_fetch_barrier_rx(false).await?;
        }
        let (recv_barrier, inputs) = self.buffer.pop_front().unwrap();
        assert_equal_dispatcher_barrier(&recv_barrier, &barrier);

        Ok((recv_barrier, inputs))
    }

    async fn continuously_fetch_barrier_rx(&mut self) -> StreamExecutorError {
        loop {
            if let Err(e) = self.try_fetch_barrier_rx(true).await {
                return e;
            }
        }
    }

    async fn try_fetch_barrier_rx(&mut self, pending_on_end: bool) -> StreamExecutorResult<()> {
        match &mut self.recv_state {
            BarrierReceiverState::ReceivingBarrier => {
                let Some(barrier) = self.barrier_rx.recv().await else {
                    if pending_on_end {
                        return pending().await;
                    } else {
                        return Err(StreamExecutorError::channel_closed(
                            "barrier channel closed unexpectedly",
                        ));
                    }
                };
                if let Some(fut) = self.pre_apply_barrier(&barrier) {
                    self.recv_state = BarrierReceiverState::CreatingNewInput(barrier, fut);
                } else {
                    self.buffer.push_back((barrier, None));
                }
            }
            BarrierReceiverState::CreatingNewInput(barrier, fut) => {
                let new_inputs = fut.await?;
                self.buffer.push_back((barrier.clone(), Some(new_inputs)));
                self.recv_state = BarrierReceiverState::ReceivingBarrier;
            }
        }
        Ok(())
    }

    fn pre_apply_barrier(&mut self, barrier: &Barrier) -> Option<BoxedNewInputsFuture> {
        let update = barrier.as_update_merge(self.actor_id, self.curr_upstream_fragment_id)?;
        let upstream_fragment_id = update
            .new_upstream_fragment_id
            .unwrap_or(self.curr_upstream_fragment_id);
        // Keep the fragment id in sync even when switching to an empty upstream. Otherwise a
        // subsequent reattach cannot be recognized as a merge update and its new input may not
        // receive the matching first barrier before the barrier is forwarded.
        self.curr_upstream_fragment_id = upstream_fragment_id;

        if !update.added_upstream_actors.is_empty() {
            let ctx = self.build_input_ctx.clone();
            let added_upstream_actors = update.added_upstream_actors.clone();
            let barrier = barrier.clone();
            let fut = async move {
                try_join_all(added_upstream_actors.iter().map(|upstream_actor| async {
                    let mut new_input = new_input(
                        &ctx.local_barrier_manager,
                        ctx.metrics.clone(),
                        ctx.actor_id,
                        ctx.fragment_id,
                        upstream_actor,
                        upstream_fragment_id,
                        ctx.actor_config.clone(),
                    )
                    .await?;

                    // Poll the first barrier from the new upstreams. It must be the same as the one we polled from
                    // original upstreams.
                    let first_barrier = expect_first_barrier(&mut new_input).await?;
                    assert_equal_dispatcher_barrier(&barrier, &first_barrier);

                    StreamExecutorResult::Ok(new_input)
                }))
                .await
            }
            .boxed();

            Some(fut)
        } else {
            None
        }
    }
}
