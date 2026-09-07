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

use std::collections::{HashMap, HashSet};
use std::mem::{replace, take};
use std::task::{Context, Poll};

use futures::FutureExt;
use prometheus::{HistogramTimer, IntCounter};
use risingwave_common::catalog::DatabaseId;
use risingwave_meta_model::WorkerId;
use risingwave_pb::id::PartialGraphId;
use risingwave_pb::meta::event_log::{Event, EventRecovery};
use risingwave_pb::stream_service::streaming_control_stream_response::ResetPartialGraphResponse;
use thiserror_ext::AsReport;
use tracing::{info, warn};

use crate::barrier::DatabaseRuntimeInfoSnapshot;
use crate::barrier::checkpoint::CheckpointControl;
use crate::barrier::checkpoint::control::DatabaseCheckpointControlStatus;
use crate::barrier::complete_task::BarrierCompleteOutput;
use crate::barrier::context::recovery::RenderedDatabaseRuntimeInfo;
use crate::barrier::partial_graph::PartialGraphManager;
use crate::barrier::rpc::{DatabaseInitialBarrierCollector, to_partial_graph_id};
use crate::barrier::worker::{
    RetryBackoffFuture, RetryBackoffStrategy, get_retry_backoff_strategy,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

/// We can treat each database as a state machine of 3 states: `Running`, `Resetting` and `Initializing`.
/// The state transition can be triggered when receiving 3 variants of response: `ReportDatabaseFailure`, `BarrierComplete`, `DatabaseReset`.
/// The logic of state transition can be summarized as followed:
///
/// `Running`
///     - on `ReportDatabaseFailure`
///         - wait for the inflight B`arrierCompletingTask` to finish if there is any, mark the database as blocked in command queue
///         - send `ResetDatabaseRequest` to all CNs.
///         - enter `Resetting` state.
///     - on `BarrierComplete`: update the `DatabaseCheckpointControl`.
///     - on `DatabaseReset`: unreachable
/// `Resetting`
///     - on `ReportDatabaseFailure` or `BarrierComplete`: ignore
///     - on `DatabaseReset`:
///         - mark the CN as collected.
///         - when all CNs have collected the response:
///             - load the database runtime info from catalog manager and fragment manager
///             - inject the initial barrier to CNs, save the set of nodes that need to collect response
///             - if any failure happened, re-enter `Resetting` state.
///             - enter `Initializing` state
/// `Initializing`
///     - on `BarrierComplete`:
///         - mark the CN as collected
///         - when all CNs have collected the response: enter Running
///     - on `ReportDatabaseFailure`
///         - send `ResetDatabaseRequest` to all CNs
///         - enter `Resetting`
///     - on `DatabaseReset`: unreachable
enum DatabaseRecoveringStage {
    Resetting {
        resetting_partial_graphs: HashSet<PartialGraphId>,
        reset_resps: Vec<(WorkerId, ResetPartialGraphResponse)>,
        backoff_future: Option<RetryBackoffFuture>,
    },
    Initializing {
        initial_barrier_collector: Box<DatabaseInitialBarrierCollector>,
    },
}

pub(crate) struct DatabaseRecoveringState {
    stage: DatabaseRecoveringStage,
    retry_backoff_strategy: RetryBackoffStrategy,
    metrics: DatabaseRecoveryMetrics,
}

pub(super) enum RecoveringStateAction {
    EnterInitializing(Vec<(WorkerId, ResetPartialGraphResponse)>),
    EnterRunning,
}

struct DatabaseRecoveryMetrics {
    recovery_failure_cnt: IntCounter,
    recovery_timer: Option<HistogramTimer>,
}

impl DatabaseRecoveryMetrics {
    fn new(database_id: DatabaseId) -> Self {
        let database_id_str = format!("database {}", database_id);
        Self {
            recovery_failure_cnt: GLOBAL_META_METRICS
                .recovery_failure_cnt
                .with_label_values(&[database_id_str.as_str()]),
            recovery_timer: Some(
                GLOBAL_META_METRICS
                    .recovery_latency
                    .with_label_values(&[database_id_str.as_str()])
                    .start_timer(),
            ),
        }
    }
}

impl DatabaseRecoveringState {
    pub(super) fn new_resetting(
        database_id: DatabaseId,
        resetting_partial_graphs: HashSet<PartialGraphId>,
    ) -> Self {
        let mut retry_backoff_strategy = get_retry_backoff_strategy();
        let backoff_future = retry_backoff_strategy.next().unwrap();
        let metrics = DatabaseRecoveryMetrics::new(database_id);
        metrics.recovery_failure_cnt.inc();
        Self {
            stage: DatabaseRecoveringStage::Resetting {
                resetting_partial_graphs,
                reset_resps: vec![],
                backoff_future: Some(backoff_future),
            },
            retry_backoff_strategy,
            metrics,
        }
    }

    fn next_retry(&mut self) -> RetryBackoffFuture {
        self.retry_backoff_strategy
            .next()
            .expect("should not be empty")
    }

    pub(super) fn partial_graph_initialized(&mut self, partial_graph_id: PartialGraphId) {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting { .. } => {
                unreachable!("should not have partial graph initialized when resetting")
            }
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
            } => {
                initial_barrier_collector.partial_graph_initialized(partial_graph_id);
            }
        }
    }

    pub(super) fn is_valid_after_worker_err(&mut self, worker_id: WorkerId) -> bool {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting { .. } => true,
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
                ..
            } => initial_barrier_collector.is_valid_after_worker_err(worker_id),
        }
    }

    pub(super) fn on_partial_graph_reset(
        &mut self,
        partial_graph_id: PartialGraphId,
        new_reset_resps: HashMap<WorkerId, ResetPartialGraphResponse>,
    ) {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting {
                resetting_partial_graphs,
                reset_resps,
                ..
            } => {
                assert!(resetting_partial_graphs.remove(&partial_graph_id));
                reset_resps.extend(new_reset_resps);
            }
            DatabaseRecoveringStage::Initializing { .. } => {
                unreachable!("all reset resp should have been received in Resetting")
            }
        }
    }

    pub(super) fn poll_next_event(&mut self, cx: &mut Context<'_>) -> Poll<RecoveringStateAction> {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting {
                resetting_partial_graphs,
                reset_resps,
                backoff_future: backoff_future_option,
            } => {
                let pass_backoff = if let Some(backoff_future) = backoff_future_option {
                    if backoff_future.poll_unpin(cx).is_ready() {
                        *backoff_future_option = None;
                        true
                    } else {
                        false
                    }
                } else {
                    true
                };
                if pass_backoff && resetting_partial_graphs.is_empty() {
                    return Poll::Ready(RecoveringStateAction::EnterInitializing(take(
                        reset_resps,
                    )));
                }
            }
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
                ..
            } => {
                if initial_barrier_collector.is_collected() {
                    return Poll::Ready(RecoveringStateAction::EnterRunning);
                }
            }
        }
        Poll::Pending
    }
}

pub(crate) struct DatabaseStatusAction<'a, A> {
    control: &'a mut CheckpointControl,
    database_id: DatabaseId,
    pub(crate) action: A,
}

impl<A> DatabaseStatusAction<'_, A> {
    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }
}

impl CheckpointControl {
    pub(super) fn new_database_status_action<A>(
        &mut self,
        database_id: DatabaseId,
        action: A,
    ) -> DatabaseStatusAction<'_, A> {
        DatabaseStatusAction {
            control: self,
            database_id,
            action,
        }
    }
}

pub(crate) struct EnterReset;

impl DatabaseStatusAction<'_, EnterReset> {
    pub(crate) fn enter(
        self,
        barrier_complete_output: Option<BarrierCompleteOutput>,
        partial_graph_manager: &mut PartialGraphManager,
    ) {
        let event_log_manager_ref = self.control.env.event_log_manager_ref();
        if let Some(output) = barrier_complete_output {
            self.control.ack_completed(partial_graph_manager, output);
        }
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(database) => {
                let mut resetting_partial_graphs = HashSet::new();
                let new_reset_partial_graphs: HashSet<_> = database
                    .independent_checkpoint_job_controls
                    .drain()
                    .filter_map(|(job_id, job)| {
                        let partial_graph_id = to_partial_graph_id(self.database_id, Some(job_id));
                        if job.reset() {
                            resetting_partial_graphs.insert(partial_graph_id);
                            None
                        } else {
                            Some(partial_graph_id)
                        }
                    })
                    .chain([to_partial_graph_id(self.database_id, None)])
                    .collect();
                resetting_partial_graphs
                    .iter()
                    .for_each(|partial_graph_id| {
                        partial_graph_manager.assert_resetting(*partial_graph_id)
                    });
                partial_graph_manager
                    .reset_partial_graphs(new_reset_partial_graphs.iter().copied());
                resetting_partial_graphs.extend(new_reset_partial_graphs);

                let metrics = DatabaseRecoveryMetrics::new(self.database_id);
                event_log_manager_ref.add_event_logs(vec![Event::Recovery(
                    EventRecovery::database_recovery_start(self.database_id.as_raw_id()),
                )]);
                *database_status =
                    DatabaseCheckpointControlStatus::Recovering(DatabaseRecoveringState {
                        stage: DatabaseRecoveringStage::Resetting {
                            resetting_partial_graphs,
                            reset_resps: vec![],
                            backoff_future: None,
                        },
                        retry_backoff_strategy: get_retry_backoff_strategy(),
                        metrics,
                    });
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match &mut state.stage {
                DatabaseRecoveringStage::Resetting { .. } => {
                    unreachable!("should not enter resetting again")
                }
                DatabaseRecoveringStage::Initializing {
                    initial_barrier_collector,
                } => {
                    let partial_graphs: HashSet<_> =
                        initial_barrier_collector.all_partial_graphs().collect();
                    event_log_manager_ref.add_event_logs(vec![Event::Recovery(
                        EventRecovery::database_recovery_failure(self.database_id.as_raw_id()),
                    )]);
                    let backoff_future = state.next_retry();
                    partial_graph_manager.reset_partial_graphs(partial_graphs.iter().copied());
                    state.metrics.recovery_failure_cnt.inc();
                    state.stage = DatabaseRecoveringStage::Resetting {
                        resetting_partial_graphs: partial_graphs,
                        reset_resps: vec![],
                        backoff_future: Some(backoff_future),
                    };
                }
            },
        }
    }
}

impl CheckpointControl {
    pub(crate) fn on_adhoc_recovery(
        &mut self,
        database_id: DatabaseId,
    ) -> Option<DatabaseStatusAction<'_, EnterReset>> {
        match self.databases.get(&database_id).expect("should exist") {
            DatabaseCheckpointControlStatus::Running(_) => {
                Some(self.new_database_status_action(database_id, EnterReset))
            }
            // An overlapping request should wait for the ongoing recovery instead of restarting it.
            DatabaseCheckpointControlStatus::Recovering(_) => None,
        }
    }

    pub(crate) fn on_report_failure(
        &mut self,
        database_id: DatabaseId,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> Option<DatabaseStatusAction<'_, EnterReset>> {
        let database_status = self.databases.get_mut(&database_id).expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                Some(self.new_database_status_action(database_id, EnterReset))
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match &mut state.stage {
                DatabaseRecoveringStage::Resetting { .. } => {
                    // ignore reported failure during resetting or backoff.
                    None
                }
                DatabaseRecoveringStage::Initializing {
                    initial_barrier_collector,
                } => {
                    warn!(database_id = %database_id, "failed to initialize database");
                    let partial_graphs: HashSet<_> =
                        initial_barrier_collector.all_partial_graphs().collect();
                    let backoff_future = state.next_retry();
                    partial_graph_manager.reset_partial_graphs(partial_graphs.iter().copied());
                    state.metrics.recovery_failure_cnt.inc();
                    state.stage = DatabaseRecoveringStage::Resetting {
                        resetting_partial_graphs: partial_graphs,
                        reset_resps: vec![],
                        backoff_future: Some(backoff_future),
                    };
                    None
                }
            },
        }
    }
}

pub(crate) struct EnterInitializing(pub(crate) Vec<(WorkerId, ResetPartialGraphResponse)>);

impl DatabaseStatusAction<'_, EnterInitializing> {
    pub(crate) fn enter(
        self,
        runtime_info: DatabaseRuntimeInfoSnapshot,
        rendered_info: RenderedDatabaseRuntimeInfo,
        partial_graph_manager: &mut PartialGraphManager,
    ) {
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        let status = match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not enter initializing when running")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match state.stage {
                DatabaseRecoveringStage::Initializing { .. } => {
                    unreachable!("can only enter initializing when resetting")
                }
                DatabaseRecoveringStage::Resetting { .. } => state,
            },
        };
        let DatabaseRuntimeInfoSnapshot {
            recovery_context,
            mut state_table_committed_epochs,
            mut state_table_log_epochs,
            mut mv_depended_subscriptions,
            mut creating_jobs,
            mut cdc_table_snapshot_splits,
        } = runtime_info;
        let fragment_relations = &recovery_context.fragment_relations;
        let RenderedDatabaseRuntimeInfo {
            job_infos,
            stream_actors,
            mut source_splits,
            batch_refresh,
        } = rendered_info;
        let mut recoverer = partial_graph_manager.start_recover();
        let result: MetaResult<_> = try {
            recoverer.inject_database_initial_barrier(
                self.database_id,
                job_infos,
                &recovery_context.job_extra_info,
                &mut state_table_committed_epochs,
                &mut state_table_log_epochs,
                fragment_relations,
                &stream_actors,
                &mut source_splits,
                &mut creating_jobs,
                &mut mv_depended_subscriptions,
                false,
                &self.control.hummock_version_stats,
                &mut cdc_table_snapshot_splits,
                batch_refresh,
            )?
        };
        match result {
            Ok(database) => {
                let initializing_partial_graphs = recoverer.all_initializing();
                info!(?initializing_partial_graphs, database_id = ?self.database_id, "database enter initializing");
                status.stage = DatabaseRecoveringStage::Initializing {
                    initial_barrier_collector: DatabaseInitialBarrierCollector {
                        database_id: self.database_id,
                        initializing_partial_graphs,
                        database,
                    }
                    .into(),
                };
            }
            Err(e) => {
                warn!(
                    database_id = %self.database_id,
                    e = %e.as_report(),
                    "failed to inject initial barrier"
                );
                let backoff_future = status.next_retry();
                let resetting_partial_graphs = recoverer.failed();
                status.metrics.recovery_failure_cnt.inc();
                status.stage = DatabaseRecoveringStage::Resetting {
                    resetting_partial_graphs,
                    reset_resps: vec![],
                    backoff_future: Some(backoff_future),
                };
            }
        }
    }

    pub(crate) fn fail_reload_runtime_info(self, e: MetaError) {
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        let status = match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not enter initializing when running")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match state.stage {
                DatabaseRecoveringStage::Initializing { .. } => {
                    unreachable!("can only enter initializing when resetting")
                }
                DatabaseRecoveringStage::Resetting { .. } => state,
            },
        };
        warn!(
            database_id = %self.database_id,
            e = %e.as_report(),
            "failed to reload runtime info"
        );
        let backoff_future = status.next_retry();
        status.metrics.recovery_failure_cnt.inc();
        status.stage = DatabaseRecoveringStage::Resetting {
            resetting_partial_graphs: HashSet::new(),
            reset_resps: vec![],
            backoff_future: Some(backoff_future),
        };
    }

    pub(crate) fn remove(self) {
        self.control
            .databases
            .remove(&self.database_id)
            .expect("should exist");
        self.control
            .env
            .shared_actor_infos()
            .remove_database(self.database_id);
    }
}

pub(crate) struct EnterRunning;

impl DatabaseStatusAction<'_, EnterRunning> {
    pub(crate) fn enter(self) {
        info!(database_id = ?self.database_id, "database enter running");
        let event_log_manager_ref = self.control.env.event_log_manager_ref();
        event_log_manager_ref.add_event_logs(vec![Event::Recovery(
            EventRecovery::database_recovery_success(self.database_id.as_raw_id()),
        )]);
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not enter running again")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => {
                let temp_place_holder = DatabaseRecoveringStage::Resetting {
                    resetting_partial_graphs: Default::default(),
                    reset_resps: vec![],
                    backoff_future: None,
                };
                match state.metrics.recovery_timer.take() {
                    Some(recovery_timer) => {
                        recovery_timer.observe_duration();
                    }
                    _ => {
                        if cfg!(debug_assertions) {
                            panic!(
                                "take database {} recovery latency for twice",
                                self.database_id
                            )
                        } else {
                            warn!(database_id = %self.database_id,"failed to take recovery latency")
                        }
                    }
                }
                match replace(&mut state.stage, temp_place_holder) {
                    DatabaseRecoveringStage::Resetting { .. } => {
                        unreachable!("can only enter running during initializing")
                    }
                    DatabaseRecoveringStage::Initializing {
                        initial_barrier_collector,
                    } => {
                        *database_status = DatabaseCheckpointControlStatus::Running(
                            initial_barrier_collector.finish(),
                        );
                    }
                }
            }
        }
    }
}
