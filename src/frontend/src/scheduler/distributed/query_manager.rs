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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures::Stream;
use pgwire::pg_server::{BoxedError, Session, SessionId};
use risingwave_batch::worker_manager::worker_node_manager::{
    WorkerNodeManagerRef, WorkerNodeSelector,
};
use risingwave_common::array::DataChunk;
use risingwave_common::session_config::QueryMode;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::sync::OwnedSemaphorePermit;
use tracing::warn;

use super::QueryExecution;
use super::stats::DistributedQueryMetrics;
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::plan_fragmenter::{Query, QueryId};
use crate::scheduler::{ExecutionContextRef, ReadSnapshot, SchedulerResult};

pub struct DistributedQueryStream {
    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
    // Used for cleaning up `QueryExecution` after all data are polled.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
}

impl DistributedQueryStream {
    pub fn query_id(&self) -> &QueryId {
        &self.query_id
    }
}

impl Stream for DistributedQueryStream {
    // TODO(error-handling): use a concrete error type.
    type Item = Result<DataChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.chunk_rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(chunk) => match chunk {
                Some(chunk_result) => match chunk_result {
                    Ok(chunk) => Poll::Ready(Some(Ok(chunk))),
                    Err(err) => Poll::Ready(Some(Err(Box::new(err)))),
                },
                None => Poll::Ready(None),
            },
        }
    }
}

impl Drop for DistributedQueryStream {
    fn drop(&mut self) {
        // Clear `QueryExecution`. Avoid holding it after execution ends.
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.delete_query(&self.query_id);
    }
}

pub struct QueryResultFetcher {
    task_output_id: TaskOutputId,
    task_host: HostAddress,

    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,

    // `query_id` and `query_execution_info` are used for cleaning up `QueryExecution` after
    // execution.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
}

/// [`QueryExecutionInfo`] stores necessary information of query executions. Currently, a
/// `QueryExecution` will be removed right after it ends execution. We might add additional fields
/// in the future.
#[derive(Clone, Default)]
pub struct QueryExecutionInfo {
    query_execution_map: HashMap<QueryId, Arc<QueryExecution>>,
}

impl QueryExecutionInfo {
    #[cfg(test)]
    pub fn new_from_map(query_execution_map: HashMap<QueryId, Arc<QueryExecution>>) -> Self {
        Self {
            query_execution_map,
        }
    }
}

pub type QueryExecutionInfoRef = Arc<RwLock<QueryExecutionInfo>>;

/// Guards the atomic handoff of a distributed query registration to [`DistributedQueryStream`].
///
/// If scheduling fails or is cancelled before the stream takes ownership, dropping this guard
/// removes the query from the execution map and aborts it. Once the stream is created,
/// [`Self::disarm`] transfers cleanup responsibility to the stream.
struct DistributedQueryRegistrationAtomicGuard {
    query_id: QueryId,
    query_execution: Arc<QueryExecution>,
    query_execution_info: QueryExecutionInfoRef,
    armed: bool,
}

impl DistributedQueryRegistrationAtomicGuard {
    /// Arms cleanup for a query that has been inserted into the execution map.
    fn new(
        query_id: QueryId,
        query_execution: Arc<QueryExecution>,
        query_execution_info: QueryExecutionInfoRef,
    ) -> Self {
        Self {
            query_id,
            query_execution,
            query_execution_info,
            armed: true,
        }
    }

    /// Transfers cleanup responsibility to the returned [`DistributedQueryStream`].
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DistributedQueryRegistrationAtomicGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.query_execution_info
            .write()
            .unwrap()
            .delete_query(&self.query_id);
        let query_execution = self.query_execution.clone();
        tokio::spawn(async move {
            query_execution
                .abort("query scheduling was cancelled".to_owned())
                .await;
        });
    }
}

impl QueryExecutionInfo {
    pub fn add_query(&mut self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        self.query_execution_map.insert(query_id, query_execution);
    }

    pub fn delete_query(&mut self, query_id: &QueryId) {
        self.query_execution_map.remove(query_id);
    }
}

/// Manages execution of distributed batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,
    query_execution_info: QueryExecutionInfoRef,
    pub query_metrics: Arc<DistributedQueryMetrics>,
    /// Limit per session.
    distributed_query_limit: Option<u64>,
    /// Limits the number of concurrent distributed queries.
    distributed_query_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Total permitted distributed query number.
    pub total_distributed_query_limit: Option<u64>,
}

impl QueryManager {
    /// Creates a distributed query manager with optional per-session and global limits.
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        query_metrics: Arc<DistributedQueryMetrics>,
        distributed_query_limit: Option<u64>,
        total_distributed_query_limit: Option<u64>,
    ) -> Self {
        let distributed_query_semaphore = total_distributed_query_limit
            .map(|limit| Arc::new(tokio::sync::Semaphore::new(limit as usize)));
        Self {
            worker_node_manager,
            compute_client_pool,
            catalog_reader,
            query_execution_info: Arc::new(RwLock::new(QueryExecutionInfo::default())),
            query_metrics,
            distributed_query_limit,
            distributed_query_semaphore,
            total_distributed_query_limit,
        }
    }

    async fn get_permit(&self) -> SchedulerResult<Option<OwnedSemaphorePermit>> {
        match self.distributed_query_semaphore {
            Some(ref semaphore) => {
                let permit = semaphore.clone().acquire_owned().await;
                match permit {
                    Ok(permit) => Ok(Some(permit)),
                    Err(_) => {
                        self.query_metrics.rejected_query_counter.inc();
                        Err(crate::scheduler::SchedulerError::QueryReachLimit(
                            QueryMode::Distributed,
                            self.total_distributed_query_limit
                                .expect("should have distributed query limit"),
                        ))
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// Schedules a distributed query and transfers cleanup ownership to its returned stream.
    ///
    /// `snapshot` supplies a caller-owned storage view. When it is `None`, the query uses the
    /// snapshot pinned by the current session transaction.
    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        mut query: Query,
        can_session_cancel: bool,
        snapshot: Option<ReadSnapshot>,
    ) -> SchedulerResult<DistributedQueryStream> {
        // TODO: if there's no table scan, we don't need to acquire snapshot.
        let pinned_snapshot = snapshot.unwrap_or_else(|| context.session().pinned_snapshot());
        pinned_snapshot.fill_batch_query_epoch(&mut query)?;

        if let Some(query_limit) = self.distributed_query_limit
            && self.query_metrics.running_query_num.get() as u64 == query_limit
        {
            self.query_metrics.rejected_query_counter.inc();
            return Err(crate::scheduler::SchedulerError::QueryReachLimit(
                QueryMode::Distributed,
                query_limit,
            ));
        }
        let query_id = query.query_id.clone();
        let permit = self.get_permit().await?;
        let query_execution = Arc::new(QueryExecution::new(
            query,
            context.session().id(),
            permit,
            can_session_cancel,
        ));

        // Add queries status when begin.
        context
            .session()
            .env()
            .query_manager()
            .add_query(query_id.clone(), query_execution.clone());

        let worker_node_manager_reader = WorkerNodeSelector::new(
            self.worker_node_manager.clone(),
            pinned_snapshot.support_barrier_read(),
        );

        // Starts the execution of the query.
        let mut registration = DistributedQueryRegistrationAtomicGuard::new(
            query_id.clone(),
            query_execution.clone(),
            self.query_execution_info.clone(),
        );
        let query_result_fetcher = query_execution
            .start(
                context.clone(),
                worker_node_manager_reader,
                self.compute_client_pool.clone(),
                self.catalog_reader.clone(),
                self.query_execution_info.clone(),
                self.query_metrics.clone(),
            )
            .await?;
        registration.disarm();
        Ok(query_result_fetcher.stream_from_channel())
    }

    /// Cancels distributed non-cursor queries in `session_id`.
    ///
    /// A PostgreSQL `CancelRequest` targets the session's current statement, so cursor-owned
    /// queries are excluded: one session can have multiple cursor queries running concurrently.
    /// When the session ends or the frontend shuts down, the session manager calls this method for
    /// ordinary queries and separately shuts down the cursor manager, whose session-scoped token
    /// terminates all cursor-owned queries.
    pub fn cancel_non_cursor_queries_in_session(&self, session_id: SessionId) {
        let query_execution_info = self.query_execution_info.read().unwrap();
        for query in query_execution_info.query_execution_map.values() {
            // `QueryExecutionInfo` might have queries from different sessions.
            if query.session_id == session_id && query.can_session_cancel() {
                let query = query.clone();
                // Spawn a task to abort. Avoid await point in this function.
                tokio::spawn(async move { query.abort("cancelled by user".to_owned()).await });
            }
        }
    }

    /// Cancels one cursor-owned distributed query without blocking its caller on teardown.
    ///
    /// Ordinary queries are deliberately rejected so their cancellation remains session-scoped.
    pub fn cancel_cursor_query(&self, query_id: &QueryId, reason: impl Into<String>) {
        let query_execution = self
            .query_execution_info
            .read()
            .unwrap()
            .query_execution_map
            .get(query_id)
            .cloned();
        if let Some(query_execution) = query_execution {
            if query_execution.can_session_cancel() {
                warn!(
                    ?query_id,
                    "Ignoring cursor cancellation for an ordinary query"
                );
                return;
            }
            let reason = reason.into();
            tokio::spawn(async move { query_execution.abort(reason).await });
        }
    }

    pub fn add_query(&self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.add_query(query_id, query_execution);
    }

    pub fn delete_query(&self, query_id: &QueryId) {
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.delete_query(query_id);
    }
}

impl QueryResultFetcher {
    pub fn new(
        task_output_id: TaskOutputId,
        task_host: HostAddress,
        chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
        query_id: QueryId,
        query_execution_info: QueryExecutionInfoRef,
    ) -> Self {
        Self {
            task_output_id,
            task_host,
            chunk_rx,
            query_id,
            query_execution_info,
        }
    }

    fn stream_from_channel(self) -> DistributedQueryStream {
        DistributedQueryStream {
            chunk_rx: self.chunk_rx,
            query_id: self.query_id,
            query_execution_info: self.query_execution_info,
        }
    }
}

impl Debug for QueryResultFetcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultFetcher")
            .field("task_output_id", &self.task_output_id)
            .field("task_host", &self.task_host)
            .finish()
    }
}

#[cfg(test)]
mod cursor_lifecycle_tests {
    use super::*;
    use crate::scheduler::distributed::query::tests::create_query;

    /// Verifies the query-registration handoff from scheduling to the result stream: dropping an
    /// armed guard cleans up an interrupted query, while disarming it preserves the registration
    /// for stream-owned cleanup.
    #[tokio::test]
    async fn test_distributed_query_registration_guard_handoff() {
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let query_execution = Arc::new(QueryExecution::new(query, (0, 0), None, false));
        let query_execution_info = Arc::new(RwLock::new(QueryExecutionInfo::default()));
        query_execution_info
            .write()
            .unwrap()
            .add_query(query_id.clone(), query_execution.clone());

        drop(DistributedQueryRegistrationAtomicGuard::new(
            query_id.clone(),
            query_execution.clone(),
            query_execution_info.clone(),
        ));

        assert!(
            !query_execution_info
                .read()
                .unwrap()
                .query_execution_map
                .contains_key(&query_id),
            "dropping an armed registration guard must remove the query"
        );
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let query_execution = Arc::new(QueryExecution::new(query, (0, 0), None, false));
        query_execution_info
            .write()
            .unwrap()
            .add_query(query_id.clone(), query_execution.clone());
        let mut registration = DistributedQueryRegistrationAtomicGuard::new(
            query_id.clone(),
            query_execution,
            query_execution_info.clone(),
        );
        registration.disarm();
        drop(registration);

        assert!(
            query_execution_info
                .read()
                .unwrap()
                .query_execution_map
                .contains_key(&query_id),
            "disarming must preserve the registration for stream ownership"
        );
        query_execution_info
            .write()
            .unwrap()
            .delete_query(&query_id);
    }
}
