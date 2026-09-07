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

use std::sync::Arc;

use risingwave_common::config::{BatchConfig, MetricLevel};
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::worker_util::WorkerNodeId;
use risingwave_connector::source::iceberg::IcebergScanMetrics;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_storage::StateStoreImpl;

use crate::monitor::{BatchExecutorMetrics, BatchManagerMetrics, BatchSpillMetrics};
use crate::task::BatchManager;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct BatchEnvironment {
    /// Endpoint the batch task manager listens on.
    server_addr: HostAddr,

    /// Reference to the task manager.
    task_manager: Arc<BatchManager>,

    /// Batch related configurations.
    config: Arc<BatchConfig>,

    /// Current worker node id.
    worker_id: WorkerNodeId,

    /// State store for table scanning.
    state_store: StateStoreImpl,

    /// Executor level metrics.
    executor_metrics: Arc<BatchExecutorMetrics>,

    /// Compute client pool for batch gRPC exchange.
    client_pool: ComputeClientPoolRef,

    /// Manages dml information.
    dml_manager: DmlManagerRef,

    /// Metrics for source.
    source_metrics: Arc<SourceMetrics>,

    /// Batch spill metrics
    spill_metrics: Arc<BatchSpillMetrics>,

    /// Metrics for iceberg scan.
    iceberg_scan_metrics: Arc<IcebergScanMetrics>,

    metric_level: MetricLevel,
}

impl BatchEnvironment {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        task_manager: Arc<BatchManager>,
        server_addr: HostAddr,
        config: Arc<BatchConfig>,
        worker_id: WorkerNodeId,
        state_store: StateStoreImpl,
        executor_metrics: Arc<BatchExecutorMetrics>,
        client_pool: ComputeClientPoolRef,
        dml_manager: DmlManagerRef,
        source_metrics: Arc<SourceMetrics>,
        spill_metrics: Arc<BatchSpillMetrics>,
        iceberg_scan_metrics: Arc<IcebergScanMetrics>,
        metric_level: MetricLevel,
    ) -> Self {
        BatchEnvironment {
            server_addr,
            task_manager,
            config,
            worker_id,
            state_store,
            executor_metrics,
            client_pool,
            dml_manager,
            source_metrics,
            spill_metrics,
            iceberg_scan_metrics,
            metric_level,
        }
    }

    // Create an instance for testing purpose.
    pub fn for_test() -> Self {
        use risingwave_dml::dml_manager::DmlManager;
        use risingwave_rpc_client::ComputeClientPool;
        use risingwave_storage::monitor::MonitoredStorageMetrics;

        BatchEnvironment {
            task_manager: Arc::new(BatchManager::new(
                BatchConfig::default(),
                BatchManagerMetrics::for_test(),
                u64::MAX,
                None,
            )),
            server_addr: "127.0.0.1:2333".parse().unwrap(),
            config: Arc::new(BatchConfig::default()),
            worker_id: WorkerNodeId::default(),
            state_store: StateStoreImpl::shared_in_memory_store(Arc::new(
                MonitoredStorageMetrics::unused(),
            )),
            client_pool: Arc::new(ComputeClientPool::for_test()),
            dml_manager: Arc::new(DmlManager::for_test()),
            source_metrics: Arc::new(SourceMetrics::default()),
            executor_metrics: BatchExecutorMetrics::for_test(),
            spill_metrics: BatchSpillMetrics::for_test(),
            iceberg_scan_metrics: IcebergScanMetrics::for_test(),
            metric_level: MetricLevel::Debug,
        }
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub fn task_manager(&self) -> Arc<BatchManager> {
        self.task_manager.clone()
    }

    pub fn config(&self) -> &BatchConfig {
        self.config.as_ref()
    }

    pub fn worker_id(&self) -> WorkerNodeId {
        self.worker_id
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }

    pub fn manager_metrics(&self) -> Arc<BatchManagerMetrics> {
        self.task_manager.metrics()
    }

    pub fn executor_metrics(&self) -> Arc<BatchExecutorMetrics> {
        self.executor_metrics.clone()
    }

    pub fn client_pool(&self) -> ComputeClientPoolRef {
        self.client_pool.clone()
    }

    pub fn dml_manager_ref(&self) -> DmlManagerRef {
        self.dml_manager.clone()
    }

    pub fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.source_metrics.clone()
    }

    pub fn spill_metrics(&self) -> Arc<BatchSpillMetrics> {
        self.spill_metrics.clone()
    }

    pub fn metric_level(&self) -> MetricLevel {
        self.metric_level
    }

    pub fn iceberg_scan_metrics(&self) -> Arc<IcebergScanMetrics> {
        self.iceberg_scan_metrics.clone()
    }
}
