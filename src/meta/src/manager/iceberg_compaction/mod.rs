// Copyright 2026 RisingWave Labs
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

mod gc;
mod manual;
mod schedule;
mod stream;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use parking_lot::{Mutex, RwLock};
use risingwave_common::id::WorkerId;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{ICEBERG_SINK, IcebergConfig};
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
use risingwave_pb::id::IcebergCompactionTaskId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::Streaming;

use super::MetaSrvEnv;
use crate::MetaResult;
use crate::controller::streaming_job::AbortCreatingJobResult;
use crate::hummock::IcebergCompactorManagerRef;
use crate::manager::MetadataManager;
use crate::rpc::metrics::MetaMetrics;

pub type IcebergCompactionManagerRef = Arc<IcebergCompactionManager>;

pub(crate) type CompactorChangeTx =
    UnboundedSender<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

pub(crate) type CompactorChangeRx =
    UnboundedReceiver<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

type ManualCompactionWaiter = tokio::sync::oneshot::Sender<MetaResult<IcebergCompactionTaskId>>;

use schedule::CompactionTrack;
pub use schedule::IcebergCompactionScheduleStatus;

pub struct IcebergCompactionManager {
    pub env: MetaSrvEnv,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,

    metadata_manager: MetadataManager,
    pub iceberg_compactor_manager: IcebergCompactorManagerRef,

    compactor_streams_change_tx: CompactorChangeTx,

    orphan_file_cleanup_locks: Mutex<HashMap<SinkId, Arc<tokio::sync::Mutex<()>>>>,

    pub metrics: Arc<MetaMetrics>,
}

struct IcebergCompactionManagerInner {
    sink_schedules: HashMap<SinkId, CompactionTrack>,
    snapshot_expiration_sink_ids: HashSet<SinkId>,
    manifest_rewrite_sink_ids: HashSet<SinkId>,
    orphan_file_cleanup_sink_ids: HashSet<SinkId>,
    manual_compaction_waiters: HashMap<SinkId, ManualCompactionWaiter>,
}

impl IcebergCompactionManager {
    fn report_timeout(&self) -> Duration {
        Duration::from_secs(self.env.opts.iceberg_compaction_report_timeout_sec)
    }

    fn config_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.env.opts.iceberg_compaction_config_refresh_interval_sec)
    }

    pub fn build(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        iceberg_compactor_manager: IcebergCompactorManagerRef,
        metrics: Arc<MetaMetrics>,
    ) -> (Arc<Self>, CompactorChangeRx) {
        let (compactor_streams_change_tx, compactor_streams_change_rx) =
            tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                env,
                inner: Arc::new(RwLock::new(IcebergCompactionManagerInner {
                    sink_schedules: HashMap::default(),
                    snapshot_expiration_sink_ids: HashSet::default(),
                    manifest_rewrite_sink_ids: HashSet::default(),
                    orphan_file_cleanup_sink_ids: HashSet::default(),
                    manual_compaction_waiters: HashMap::default(),
                })),
                metadata_manager,
                iceberg_compactor_manager,
                compactor_streams_change_tx,
                orphan_file_cleanup_locks: Mutex::new(HashMap::default()),
                metrics,
            }),
            compactor_streams_change_rx,
        )
    }

    async fn get_sink_param(&self, sink_id: SinkId) -> MetaResult<SinkParam> {
        let prost_sink_catalog = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(sink_id)
            .await?
            .ok_or_else(|| anyhow!("Sink not found: {}", sink_id))?;
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        Ok(param)
    }

    async fn load_iceberg_config(&self, sink_id: SinkId) -> MetaResult<IcebergConfig> {
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties)?;
        Ok(iceberg_config)
    }

    fn orphan_file_cleanup_lock(&self, sink_id: SinkId) -> Arc<tokio::sync::Mutex<()>> {
        self.orphan_file_cleanup_locks
            .lock()
            .entry(sink_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Clear the iceberg maintenance state of the sink aborted by
    /// `try_abort_creating_streaming_job`, if any.
    pub fn clear_maintenance_for_aborted_job(&self, abort_result: &AbortCreatingJobResult) {
        for &sink_id in &abort_result.aborted_sink_ids {
            self.clear_iceberg_maintenance_by_sink_id(sink_id);
        }
    }
}

/// User-created iceberg sinks have arbitrary names, so identify them by the
/// connector property instead of the `__iceberg_sink_` name prefix.
pub fn is_iceberg_sink(properties: &BTreeMap<String, String>) -> bool {
    properties
        .get(UPSTREAM_SOURCE_KEY)
        .is_some_and(|connector| connector.eq_ignore_ascii_case(ICEBERG_SINK))
}
