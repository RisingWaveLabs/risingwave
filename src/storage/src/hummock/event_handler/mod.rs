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
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::{RwLock, RwLockReadGuard};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{HummockEpoch, HummockRawObjectId};
use thiserror_ext::AsReport;
use tokio::sync::oneshot;

use crate::hummock::HummockResult;
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferBatchId};
use crate::mem_table::ImmutableMemtable;
use crate::store::SealCurrentEpochOptions;

pub mod hummock_event_handler;
pub mod refiller;
pub mod uploader;

pub(crate) use hummock_event_handler::HummockEventHandler;
use risingwave_hummock_sdk::vector_index::VectorIndexAdd;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_pb::meta::PbTableRefillRuntimeConfig;
use risingwave_pb::meta::subscribe_response::Operation;

use super::store::version::HummockReadVersion;
use crate::hummock::event_handler::hummock_event_handler::HummockEventSender;
use crate::hummock::event_handler::refiller::TableCacheRefillMonitorSnapshot;
use crate::hummock::event_handler::uploader::SyncedData;
use crate::hummock::utils::MemoryTracker;

#[derive(Debug)]
pub struct BufferWriteRequest {
    pub batch: SharedBufferBatch,
    pub epoch: HummockEpoch,
    pub grant_sender: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum HummockVersionUpdate {
    VersionDeltas(Vec<HummockVersionDelta>),
    PinnedVersion(Box<HummockVersion>),
}

#[derive(Debug)]
pub enum HummockObserverEvent {
    VersionUpdate(HummockVersionUpdate),
    TableRefillRuntimeConfig(Operation, PbTableRefillRuntimeConfig),
}

pub enum HummockEvent {
    /// Notify that we may flush the shared buffer.
    BufferMayFlush,

    /// An epoch is going to be synced. Once the event is processed, there will be no more flush
    /// task on this epoch. Previous concurrent flush task join handle will be returned by the join
    /// handle sender.
    SyncEpoch {
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    },

    /// Clear shared buffer and reset all states
    Clear(oneshot::Sender<()>, Option<HashSet<TableId>>),

    Shutdown,

    ImmToUploader {
        instance_id: SharedBufferBatchId,
        imms: Vec<(ImmutableMemtable, MemoryTracker)>,
    },

    StartEpoch {
        epoch: HummockEpoch,
        table_ids: HashSet<TableId>,
    },

    InitEpoch {
        instance_id: LocalInstanceId,
        init_epoch: HummockEpoch,
    },

    LocalSealEpoch {
        instance_id: LocalInstanceId,
        next_epoch: HummockEpoch,
        opts: SealCurrentEpochOptions,
    },

    #[cfg(any(test, feature = "test"))]
    /// Flush all previous event. When all previous events has been consumed, the event handler
    /// will notify
    FlushEvent(oneshot::Sender<()>),

    RegisterReadVersion {
        table_id: TableId,
        new_read_version_sender: oneshot::Sender<(HummockReadVersionRef, LocalInstanceGuard)>,
        is_replicated: bool,
        vnodes: Arc<Bitmap>,
    },

    DestroyReadVersion {
        instance_id: LocalInstanceId,
    },

    RegisterVectorWriter {
        table_id: TableId,
        init_epoch: HummockEpoch,
    },

    VectorWriterSealEpoch {
        table_id: TableId,
        next_epoch: HummockEpoch,
        add: Option<VectorIndexAdd>,
    },

    DropVectorWriter {
        table_id: TableId,
    },

    GetMinUncommittedObjectId {
        result_tx: oneshot::Sender<Option<HummockRawObjectId>>,
    },

    GetTableCacheRefillMonitorSnapshot {
        result_tx: oneshot::Sender<TableCacheRefillMonitorSnapshot>,
    },
}

impl HummockEvent {
    fn to_debug_string(&self) -> String {
        match self {
            HummockEvent::BufferMayFlush => "BufferMayFlush".to_owned(),

            HummockEvent::SyncEpoch {
                sync_result_sender: _,
                sync_table_epochs,
            } => format!("AwaitSyncEpoch epoch {:?}", sync_table_epochs),

            HummockEvent::Clear(_, table_ids) => {
                format!("Clear {:?}", table_ids)
            }

            HummockEvent::Shutdown => "Shutdown".to_owned(),

            HummockEvent::StartEpoch { epoch, table_ids } => {
                format!("StartEpoch {} {:?}", epoch, table_ids)
            }

            HummockEvent::InitEpoch {
                instance_id,
                init_epoch,
            } => {
                format!("InitEpoch {} {}", instance_id, init_epoch)
            }

            HummockEvent::ImmToUploader { instance_id, imms } => {
                format!(
                    "ImmToUploader {} {:?}",
                    instance_id,
                    imms.iter().map(|(imm, _)| imm.batch_id()).collect_vec()
                )
            }

            HummockEvent::LocalSealEpoch {
                instance_id,
                next_epoch,
                opts,
            } => {
                format!(
                    "LocalSealEpoch next_epoch: {}, instance_id: {}, opts: {:?}",
                    next_epoch, instance_id, opts
                )
            }

            HummockEvent::RegisterReadVersion {
                table_id,
                new_read_version_sender: _,
                is_replicated,
                vnodes: _,
            } => format!(
                "RegisterReadVersion table_id {:?}, is_replicated: {:?}",
                table_id, is_replicated
            ),

            HummockEvent::DestroyReadVersion { instance_id } => {
                format!("DestroyReadVersion instance_id {:?}", instance_id)
            }

            #[cfg(any(test, feature = "test"))]
            HummockEvent::FlushEvent(_) => "FlushEvent".to_owned(),
            HummockEvent::GetMinUncommittedObjectId { .. } => {
                "GetMinUncommittedObjectId".to_owned()
            }
            HummockEvent::GetTableCacheRefillMonitorSnapshot { .. } => {
                "GetTableCacheRefillMonitorSnapshot".to_owned()
            }
            HummockEvent::RegisterVectorWriter { .. } => "RegisterVectorWriter".to_owned(),
            HummockEvent::VectorWriterSealEpoch { .. } => "VectorWriterSealEpoch".to_owned(),
            HummockEvent::DropVectorWriter { .. } => "DropVectorWriter".to_owned(),
        }
    }

    pub fn event_name(&self) -> &'static str {
        match self {
            HummockEvent::BufferMayFlush => "BufferMayFlush",
            HummockEvent::SyncEpoch { .. } => "SyncEpoch",
            HummockEvent::Clear(..) => "Clear",
            HummockEvent::Shutdown => "Shutdown",
            HummockEvent::ImmToUploader { .. } => "ImmToUploader",
            HummockEvent::StartEpoch { .. } => "StartEpoch",
            HummockEvent::InitEpoch { .. } => "InitEpoch",
            HummockEvent::LocalSealEpoch { .. } => "LocalSealEpoch",
            #[cfg(any(test, feature = "test"))]
            HummockEvent::FlushEvent(_) => "FlushEvent",
            HummockEvent::RegisterReadVersion { .. } => "RegisterReadVersion",
            HummockEvent::DestroyReadVersion { .. } => "DestroyReadVersion",
            HummockEvent::RegisterVectorWriter { .. } => "RegisterVectorWriter",
            HummockEvent::VectorWriterSealEpoch { .. } => "VectorWriterSealEpoch",
            HummockEvent::DropVectorWriter { .. } => "DropVectorWriter",
            HummockEvent::GetMinUncommittedObjectId { .. } => "GetMinUncommittedObjectId",
            HummockEvent::GetTableCacheRefillMonitorSnapshot { .. } => {
                "GetTableCacheRefillMonitorSnapshot"
            }
        }
    }
}

impl std::fmt::Debug for HummockEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HummockEvent")
            .field("debug_string", &self.to_debug_string())
            .finish()
    }
}

pub type LocalInstanceId = u64;
pub const TEST_LOCAL_INSTANCE_ID: LocalInstanceId = 233;
pub type HummockReadVersionRef = Arc<RwLock<HummockReadVersion>>;
pub type ReadVersionMappingType = HashMap<TableId, HashMap<LocalInstanceId, HummockReadVersionRef>>;
pub type ReadOnlyReadVersionMapping = ReadOnlyRwLockRef<ReadVersionMappingType>;

pub struct ReadOnlyRwLockRef<T>(Arc<RwLock<T>>);

impl<T> Clone for ReadOnlyRwLockRef<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> ReadOnlyRwLockRef<T> {
    pub fn new(inner: Arc<RwLock<T>>) -> Self {
        Self(inner)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read()
    }
}

pub struct LocalInstanceGuard {
    pub table_id: TableId,
    pub instance_id: LocalInstanceId,
    // Only send destroy event when event_sender when is_some
    event_sender: Option<HummockEventSender>,
}

impl Drop for LocalInstanceGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.event_sender.take() {
            // If sending fails, it means that event_handler and event_channel have been destroyed, no
            // need to handle failure
            sender
                .send(HummockEvent::DestroyReadVersion {
                    instance_id: self.instance_id,
                })
                .unwrap_or_else(|err| {
                    tracing::debug!(
                        error = %err.as_report(),
                        table_id = %self.table_id,
                        instance_id = self.instance_id,
                        "LocalInstanceGuard Drop SendError",
                    )
                })
        }
    }
}
