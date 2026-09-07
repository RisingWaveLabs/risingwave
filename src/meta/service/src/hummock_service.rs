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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use compact_task::PbTaskStatus;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::catalog::SYS_CATALOG_START_ID;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_meta::backup_restore::BackupManagerRef;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::manager::iceberg_compaction::IcebergCompactionManagerRef;
use risingwave_pb::hummock::get_compaction_score_response::PickerInfo;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::subscribe_compaction_event_request::Event as RequestEvent;
use risingwave_pb::hummock::*;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::Event as IcebergRequestEvent;
use risingwave_pb::iceberg_compaction::{
    SubscribeIcebergCompactionEventRequest, SubscribeIcebergCompactionEventResponse,
};
use tonic::{Request, Response, Status, Streaming};

use crate::RwReceiverStream;
use crate::hummock::compaction::selector::ManualCompactionOption;
use crate::hummock::{HummockManagerRef, ManualCompactionTriggerResult};

pub struct HummockServiceImpl {
    hummock_manager: HummockManagerRef,
    metadata_manager: MetadataManager,
    backup_manager: BackupManagerRef,
    iceberg_compaction_manager: IcebergCompactionManagerRef,
}

impl HummockServiceImpl {
    pub fn new(
        hummock_manager: HummockManagerRef,
        metadata_manager: MetadataManager,
        backup_manager: BackupManagerRef,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
    ) -> Self {
        HummockServiceImpl {
            hummock_manager,
            metadata_manager,
            backup_manager,
            iceberg_compaction_manager,
        }
    }
}

macro_rules! fields_to_kvs {
    ($struct:ident, $($field:ident),*) => {
        {
            let mut kvs = HashMap::default();
            $(
                kvs.insert(stringify!($field).to_string(), $struct.$field.to_string());
            )*
            kvs
        }
    }
}

#[async_trait::async_trait]
impl HummockManagerService for HummockServiceImpl {
    type SubscribeCompactionEventStream = RwReceiverStream<SubscribeCompactionEventResponse>;
    type SubscribeIcebergCompactionEventStream =
        RwReceiverStream<SubscribeIcebergCompactionEventResponse>;

    async fn unpin_version_before(
        &self,
        request: Request<UnpinVersionBeforeRequest>,
    ) -> Result<Response<UnpinVersionBeforeResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager
            .unpin_version_before(req.context_id, req.unpin_version_before)
            .await?;
        Ok(Response::new(UnpinVersionBeforeResponse { status: None }))
    }

    async fn get_current_version(
        &self,
        _request: Request<GetCurrentVersionRequest>,
    ) -> Result<Response<GetCurrentVersionResponse>, Status> {
        let current_version = self
            .hummock_manager
            .on_current_version(|version| version.into())
            .await;
        Ok(Response::new(GetCurrentVersionResponse {
            status: None,
            current_version: Some(current_version),
        }))
    }

    async fn replay_version_delta(
        &self,
        request: Request<ReplayVersionDeltaRequest>,
    ) -> Result<Response<ReplayVersionDeltaResponse>, Status> {
        #[cfg(any(test, feature = "test"))]
        {
            let req = request.into_inner();
            let (version, compaction_groups) = self
                .hummock_manager
                .replay_version_delta(HummockVersionDelta::from_rpc_protobuf(
                    &req.version_delta.unwrap(),
                ))
                .await?;
            Ok(Response::new(ReplayVersionDeltaResponse {
                version: Some(version.into()),
                modified_compaction_groups: compaction_groups,
            }))
        }
        #[cfg(not(any(test, feature = "test")))]
        {
            let _ = request;
            Err(Status::unimplemented(
                "replay_version_delta is only available in test builds",
            ))
        }
    }

    async fn trigger_compaction_deterministic(
        &self,
        request: Request<TriggerCompactionDeterministicRequest>,
    ) -> Result<Response<TriggerCompactionDeterministicResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager
            .trigger_compaction_deterministic(req.version_id, req.compaction_groups)
            .await?;
        Ok(Response::new(TriggerCompactionDeterministicResponse {}))
    }

    async fn disable_commit_epoch(
        &self,
        _request: Request<DisableCommitEpochRequest>,
    ) -> Result<Response<DisableCommitEpochResponse>, Status> {
        let version = self.hummock_manager.disable_commit_epoch().await;
        Ok(Response::new(DisableCommitEpochResponse {
            current_version: Some(PbHummockVersion::from(&*version)),
        }))
    }

    async fn list_version_deltas(
        &self,
        request: Request<ListVersionDeltasRequest>,
    ) -> Result<Response<ListVersionDeltasResponse>, Status> {
        let req = request.into_inner();
        let version_deltas = self
            .hummock_manager
            .list_version_deltas(req.start_id, req.num_limit)
            .await?;
        let resp = ListVersionDeltasResponse {
            version_deltas: Some(PbHummockVersionDeltas {
                version_deltas: version_deltas
                    .into_iter()
                    .map(HummockVersionDelta::into)
                    .collect(),
            }),
        };
        Ok(Response::new(resp))
    }

    async fn get_new_object_ids(
        &self,
        request: Request<GetNewObjectIdsRequest>,
    ) -> Result<Response<GetNewObjectIdsResponse>, Status> {
        let object_id_range = self
            .hummock_manager
            .get_new_object_ids(request.into_inner().number)
            .await?;
        Ok(Response::new(GetNewObjectIdsResponse {
            status: None,
            start_id: object_id_range.start_id,
            end_id: object_id_range.end_id,
        }))
    }

    async fn trigger_manual_compaction(
        &self,
        request: Request<TriggerManualCompactionRequest>,
    ) -> Result<Response<TriggerManualCompactionResponse>, Status> {
        let request = request.into_inner();
        let compaction_group_id = request.compaction_group_id;
        let mut option = ManualCompactionOption {
            level: request.level as usize,
            target_level: request
                .target_level
                .map(|target_level| target_level as usize),
            sst_ids: request.sst_ids,
            exclusive: request.exclusive.unwrap_or(false),
            ..Default::default()
        };

        // rewrite the key_range
        match request.key_range {
            Some(pb_key_range) => {
                option.key_range = KeyRange {
                    left: pb_key_range.left.into(),
                    right: pb_key_range.right.into(),
                    right_exclusive: pb_key_range.right_exclusive,
                };
            }

            None => {
                option.key_range = KeyRange::default();
            }
        }

        // get internal_table_id by metadata_manger
        if request.table_id.as_raw_id() < SYS_CATALOG_START_ID as u32 {
            // We need to make sure to use the correct table_id to filter sst
            let job_id = request.table_id;
            if let Ok(table_fragment) = self.metadata_manager.get_job_fragments_by_id(job_id).await
            {
                option.internal_table_id = HashSet::from_iter(table_fragment.all_table_ids());
            }
        }

        assert!(
            option
                .internal_table_id
                .iter()
                .all(|table_id| table_id.as_raw_id() < SYS_CATALOG_START_ID as u32),
        );

        tracing::info!(
            "Try trigger_manual_compaction compaction_group_id {} option {:?}",
            compaction_group_id,
            &option
        );

        let should_retry = match self
            .hummock_manager
            .trigger_manual_compaction(compaction_group_id, option)
            .await?
        {
            ManualCompactionTriggerResult::Submitted => false,
            ManualCompactionTriggerResult::Retry => true,
        };

        Ok(Response::new(TriggerManualCompactionResponse {
            status: None,
            should_retry: Some(should_retry),
        }))
    }

    async fn trigger_full_gc(
        &self,
        request: Request<TriggerFullGcRequest>,
    ) -> Result<Response<TriggerFullGcResponse>, Status> {
        let req = request.into_inner();
        let backup_manager_2 = self.backup_manager.clone();
        let hummock_manager_2 = self.hummock_manager.clone();
        tokio::task::spawn(async move {
            use thiserror_ext::AsReport;
            let _ = hummock_manager_2
                .start_full_gc(
                    Duration::from_secs(req.sst_retention_time_sec),
                    req.prefix,
                    Some(backup_manager_2),
                )
                .await
                .inspect_err(|e| tracing::warn!(error = %e.as_report(), "Failed to start GC."));
        });
        Ok(Response::new(TriggerFullGcResponse { status: None }))
    }

    async fn rise_ctl_get_pinned_versions_summary(
        &self,
        _request: Request<RiseCtlGetPinnedVersionsSummaryRequest>,
    ) -> Result<Response<RiseCtlGetPinnedVersionsSummaryResponse>, Status> {
        let pinned_versions = self.hummock_manager.list_pinned_version().await;
        let workers = self
            .hummock_manager
            .list_workers(&pinned_versions.iter().map(|v| v.context_id).collect_vec())
            .await?;
        Ok(Response::new(RiseCtlGetPinnedVersionsSummaryResponse {
            summary: Some(PinnedVersionsSummary {
                pinned_versions,
                workers,
            }),
        }))
    }

    async fn get_assigned_compact_task_num(
        &self,
        _request: Request<GetAssignedCompactTaskNumRequest>,
    ) -> Result<Response<GetAssignedCompactTaskNumResponse>, Status> {
        let num_tasks = self.hummock_manager.get_assigned_compact_task_num().await;
        Ok(Response::new(GetAssignedCompactTaskNumResponse {
            num_tasks: num_tasks as u32,
        }))
    }

    async fn rise_ctl_list_compaction_group(
        &self,
        _request: Request<RiseCtlListCompactionGroupRequest>,
    ) -> Result<Response<RiseCtlListCompactionGroupResponse>, Status> {
        let compaction_groups = self.hummock_manager.list_compaction_group().await;
        Ok(Response::new(RiseCtlListCompactionGroupResponse {
            status: None,
            compaction_groups,
        }))
    }

    async fn rise_ctl_update_compaction_config(
        &self,
        request: Request<RiseCtlUpdateCompactionConfigRequest>,
    ) -> Result<Response<RiseCtlUpdateCompactionConfigResponse>, Status> {
        let RiseCtlUpdateCompactionConfigRequest {
            compaction_group_ids,
            configs,
        } = request.into_inner();
        self.hummock_manager
            .update_compaction_config(
                compaction_group_ids.as_slice(),
                configs
                    .into_iter()
                    .map(|c| c.mutable_config.unwrap())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .await?;
        Ok(Response::new(RiseCtlUpdateCompactionConfigResponse {
            status: None,
        }))
    }

    async fn init_metadata_for_replay(
        &self,
        request: Request<InitMetadataForReplayRequest>,
    ) -> Result<Response<InitMetadataForReplayResponse>, Status> {
        let InitMetadataForReplayRequest {
            tables,
            compaction_groups,
        } = request.into_inner();

        self.hummock_manager
            .init_metadata_for_version_replay(tables, compaction_groups)?;
        Ok(Response::new(InitMetadataForReplayResponse {}))
    }

    async fn pin_version(
        &self,
        request: Request<PinVersionRequest>,
    ) -> Result<Response<PinVersionResponse>, Status> {
        let req = request.into_inner();
        let version = self.hummock_manager.pin_version(req.context_id).await?;
        Ok(Response::new(PinVersionResponse {
            pinned_version: Some(PbHummockVersion::from(&*version)),
        }))
    }

    async fn split_compaction_group(
        &self,
        request: Request<SplitCompactionGroupRequest>,
    ) -> Result<Response<SplitCompactionGroupResponse>, Status> {
        let req = request.into_inner();
        let new_group_id = self
            .hummock_manager
            .move_state_tables_to_dedicated_compaction_group(
                req.group_id,
                &req.table_ids,
                if req.partition_vnode_count > 0 {
                    Some(req.partition_vnode_count)
                } else {
                    None
                },
            )
            .await?
            .0;
        Ok(Response::new(SplitCompactionGroupResponse { new_group_id }))
    }

    async fn rise_ctl_pause_version_checkpoint(
        &self,
        _request: Request<RiseCtlPauseVersionCheckpointRequest>,
    ) -> Result<Response<RiseCtlPauseVersionCheckpointResponse>, Status> {
        self.hummock_manager.pause_version_checkpoint();
        Ok(Response::new(RiseCtlPauseVersionCheckpointResponse {}))
    }

    async fn rise_ctl_resume_version_checkpoint(
        &self,
        _request: Request<RiseCtlResumeVersionCheckpointRequest>,
    ) -> Result<Response<RiseCtlResumeVersionCheckpointResponse>, Status> {
        self.hummock_manager.resume_version_checkpoint();
        Ok(Response::new(RiseCtlResumeVersionCheckpointResponse {}))
    }

    async fn rise_ctl_get_checkpoint_version(
        &self,
        _request: Request<RiseCtlGetCheckpointVersionRequest>,
    ) -> Result<Response<RiseCtlGetCheckpointVersionResponse>, Status> {
        let checkpoint_version = self.hummock_manager.get_checkpoint_version().await;
        Ok(Response::new(RiseCtlGetCheckpointVersionResponse {
            checkpoint_version: Some(PbHummockVersion::from(&*checkpoint_version)),
        }))
    }

    async fn rise_ctl_list_compaction_status(
        &self,
        _request: Request<RiseCtlListCompactionStatusRequest>,
    ) -> Result<Response<RiseCtlListCompactionStatusResponse>, Status> {
        let (compaction_statuses, task_assignment) =
            self.hummock_manager.list_compaction_status().await;
        let task_progress = self.hummock_manager.compactor_manager.get_progress();
        Ok(Response::new(RiseCtlListCompactionStatusResponse {
            compaction_statuses,
            task_assignment,
            task_progress,
        }))
    }

    async fn subscribe_compaction_event(
        &self,
        request: Request<Streaming<SubscribeCompactionEventRequest>>,
    ) -> Result<Response<Self::SubscribeCompactionEventStream>, tonic::Status> {
        let mut request_stream: Streaming<SubscribeCompactionEventRequest> = request.into_inner();
        let register_req = {
            let req = request_stream.next().await.ok_or_else(|| {
                Status::invalid_argument("subscribe_compaction_event request is empty")
            })??;

            match req.event {
                Some(RequestEvent::Register(register)) => register,
                _ => {
                    return Err(Status::invalid_argument(
                        "the first message must be `Register`",
                    ));
                }
            }
        };

        let context_id = register_req.context_id;

        // check_context and add_compactor as a whole is not atomic, but compactor_manager will
        // remove invalid compactor eventually.
        if !self.hummock_manager.check_context(context_id).await? {
            return Err(Status::new(
                tonic::Code::Internal,
                format!("invalid hummock context {}", context_id),
            ));
        }
        let compactor_manager = self.hummock_manager.compactor_manager.clone();

        let rx: tokio::sync::mpsc::UnboundedReceiver<
            Result<SubscribeCompactionEventResponse, crate::MetaError>,
        > = compactor_manager.add_compactor(context_id);

        // register request stream to hummock
        self.hummock_manager
            .add_compactor_stream(context_id, request_stream);

        // Trigger compaction on all compaction groups.
        for cg_id in self.hummock_manager.compaction_group_ids().await {
            self.hummock_manager
                .try_send_compaction_request(cg_id, compact_task::TaskType::Dynamic);
        }

        Ok(Response::new(RwReceiverStream::new(rx)))
    }

    async fn subscribe_iceberg_compaction_event(
        &self,
        request: Request<Streaming<SubscribeIcebergCompactionEventRequest>>,
    ) -> Result<Response<Self::SubscribeIcebergCompactionEventStream>, tonic::Status> {
        let mut request_stream: Streaming<SubscribeIcebergCompactionEventRequest> =
            request.into_inner();
        let register_req = {
            let req = request_stream.next().await.ok_or_else(|| {
                Status::invalid_argument("subscribe_compaction_event request is empty")
            })??;

            match req.event {
                Some(IcebergRequestEvent::Register(register)) => register,
                _ => {
                    return Err(Status::invalid_argument(
                        "the first message must be `Register`",
                    ));
                }
            }
        };

        let context_id = register_req.context_id;

        // check_context and add_compactor as a whole is not atomic, but compactor_manager will
        // remove invalid compactor eventually.
        if !self.hummock_manager.check_context(context_id).await? {
            return Err(Status::new(
                tonic::Code::Internal,
                format!("invalid hummock context {}", context_id),
            ));
        }

        let rx: tokio::sync::mpsc::UnboundedReceiver<
            Result<SubscribeIcebergCompactionEventResponse, crate::MetaError>,
        > = self
            .iceberg_compaction_manager
            .iceberg_compactor_manager
            .add_compactor(context_id);

        self.iceberg_compaction_manager
            .add_compactor_stream(context_id, request_stream);

        // TODO: Trigger iceberg compaction

        Ok(Response::new(RwReceiverStream::new(rx)))
    }

    async fn report_compaction_task(
        &self,
        _request: Request<ReportCompactionTaskRequest>,
    ) -> Result<Response<ReportCompactionTaskResponse>, Status> {
        unreachable!()
    }

    async fn list_branched_object(
        &self,
        _request: Request<ListBranchedObjectRequest>,
    ) -> Result<Response<ListBranchedObjectResponse>, Status> {
        let branched_objects = self
            .hummock_manager
            .list_branched_objects()
            .await
            .into_iter()
            .flat_map(|(object_id, v)| {
                v.into_iter()
                    .map(move |(compaction_group_id, sst_ids)| BranchedObject {
                        object_id,
                        sst_id: sst_ids,
                        compaction_group_id,
                    })
            })
            .collect();
        Ok(Response::new(ListBranchedObjectResponse {
            branched_objects,
        }))
    }

    async fn list_active_write_limit(
        &self,
        _request: Request<ListActiveWriteLimitRequest>,
    ) -> Result<Response<ListActiveWriteLimitResponse>, Status> {
        Ok(Response::new(ListActiveWriteLimitResponse {
            write_limits: self.hummock_manager.write_limits().await,
        }))
    }

    async fn list_hummock_meta_config(
        &self,
        _request: Request<ListHummockMetaConfigRequest>,
    ) -> Result<Response<ListHummockMetaConfigResponse>, Status> {
        let opt = &self.hummock_manager.env.opts;
        let configs = fields_to_kvs!(
            opt,
            vacuum_interval_sec,
            vacuum_spin_interval_ms,
            hummock_version_checkpoint_interval_sec,
            min_delta_log_num_for_hummock_version_checkpoint,
            min_sst_retention_time_sec,
            full_gc_interval_sec,
            periodic_compaction_interval_sec,
            periodic_space_reclaim_compaction_interval_sec,
            periodic_ttl_reclaim_compaction_interval_sec,
            periodic_tombstone_reclaim_compaction_interval_sec,
            periodic_scheduling_compaction_group_split_interval_sec,
            enable_compaction_group_normalize,
            max_normalize_splits_per_round,
            do_not_config_object_storage_lifecycle,
            partition_vnode_count,
            table_high_write_throughput_threshold,
            table_low_write_throughput_threshold,
            compaction_task_max_heartbeat_interval_secs,
            periodic_scheduling_compaction_group_merge_interval_sec
        );
        Ok(Response::new(ListHummockMetaConfigResponse { configs }))
    }

    async fn rise_ctl_rebuild_table_stats(
        &self,
        _request: Request<RiseCtlRebuildTableStatsRequest>,
    ) -> Result<Response<RiseCtlRebuildTableStatsResponse>, Status> {
        self.hummock_manager.rebuild_table_stats().await?;
        Ok(Response::new(RiseCtlRebuildTableStatsResponse {}))
    }

    async fn get_compaction_score(
        &self,
        request: Request<GetCompactionScoreRequest>,
    ) -> Result<Response<GetCompactionScoreResponse>, Status> {
        let compaction_group_id = request.into_inner().compaction_group_id;
        let scores = self
            .hummock_manager
            .get_compaction_scores(compaction_group_id)
            .await
            .into_iter()
            .map(|s| PickerInfo {
                score: s.score,
                select_level: s.select_level as _,
                target_level: s.target_level as _,
                picker_type: s.picker_type.to_string(),
            })
            .collect();
        Ok(Response::new(GetCompactionScoreResponse {
            compaction_group_id,
            scores,
        }))
    }

    async fn list_compact_task_assignment(
        &self,
        _request: Request<ListCompactTaskAssignmentRequest>,
    ) -> Result<Response<ListCompactTaskAssignmentResponse>, Status> {
        let (_compaction_statuses, task_assignment) =
            self.hummock_manager.list_compaction_status().await;
        Ok(Response::new(ListCompactTaskAssignmentResponse {
            task_assignment,
        }))
    }

    async fn list_compact_task_progress(
        &self,
        _request: Request<ListCompactTaskProgressRequest>,
    ) -> Result<Response<ListCompactTaskProgressResponse>, Status> {
        let task_progress = self.hummock_manager.compactor_manager.get_progress();

        Ok(Response::new(ListCompactTaskProgressResponse {
            task_progress,
        }))
    }

    async fn cancel_compact_task(
        &self,
        request: Request<CancelCompactTaskRequest>,
    ) -> Result<Response<CancelCompactTaskResponse>, Status> {
        let request = request.into_inner();
        let ret = self
            .hummock_manager
            .cancel_compact_task(
                request.task_id,
                PbTaskStatus::try_from(request.task_status).unwrap(),
            )
            .await?;

        let response = Response::new(CancelCompactTaskResponse { ret });
        return Ok(response);
    }

    async fn get_version_by_epoch(
        &self,
        request: Request<GetVersionByEpochRequest>,
    ) -> Result<Response<GetVersionByEpochResponse>, Status> {
        let GetVersionByEpochRequest { epoch, table_id } = request.into_inner();
        let version = self
            .hummock_manager
            .epoch_to_version(epoch, table_id)
            .await?;
        Ok(Response::new(GetVersionByEpochResponse {
            version: Some(version.to_protobuf()),
        }))
    }

    async fn merge_compaction_group(
        &self,
        request: Request<MergeCompactionGroupRequest>,
    ) -> Result<Response<MergeCompactionGroupResponse>, Status> {
        let req = request.into_inner();
        self.hummock_manager
            .merge_compaction_group(req.left_group_id, req.right_group_id)
            .await?;
        Ok(Response::new(MergeCompactionGroupResponse {}))
    }

    async fn get_table_change_logs(
        &self,
        request: Request<GetTableChangeLogsRequest>,
    ) -> Result<Response<GetTableChangeLogsResponse>, Status> {
        let GetTableChangeLogsRequest {
            epoch_only,
            start_epoch_inclusive,
            end_epoch_inclusive,
            table_ids,
            exclude_empty,
            limit,
        } = request.into_inner();
        let table_change_logs = self
            .hummock_manager
            .get_table_change_logs(
                epoch_only,
                start_epoch_inclusive,
                end_epoch_inclusive,
                table_ids
                    .map(|t| t.table_ids.into_iter().collect::<HashSet<_>>())
                    .clone(),
                exclude_empty,
                limit,
            )
            .await?
            .into_iter()
            .map(|(i, l)| (i.as_raw_id(), l.to_protobuf()))
            .collect();
        Ok(Response::new(GetTableChangeLogsResponse {
            table_change_logs,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[test]
    fn test_fields_to_kvs() {
        struct S {
            foo: u64,
            bar: String,
        }
        let s = S {
            foo: 15,
            bar: "foobar".to_owned(),
        };
        let kvs: HashMap<String, String> = fields_to_kvs!(s, foo, bar);
        assert_eq!(kvs.len(), 2);
        assert_eq!(kvs.get("foo").unwrap(), "15");
        assert_eq!(kvs.get("bar").unwrap(), "foobar");
    }
}
