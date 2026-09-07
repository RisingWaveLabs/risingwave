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

use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use futures::stream::{FuturesOrdered, StreamExt};
use risingwave_common::array::StreamChunk;
use risingwave_common::util::tracing::TracingContext;
use risingwave_dml::TableDmlHandleRef;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_dml::error::DmlError;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::task_service::task_service_server::TaskService;
use risingwave_pb::task_service::{
    CancelTaskRequest, CancelTaskResponse, CreateTaskRequest, ExecuteRequest, FastInsertRequest,
    FastInsertResponse, GetDataResponse, IngestDmlAckResponse, IngestDmlInitRequest,
    IngestDmlInitResponse, IngestDmlPayloadRequest, IngestDmlRequest, IngestDmlResponse,
    TaskInfoResponse, fast_insert_response, ingest_dml_request, ingest_dml_response,
};
use thiserror_ext::AsReport;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::error::BatchError;
use crate::executor::{FastInsertExecutor, inject_optional_row_id_column};
use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::task::{
    BatchEnvironment, BatchManager, BatchTaskExecution, ComputeNodeContext, StateReporter,
    TASK_STATUS_BUFFER_SIZE,
};

#[derive(Clone)]
pub struct BatchServiceImpl {
    mgr: Arc<BatchManager>,
    env: BatchEnvironment,
}

impl BatchServiceImpl {
    pub fn new(mgr: Arc<BatchManager>, env: BatchEnvironment) -> Self {
        BatchServiceImpl { mgr, env }
    }
}

pub type TaskInfoResponseResult = Result<TaskInfoResponse, Status>;
pub type GetDataResponseResult = Result<GetDataResponse, Status>;
pub type IngestDmlResponseResult = Result<IngestDmlResponse, Status>;

#[async_trait::async_trait]
impl TaskService for BatchServiceImpl {
    type CreateTaskStream = ReceiverStream<TaskInfoResponseResult>;
    type ExecuteStream = ReceiverStream<GetDataResponseResult>;
    type IngestDmlStream = ReceiverStream<IngestDmlResponseResult>;

    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<Self::CreateTaskStream>, Status> {
        #[cfg(madsim)]
        crate::rpc::service::madsim_test_utils::record_create_task();

        let CreateTaskRequest {
            task_id,
            plan,
            tracing_context,
            expr_context,
        } = request.into_inner();

        let (state_tx, state_rx) = tokio::sync::mpsc::channel(TASK_STATUS_BUFFER_SIZE);
        let state_reporter = StateReporter::new_with_dist_sender(state_tx);
        let res = self
            .mgr
            .fire_task(
                task_id.as_ref().expect("no task id found"),
                plan.expect("no plan found").clone(),
                ComputeNodeContext::create(self.env.clone()),
                state_reporter,
                TracingContext::from_protobuf(&tracing_context),
                expr_context.expect("no expression context found"),
            )
            .await;
        match res {
            Ok(_) => Ok(Response::new(ReceiverStream::new(
                // Create receiver stream from state receiver.
                // The state receiver is init in `.async_execute()`.
                // Will be used for receive task status update.
                // Note: we introduce this hack cuz `.execute()` do not produce a status stream,
                // but still share `.async_execute()` and `.try_execute()`.
                state_rx,
            ))),
            Err(e) => {
                error!(error = %e.as_report(), "failed to fire task");
                Err(e.into())
            }
        }
    }

    async fn cancel_task(
        &self,
        req: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = req.into_inner();
        tracing::trace!("Aborting task: {:?}", req.get_task_id().unwrap());
        self.mgr
            .cancel_task(req.get_task_id().expect("no task id found"));
        Ok(Response::new(CancelTaskResponse { status: None }))
    }

    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let req = req.into_inner();
        let env = self.env.clone();
        let mgr = self.mgr.clone();
        BatchServiceImpl::get_execute_stream(env, mgr, req).await
    }

    async fn fast_insert(
        &self,
        request: Request<FastInsertRequest>,
    ) -> Result<Response<FastInsertResponse>, Status> {
        let req = request.into_inner();
        let res = self.do_fast_insert(req).await;
        match res {
            Ok(_) => Ok(Response::new(FastInsertResponse {
                status: fast_insert_response::Status::Succeeded.into(),
                error_message: "".to_owned(),
            })),
            Err(e) => match e {
                BatchError::Dml(e) => Ok(Response::new(FastInsertResponse {
                    status: fast_insert_response::Status::DmlFailed.into(),
                    error_message: format!("{}", e.as_report()),
                })),
                _ => {
                    error!(error = %e.as_report(), "failed to fast insert");
                    Err(e.into())
                }
            },
        }
    }

    async fn ingest_dml(
        &self,
        request: Request<tonic::Streaming<IngestDmlRequest>>,
    ) -> Result<Response<Self::IngestDmlStream>, Status> {
        let mut req_stream = request.into_inner();
        let init = match req_stream.message().await? {
            Some(req) => match req.request {
                Some(ingest_dml_request::Request::Init(init)) => init,
                Some(ingest_dml_request::Request::Payload(_)) => {
                    return Err(Status::invalid_argument(
                        "first ingest dml message must be init",
                    ));
                }
                None => return Err(Status::invalid_argument("empty ingest dml request")),
            },
            None => return Err(Status::invalid_argument("empty ingest dml stream")),
        };

        let (tx, rx) = tokio::sync::mpsc::channel(64);

        let (table_dml_handle, request_id, row_id_index) = self.init_ingest_dml(&init)?;
        let _ = tx.send(Ok(Self::ingest_dml_init_response())).await;

        let dml_manager = self.env.dml_manager_ref();
        tokio::spawn(async move {
            let result: Result<(), String> = async {
                let mut pending_acks = FuturesOrdered::new();

                loop {
                    tokio::select! {
                        req = req_stream.message() => {
                            let req = req
                                .map_err(|err| format!("ingest dml stream read failed: {}", err.as_report()))?
                                .ok_or_else(|| "ingest dml stream closed unexpectedly".to_owned())?;
                            let payload = match req.request {
                                Some(ingest_dml_request::Request::Payload(payload)) => payload,
                                Some(ingest_dml_request::Request::Init(_)) | None => {
                                    Err("unexpected non-payload request in ingest dml stream".to_owned())?
                                }
                            };

                            let dml_batch_id = payload.dml_batch_id;
                            let wait_fut = Self::do_ingest_dml_payload(
                                table_dml_handle.clone(),
                                dml_manager.clone(),
                                request_id,
                                row_id_index,
                                payload,
                            )
                            .await
                            .map_err(|err| format!("ingest dml batch {} failed: {}", dml_batch_id, err.as_report()))?;

                            pending_acks.push_back(async move { wait_fut.await.map(|()| dml_batch_id) });
                        }
                        ack = pending_acks.next(), if !pending_acks.is_empty() => {
                            let ack_dml_batch_id = ack
                                .expect("branch guarded by non-empty pending_acks")
                                .map_err(|err: DmlError| format!("ingest dml persistence failed: {}", err.as_report()))?;

                            if tx
                                .send(Ok(BatchServiceImpl::ingest_dml_ack_response(ack_dml_batch_id)))
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            .await;

            if let Err(err) = result {
                let _ = tx.send(Err(Status::internal(err))).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl BatchServiceImpl {
    async fn get_execute_stream(
        env: BatchEnvironment,
        mgr: Arc<BatchManager>,
        req: ExecuteRequest,
    ) -> Result<Response<ReceiverStream<GetDataResponseResult>>, Status> {
        let ExecuteRequest {
            task_id,
            plan,
            tracing_context,
            expr_context,
        } = req;

        let task_id = task_id.expect("no task id found");
        let plan = plan.expect("no plan found").clone();
        let tracing_context = TracingContext::from_protobuf(&tracing_context);
        let expr_context = expr_context.expect("no expression context found");

        let context = ComputeNodeContext::create(env.clone());
        trace!(
            "local execute request: plan:{:?} with task id:{:?}",
            plan, task_id
        );
        let task = BatchTaskExecution::new(
            &task_id,
            plan,
            context,
            mgr.runtime(),
            mgr.await_tree_reg().cloned(),
        )?;
        let task = Arc::new(task);
        let (tx, rx) = tokio::sync::mpsc::channel(mgr.config().developer.local_execute_buffer_size);
        if let Err(e) = task
            .clone()
            .async_execute(None, tracing_context, expr_context)
            .await
        {
            error!(
                error = %e.as_report(),
                ?task_id,
                "failed to build executors and trigger execution"
            );
            return Err(e.into());
        }

        let pb_task_output_id = TaskOutputId {
            task_id: Some(task_id.clone()),
            // Since this is local execution path, the exchange would follow single distribution,
            // therefore we would only have one data output.
            output_id: 0,
        };
        let mut output = task.get_task_output(&pb_task_output_id).inspect_err(|e| {
            error!(
                error = %e.as_report(),
                ?task_id,
                "failed to get task output in local execution mode",
            );
        })?;
        let mut writer = GrpcExchangeWriter::new(tx.clone());
        // Always spawn a task and do not block current function.
        mgr.runtime().spawn(async move {
            match output.take_data(&mut writer).await {
                Ok(_) => Ok(()),
                Err(e) => tx.send(Err(e.into())).await,
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn do_fast_insert(&self, insert_req: FastInsertRequest) -> Result<(), BatchError> {
        let wait_for_persistence = insert_req.wait_for_persistence;
        let (executor, data_chunk) =
            FastInsertExecutor::build(self.env.dml_manager_ref(), insert_req)?;
        executor
            .do_execute(data_chunk, wait_for_persistence)
            .await?;
        Ok(())
    }

    fn init_ingest_dml(
        &self,
        init: &IngestDmlInitRequest,
    ) -> Result<(TableDmlHandleRef, u32, Option<u32>), Status> {
        let table_id = init.table_id;
        let table_version_id = init.table_version_id;
        let table_dml_handle = self
            .env
            .dml_manager_ref()
            .table_dml_handle(table_id, table_version_id)
            .map_err(|err| Status::internal(format!("{}", err.as_report())))?;
        Ok((table_dml_handle, init.request_id, init.row_id_index))
    }

    async fn do_ingest_dml_payload(
        table_dml_handle: TableDmlHandleRef,
        dml_manager: DmlManagerRef,
        request_id: u32,
        row_id_index: Option<u32>,
        payload: IngestDmlPayloadRequest,
    ) -> Result<impl Future<Output = risingwave_dml::error::Result<()>> + Send + 'static, BatchError>
    {
        let pb_chunk = payload.chunk.ok_or_else(|| {
            BatchError::Internal(anyhow::anyhow!("no chunk in IngestDmlPayloadRequest"))
        })?;
        let mut chunk = StreamChunk::from_protobuf(&pb_chunk)
            .context("failed to decode chunk")
            .map_err(BatchError::Internal)?;
        chunk = inject_optional_row_id_column(chunk, row_id_index.map(|index| index as usize));
        let txn_id = dml_manager.gen_txn_id();
        let mut write_handle = table_dml_handle
            .write_handle(request_id, txn_id)
            .map_err(BatchError::Dml)?;

        write_handle.begin().map_err(BatchError::Dml)?;
        write_handle
            .write_chunk(chunk)
            .await
            .map_err(BatchError::Dml)?;
        let persistence_future = write_handle
            .end_wait_persistence()
            .map_err(BatchError::Dml)?;
        Ok(persistence_future)
    }

    fn ingest_dml_init_response() -> IngestDmlResponse {
        IngestDmlResponse {
            response: Some(ingest_dml_response::Response::Init(
                IngestDmlInitResponse {},
            )),
        }
    }

    fn ingest_dml_ack_response(dml_batch_id: u64) -> IngestDmlResponse {
        IngestDmlResponse {
            response: Some(ingest_dml_response::Response::Ack(IngestDmlAckResponse {
                dml_batch_id,
            })),
        }
    }
}
