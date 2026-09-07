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

use std::fmt::{Debug, Formatter};

use await_tree::InstrumentAwait;
use futures::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_expr::expr_context::capture_expr_context;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::{self, Plan};
use risingwave_pb::task_service::{ExecuteRequest, GetDataResponse};
use risingwave_rpc_client::ComputeClient;
use risingwave_rpc_client::error::RpcError;
use tonic::Streaming;

use crate::error::Result;
use crate::exchange_source::ExchangeSource;
use crate::task::TaskId;

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    stream: Streaming<GetDataResponse>,

    task_output_id: TaskOutputId,

    take_data_span: await_tree::Span,
}

impl GrpcExchangeSource {
    pub async fn create(
        client: ComputeClient,
        task_output_id: TaskOutputId,
        local_execute_plan: Option<LocalExecutePlan>,
    ) -> Result<Self> {
        let task_id = task_output_id.get_task_id()?.clone();
        let take_data_span = await_tree::span!(
            "grpc_exchange_take_data (query {} stage {} task {} output {})",
            task_id.query_id,
            task_id.stage_id,
            task_id.task_id,
            task_output_id.output_id
        );
        let stream = match local_execute_plan {
            // When in the local execution mode, `GrpcExchangeSource` would send out
            // `ExecuteRequest` and get the data chunks back in a single RPC.
            Some(local_execute_plan) => {
                let plan = try_match_expand!(local_execute_plan, Plan)?;
                let execute_request = ExecuteRequest {
                    task_id: Some(task_id),
                    plan: plan.plan,
                    tracing_context: plan.tracing_context,
                    expr_context: Some(capture_expr_context()?),
                };
                client.execute(execute_request).await?
            }
            None => client.get_data(task_output_id.clone()).await?,
        };
        let source = Self {
            stream,
            task_output_id,
            take_data_span,
        };
        Ok(source)
    }
}

impl Debug for GrpcExchangeSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcExchangeSource")
            .field("task_output_id", &self.task_output_id)
            .finish()
    }
}

impl ExchangeSource for GrpcExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        let res = match self
            .stream
            .next()
            .instrument_await(self.take_data_span.clone())
            .await
        {
            None => {
                return Ok(None);
            }
            Some(r) => r,
        };
        let task_data = res.map_err(RpcError::from_batch_status)?;
        let data = DataChunk::from_protobuf(task_data.get_record_batch()?)?.compact_vis();
        trace!(
            "Receiver taskOutput = {:?}, data = {:?}",
            self.task_output_id, data
        );

        Ok(Some(data))
    }

    fn get_task_id(&self) -> TaskId {
        TaskId::from(self.task_output_id.get_task_id().unwrap())
    }
}
