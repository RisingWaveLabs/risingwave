// Copyright 2025 RisingWave Labs
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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use risingwave_batch::task::BatchManager;
use risingwave_common_service::GrpcCall;
use risingwave_pb::task_service::batch_exchange_service_server::BatchExchangeService;
use risingwave_pb::task_service::{GetDataRequest, GetDataResponse};
use thiserror_ext::AsReport;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

type BatchData = std::result::Result<GetDataResponse, Status>;

pub struct BatchDataStream {
    inner: ReceiverStream<BatchData>,
    await_tree_root: Option<await_tree::TreeRoot>,
}

impl BatchDataStream {
    fn new(
        receiver: tokio::sync::mpsc::Receiver<BatchData>,
        await_tree_root: Option<await_tree::TreeRoot>,
    ) -> Self {
        Self {
            inner: ReceiverStream::new(receiver),
            await_tree_root,
        }
    }
}

impl tokio_stream::Stream for BatchDataStream {
    type Item = BatchData;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_next(cx);
        if matches!(&result, Poll::Ready(None)) {
            this.await_tree_root.take();
        }
        result
    }
}

#[derive(Clone)]
pub struct BatchExchangeServiceImpl {
    batch_mgr: Arc<BatchManager>,
}

impl BatchExchangeServiceImpl {
    pub fn new(batch_mgr: Arc<BatchManager>) -> Self {
        Self { batch_mgr }
    }
}

#[async_trait::async_trait]
impl BatchExchangeService for BatchExchangeServiceImpl {
    type GetDataStream = BatchDataStream;

    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let pb_task_output_id = request
            .into_inner()
            .task_output_id
            .expect("Failed to get task output id.");
        let (tx, rx) =
            tokio::sync::mpsc::channel(self.batch_mgr.config().developer.receiver_channel_size);
        if let Err(e) = self.batch_mgr.get_data(tx, peer_addr, &pb_task_output_id) {
            error!(
                %peer_addr,
                error = %e.as_report(),
                "Failed to serve exchange RPC"
            );
            return Err(e.into());
        }

        let await_tree_root = self.batch_mgr.await_tree_reg().map(|registry| {
            let key = GrpcCall::new(format!(
                "{peer_addr} - /task_service.BatchExchangeService/GetData - \
                 {pb_task_output_id:?}"
            ));
            registry.register(key, "/task_service.BatchExchangeService/GetData")
        });
        Ok(Response::new(BatchDataStream::new(rx, await_tree_root)))
    }
}
