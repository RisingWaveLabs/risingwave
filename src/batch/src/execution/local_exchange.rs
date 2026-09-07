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

use risingwave_common::array::DataChunk;

use crate::error::Result;
use crate::exchange_source::ExchangeSource;
use crate::task::{BatchTaskContext, TaskId, TaskOutput, TaskOutputId};

/// Exchange data from a local task execution.
pub struct LocalExchangeSource {
    task_output: TaskOutput,

    /// Id of task which contains the `ExchangeExecutor` of this source.
    task_id: TaskId,

    take_data_span: await_tree::Span,
}

impl LocalExchangeSource {
    pub fn create(
        output_id: TaskOutputId,
        context: &dyn BatchTaskContext,
        task_id: TaskId,
    ) -> Result<Self> {
        let task_output = context.get_task_output(output_id)?;
        let take_data_span = await_tree::span!(
            "local_exchange_take_data (task_output {:?})",
            task_output.id()
        );
        Ok(Self {
            task_output,
            task_id,
            take_data_span,
        })
    }
}

impl Debug for LocalExchangeSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExchangeSource")
            .field("task_output_id", self.task_output.id())
            .finish()
    }
}

impl ExchangeSource for LocalExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        use await_tree::InstrumentAwait;
        let ret = self
            .task_output
            .direct_take_data()
            .instrument_await(self.take_data_span.clone())
            .await?;
        if let Some(data) = ret {
            let data = data.compact_vis();
            trace!(
                "Receiver task: {:?}, source task output: {:?}, data: {:?}",
                self.task_id,
                self.task_output.id(),
                data
            );
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    fn get_task_id(&self) -> TaskId {
        self.task_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use risingwave_common::config::RpcClientConfig;
    use risingwave_pb::batch_plan::{TaskId, TaskOutputId};
    use risingwave_pb::data::DataChunk;
    use risingwave_pb::task_service::batch_exchange_service_server::{
        BatchExchangeService, BatchExchangeServiceServer,
    };
    use risingwave_pb::task_service::{GetDataRequest, GetDataResponse};
    use risingwave_rpc_client::ComputeClient;
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::exchange_source::ExchangeSource;
    use crate::execution::grpc_exchange::GrpcExchangeSource;

    struct FakeExchangeService {
        rpc_called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl BatchExchangeService for FakeExchangeService {
        type GetDataStream = ReceiverStream<Result<GetDataResponse, Status>>;

        async fn get_data(
            &self,
            _: Request<GetDataRequest>,
        ) -> Result<Response<Self::GetDataStream>, Status> {
            let (tx, rx) = tokio::sync::mpsc::channel(10);
            self.rpc_called.store(true, Ordering::SeqCst);
            for _ in 0..3 {
                tx.send(Ok(GetDataResponse {
                    record_batch: Some(DataChunk::default()),
                }))
                .await
                .unwrap();
            }
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test]
    async fn test_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        // Start a server.
        let (shutdown_send, shutdown_recv) = tokio::sync::oneshot::channel();
        let exchange_svc = BatchExchangeServiceServer::new(FakeExchangeService {
            rpc_called: rpc_called.clone(),
        });
        let cp_server_run = server_run.clone();
        let join_handle = tokio::spawn(async move {
            cp_server_run.store(true, Ordering::SeqCst);
            tonic::transport::Server::builder()
                .add_service(exchange_svc)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.await.unwrap();
                })
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(1)).await;
        assert!(server_run.load(Ordering::SeqCst));

        let client = ComputeClient::new(addr.into(), &RpcClientConfig::default())
            .await
            .unwrap();
        let task_output_id = TaskOutputId {
            task_id: Some(TaskId::default()),
            ..Default::default()
        };
        let mut src = GrpcExchangeSource::create(client, task_output_id, None)
            .await
            .unwrap();
        for _ in 0..3 {
            assert!(src.take_data().await.unwrap().is_some());
        }
        assert!(src.take_data().await.unwrap().is_none());
        assert!(rpc_called.load(Ordering::SeqCst));

        // Gracefully terminate the server.
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
