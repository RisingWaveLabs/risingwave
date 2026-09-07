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

use anyhow::anyhow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::id::SinkId;
use risingwave_pb::stream_plan::IcebergWithPkIndexPositionDeleteMergerNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{
    Executor, PositionDeleteHandlerImpl, PositionDeleteMergerExecutor, StreamExecutorError,
};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct IcebergWithPkIndexPositionDeleteMergerExecutorBuilder;

impl_stream_node_body!(IcebergWithPkIndexPositionDeleteMerger(IcebergWithPkIndexPositionDeleteMergerNode) => IcebergWithPkIndexPositionDeleteMergerExecutorBuilder);

impl ExecutorBuilder for IcebergWithPkIndexPositionDeleteMergerExecutorBuilder {
    type Node = IcebergWithPkIndexPositionDeleteMergerNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_id: SinkId = sink_desc.get_id();

        let properties_with_secret = LocalSecretManager::global().fill_secrets(
            sink_desc.get_properties().clone(),
            sink_desc.get_secret_refs().clone(),
        )?;
        let config = IcebergConfig::from_btreemap(properties_with_secret)
            .map_err(|err| StreamExecutorError::sink_error(err, sink_id))?;
        let meta_client = params.env.meta_client().ok_or_else(|| {
            anyhow!("meta client is required for iceberg pk-index position-delete merger")
        })?;

        let handler = PositionDeleteHandlerImpl::new(
            config,
            params.actor_context.id,
            params.vnode_bitmap.clone(),
            sink_id,
            meta_client,
        );

        let exec = PositionDeleteMergerExecutor::new(
            params.actor_context.id,
            sink_id,
            params.local_barrier_manager.clone(),
            input,
            handler,
        );
        Ok((params.info, exec).into())
    }
}
