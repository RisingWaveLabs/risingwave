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

use crate::executor::prelude::*;
use crate::task::CreateMviewProgressReporter;

/// [`ChainExecutor`] is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, [`ChainExecutor`] is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
pub struct ChainExecutor {
    upstream: Executor,

    progress: CreateMviewProgressReporter,
}

impl ChainExecutor {
    pub fn new(upstream: Executor, progress: CreateMviewProgressReporter) -> Self {
        Self { upstream, progress }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = self.upstream.execute();

        // 1. Poll the upstream to get the first barrier.
        let barrier = expect_first_barrier(&mut upstream).await?;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        // 2. Continuously consume the upstream. Report completion on the first barrier after the
        // initial barrier, before propagating it, so the progress is collected with that barrier.
        let mut progress_finished = false;
        #[for_await]
        for msg in upstream {
            let msg = msg?;
            if !progress_finished && let Message::Barrier(barrier) = &msg {
                self.progress.finish(barrier.epoch, 0);
                progress_finished = true;
            }
            yield msg;
        }
    }
}

impl Execute for ChainExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod test {

    use futures::StreamExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::Dispatcher;

    use super::ChainExecutor;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{AddMutation, Barrier, Execute, Message, Mutation, StreamKey};
    use crate::task::CreateMviewProgressReporter;
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;

    #[tokio::test]
    async fn test_basic() {
        let test_env = LocalBarrierTestEnv::for_test().await;
        let barrier_manager = test_env.local_barrier_manager.clone();
        let progress = CreateMviewProgressReporter::for_test(barrier_manager);
        let actor_id = progress.actor_id();

        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let upstream = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1)).with_mutation(
                Mutation::Add(AddMutation {
                    adds: maplit::hashmap! {
                        0.into() => vec![Dispatcher {
                            downstream_actor_id: vec![actor_id],
                            ..Default::default()
                        }],
                    },
                    added_actors: maplit::hashset! { actor_id },
                    ..Default::default()
                }),
            )),
            Message::Chunk(StreamChunk::from_pretty("I\n + 3")),
            Message::Chunk(StreamChunk::from_pretty("I\n + 4")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let chain = ChainExecutor::new(upstream, progress);

        let mut chain = chain.boxed().execute();
        chain.next().await;

        assert_eq!(
            chain.next().await.transpose().unwrap(),
            Some(Message::Chunk(StreamChunk::from_pretty("I\n + 3")))
        );
        assert_eq!(
            chain.next().await.transpose().unwrap(),
            Some(Message::Chunk(StreamChunk::from_pretty("I\n + 4")))
        );
        assert!(matches!(
            chain.next().await.transpose().unwrap(),
            Some(Message::Barrier(_))
        ));
    }
}
