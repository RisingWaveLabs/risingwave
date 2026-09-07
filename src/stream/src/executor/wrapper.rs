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

use futures::future::Either;

use crate::executor::prelude::*;

mod epoch_check;
mod epoch_provide;
mod schema_check;
mod stream_node_metrics;
mod trace;
mod update_check;

/// [`WrapperExecutor`] will do some sanity checks and logging for the wrapped executor.
pub struct WrapperExecutor {
    input: Executor,
    actor_ctx: ActorContextRef,
}

impl WrapperExecutor {
    pub fn new(input: Executor, actor_ctx: ActorContextRef) -> Self {
        Self { input, actor_ctx }
    }

    #[expect(clippy::let_and_return)]
    fn wrap_debug(
        info: Arc<ExecutorInfo>,
        stream: impl MessageStream + 'static,
    ) -> impl MessageStream + 'static {
        // Update check
        let stream = update_check::update_check(info, stream);

        stream
    }

    fn wrap(
        info: Arc<ExecutorInfo>,
        actor_ctx: ActorContextRef,
        stream: impl MessageStream + 'static,
    ) -> BoxedMessageStream {
        // -- Shared wrappers --

        // Schema check
        let stream = schema_check::schema_check(info.clone(), stream);
        // Epoch check
        let stream = epoch_check::epoch_check(info.clone(), stream);

        // Epoch provide
        let stream = epoch_provide::epoch_provide(stream);

        // Trace
        let stream = trace::trace(info.clone(), actor_ctx.clone(), stream);

        // operator-level metrics
        let stream = stream_node_metrics::stream_node_metrics(info.clone(), stream, actor_ctx);

        // -- Debug-only wrappers --
        let stream = if cfg!(debug_assertions) {
            Either::Left(Self::wrap_debug(info.clone(), stream))
        } else {
            Either::Right(stream)
        };

        // Await tree
        // This should be the last wrapper, so that code in other wrappers are also instrumented with
        // the span of the current executor.
        trace::instrument_await_tree(info, stream).boxed()
    }
}

impl Execute for WrapperExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let info = Arc::new(self.input.info().clone());
        Self::wrap(info, self.actor_ctx, self.input.execute()).boxed()
    }
}
