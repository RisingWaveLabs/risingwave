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

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::{MatchRecognizeInputMode, MatchRecognizeNode};
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::Executor;
use crate::executor::match_recognize::executor::{
    CompiledDefine, CompiledMeasure, DeadlineErrorReport, MatchRecognizeExecutor,
    MatchRecognizeExecutorArgs,
};
use crate::executor::match_recognize::nfa::{Nfa, SkipMode};
use crate::executor::match_recognize::proto::pattern_from_protobuf;
use crate::task::ExecutorParams;

pub struct MatchRecognizeExecutorBuilder;

impl_stream_node_body!(MatchRecognize(MatchRecognizeNode) => MatchRecognizeExecutorBuilder);

impl ExecutorBuilder for MatchRecognizeExecutorBuilder {
    type Node = MatchRecognizeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &MatchRecognizeNode,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        // This executor's entire correctness rests on the ordered-input contract the EVENT_TIME
        // plan (a WatermarkSort upstream in the same fragment) provides. A different input mode —
        // PROCESSING_TIME is reserved, unimplemented — must fail here, not silently run against
        // rows whose ordering guarantee does not hold.
        // An out-of-range wire value decodes as `Unspecified` through the accessor, which would
        // silently run an unknown future mode as event-time — the one contract this executor's
        // correctness rests on. Reject it like every other enum in this decode path; a raw 0
        // (genuinely unset) is accepted as event-time since this frontend always writes it.
        if node.input_mode != 0 && node.input_mode() == MatchRecognizeInputMode::Unspecified {
            return Err(
                anyhow::anyhow!("unknown MATCH_RECOGNIZE input mode: {}", node.input_mode).into(),
            );
        }
        match node.input_mode() {
            MatchRecognizeInputMode::Unspecified | MatchRecognizeInputMode::EventTime => {}
            other => {
                return Err(
                    anyhow::anyhow!("unsupported MATCH_RECOGNIZE input mode: {other:?}").into(),
                );
            }
        }

        let partition_key_indices = node.partition_by.iter().map(|&i| i as usize).collect();
        // ORDER BY is carried as `ColumnOrder`. v1 only supports the default ascending order (the
        // binder rejects anything else); assert it here too so a non-ascending plan fails fast
        // rather than being silently sorted ascending by the executor.
        let order_key_indices = node
            .order_by
            .iter()
            .map(|c| {
                let co = ColumnOrder::from_protobuf(c);
                if co.order_type != OrderType::ascending() {
                    return Err(anyhow::anyhow!(
                        "MATCH_RECOGNIZE only supports the default ascending ORDER BY, got {:?}",
                        co.order_type
                    )
                    .into());
                }
                Ok(co.column_index)
            })
            .collect::<StreamResult<Vec<usize>>>()?;
        // The executor reads the leading ORDER BY column unconditionally; an empty list is a
        // corrupt plan and must fail here, not index-panic there.
        if order_key_indices.is_empty() {
            return Err(anyhow::anyhow!("MATCH_RECOGNIZE plan carries an empty ORDER BY").into());
        }

        let defines = node
            .defines
            .iter()
            .map(|d| CompiledDefine::from_protobuf(d, params.eval_error_report.clone()))
            .collect::<crate::executor::StreamExecutorResult<Vec<_>>>()?;
        let measures = node
            .measures
            .iter()
            .map(|m| CompiledMeasure::from_protobuf(m, params.eval_error_report.clone()))
            .collect::<crate::executor::StreamExecutorResult<Vec<_>>>()?;

        let pattern_node = node
            .pattern_node
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE node missing pattern"))?;
        let pattern = pattern_from_protobuf(pattern_node)
            .map_err(|e| anyhow::anyhow!("invalid MATCH_RECOGNIZE pattern: {e}"))?;
        let nfa = Nfa::compile(&pattern);

        // Fail fast on anything malformed rather than silently defaulting to PAST LAST ROW, which
        // would mask a corrupt plan or a version skew.
        let skip = {
            use risingwave_pb::stream_plan::match_recognize_after_match_skip::Mode;
            let pb_skip = node
                .after_match_skip
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE node missing after_match_skip"))?;
            let target = || {
                pb_skip.target.clone().ok_or_else(|| {
                    anyhow::anyhow!("AFTER MATCH SKIP TO FIRST/LAST missing its target variable")
                })
            };
            match pb_skip.mode() {
                Mode::PastLastRow => SkipMode::PastLastRow,
                Mode::ToNextRow => SkipMode::ToNextRow,
                Mode::ToFirst => SkipMode::ToFirst(target()?),
                Mode::ToLast => SkipMode::ToLast(target()?),
                Mode::Unspecified => {
                    return Err(anyhow::anyhow!(
                        "invalid MATCH_RECOGNIZE after_match_skip mode: {}",
                        pb_skip.mode
                    )
                    .into());
                }
            }
        };

        let within = node
            .within
            .as_ref()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .transpose()?;
        // Over `DeadlineErrorReport`, not the actor's report directly: `first + bound` leaving the
        // order key's range is the window that never closes, not a compute error to count and log
        // per row. See `eval_deadline` in the executor.
        let within_deadline = node
            .within_deadline
            .as_ref()
            .map(|e| {
                build_non_strict_from_prost(
                    e,
                    DeadlineErrorReport::new(params.eval_error_report.clone()),
                )
            })
            .transpose()?;
        // The two WITHIN expressions are a correctness-coupled pair, and the coupling tightened when
        // the executor's span check started reading the cached deadline instead of evaluating the
        // predicate: `within` present with `within_deadline` absent now rejects EVERY candidate, so
        // the view would silently produce zero rows. The binder only ever emits both or neither
        // (`lower_within`), so a plan carrying one is corrupt — fail loud, as the rest of this
        // decoder does, rather than emitting nothing forever.
        if within.is_some() != within_deadline.is_some() {
            return Err(anyhow::anyhow!(
                "MATCH_RECOGNIZE carries only one of the two WITHIN expressions \
                 (predicate: {}, deadline: {}); the binder emits both or neither",
                within.is_some(),
                within_deadline.is_some(),
            )
            .into());
        }
        // The deadline is compared directly against the order key and the watermark
        // (`ScalarRefImpl::default_cmp` panics across variants — an actor crash loop that recovery
        // replays), and the span predicate is a boolean. The binder guarantees both
        // (`lower_within`); re-state it here so a skewed or corrupt plan fails at build time.
        let order_key_type = input
            .schema()
            .fields
            .get(order_key_indices[0])
            .map(|f| f.data_type())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "MATCH_RECOGNIZE ORDER BY column {} is out of range for an input of {} columns",
                    order_key_indices[0],
                    input.schema().len()
                )
            })?;
        if let Some(deadline) = &within_deadline
            && deadline.return_type() != order_key_type
        {
            return Err(anyhow::anyhow!(
                "MATCH_RECOGNIZE WITHIN deadline has type {} but the ORDER BY column has type {}; \
                 the two are compared directly",
                deadline.return_type(),
                order_key_type,
            )
            .into());
        }
        if let Some(predicate) = &within
            && predicate.return_type() != DataType::Boolean
        {
            return Err(anyhow::anyhow!(
                "MATCH_RECOGNIZE WITHIN span predicate has type {}, expected boolean",
                predicate.return_type(),
            )
            .into());
        }

        let vnode_bitmap = params.vnode_bitmap.clone().map(std::sync::Arc::new);
        let state_table_catalog = node
            .state_table
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE node missing its state table"))?;
        let state_table =
            StateTableBuilder::new(state_table_catalog, store.clone(), vnode_bitmap.clone())
                .forbid_preload_all_rows()
                .build()
                .await;
        let exec = MatchRecognizeExecutor::new(MatchRecognizeExecutorArgs {
            ctx: params.actor_context,
            input,
            schema: params.info.schema.clone(),
            chunk_size: params.config.developer.chunk_size,
            partition_key_indices,
            order_key_indices,
            measures,
            defines,
            within,
            nfa,
            skip,
            eval_error_report: params.eval_error_report,
            within_deadline,
            state_table,
        });

        Ok((params.info, exec).into())
    }
}
