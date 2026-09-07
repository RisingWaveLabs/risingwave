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

use itertools::Itertools;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragment;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{SourceBackfillNode, StreamNode, StreamScanNode, agg_call_state};

#[macro_export]
macro_rules! dispatch_stream_node_body {
    ($body:expr, $node_body:ident, $node:ident => $call:expr) => {
        ::risingwave_pb::__dispatch_stream_node_body!($body, $node_body, $node => $call)
    };
}

/// A utility for visiting and mutating the [`NodeBody`] of the [`StreamNode`]s recursively.
pub fn visit_stream_node_mut(stream_node: &mut StreamNode, mut f: impl FnMut(&mut NodeBody)) {
    visit_stream_node_cont_mut(stream_node, |stream_node| {
        f(stream_node.node_body.as_mut().unwrap());
        true
    })
}

/// A utility for to accessing the [`StreamNode`] mutably. The returned bool is used to determine whether the access needs to continue.
pub fn visit_stream_node_cont_mut<F>(stream_node: &mut StreamNode, mut f: F)
where
    F: FnMut(&mut StreamNode) -> bool,
{
    fn visit_inner<F>(stream_node: &mut StreamNode, f: &mut F)
    where
        F: FnMut(&mut StreamNode) -> bool,
    {
        if !f(stream_node) {
            return;
        }
        for input in &mut stream_node.input {
            visit_inner(input, f);
        }
    }

    visit_inner(stream_node, &mut f)
}

/// A utility for visiting the [`NodeBody`] of the [`StreamNode`]s recursively.
pub fn visit_stream_node_body(stream_node: &StreamNode, mut f: impl FnMut(&NodeBody)) {
    visit_stream_node_cont(stream_node, |stream_node| {
        f(stream_node.node_body.as_ref().unwrap());
        true
    })
}

pub fn visit_stream_node(stream_node: &StreamNode, mut f: impl FnMut(&StreamNode)) {
    visit_stream_node_cont(stream_node, |stream_node| {
        f(stream_node);
        true
    })
}

/// A utility for to accessing the [`StreamNode`] immutably. The returned bool is used to determine whether the access needs to continue.
pub fn visit_stream_node_cont<'a, F>(stream_node: &'a StreamNode, mut f: F)
where
    F: FnMut(&'a StreamNode) -> bool,
{
    fn visit_inner<'a, F>(stream_node: &'a StreamNode, f: &mut F)
    where
        F: FnMut(&'a StreamNode) -> bool,
    {
        if !f(stream_node) {
            return;
        }
        for input in &stream_node.input {
            visit_inner(input, f);
        }
    }

    visit_inner(stream_node, &mut f)
}

/// A utility for visiting and mutating the [`NodeBody`] of the [`StreamNode`]s in a
/// [`StreamFragment`] recursively.
pub fn visit_fragment_mut(fragment: &mut StreamFragment, f: impl FnMut(&mut NodeBody)) {
    visit_stream_node_mut(fragment.node.as_mut().unwrap(), f)
}

/// A utility for visiting the [`NodeBody`] of the [`StreamNode`]s in a
/// [`StreamFragment`] recursively.
pub fn visit_fragment(fragment: &StreamFragment, f: impl FnMut(&NodeBody)) {
    visit_stream_node_body(fragment.node.as_ref().unwrap(), f)
}

/// Visit the tables of a [`StreamNode`].
pub fn visit_stream_node_tables_inner<F>(
    stream_node: &mut StreamNode,
    internal_tables_only: bool,
    visit_child_recursively: bool,
    mut f: F,
) where
    F: FnMut(&mut Table, &str),
{
    macro_rules! always {
        ($table:expr, $name:expr) => {{
            let table = $table
                .as_mut()
                .unwrap_or_else(|| panic!("internal table {} should always exist", $name));
            f(table, $name);
        }};
    }

    macro_rules! optional {
        ($table:expr, $name:expr) => {
            if let Some(table) = &mut $table {
                f(table, $name);
            }
        };
    }

    macro_rules! repeated {
        ($tables:expr, $name:expr) => {
            for table in &mut $tables {
                f(table, $name);
            }
        };
    }

    let mut visit_body = |body: &mut NodeBody| {
        match body {
            // Join
            NodeBody::HashJoin(node) => {
                // TODO: make the degree table optional
                always!(node.left_table, "HashJoinLeft");
                always!(node.left_degree_table, "HashJoinDegreeLeft");
                always!(node.right_table, "HashJoinRight");
                always!(node.right_degree_table, "HashJoinDegreeRight");
            }
            NodeBody::TemporalJoin(node) => {
                optional!(node.memo_table, "TemporalJoinMemo");
            }
            NodeBody::DynamicFilter(node) => {
                always!(node.left_table, "DynamicFilterLeft");
                always!(node.right_table, "DynamicFilterRight");
            }

            // Aggregation
            NodeBody::HashAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.intermediate_state_table, "HashAggState");
                for (call_idx, state) in node.agg_call_states.iter_mut().enumerate() {
                    match state.inner.as_mut().unwrap() {
                        agg_call_state::Inner::ValueState(_) => {}
                        agg_call_state::Inner::MaterializedInputState(s) => {
                            always!(s.table, &format!("HashAggCall{}", call_idx));
                        }
                    }
                }
                for (distinct_col, dedup_table) in node
                    .distinct_dedup_tables
                    .iter_mut()
                    .sorted_by_key(|(i, _)| *i)
                {
                    f(dedup_table, &format!("HashAggDedupForCol{}", distinct_col));
                }
            }
            NodeBody::SimpleAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.intermediate_state_table, "SimpleAggState");
                for (call_idx, state) in node.agg_call_states.iter_mut().enumerate() {
                    match state.inner.as_mut().unwrap() {
                        agg_call_state::Inner::ValueState(_) => {}
                        agg_call_state::Inner::MaterializedInputState(s) => {
                            always!(s.table, &format!("SimpleAggCall{}", call_idx));
                        }
                    }
                }
                for (distinct_col, dedup_table) in node
                    .distinct_dedup_tables
                    .iter_mut()
                    .sorted_by_key(|(i, _)| *i)
                {
                    f(
                        dedup_table,
                        &format!("SimpleAggDedupForCol{}", distinct_col),
                    );
                }
            }

            // Top-N
            NodeBody::AppendOnlyTopN(node) => {
                always!(node.table, "AppendOnlyTopN");
            }
            NodeBody::TopN(node) => {
                always!(node.table, "TopN");
            }
            NodeBody::AppendOnlyGroupTopN(node) => {
                always!(node.table, "AppendOnlyGroupTopN");
            }
            NodeBody::GroupTopN(node) => {
                always!(node.table, "GroupTopN");
            }

            // Source
            NodeBody::Source(node) => {
                if let Some(source) = &mut node.source_inner {
                    always!(source.state_table, "Source");
                }
            }
            NodeBody::StreamFsFetch(node) => {
                if let Some(source) = &mut node.node_inner {
                    always!(source.state_table, "FsFetch");
                }
            }
            NodeBody::SourceBackfill(node) => {
                always!(node.state_table, "SourceBackfill")
            }

            // Sink
            NodeBody::Sink(node) => {
                // A sink with a kv log store should have a state table.
                optional!(node.table, "Sink")
            }

            // Now
            NodeBody::Now(node) => {
                always!(node.state_table, "Now");
            }

            // Watermark filter
            NodeBody::WatermarkFilter(node) => {
                assert!(!node.tables.is_empty());
                repeated!(node.tables, "WatermarkFilter");
            }

            // Shared arrangement
            NodeBody::Arrange(node) => {
                always!(node.table, "Arrange");
            }

            // Dedup
            NodeBody::AppendOnlyDedup(node) => {
                always!(node.state_table, "AppendOnlyDedup");
            }

            // EOWC over window
            NodeBody::EowcOverWindow(node) => {
                always!(node.state_table, "EowcOverWindow");
                optional!(
                    node.intermediate_state_table,
                    "EowcOverWindowIntermediateState"
                );
            }

            NodeBody::OverWindow(node) => {
                always!(node.state_table, "OverWindow");
            }

            // Sort
            NodeBody::Sort(node) => {
                always!(node.state_table, "Sort");
            }

            // Stream Scan
            NodeBody::StreamScan(node) => {
                optional!(node.state_table, "StreamScan")
            }

            // Stream Cdc Scan
            NodeBody::StreamCdcScan(node) => {
                always!(node.state_table, "StreamCdcScan")
            }

            NodeBody::EowcGapFill(node) => {
                always!(node.prev_row_table, "EowcGapFillPrevRowTable");
            }

            NodeBody::GapFill(node) => {
                always!(node.state_table, "GapFillStateTable");
            }

            NodeBody::MatchRecognize(node) => {
                always!(node.state_table, "MatchRecognizeStateTable");
            }

            // Note: add internal tables for new nodes here.
            NodeBody::Materialize(node) => {
                if !internal_tables_only {
                    // Note: Plan directly generated by frontend may not have the table filled,
                    // as it is filled by the meta service during building fragments.
                    // However, if caller requests to visit this table via `!internal_tables_only`,
                    // we still assert on its existence to avoid undefined behavior.
                    always!(node.table, "Materialize");
                }
                // Also visit the staging table if it exists (for refreshable tables)
                optional!(node.staging_table, "MaterializeStaging");
                optional!(node.refresh_progress_table, "MaterializeRefreshProgress");
            }

            // Global Approx Percentile
            NodeBody::GlobalApproxPercentile(node) => {
                always!(node.bucket_state_table, "GlobalApproxPercentileBucketState");
                always!(node.count_state_table, "GlobalApproxPercentileCountState");
            }

            // AsOf join
            NodeBody::AsOfJoin(node) => {
                always!(node.left_table, "AsOfJoinLeft");
                always!(node.right_table, "AsOfJoinRight");
            }

            // Synced Log Store
            NodeBody::SyncLogStore(node) => {
                always!(node.log_store_table, "StreamSyncLogStore");
            }

            // MaterializedExprs
            NodeBody::MaterializedExprs(node) => {
                always!(node.state_table, "MaterializedExprs");
            }

            // Vector Index Write
            NodeBody::VectorIndexWrite(node) if !internal_tables_only => {
                always!(node.table, "StreamVectorIndexWrite");
            }

            NodeBody::LocalityProvider(node) => {
                always!(node.state_table, "LocalityProviderState");
                always!(node.progress_table, "LocalityProviderProgress");
            }

            NodeBody::IcebergWithPkIndexWriter(node) => {
                always!(node.pk_index_table, "IcebergWithPkIndexWriter");
            }

            _ => {}
        }
    };
    if visit_child_recursively {
        visit_stream_node_mut(stream_node, visit_body)
    } else {
        visit_body(stream_node.node_body.as_mut().unwrap())
    }
}

pub fn visit_stream_node_stream_scan(stream_node: &StreamNode, mut f: impl FnMut(&StreamScanNode)) {
    visit_stream_node_body(stream_node, |body| {
        if let NodeBody::StreamScan(node) = body {
            f(node)
        }
    })
}

pub fn visit_stream_node_source_backfill(
    stream_node: &StreamNode,
    mut f: impl FnMut(&SourceBackfillNode),
) {
    visit_stream_node_body(stream_node, |body| {
        if let NodeBody::SourceBackfill(node) = body {
            f(node)
        }
    })
}

pub fn visit_stream_node_internal_tables<F>(stream_node: &mut StreamNode, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_tables_inner(stream_node, true, true, f)
}

pub fn visit_stream_node_tables<F>(stream_node: &mut StreamNode, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_tables_inner(stream_node, false, true, f)
}

/// Visit the internal tables of a [`StreamFragment`].
pub fn visit_internal_tables<F>(fragment: &mut StreamFragment, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_internal_tables(fragment.node.as_mut().unwrap(), f)
}

/// Visit the tables of a [`StreamFragment`].
///
/// Compared to [`visit_internal_tables`], this function also visits the table of `Materialize` node.
pub fn visit_tables<F>(fragment: &mut StreamFragment, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_tables(fragment.node.as_mut().unwrap(), f)
}
