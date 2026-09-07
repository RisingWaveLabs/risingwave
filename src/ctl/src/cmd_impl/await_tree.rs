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

use risingwave_common::util::StackTraceResponseExt;
use risingwave_pb::common::WorkerType;
use risingwave_pb::id::WorkerId;
use risingwave_pb::monitor_service::StackTraceRequest;
use risingwave_pb::monitor_service::stack_trace_request::ActorTracesFormat;
use risingwave_rpc_client::MonitorClientPool;
use rw_diagnose_tools::await_tree::AnalyzeSummary;
use thiserror_ext::AsReport as _;

use crate::CtlContext;

pub async fn dump(context: &CtlContext, actor_traces_format: Option<String>) -> anyhow::Result<()> {
    let actor_traces_format = match actor_traces_format.as_deref() {
        Some("text") => ActorTracesFormat::Text,
        Some("json") | None => ActorTracesFormat::Json,
        _ => return Err(anyhow::anyhow!("Invalid actor traces format")),
    };

    // Query the meta node for the await tree of all nodes in the cluster.
    let meta_client = context.meta_client().await?;
    let all = meta_client
        .get_cluster_stack_trace(actor_traces_format)
        .await?;

    if all.actor_traces.is_empty()
        && all.rpc_traces.is_empty()
        && all.batch_traces.is_empty()
        && all.compaction_task_traces.is_empty()
        && all.inflight_barrier_traces.is_empty()
    {
        eprintln!("No actors are running, or `--async-stack-trace` not set?");
    }
    println!("{}", all.output());

    Ok(())
}

pub async fn bottleneck_detect(context: &CtlContext, path: Option<String>) -> anyhow::Result<()> {
    let summary = if let Some(path) = path {
        rw_diagnose_tools::await_tree::bottleneck_detect_from_file(&path)?
    } else {
        bottleneck_detect_real_time(context).await?
    };
    println!("{}", summary);
    Ok(())
}

async fn bottleneck_detect_real_time(context: &CtlContext) -> anyhow::Result<AnalyzeSummary> {
    let meta_client = context.meta_client().await?;

    let compute_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::ComputeNode))
        .await?;
    let clients = MonitorClientPool::adhoc();

    // request for json actor traces
    let req = StackTraceRequest::default();

    let mut summary = AnalyzeSummary::new();
    let mut errors: Vec<(WorkerId, String)> = Vec::new();
    for cn in compute_nodes {
        let worker_id = cn.id;

        let client = match clients.get(&cn).await {
            Ok(client) => client,
            Err(e) => {
                errors.push((worker_id, format!("failed to connect: {}", e.as_report())));
                continue;
            }
        };

        let response = match client.await_tree(req).await {
            Ok(resp) => resp,
            Err(e) => {
                errors.push((
                    worker_id,
                    format!("failed to collect stack trace: {}", e.as_report()),
                ));
                continue;
            }
        };

        let partial_summary = AnalyzeSummary::from_traces(&response.actor_traces)?;
        summary.merge_other(&partial_summary);
    }
    if !errors.is_empty() {
        eprintln!("Some compute nodes failed to dump await tree:");
        for (worker_id, err) in errors {
            eprintln!("  - worker {worker_id}: {err}");
        }
    }
    Ok(summary)
}
