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

use std::collections::HashSet;
use std::path::PathBuf;

use chrono::prelude::Local;
use clap::ValueEnum;
use futures::future::try_join_all;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::ProfilingResponse;
use risingwave_rpc_client::MonitorClientPool;
use thiserror_ext::AsReport;
use tokio::fs::{File, create_dir_all};
use tokio::io::AsyncWriteExt;

use crate::CtlContext;

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum ProfileWorkerType {
    Frontend,
    ComputeNode,
    Compactor,
    Meta,
}

pub async fn cpu_profile(
    context: &CtlContext,
    sleep_s: u64,
    worker_types: Vec<ProfileWorkerType>,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let workers = meta_client.list_worker_nodes(None).await?;
    let target_types = selected_worker_types(&worker_types);
    let target_nodes = workers
        .into_iter()
        .filter(|w| target_types.contains(&w.r#type()));

    let clients = MonitorClientPool::adhoc();

    let profile_root_path = std::env::var("PREFIX_PROFILING").unwrap_or_else(|_| {
        tracing::info!("PREFIX_PROFILING is not set, using current directory");
        "./".to_owned()
    });
    let profile_root_path = PathBuf::from(&profile_root_path);
    let dir_name = Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
    let dir_path = profile_root_path.join(dir_name);
    create_dir_all(&dir_path).await?;

    let mut profile_futs = vec![];

    // FIXME: the node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all nodes in the future.
    for cn in target_nodes {
        let client = clients.get(&cn).await?;

        let dir_path_ref = &dir_path;

        let fut = async move {
            let response = client.profile(sleep_s).await;
            let host_addr = cn.get_host().expect("Should have host address");
            let node_name = format!(
                "{}-{}-{}",
                worker_type_label(cn.r#type()),
                host_addr.get_host().replace('.', "-"),
                host_addr.get_port()
            );
            let svg_file_name = format!("{}.svg", node_name);
            match response {
                Ok(ProfilingResponse { result }) => {
                    let mut file = File::create(dir_path_ref.join(svg_file_name)).await?;
                    file.write_all(&result).await?;
                    file.flush().await?;
                }
                Err(err) => {
                    tracing::error!(
                        error = %err.as_report(),
                        node_name,
                        "Failed to get profiling result",
                    );
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        profile_futs.push(fut);
    }

    try_join_all(profile_futs).await?;

    println!("Profiling results are saved at {}", dir_path.display());

    Ok(())
}

pub async fn heap_profile(
    context: &CtlContext,
    dir: Option<String>,
    worker_types: Vec<ProfileWorkerType>,
) -> anyhow::Result<()> {
    let dir = dir.unwrap_or_default();
    let meta_client = context.meta_client().await?;

    let workers = meta_client.list_worker_nodes(None).await?;
    let target_types = selected_worker_types(&worker_types);
    let target_nodes = workers
        .into_iter()
        .filter(|w| target_types.contains(&w.r#type()));

    let clients = MonitorClientPool::adhoc();

    let mut profile_futs = vec![];

    // FIXME: the node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all nodes in the future.
    for cn in target_nodes {
        let client = clients.get(&cn).await?;
        let dir = &dir;

        let fut = async move {
            let response = client.heap_profile(dir.clone()).await;
            let host_addr = cn.get_host().expect("Should have host address");

            let node_name = format!(
                "{}-{}-{}",
                worker_type_label(cn.r#type()),
                host_addr.get_host().replace('.', "-"),
                host_addr.get_port()
            );

            if let Err(err) = response {
                tracing::error!(
                    error = %err.as_report(),
                    node_name,
                    "Failed to dump profile",
                );
            }
            Ok::<_, anyhow::Error>(())
        };
        profile_futs.push(fut);
    }

    try_join_all(profile_futs).await?;

    println!(
        "Profiling results are saved at {} on each target node",
        PathBuf::from(dir).display()
    );

    Ok(())
}

fn selected_worker_types(values: &[ProfileWorkerType]) -> HashSet<WorkerType> {
    // Use default set when no filter is provided.
    if values.is_empty() {
        return HashSet::from([
            WorkerType::Frontend,
            WorkerType::ComputeNode,
            WorkerType::Compactor,
            WorkerType::Meta,
        ]);
    }
    values.iter().map(|value| to_worker_type(*value)).collect()
}

fn to_worker_type(value: ProfileWorkerType) -> WorkerType {
    match value {
        ProfileWorkerType::Frontend => WorkerType::Frontend,
        ProfileWorkerType::ComputeNode => WorkerType::ComputeNode,
        ProfileWorkerType::Compactor => WorkerType::Compactor,
        ProfileWorkerType::Meta => WorkerType::Meta,
    }
}

fn worker_type_label(worker_type: WorkerType) -> &'static str {
    match worker_type {
        WorkerType::Frontend => "frontend",
        WorkerType::ComputeNode => "compute-node",
        WorkerType::Compactor => "compactor",
        WorkerType::Meta => "meta",
        _ => "unknown",
    }
}
