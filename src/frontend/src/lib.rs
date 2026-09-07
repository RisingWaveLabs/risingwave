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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(map_try_insert)]
#![feature(coroutines)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(trait_alias)]
#![feature(box_patterns)]
#![feature(min_specialization)]
#![feature(extend_one)]
#![feature(impl_trait_in_assoc_type)]
#![feature(error_generic_member_access)]
#![feature(iterator_try_collect)]
#![feature(used_with_arg)]
#![feature(try_trait_v2)]
#![feature(never_type)]
#![recursion_limit = "256"]

#[cfg(test)]
risingwave_expr_impl::enable!();
#[cfg(test)]
risingwave_batch_executors::enable!();

#[macro_use]
mod catalog;

use std::collections::HashSet;
use std::time::Duration;

pub use catalog::TableCatalog;
mod binder;
pub use binder::{Binder, bind_data_type};
pub mod expr;
pub mod handler;
pub use handler::PgResponseStream;
mod observer;
pub mod optimizer;
pub use optimizer::{Explain, OptimizerContext, OptimizerContextRef, PlanRef};
mod planner;
use pgwire::net::TcpKeepalive;
pub use planner::Planner;
mod scheduler;
pub mod session;
mod stream_fragmenter;
use risingwave_common::config::{MetricLevel, OverrideConfig};
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use risingwave_common::util::tokio_util::sync::CancellationToken;
pub use stream_fragmenter::build_graph;
mod utils;
pub use utils::data_type::DataTypeToAst;
pub use utils::{WithOptions, WithOptionsSecResolved, explain_stream_graph};
pub(crate) mod error;
mod meta_client;
pub mod metrics_reader;
pub use metrics_reader::MetricsReaderImpl;

#[cfg(feature = "datafusion")]
pub mod datafusion;

pub mod test_utils;
mod user;
pub mod webhook;

pub mod health_service;
mod monitor;

pub mod rpc;
mod telemetry;

use std::ffi::OsString;
use std::iter;
use std::sync::Arc;

use clap::Parser;
use pgwire::pg_server::pg_serve;
use session::SessionManagerImpl;

/// Command-line arguments for frontend-node.
#[derive(Parser, Clone, Debug, OverrideConfig)]
#[command(
    version,
    about = "The stateless proxy that parses SQL queries and performs planning and optimizations of query jobs"
)]
pub struct FrontendOpts {
    // TODO: rename to listen_addr and separate out the port.
    /// The address that this service listens to.
    /// Usually the localhost + desired port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "0.0.0.0:4566")]
    pub listen_addr: String,

    /// The amount of time with no network activity after which the server will send a
    /// TCP keepalive message to the client.
    #[clap(long, env = "RW_TCP_KEEPALIVE_IDLE_SECS", default_value = "300")]
    pub tcp_keepalive_idle_secs: usize,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// Optional, we will use `listen_addr` if not specified.
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    /// The address via which we will attempt to connect to a leader meta node.
    #[clap(long, env = "RW_META_ADDR", default_value = "http://127.0.0.1:5690")]
    pub meta_addr: MetaAddressStrategy,

    /// We will start a http server at this address via `MetricsManager`.
    /// Then the prometheus instance will poll the metrics from this address.
    #[clap(
        long,
        env = "RW_PROMETHEUS_LISTENER_ADDR",
        default_value = "127.0.0.1:2222"
    )]
    pub prometheus_listener_addr: String,

    #[clap(
        long,
        alias = "health-check-listener-addr",
        env = "RW_HEALTH_CHECK_LISTENER_ADDR",
        default_value = "0.0.0.0:6786"
    )]
    pub frontend_rpc_listener_addr: String,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    ///
    /// Note that internal system parameters should be defined in the configuration file at
    /// [`risingwave_common::config`] instead of command line arguments.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    /// Used for control the metrics level, similar to log level.
    ///
    /// level = 0: disable metrics
    /// level > 0: enable metrics
    #[clap(long, hide = true, env = "RW_METRICS_LEVEL")]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<MetricLevel>,

    /// Enable heap profile dump when memory usage is high.
    #[clap(long, hide = true, env = "RW_HEAP_PROFILING_DIR")]
    #[override_opts(path = server.heap_profiling.dir)]
    pub heap_profiling_dir: Option<String>,

    #[clap(long, hide = true, env = "ENABLE_BARRIER_READ")]
    #[override_opts(path = batch.enable_barrier_read)]
    pub enable_barrier_read: Option<bool>,

    /// The path of the temp secret file directory.
    #[clap(
        long,
        hide = true,
        env = "RW_TEMP_SECRET_FILE_DIR",
        default_value = "./secrets"
    )]
    pub temp_secret_file_dir: String,

    /// Total available memory for the frontend node in bytes. Used for batch computing.
    #[clap(long, env = "RW_FRONTEND_TOTAL_MEMORY_BYTES", default_value_t = default_frontend_total_memory_bytes())]
    pub frontend_total_memory_bytes: usize,

    /// The address that the webhook service listens to.
    /// Usually the localhost + desired port.
    #[clap(long, env = "RW_WEBHOOK_LISTEN_ADDR", default_value = "0.0.0.0:4560")]
    pub webhook_listen_addr: String,

    /// Deprecated. Configure the serverless backfill controller address on meta nodes instead.
    #[deprecated(
        note = "Configure the serverless backfill controller address on meta nodes instead"
    )]
    #[clap(long, env = "RW_SBC_ADDR", default_value = "")]
    pub serverless_backfill_controller_addr: String,

    /// Prometheus endpoint URL for querying metrics.
    /// Optional, used for querying Prometheus metrics from the frontend.
    #[clap(long, env = "RW_PROMETHEUS_ENDPOINT")]
    pub prometheus_endpoint: Option<String>,

    /// The additional selector used when querying Prometheus.
    ///
    /// The format is same as `PromQL`. Example: `instance="foo",namespace="bar"`
    #[clap(long, env = "RW_PROMETHEUS_SELECTOR")]
    pub prometheus_selector: Option<String>,
}

impl risingwave_common::opts::Opts for FrontendOpts {
    fn name() -> &'static str {
        "frontend"
    }

    fn meta_addr(&self) -> MetaAddressStrategy {
        self.meta_addr.clone()
    }
}

impl Default for FrontendOpts {
    fn default() -> Self {
        FrontendOpts::parse_from(iter::empty::<OsString>())
    }
}

use std::future::Future;
use std::pin::Pin;

use pgwire::memory_manager::MessageMemoryManager;
use pgwire::pg_protocol::{ConnectionContext, TlsConfig};

use crate::session::SESSION_MANAGER;

/// Start frontend
pub fn start(
    opts: FrontendOpts,
    shutdown: CancellationToken,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let listen_addr = opts.listen_addr.clone();
        let webhook_listen_addr = opts.webhook_listen_addr.parse().unwrap();
        let tcp_keepalive =
            TcpKeepalive::new().with_time(Duration::from_secs(opts.tcp_keepalive_idle_secs as _));
        let tls_config = TlsConfig::new_default().unwrap();

        let session_mgr = Arc::new(SessionManagerImpl::new(opts).await.unwrap());
        SESSION_MANAGER.get_or_init(|| session_mgr.clone());
        let redact_sql_option_keywords = Arc::new(
            session_mgr
                .env()
                .batch_config()
                .redact_sql_option_keywords
                .iter()
                .map(|s| s.to_lowercase())
                .collect::<HashSet<_>>(),
        );
        let frontend_config = &session_mgr.env().frontend_config();
        let message_memory_manager = Arc::new(MessageMemoryManager::new(
            frontend_config.max_total_query_size_bytes,
            frontend_config.min_single_query_size_bytes,
            frontend_config.max_single_query_size_bytes,
        ));

        let webhook_service =
            crate::webhook::WebhookService::new(webhook_listen_addr, tls_config.clone());
        let _task = tokio::spawn(webhook_service.serve());
        pg_serve(
            &listen_addr,
            tcp_keepalive,
            session_mgr.clone(),
            ConnectionContext {
                tls_config,
                redact_sql_option_keywords: Some(redact_sql_option_keywords),
                message_memory_manager,
                stream_flush_threshold_bytes: frontend_config.stream_flush_threshold_bytes,
            },
            shutdown,
        )
        .await
        .unwrap()
    })
}

pub fn default_frontend_total_memory_bytes() -> usize {
    system_memory_available_bytes()
}
